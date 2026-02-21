"""
검색어 -> SerpAPI -> Pass 검증 -> 최종 저장 파이프라인용 DB 유틸

복잡도를 낮추기 위해:
- 테이블 생성
- 검색어 시드/조회
- 실행(run) 기록
- Serp 캐시
- 이미지 분석 캐시
- 최종 결과 저장
만 제공한다.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any


def normalize_query(query_text: str) -> str:
    value = re.sub(r"\s+", " ", str(query_text or "").strip().lower())
    # 검색어 중복 방지용 정규화: 공백/기호를 최대한 단순화
    value = re.sub(r"[^0-9a-zA-Z가-힣 ]+", "", value)
    return value.strip()


def hash_text(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def init_query_pipeline_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS query_pool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_text TEXT NOT NULL,
            query_norm TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'manual',
            priority_score REAL NOT NULL DEFAULT 0,
            target_segment_score REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            run_count INTEGER NOT NULL DEFAULT 0,
            last_run_at TEXT,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_query_pool_norm ON query_pool(query_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_query_pool_status_priority ON query_pool(status, priority_score DESC)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS query_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER NOT NULL,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ended_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            total_images INTEGER NOT NULL DEFAULT 0,
            analyzed_images INTEGER NOT NULL DEFAULT 0,
            pass2b_pass_count INTEGER NOT NULL DEFAULT 0,
            pass4_pass_count INTEGER NOT NULL DEFAULT 0,
            final_saved_count INTEGER NOT NULL DEFAULT 0,
            api_calls INTEGER NOT NULL DEFAULT 0,
            quality_score REAL NOT NULL DEFAULT 0,
            yield_score REAL NOT NULL DEFAULT 0,
            efficiency_score REAL NOT NULL DEFAULT 0,
            overall_score REAL NOT NULL DEFAULT 0,
            error_message TEXT,
            FOREIGN KEY (query_id) REFERENCES query_pool(id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_query_runs_query_started ON query_runs(query_id, started_at DESC)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS serp_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER NOT NULL,
            query_hash TEXT NOT NULL,
            page INTEGER NOT NULL,
            page_size INTEGER NOT NULL DEFAULT 100,
            image_url TEXT NOT NULL,
            title TEXT,
            source TEXT,
            rank_in_page INTEGER,
            fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            run_id INTEGER,
            FOREIGN KEY (query_id) REFERENCES query_pool(id),
            FOREIGN KEY (run_id) REFERENCES query_runs(id)
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_serp_cache_key "
        "ON serp_cache(query_hash, page, page_size, image_url)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_serp_cache_hash_page ON serp_cache(query_hash, page, page_size)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS query_image_analysis_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_url TEXT NOT NULL,
            image_url_hash TEXT NOT NULL,
            last_run_id INTEGER,
            pass1_ok INTEGER,
            pass2a_ok INTEGER,
            pass2b_ok INTEGER,
            pass3_ok INTEGER,
            pass4_ok INTEGER,
            fail_stage TEXT,
            fail_reason TEXT,
            raw_pass2a TEXT,
            raw_pass2b TEXT,
            raw_pass3 TEXT,
            raw_pass4 TEXT,
            retryable INTEGER NOT NULL DEFAULT 0,
            analyzed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (last_run_id) REFERENCES query_runs(id)
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_query_image_analysis_url ON query_image_analysis_cache(image_url)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_query_image_analysis_hash ON query_image_analysis_cache(image_url_hash)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS food_final (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT,
            item_mnftr_rpt_no TEXT,
            ingredients_text TEXT,
            ingredients_hash TEXT,
            nutrition_text TEXT,
            nutrition_source TEXT NOT NULL DEFAULT 'none',
            source_image_url TEXT NOT NULL,
            source_query_id INTEGER,
            source_run_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (source_query_id) REFERENCES query_pool(id),
            FOREIGN KEY (source_run_id) REFERENCES query_runs(id)
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_food_final_report_no "
        "ON food_final(item_mnftr_rpt_no) "
        "WHERE item_mnftr_rpt_no IS NOT NULL AND item_mnftr_rpt_no != ''"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_food_final_fallback "
        "ON food_final(product_name, ingredients_hash) "
        "WHERE (item_mnftr_rpt_no IS NULL OR item_mnftr_rpt_no = '')"
    )
    conn.commit()


def upsert_query(
    conn: sqlite3.Connection,
    query_text: str,
    *,
    source: str = "manual",
    priority_score: float = 0.0,
    target_segment_score: float = 0.0,
    status: str = "pending",
    notes: str | None = None,
) -> int:
    norm = normalize_query(query_text)
    if not norm:
        raise ValueError("빈 검색어는 저장할 수 없습니다.")

    conn.execute(
        """
        INSERT INTO query_pool
            (query_text, query_norm, source, priority_score, target_segment_score, status, notes, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(query_norm) DO UPDATE SET
            query_text=excluded.query_text,
            source=excluded.source,
            priority_score=MAX(query_pool.priority_score, excluded.priority_score),
            target_segment_score=MAX(query_pool.target_segment_score, excluded.target_segment_score),
            status=CASE WHEN query_pool.status='paused' THEN 'paused' ELSE excluded.status END,
            notes=COALESCE(excluded.notes, query_pool.notes),
            updated_at=CURRENT_TIMESTAMP
        """,
        (query_text.strip(), norm, source, float(priority_score), float(target_segment_score), status, notes),
    )
    row = conn.execute("SELECT id FROM query_pool WHERE query_norm = ?", (norm,)).fetchone()
    conn.commit()
    return int(row[0])


def list_next_queries(conn: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT *
        FROM query_pool
        WHERE status IN ('pending', 'failed', 'done')
        ORDER BY priority_score DESC, target_segment_score DESC, COALESCE(last_run_at, '') ASC, id ASC
        LIMIT ?
        """,
        (limit,),
    )
    return list(cur.fetchall())


def start_query_run(conn: sqlite3.Connection, query_id: int) -> int:
    conn.execute(
        "INSERT INTO query_runs (query_id, status) VALUES (?, 'running')",
        (query_id,),
    )
    run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        """
        UPDATE query_pool
        SET status='running', run_count=run_count+1, last_run_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
        WHERE id=?
        """,
        (query_id,),
    )
    conn.commit()
    return run_id


def finish_query_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    total_images: int = 0,
    analyzed_images: int = 0,
    pass2b_pass_count: int = 0,
    pass4_pass_count: int = 0,
    final_saved_count: int = 0,
    api_calls: int = 0,
    error_message: str | None = None,
) -> None:
    total = max(1, int(total_images))
    analyzed = max(1, int(analyzed_images))
    calls = max(1, int(api_calls))

    quality_score = round((int(pass4_pass_count) / total) * 100.0, 2)
    yield_score = round((int(final_saved_count) / analyzed) * 100.0, 2)
    efficiency_score = round((int(final_saved_count) / calls) * 100.0, 2)
    overall_score = round(quality_score * 0.5 + yield_score * 0.3 + efficiency_score * 0.2, 2)

    conn.execute(
        """
        UPDATE query_runs
        SET ended_at=CURRENT_TIMESTAMP,
            status=?,
            total_images=?,
            analyzed_images=?,
            pass2b_pass_count=?,
            pass4_pass_count=?,
            final_saved_count=?,
            api_calls=?,
            quality_score=?,
            yield_score=?,
            efficiency_score=?,
            overall_score=?,
            error_message=?
        WHERE id=?
        """,
        (
            status,
            int(total_images),
            int(analyzed_images),
            int(pass2b_pass_count),
            int(pass4_pass_count),
            int(final_saved_count),
            int(api_calls),
            quality_score,
            yield_score,
            efficiency_score,
            overall_score,
            error_message,
            run_id,
        ),
    )

    row = conn.execute("SELECT query_id FROM query_runs WHERE id = ?", (run_id,)).fetchone()
    query_id = int(row[0]) if row else None
    if query_id is not None:
        conn.execute(
            """
            UPDATE query_pool
            SET status=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            ("done" if status == "done" else "failed", query_id),
        )
    conn.commit()


def cache_serp_images(
    conn: sqlite3.Connection,
    *,
    query_id: int,
    page: int,
    page_size: int,
    images: list[dict[str, Any]],
    run_id: int | None = None,
) -> int:
    row = conn.execute("SELECT query_norm FROM query_pool WHERE id = ?", (query_id,)).fetchone()
    if not row:
        raise ValueError(f"존재하지 않는 query_id: {query_id}")
    query_hash = hash_text(str(row[0]))

    saved = 0
    for item in images:
        image_url = str(item.get("image_url") or "").strip()
        if not image_url:
            continue
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO serp_cache
                (query_id, query_hash, page, page_size, image_url, title, source, rank_in_page, run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                query_id,
                query_hash,
                int(page),
                int(page_size),
                image_url,
                item.get("title"),
                item.get("source"),
                item.get("rank_in_page"),
                run_id,
            ),
        )
        if cur.rowcount and cur.rowcount > 0:
            saved += 1
    conn.commit()
    return saved


def get_cached_serp_images(
    conn: sqlite3.Connection,
    *,
    query_text: str,
    page: int,
    page_size: int,
    ttl_days: int = 7,
) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    qh = hash_text(normalize_query(query_text))
    min_time = (datetime.utcnow() - timedelta(days=max(0, ttl_days))).strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute(
        """
        SELECT *
        FROM serp_cache
        WHERE query_hash=? AND page=? AND page_size=? AND fetched_at >= ?
        ORDER BY rank_in_page ASC, id ASC
        """,
        (qh, int(page), int(page_size), min_time),
    )
    return list(cur.fetchall())


def upsert_image_analysis_cache(
    conn: sqlite3.Connection,
    *,
    image_url: str,
    run_id: int | None,
    pass1_ok: bool | None,
    pass2a_ok: bool | None,
    pass2b_ok: bool | None,
    pass3_ok: bool | None,
    pass4_ok: bool | None,
    fail_stage: str | None,
    fail_reason: str | None,
    raw_pass2a: str | None,
    raw_pass2b: str | None,
    raw_pass3: str | None = None,
    raw_pass4: str | None = None,
    retryable: bool = False,
) -> None:
    image_url = str(image_url or "").strip()
    if not image_url:
        return
    h = hash_text(image_url)
    conn.execute(
        """
        INSERT INTO query_image_analysis_cache (
            image_url, image_url_hash, last_run_id,
            pass1_ok, pass2a_ok, pass2b_ok, pass3_ok, pass4_ok,
            fail_stage, fail_reason,
            raw_pass2a, raw_pass2b, raw_pass3, raw_pass4,
            retryable, analyzed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(image_url) DO UPDATE SET
            image_url_hash=excluded.image_url_hash,
            last_run_id=excluded.last_run_id,
            pass1_ok=excluded.pass1_ok,
            pass2a_ok=excluded.pass2a_ok,
            pass2b_ok=excluded.pass2b_ok,
            pass3_ok=excluded.pass3_ok,
            pass4_ok=excluded.pass4_ok,
            fail_stage=excluded.fail_stage,
            fail_reason=excluded.fail_reason,
            raw_pass2a=excluded.raw_pass2a,
            raw_pass2b=excluded.raw_pass2b,
            raw_pass3=excluded.raw_pass3,
            raw_pass4=excluded.raw_pass4,
            retryable=excluded.retryable,
            analyzed_at=CURRENT_TIMESTAMP
        """,
        (
            image_url,
            h,
            run_id,
            int(pass1_ok) if pass1_ok is not None else None,
            int(pass2a_ok) if pass2a_ok is not None else None,
            int(pass2b_ok) if pass2b_ok is not None else None,
            int(pass3_ok) if pass3_ok is not None else None,
            int(pass4_ok) if pass4_ok is not None else None,
            fail_stage,
            fail_reason,
            raw_pass2a,
            raw_pass2b,
            raw_pass3,
            raw_pass4,
            1 if retryable else 0,
        ),
    )
    conn.commit()


def get_image_analysis_cache(conn: sqlite3.Connection, image_url: str) -> sqlite3.Row | None:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT * FROM query_image_analysis_cache WHERE image_url = ?",
        (str(image_url or "").strip(),),
    )
    return cur.fetchone()


def upsert_food_final(
    conn: sqlite3.Connection,
    *,
    product_name: str | None,
    item_mnftr_rpt_no: str | None,
    ingredients_text: str | None,
    nutrition_text: str | None,
    nutrition_source: str = "none",
    source_image_url: str,
    source_query_id: int | None = None,
    source_run_id: int | None = None,
) -> int:
    product_name = (product_name or "").strip() or None
    item_no = (item_mnftr_rpt_no or "").strip() or None
    ingredients_text = (ingredients_text or "").strip() or None
    nutrition_text = (nutrition_text or "").strip() or None
    ing_hash = hash_text(ingredients_text) if ingredients_text else None

    if item_no:
        conn.execute(
            """
            INSERT INTO food_final (
                product_name, item_mnftr_rpt_no, ingredients_text, ingredients_hash,
                nutrition_text, nutrition_source, source_image_url, source_query_id, source_run_id,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(item_mnftr_rpt_no) DO UPDATE SET
                product_name=COALESCE(excluded.product_name, food_final.product_name),
                ingredients_text=COALESCE(excluded.ingredients_text, food_final.ingredients_text),
                ingredients_hash=COALESCE(excluded.ingredients_hash, food_final.ingredients_hash),
                nutrition_text=COALESCE(excluded.nutrition_text, food_final.nutrition_text),
                nutrition_source=CASE
                    WHEN excluded.nutrition_text IS NOT NULL AND excluded.nutrition_text != '' THEN excluded.nutrition_source
                    ELSE food_final.nutrition_source
                END,
                source_image_url=excluded.source_image_url,
                source_query_id=COALESCE(excluded.source_query_id, food_final.source_query_id),
                source_run_id=COALESCE(excluded.source_run_id, food_final.source_run_id),
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                product_name,
                item_no,
                ingredients_text,
                ing_hash,
                nutrition_text,
                nutrition_source,
                source_image_url,
                source_query_id,
                source_run_id,
            ),
        )
        row = conn.execute("SELECT id FROM food_final WHERE item_mnftr_rpt_no = ?", (item_no,)).fetchone()
        conn.commit()
        return int(row[0]) if row else 0

    existing = None
    if product_name and ing_hash:
        existing = conn.execute(
            """
            SELECT id
            FROM food_final
            WHERE (item_mnftr_rpt_no IS NULL OR item_mnftr_rpt_no = '')
              AND product_name = ?
              AND ingredients_hash = ?
            LIMIT 1
            """,
            (product_name, ing_hash),
        ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE food_final
            SET ingredients_text=COALESCE(?, ingredients_text),
                nutrition_text=COALESCE(?, nutrition_text),
                nutrition_source=CASE
                    WHEN ? IS NOT NULL AND ? != '' THEN ?
                    ELSE nutrition_source
                END,
                source_image_url=?,
                source_query_id=COALESCE(?, source_query_id),
                source_run_id=COALESCE(?, source_run_id),
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            (
                ingredients_text,
                nutrition_text,
                nutrition_text,
                nutrition_text,
                nutrition_source,
                source_image_url,
                source_query_id,
                source_run_id,
                int(existing[0]),
            ),
        )
        conn.commit()
        return int(existing[0])

    conn.execute(
        """
        INSERT INTO food_final (
            product_name, item_mnftr_rpt_no, ingredients_text, ingredients_hash,
            nutrition_text, nutrition_source, source_image_url, source_query_id, source_run_id,
            created_at, updated_at
        )
        VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (
            product_name,
            ingredients_text,
            ing_hash,
            nutrition_text,
            nutrition_source,
            source_image_url,
            source_query_id,
            source_run_id,
        ),
    )
    row = conn.execute("SELECT last_insert_rowid()").fetchone()
    conn.commit()
    return int(row[0]) if row else 0


def seed_queries_from_categories(conn: sqlite3.Connection, limit: int = 200) -> int:
    """
    processed_food_info 카테고리 기반 검색어 초기 시드.
    타깃(20~40 여성, 다이어트/대사 관심)을 반영한 최소 규칙 점수 적용.
    """
    cur = conn.execute(
        """
        SELECT foodLv3Nm, foodLv4Nm, COUNT(*) AS cnt
        FROM processed_food_info
        WHERE COALESCE(foodLv3Nm, '') != '' AND COALESCE(foodLv4Nm, '') != ''
        GROUP BY foodLv3Nm, foodLv4Nm
        ORDER BY cnt DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    saved = 0
    for lv3, lv4, cnt in rows:
        base_query = f"{lv3} {lv4} 원재료명 성분표"
        text = str(base_query).strip()
        score = float(cnt or 0)
        seg = 0.0
        joined = f"{lv3} {lv4}".lower()
        for k in ("체중조절", "단백질", "저당", "소스", "콤부차", "특수영양", "드레싱"):
            if k in joined:
                seg += 20.0
        query_id = upsert_query(
            conn,
            text,
            source="category_seed",
            priority_score=score + seg,
            target_segment_score=seg,
            status="pending",
            notes="auto_seed_from_public_category",
        )
        if query_id:
            saved += 1
    return saved


def get_pipeline_overview(conn: sqlite3.Connection) -> dict[str, int]:
    out: dict[str, int] = {}
    for table in ("query_pool", "query_runs", "serp_cache", "query_image_analysis_cache", "food_final"):
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        out[table] = int(row[0]) if row else 0
    return out


def list_recent_runs(conn: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT
            r.id,
            r.query_id,
            q.query_text,
            r.status,
            r.started_at,
            r.ended_at,
            r.total_images,
            r.analyzed_images,
            r.pass4_pass_count,
            r.final_saved_count,
            r.overall_score
        FROM query_runs r
        JOIN query_pool q ON q.id = r.query_id
        ORDER BY r.id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return list(cur.fetchall())
