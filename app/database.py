"""
SQLite DB 초기화 및 데이터 삽입 기능
"""

import json
import sqlite3

from app.config import COLUMNS

FOOD_TABLE = "processed_food_info"
LEGACY_FOOD_TABLE = "food_info"
HACCP_TABLE = "haccp_product_info"
HACCP_PROGRESS_TABLE = "haccp_ingest_progress"
HACCP_PARSED_TABLE = "haccp_parsed_cache"
HACCP_BLOCK_TABLE = "haccp_parse_blocklist"
HACCP_COLUMNS = [
    "rnum",
    "prdlstReportNo",
    "productGb",
    "prdlstNm",
    "rawmtrl",
    "allergy",
    "nutrient",
    "barcode",
    "prdkind",
    "prdkindstate",
    "manufacture",
    "seller",
    "capacity",
    "imgurl1",
    "imgurl2",
]


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row and row[0] > 0)


def ensure_processed_food_table(conn: sqlite3.Connection) -> None:
    """레거시 food_info를 processed_food_info로 자동 마이그레이션."""
    has_new = _table_exists(conn, FOOD_TABLE)
    has_legacy = _table_exists(conn, LEGACY_FOOD_TABLE)
    if has_new:
        return
    if has_legacy:
        conn.execute(f"ALTER TABLE {LEGACY_FOOD_TABLE} RENAME TO {FOOD_TABLE}")
        conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    """테이블이 없으면 생성 후 유니크 인덱스 보장"""
    ensure_processed_food_table(conn)
    cols_def = ", ".join(f'"{col}" TEXT' for col in COLUMNS)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {FOOD_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {cols_def}
        )
    """)
    conn.commit()
    _ensure_unique_index(conn)
    _ensure_food_code_unique_index(conn)


def _ensure_unique_index(conn: sqlite3.Connection) -> None:
    """itemMnftrRptNo 컬럼에 유니크 인덱스가 없으면 생성.
    기존 테이블에 중복 데이터가 있을 경우 id가 가장 작은 행만 남기고 제거한다.
    NULL이거나 빈 문자열인 행은 중복 대상에서 제외한다.
    """
    cursor = conn.execute(
        "SELECT name FROM sqlite_master "
        f"WHERE type='index' AND tbl_name='{FOOD_TABLE}' AND name='uq_itemMnftrRptNo'"
    )
    if cursor.fetchone():
        return  # 이미 존재함

    # 중복 데이터 제거: 같은 itemMnftrRptNo 중 id가 가장 작은 행만 유지
    conn.execute("""
        DELETE FROM processed_food_info
        WHERE itemMnftrRptNo IS NOT NULL
          AND itemMnftrRptNo != ''
          AND id NOT IN (
              SELECT MIN(id)
              FROM processed_food_info
              WHERE itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != ''
              GROUP BY itemMnftrRptNo
          )
    """)

    # NULL·빈 문자열을 제외한 부분 유니크 인덱스 생성
    conn.execute("""
        CREATE UNIQUE INDEX uq_itemMnftrRptNo
        ON processed_food_info("itemMnftrRptNo")
        WHERE "itemMnftrRptNo" IS NOT NULL AND "itemMnftrRptNo" != ''
    """)
    conn.commit()


def _ensure_food_code_unique_index(conn: sqlite3.Connection) -> None:
    """foodCd 기준 중복 제거 후 유니크 인덱스 생성.
    동일 foodCd가 여러 건이면 신뢰도 높은 행 1건만 남긴다.
    우선순위:
    1) itemMnftrRptNo 존재
    2) mfrNm 유효값(빈값/해당없음 제외)
    3) crtrYmd 최신
    4) id 최신
    """
    cursor = conn.execute(
        "SELECT name FROM sqlite_master "
        f"WHERE type='index' AND tbl_name='{FOOD_TABLE}' AND name='uq_foodCd'"
    )
    if cursor.fetchone():
        return

    conn.execute("""
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY foodCd
                    ORDER BY
                        CASE
                            WHEN itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != '' THEN 0
                            ELSE 1
                        END,
                        CASE
                            WHEN mfrNm IS NOT NULL AND mfrNm != '' AND mfrNm != '해당없음' THEN 0
                            ELSE 1
                        END,
                        CASE
                            WHEN crtrYmd IS NOT NULL AND crtrYmd != '' THEN 0
                            ELSE 1
                        END,
                        crtrYmd DESC,
                        id DESC
                ) AS rn
            FROM processed_food_info
            WHERE foodCd IS NOT NULL AND foodCd != ''
        )
        DELETE FROM processed_food_info
        WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
    """)

    conn.execute("""
        CREATE UNIQUE INDEX uq_foodCd
        ON processed_food_info("foodCd")
        WHERE "foodCd" IS NOT NULL AND "foodCd" != ''
    """)
    conn.commit()


def insert_rows(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """rows를 DB에 삽입. itemMnftrRptNo가 이미 존재하는 행은 무시(중복 방지)."""
    placeholders = ", ".join("?" for _ in COLUMNS)
    col_names = ", ".join(f'"{col}"' for col in COLUMNS)
    sql = f'INSERT OR IGNORE INTO processed_food_info ({col_names}) VALUES ({placeholders})'

    values = [tuple(row.get(col) for col in COLUMNS) for row in rows]
    conn.executemany(sql, values)
    conn.commit()


def init_progress_table(conn: sqlite3.Connection) -> None:
    """페이지 수집 진행 상태 저장 테이블 준비."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_progress (
            page_no INTEGER NOT NULL,
            num_of_rows INTEGER NOT NULL,
            status TEXT NOT NULL,
            saved_rows INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (page_no, num_of_rows)
        )
    """)
    conn.commit()


def get_completed_pages(conn: sqlite3.Connection, num_of_rows: int) -> set[int]:
    """완료(status='done')된 페이지 번호 집합 반환."""
    rows = conn.execute(
        "SELECT page_no FROM ingest_progress WHERE num_of_rows = ? AND status = 'done'",
        (num_of_rows,),
    ).fetchall()
    return {row[0] for row in rows}


def mark_page_done(
    conn: sqlite3.Connection,
    page_no: int,
    num_of_rows: int,
    saved_rows: int,
) -> None:
    """해당 페이지 수집 완료 상태를 upsert."""
    conn.execute(
        """
        INSERT INTO ingest_progress (page_no, num_of_rows, status, saved_rows, updated_at)
        VALUES (?, ?, 'done', ?, CURRENT_TIMESTAMP)
        ON CONFLICT(page_no, num_of_rows) DO UPDATE SET
            status = 'done',
            saved_rows = excluded.saved_rows,
            updated_at = CURRENT_TIMESTAMP
        """,
        (page_no, num_of_rows, saved_rows),
    )
    conn.commit()


def init_haccp_tables(conn: sqlite3.Connection) -> None:
    """HACCP 원본 테이블/진행 캐시 테이블 준비."""
    cols_def = ", ".join(f'"{col}" TEXT' for col in HACCP_COLUMNS)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HACCP_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {cols_def},
            raw_json TEXT,
            fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(prdlstReportNo)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HACCP_PROGRESS_TABLE} (
            page_no INTEGER NOT NULL,
            num_of_rows INTEGER NOT NULL,
            status TEXT NOT NULL,
            saved_rows INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (page_no, num_of_rows)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HACCP_PARSED_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prdlstReportNo TEXT NOT NULL UNIQUE,
            ingredients_items_json TEXT,
            nutrition_items_json TEXT,
            parse_status TEXT NOT NULL DEFAULT 'ok',
            parse_error TEXT,
            parser_version TEXT NOT NULL DEFAULT 'code_v1',
            source_rawmtrl TEXT,
            source_nutrient TEXT,
            parsed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HACCP_BLOCK_TABLE} (
            prdlstReportNo TEXT NOT NULL PRIMARY KEY,
            is_blocked INTEGER NOT NULL DEFAULT 1,
            reason TEXT,
            blocked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def upsert_haccp_rows(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """HACCP rows를 prdlstReportNo 기준 upsert."""
    if not rows:
        return 0
    placeholders = ", ".join("?" for _ in HACCP_COLUMNS)
    col_names = ", ".join(HACCP_COLUMNS)
    update_cols = ", ".join(
        [f'{c}=excluded.{c}' for c in HACCP_COLUMNS if c != "prdlstReportNo"]
        + ["raw_json=excluded.raw_json", "fetched_at=CURRENT_TIMESTAMP"]
    )
    sql = (
        f"INSERT INTO {HACCP_TABLE} ({col_names}, raw_json) "
        f"VALUES ({placeholders}, ?) "
        "ON CONFLICT(prdlstReportNo) DO UPDATE SET "
        f"{update_cols}"
    )
    values: list[tuple] = []
    for row in rows:
        raw_json = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
        values.append(tuple(str(row.get(c) or "") for c in HACCP_COLUMNS) + (raw_json,))
    conn.executemany(sql, values)
    conn.commit()
    return len(values)


def get_haccp_completed_pages(conn: sqlite3.Connection, num_of_rows: int) -> set[int]:
    rows = conn.execute(
        f"SELECT page_no FROM {HACCP_PROGRESS_TABLE} WHERE num_of_rows=? AND status='done'",
        (num_of_rows,),
    ).fetchall()
    return {int(r[0]) for r in rows}


def mark_haccp_page_done(
    conn: sqlite3.Connection,
    page_no: int,
    num_of_rows: int,
    saved_rows: int,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {HACCP_PROGRESS_TABLE} (page_no, num_of_rows, status, saved_rows, updated_at)
        VALUES (?, ?, 'done', ?, CURRENT_TIMESTAMP)
        ON CONFLICT(page_no, num_of_rows) DO UPDATE SET
            status='done',
            saved_rows=excluded.saved_rows,
            updated_at=CURRENT_TIMESTAMP
        """,
        (page_no, num_of_rows, saved_rows),
    )
    conn.commit()


def clear_haccp_progress(conn: sqlite3.Connection) -> int:
    cur = conn.execute(f"DELETE FROM {HACCP_PROGRESS_TABLE}")
    conn.commit()
    return int(cur.rowcount)


def clear_haccp_data(conn: sqlite3.Connection) -> int:
    cur = conn.execute(f"DELETE FROM {HACCP_TABLE}")
    conn.commit()
    return int(cur.rowcount)


def upsert_haccp_parsed_row(
    conn: sqlite3.Connection,
    *,
    report_no: str,
    ingredients_items_json: str | None,
    nutrition_items_json: str | None,
    parse_status: str,
    parse_error: str | None,
    parser_version: str = "code_v1",
    source_rawmtrl: str | None = None,
    source_nutrient: str | None = None,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {HACCP_PARSED_TABLE} (
            prdlstReportNo, ingredients_items_json, nutrition_items_json,
            parse_status, parse_error, parser_version, source_rawmtrl, source_nutrient, parsed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(prdlstReportNo) DO UPDATE SET
            ingredients_items_json=excluded.ingredients_items_json,
            nutrition_items_json=excluded.nutrition_items_json,
            parse_status=excluded.parse_status,
            parse_error=excluded.parse_error,
            parser_version=excluded.parser_version,
            source_rawmtrl=excluded.source_rawmtrl,
            source_nutrient=excluded.source_nutrient,
            parsed_at=CURRENT_TIMESTAMP
        """,
        (
            report_no,
            ingredients_items_json,
            nutrition_items_json,
            parse_status,
            parse_error,
            parser_version,
            source_rawmtrl,
            source_nutrient,
        ),
    )
    conn.commit()


def clear_haccp_parsed_cache(conn: sqlite3.Connection) -> int:
    cur = conn.execute(f"DELETE FROM {HACCP_PARSED_TABLE}")
    conn.commit()
    return int(cur.rowcount)


def upsert_haccp_parse_block(
    conn: sqlite3.Connection,
    *,
    report_no: str,
    reason: str | None = None,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {HACCP_BLOCK_TABLE}
        (prdlstReportNo, is_blocked, reason, blocked_at, updated_at)
        VALUES (?, 1, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(prdlstReportNo) DO UPDATE SET
            is_blocked=1,
            reason=excluded.reason,
            updated_at=CURRENT_TIMESTAMP
        """,
        (str(report_no or "").strip(), str(reason or "").strip() or None),
    )
    conn.commit()


def clear_haccp_parse_block(conn: sqlite3.Connection, report_no: str) -> int:
    cur = conn.execute(
        f"DELETE FROM {HACCP_BLOCK_TABLE} WHERE prdlstReportNo=?",
        (str(report_no or "").strip(),),
    )
    conn.commit()
    return int(cur.rowcount or 0)
