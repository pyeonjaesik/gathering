"""
원재료명 수집 파이프라인 (SerpAPI 이미지 검색 + analyze(url) 결과 저장)

- 이미지는 파일로 저장하지 않고 URL만 사용한다.
- analyze(url)은 현재 모의 함수(mock_analyze)로 동작한다.
- 결과는 모두 DB에 영구 저장한다. (캐시 아님)
"""

import argparse
import json
import os
import random
import sqlite3
import time
from dataclasses import dataclass

import requests

from config import DB_FILE

SERPAPI_URL = "https://serpapi.com/search.json"
SERPAPI_TIMEOUT = 25
SERPAPI_RETRIES = 2
SERPAPI_RETRY_BACKOFF = 0.6
TOP_IMAGES = 10


@dataclass
class Product:
    item_rpt_no: str
    food_name: str
    mfr_name: str


def _bar(char: str = "─", width: int = 72) -> str:
    return "  " + char * (width - 4)


def _progress_bar(done: int, total: int, width: int = 28) -> str:
    if total <= 0:
        total = 1
    ratio = min(1.0, max(0.0, done / total))
    filled = int(width * ratio)
    return "█" * filled + "░" * (width - filled)


def _short(text: str | None, max_len: int = 72) -> str:
    value = (text or "").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def init_ingredient_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingredient_info (
            itemMnftrRptNo TEXT PRIMARY KEY,
            ingredients_text TEXT NOT NULL,
            source_image_url TEXT,
            source_query TEXT,
            source_food_name TEXT,
            source_mfr_name TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingredient_attempts (
            query_itemMnftrRptNo TEXT PRIMARY KEY,
            query_food_name TEXT NOT NULL,
            query_mfr_name TEXT,
            status TEXT NOT NULL,
            searched_query TEXT,
            images_requested INTEGER NOT NULL DEFAULT 0,
            images_analyzed INTEGER NOT NULL DEFAULT 0,
            matched_itemMnftrRptNo TEXT,
            error_message TEXT,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingredient_extractions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_itemMnftrRptNo TEXT NOT NULL,
            query_food_name TEXT NOT NULL,
            query_mfr_name TEXT,
            image_rank INTEGER NOT NULL,
            image_url TEXT NOT NULL,
            extracted_itemMnftrRptNo TEXT,
            ingredients_text TEXT,
            matched_target INTEGER NOT NULL DEFAULT 0,
            raw_payload TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def build_search_query(product: Product) -> str:
    if product.mfr_name:
        return f"{product.food_name} {product.mfr_name} 성분표"
    return f"{product.food_name} 성분표"


def search_image_urls(query: str, api_key: str, top_k: int = TOP_IMAGES) -> list[str]:
    params = {
        "engine": "google_images",
        "q": query,
        "hl": "ko",
        "gl": "kr",
        "num": top_k,
        "api_key": api_key,
        "no_cache": "true",
    }

    last_error = None
    for attempt in range(SERPAPI_RETRIES + 1):
        try:
            response = requests.get(SERPAPI_URL, params=params, timeout=SERPAPI_TIMEOUT)
            data = response.json()
            api_error = data.get("error")

            if response.status_code == 200 and api_error is None:
                images = data.get("images_results") or []
                urls: list[str] = []
                for item in images:
                    url = item.get("original") or item.get("thumbnail")
                    if url:
                        urls.append(url)
                return urls[:top_k]

            last_error = f"http={response.status_code} api_error={api_error}"
            if response.status_code in (429, 500, 502, 503, 504) and attempt < SERPAPI_RETRIES:
                time.sleep(SERPAPI_RETRY_BACKOFF * (attempt + 1))
                continue
            break
        except requests.exceptions.Timeout:
            last_error = "timeout"
            if attempt < SERPAPI_RETRIES:
                time.sleep(SERPAPI_RETRY_BACKOFF * (attempt + 1))
                continue
            break
        except Exception as exc:  # pylint: disable=broad-except
            last_error = f"exception={type(exc).__name__}"
            if attempt < SERPAPI_RETRIES:
                time.sleep(SERPAPI_RETRY_BACKOFF * (attempt + 1))
                continue
            break

    raise RuntimeError(f"SerpAPI 검색 실패: {last_error}")


def load_report_number_pool(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT itemMnftrRptNo
        FROM food_info
        WHERE itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != ''
        """
    ).fetchall()
    return [row[0] for row in rows]


def mock_analyze(image_url: str, target_item_rpt_no: str, report_pool: list[str]) -> dict:
    """
    친구가 만든 analyze(url) 대신 사용하는 모의 함수.
    경우의 수:
    - 품목보고번호 미검출
    - 타깃 품목보고번호 검출
    - 다른 품목보고번호 검출
    """
    _ = image_url
    roll = random.random()

    if roll < 0.35:
        return {
            "itemMnftrRptNo": None,
            "ingredients_text": None,
            "note": "번호 미검출",
        }
    if roll < 0.65:
        return {
            "itemMnftrRptNo": target_item_rpt_no,
            "ingredients_text": "정제수, 설탕, 식물성유지, 혼합제제(모의데이터)",
            "note": "타깃 번호 검출",
        }

    other = target_item_rpt_no
    if report_pool:
        for _ in range(10):
            candidate = random.choice(report_pool)
            if candidate != target_item_rpt_no:
                other = candidate
                break
    return {
        "itemMnftrRptNo": other,
        "ingredients_text": "밀가루, 팜유, 포도당, 합성향료(모의데이터)",
        "note": "다른 번호 검출",
    }


def upsert_ingredient_info(
    conn: sqlite3.Connection,
    extracted_item_rpt_no: str,
    ingredients_text: str,
    image_url: str,
    query: str,
    product: Product,
) -> None:
    conn.execute(
        """
        INSERT INTO ingredient_info (
            itemMnftrRptNo,
            ingredients_text,
            source_image_url,
            source_query,
            source_food_name,
            source_mfr_name,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(itemMnftrRptNo) DO UPDATE SET
            ingredients_text = excluded.ingredients_text,
            source_image_url = excluded.source_image_url,
            source_query = excluded.source_query,
            source_food_name = excluded.source_food_name,
            source_mfr_name = excluded.source_mfr_name,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            extracted_item_rpt_no,
            ingredients_text,
            image_url,
            query,
            product.food_name,
            product.mfr_name,
        ),
    )


def insert_extraction_log(
    conn: sqlite3.Connection,
    product: Product,
    image_rank: int,
    image_url: str,
    extracted_item_rpt_no: str | None,
    ingredients_text: str | None,
    matched_target: bool,
    raw_payload: dict,
) -> None:
    conn.execute(
        """
        INSERT INTO ingredient_extractions (
            query_itemMnftrRptNo,
            query_food_name,
            query_mfr_name,
            image_rank,
            image_url,
            extracted_itemMnftrRptNo,
            ingredients_text,
            matched_target,
            raw_payload
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            product.item_rpt_no,
            product.food_name,
            product.mfr_name,
            image_rank,
            image_url,
            extracted_item_rpt_no,
            ingredients_text,
            1 if matched_target else 0,
            json.dumps(raw_payload, ensure_ascii=False),
        ),
    )


def upsert_attempt(
    conn: sqlite3.Connection,
    product: Product,
    status: str,
    query: str | None,
    images_requested: int = 0,
    images_analyzed: int = 0,
    matched_item_rpt_no: str | None = None,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO ingredient_attempts (
            query_itemMnftrRptNo,
            query_food_name,
            query_mfr_name,
            status,
            searched_query,
            images_requested,
            images_analyzed,
            matched_itemMnftrRptNo,
            error_message,
            finished_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ?='in_progress' THEN NULL ELSE CURRENT_TIMESTAMP END)
        ON CONFLICT(query_itemMnftrRptNo) DO UPDATE SET
            status = excluded.status,
            searched_query = excluded.searched_query,
            images_requested = excluded.images_requested,
            images_analyzed = excluded.images_analyzed,
            matched_itemMnftrRptNo = excluded.matched_itemMnftrRptNo,
            error_message = excluded.error_message,
            finished_at = CASE
                WHEN excluded.status='in_progress' THEN NULL
                ELSE CURRENT_TIMESTAMP
            END
        """,
        (
            product.item_rpt_no,
            product.food_name,
            product.mfr_name,
            status,
            query,
            images_requested,
            images_analyzed,
            matched_item_rpt_no,
            error_message,
            status,
        ),
    )


def fetch_target_products(conn: sqlite3.Connection, limit: int | None) -> list[Product]:
    sql = """
        SELECT fi.itemMnftrRptNo, fi.foodNm, COALESCE(fi.mfrNm, '')
        FROM food_info fi
        WHERE fi.itemMnftrRptNo IS NOT NULL
          AND fi.itemMnftrRptNo != ''
          AND NOT EXISTS (
              SELECT 1 FROM ingredient_info ii
              WHERE ii.itemMnftrRptNo = fi.itemMnftrRptNo
          )
          AND NOT EXISTS (
              SELECT 1 FROM ingredient_attempts ia
              WHERE ia.query_itemMnftrRptNo = fi.itemMnftrRptNo
          )
        ORDER BY fi.id
    """
    params: tuple = ()
    if limit is not None and limit > 0:
        sql += " LIMIT ?"
        params = (limit,)

    rows = conn.execute(sql, params).fetchall()
    return [Product(item_rpt_no=r[0], food_name=r[1] or "", mfr_name=r[2] or "") for r in rows]


def process_product(
    conn: sqlite3.Connection,
    product: Product,
    api_key: str,
    report_pool: list[str],
    verbose: bool = True,
) -> dict:
    query = build_search_query(product)
    upsert_attempt(conn, product, status="in_progress", query=query)
    conn.commit()

    if verbose:
        print(_bar())
        print(f"  상품: {product.food_name}")
        print(f"  품목보고번호: {product.item_rpt_no}")
        print(f"  검색어: {query}")

    try:
        image_urls = search_image_urls(query, api_key=api_key, top_k=TOP_IMAGES)
    except Exception as exc:  # pylint: disable=broad-except
        upsert_attempt(
            conn,
            product,
            status="failed",
            query=query,
            error_message=str(exc)[:400],
        )
        conn.commit()
        if verbose:
            print(f"  [실패] SerpAPI 호출 실패: {exc}")
        return {
            "status": "failed",
            "images_requested": 0,
            "images_analyzed": 0,
            "matched_image_url": None,
            "saved_records": 0,
            "query": query,
        }

    matched = False
    analyzed_count = 0
    saved_records = 0
    matched_image_url = None

    if verbose:
        print(f"  이미지 후보: {len(image_urls)}개")

    for rank, image_url in enumerate(image_urls, start=1):
        analyzed_count += 1
        analysis = mock_analyze(
            image_url=image_url,
            target_item_rpt_no=product.item_rpt_no,
            report_pool=report_pool,
        )
        extracted = analysis.get("itemMnftrRptNo")
        ingredients = analysis.get("ingredients_text")
        is_match = bool(extracted and extracted == product.item_rpt_no)

        insert_extraction_log(
            conn,
            product=product,
            image_rank=rank,
            image_url=image_url,
            extracted_item_rpt_no=extracted,
            ingredients_text=ingredients,
            matched_target=is_match,
            raw_payload=analysis,
        )

        if extracted and ingredients:
            upsert_ingredient_info(
                conn,
                extracted_item_rpt_no=extracted,
                ingredients_text=ingredients,
                image_url=image_url,
                query=query,
                product=product,
            )
            saved_records += 1

        conn.commit()

        if verbose:
            extracted_text = extracted or "미검출"
            result_tag = "MATCH" if is_match else "NO-MATCH"
            print(
                f"    [{rank:02d}/{len(image_urls):02d}] {result_tag} "
                f"번호={extracted_text} | url={_short(image_url, 84)}"
            )
            if extracted and ingredients:
                print(
                    f"      └ 저장됨: itemMnftrRptNo={extracted} | 원재료 길이={len(ingredients)}"
                )

        if is_match:
            matched = True
            matched_image_url = image_url
            break

    if matched:
        upsert_attempt(
            conn,
            product,
            status="matched",
            query=query,
            images_requested=len(image_urls),
            images_analyzed=analyzed_count,
            matched_item_rpt_no=product.item_rpt_no,
        )
        conn.commit()
        if verbose:
            print(f"  [완료] 매칭 성공 | source_url={_short(matched_image_url, 96)}")
        return {
            "status": "matched",
            "images_requested": len(image_urls),
            "images_analyzed": analyzed_count,
            "matched_image_url": matched_image_url,
            "saved_records": saved_records,
            "query": query,
        }

    upsert_attempt(
        conn,
        product,
        status="unmatched",
        query=query,
        images_requested=len(image_urls),
        images_analyzed=analyzed_count,
    )
    conn.commit()
    if verbose:
        print("  [완료] 미매칭 (다음 상품 진행)")
    return {
        "status": "unmatched",
        "images_requested": len(image_urls),
        "images_analyzed": analyzed_count,
        "matched_image_url": None,
        "saved_records": saved_records,
        "query": query,
    }


def summarize(conn: sqlite3.Connection) -> None:
    total_attempts = conn.execute("SELECT COUNT(*) FROM ingredient_attempts").fetchone()[0]
    matched = conn.execute(
        "SELECT COUNT(*) FROM ingredient_attempts WHERE status='matched'"
    ).fetchone()[0]
    unmatched = conn.execute(
        "SELECT COUNT(*) FROM ingredient_attempts WHERE status='unmatched'"
    ).fetchone()[0]
    failed = conn.execute(
        "SELECT COUNT(*) FROM ingredient_attempts WHERE status='failed'"
    ).fetchone()[0]
    ingredient_rows = conn.execute("SELECT COUNT(*) FROM ingredient_info").fetchone()[0]
    extraction_rows = conn.execute("SELECT COUNT(*) FROM ingredient_extractions").fetchone()[0]
    avg_images = conn.execute(
        "SELECT ROUND(AVG(images_analyzed), 2) FROM ingredient_attempts WHERE status IN ('matched', 'unmatched')"
    ).fetchone()[0]

    print("\n[요약]")
    print(
        f"- 시도 이력: {total_attempts:,}건 "
        f"(matched={matched:,}, unmatched={unmatched:,}, failed={failed:,})"
    )
    print(f"- 원재료 마스터(ingredient_info): {ingredient_rows:,}건")
    print(f"- 분석 로그(ingredient_extractions): {extraction_rows:,}건")
    print(f"- 평균 분석 이미지 수: {avg_images if avg_images is not None else '—'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="원재료명 수집 파이프라인 (모의 analyze 사용)")
    parser.add_argument("--limit", type=int, default=20, help="이번 실행에서 처리할 최대 상품 수")
    parser.add_argument("--seed", type=int, default=7, help="모의 analyze 랜덤 시드")
    parser.add_argument("--db", type=str, default=DB_FILE, help="SQLite DB 파일 경로")
    parser.add_argument("--quiet", action="store_true", help="이미지별 상세 로그 출력 생략")
    args = parser.parse_args()

    random.seed(args.seed)

    api_key = os.getenv("SERPAPI_KEY")
    if not api_key:
        raise SystemExit("SERPAPI_KEY 환경변수를 설정해주세요.")

    conn = sqlite3.connect(args.db)
    init_ingredient_tables(conn)
    report_pool = load_report_number_pool(conn)

    targets = fetch_target_products(conn, limit=args.limit)
    if not targets:
        print("처리할 대상이 없습니다. (이미 ingredient_info/ingredient_attempts에 존재)")
        summarize(conn)
        conn.close()
        return

    print("\n╔══════════════════════════════════════════════════════════════════╗")
    print("║                원재료 수집 실행 (URL 분석 모드)                ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print(f"  처리 대상: {len(targets):,}건")
    print(f"  이미지 상한: 상품당 {TOP_IMAGES}개")

    started = time.time()
    stats = {"matched": 0, "unmatched": 0, "failed": 0}

    for idx, product in enumerate(targets, start=1):
        result = process_product(
            conn,
            product,
            api_key=api_key,
            report_pool=report_pool,
            verbose=not args.quiet,
        )
        stats[result["status"]] += 1

        elapsed = time.time() - started
        speed = idx / elapsed if elapsed > 0 else 0
        remain = len(targets) - idx
        eta_sec = remain / speed if speed > 0 else 0

        print(
            f"\n  진행률 [{_progress_bar(idx, len(targets))}] "
            f"{idx:,}/{len(targets):,} | "
            f"matched={stats['matched']:,} unmatched={stats['unmatched']:,} failed={stats['failed']:,} | "
            f"ETA {eta_sec:.1f}s"
        )
        print(
            f"  결과 요약: status={result['status']} | images={result['images_analyzed']}/{result['images_requested']} "
            f"| 저장={result['saved_records']}"
        )
        if result["matched_image_url"]:
            print(f"  매칭 출처 URL: {_short(result['matched_image_url'], 100)}")

    elapsed = time.time() - started
    print(f"\n실행 완료: {elapsed:.1f}초")
    print(
        f"- matched={stats['matched']:,}, unmatched={stats['unmatched']:,}, failed={stats['failed']:,}"
    )
    summarize(conn)
    conn.close()


if __name__ == "__main__":
    main()
