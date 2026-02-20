"""
원재료명 수집 파이프라인 (SerpAPI 이미지 검색 + Gemini analyze(url) 결과 저장)

- 이미지는 파일로 저장하지 않고 URL만 사용한다.
- 결과는 모두 DB에 영구 저장한다. (캐시 아님)
"""

import argparse
import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from collections import Counter

import requests

from app.config import DB_FILE
from app.analyzer import URLIngredientAnalyzer

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


def _priority_score(lv3: str, lv4: str) -> int:
    """대사/저당/단백질 관심군 기준 중분류 우선순위 점수."""
    s = 0
    # 대분류 기본 점수
    if lv3 == "특수영양식품":
        s += 10
    elif lv3 == "음료류":
        s += 9
    elif lv3 == "즉석식품류":
        s += 8
    elif lv3 == "유가공품류":
        s += 8
    elif lv3 == "식육가공품 및 포장육":
        s += 7
    elif lv3 == "조미식품":
        s += 8
    elif lv3 == "농산가공식품류":
        s += 6
    elif lv3 == "면류":
        s += 5
    elif lv3 == "두부류 또는 묵류":
        s += 6
    elif lv3 in ("과자류·빵류 또는 떡류", "코코아가공품류 또는 초콜릿류", "빙과류", "당류"):
        s += 3
    elif lv3 in ("절임류 또는 조림류", "수산가공식품류", "장류", "식용유지류", "기타식품류"):
        s += 4
    elif lv3 in ("특수의료용도식품", "알가공품류", "동물성가공식품류"):
        s += 4
    elif lv3 in ("주류", "벌꿀 및 화분가공 식품류"):
        s += 1

    # 중분류 가산
    if "체중조절" in lv4:
        s += 8
    if lv4 in ("두유", "식육간편조리", "샐러드", "도시락", "죽", "시리얼바/에너지바/영양바"):
        s += 6
    if lv4 in ("발효유", "농후발효유", "가공우유", "우유", "유당분해우유", "치즈"):
        s += 4
    if lv4 in ("액상음료", "농축음료/베이스", "과·채음료", "과·채주스", "액상차", "액상커피"):
        s += 5
    if lv4 in ("기타 소스류", "드레싱", "마요네즈", "토마토케첩", "복합조미식품", "분말스프"):
        s += 8
    if lv4 == "간장":
        s += 10
    if "콤부차" in lv4:
        s += 10

    # 후순위 감점
    if lv4 in ("사탕", "젤리", "비스킷/쿠키/크래커", "초콜릿", "탄산음료", "아이스크림", "빙과", "주류"):
        s -= 3
    return s


def _priority_label(score: int) -> str:
    if score >= 19:
        return "P1"
    if score >= 16:
        return "P2"
    if score >= 12:
        return "P3"
    if score >= 8:
        return "P4"
    return "P5"


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


def _has_text(value: str | None) -> bool:
    return bool(value and value.strip())


def _diagnose_analysis(analysis: dict, target_item_rpt_no: str) -> tuple[str, str]:
    """분석 결과를 사용자 친화적인 상태/사유로 변환."""
    extracted = (analysis.get("itemMnftrRptNo") or "").strip()
    ingredients = (analysis.get("ingredients_text") or "").strip()
    error = (analysis.get("error") or "").strip()
    note = (analysis.get("note") or "").strip()
    has_ingredients = analysis.get("has_ingredients")
    is_flat = analysis.get("is_flat")
    is_designed_graphic = analysis.get("is_designed_graphic")
    has_real_world_objects = analysis.get("has_real_world_objects")

    if error:
        lower = error.lower()
        if "api key not found" in lower:
            return ("분석실패", "Gemini API 키가 유효하지 않거나 인식되지 않음")
        if "permission" in lower or "forbidden" in lower:
            return ("분석실패", "Gemini 권한/접근 오류")
        if "timeout" in lower:
            return ("분석실패", "Gemini 응답 시간 초과")
        if "image download failed" in lower:
            return ("분석실패", "이미지 URL 접근 실패(403/404 등)")
        if "image too large" in lower:
            return ("분석실패", "이미지 용량이 너무 큼")
        return ("분석실패", _short(error, 96))

    if is_designed_graphic is False:
        return ("실사이미지", "디자인된 정보형 이미지가 아님")
    if has_real_world_objects is True:
        return ("실사이미지", "사람/제품 실물/배경 사물이 포함됨")

    if extracted and extracted == target_item_rpt_no and _has_text(ingredients) and is_flat is True:
        return ("매칭성공", "타깃 품목보고번호 + 원재료명 둘 다 확인")
    if extracted and extracted == target_item_rpt_no and _has_text(ingredients) and is_flat is False:
        return ("비평면", "번호/원재료는 있으나 비평면 이미지라 신뢰 불가")
    if extracted and extracted == target_item_rpt_no and _has_text(ingredients) and is_flat is None:
        return ("평면불명", "번호/원재료는 있으나 평면 여부 판정 실패")
    if extracted and extracted == target_item_rpt_no and not _has_text(ingredients):
        return ("부분성공", "번호는 일치하지만 원재료명 텍스트를 읽지 못함")
    if extracted and extracted != target_item_rpt_no and _has_text(ingredients):
        return ("타상품검출", f"다른 품목보고번호 검출({extracted})")
    if extracted and extracted != target_item_rpt_no and not _has_text(ingredients):
        return ("타상품검출", f"다른 품목보고번호만 검출({extracted})")
    if not extracted and _has_text(ingredients):
        return ("번호미검출", "원재료는 읽혔지만 품목보고번호를 찾지 못함")
    if extracted and _has_text(ingredients) and is_flat is False:
        return ("비평면", "원재료는 있으나 비평면 이미지라 신뢰 불가")
    if has_ingredients is False:
        return ("원재료없음", "이미지에서 원재료 영역 자체를 찾지 못함")
    if note:
        return ("미판독", _short(note, 96))
    return ("미판독", "번호/원재료 모두 판독되지 않음(화질, 각도, 가림 가능)")


def _analysis_outcome_code(analysis: dict, target_item_rpt_no: str) -> str:
    """이미지 분석 결과를 배치 통계용 코드로 변환."""
    if analysis.get("error"):
        return "analysis_error"

    extracted = (analysis.get("itemMnftrRptNo") or "").strip()
    ingredients = (analysis.get("ingredients_text") or "").strip()
    is_flat = analysis.get("is_flat")

    if not extracted:
        return "missing_report_no"
    if not ingredients:
        return "missing_ingredients"
    if is_flat is not True:
        return "non_flat_or_unknown"
    if target_item_rpt_no and extracted == target_item_rpt_no:
        return "matched_target"
    if target_item_rpt_no:
        return "other_report_no"
    return "has_report_and_ingredients"


def diagnose_analysis(analysis: dict, target_item_rpt_no: str) -> tuple[str, str]:
    """외부 UI/리포트에서 사용할 진단 함수."""
    return _diagnose_analysis(analysis, target_item_rpt_no)


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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS image_analysis_cache (
            image_url_hash TEXT PRIMARY KEY,
            image_url TEXT NOT NULL,
            extracted_itemMnftrRptNo TEXT,
            ingredients_text TEXT,
            raw_payload TEXT,
            analyzed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def build_search_query(product: Product) -> str:
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


def _normalize_image_url(image_url: str) -> str:
    """동일 URL 판정을 위한 최소 정규화."""
    return (image_url or "").strip()


def _image_url_hash(image_url: str) -> str:
    normalized = _normalize_image_url(image_url)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def get_cached_image_analysis(conn: sqlite3.Connection, image_url: str) -> dict | None:
    key = _image_url_hash(image_url)
    row = conn.execute(
        """
        SELECT extracted_itemMnftrRptNo, ingredients_text, raw_payload
        FROM image_analysis_cache
        WHERE image_url_hash = ?
        """,
        (key,),
    ).fetchone()
    if not row:
        return None
    extracted, ingredients, raw_payload = row
    payload_dict: dict = {}
    if raw_payload:
        try:
            payload_dict = json.loads(raw_payload)
        except json.JSONDecodeError:
            payload_dict = {}
    return {
        "itemMnftrRptNo": extracted,
        "ingredients_text": ingredients,
        "note": payload_dict.get("note", "cache-hit"),
        "is_flat": payload_dict.get("is_flat"),
        "is_table_format": payload_dict.get("is_table_format"),
        "is_designed_graphic": payload_dict.get("is_designed_graphic"),
        "has_real_world_objects": payload_dict.get("has_real_world_objects"),
        "brand": payload_dict.get("brand"),
        "error": payload_dict.get("error"),
        "has_ingredients": payload_dict.get("has_ingredients"),
        "product_name_in_image": payload_dict.get("product_name_in_image"),
        "manufacturer": payload_dict.get("manufacturer"),
        "full_text": payload_dict.get("full_text"),
    }


def upsert_image_analysis_cache(
    conn: sqlite3.Connection,
    image_url: str,
    analysis: dict,
) -> None:
    key = _image_url_hash(image_url)
    conn.execute(
        """
        INSERT INTO image_analysis_cache (
            image_url_hash,
            image_url,
            extracted_itemMnftrRptNo,
            ingredients_text,
            raw_payload,
            analyzed_at
        )
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(image_url_hash) DO UPDATE SET
            extracted_itemMnftrRptNo = excluded.extracted_itemMnftrRptNo,
            ingredients_text = excluded.ingredients_text,
            raw_payload = excluded.raw_payload,
            analyzed_at = CURRENT_TIMESTAMP
        """,
        (
            key,
            _normalize_image_url(image_url),
            analysis.get("itemMnftrRptNo"),
            analysis.get("ingredients_text"),
            json.dumps(analysis, ensure_ascii=False),
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


def fetch_target_products_by_category(
    conn: sqlite3.Connection,
    lv3: str,
    lv4: str,
    limit: int | None,
) -> list[Product]:
    """선택한 대/중분류에서 아직 시도/성공되지 않은 대상만 조회."""
    sql = """
        SELECT fi.itemMnftrRptNo, fi.foodNm, COALESCE(fi.mfrNm, '')
        FROM food_info fi
        WHERE fi.itemMnftrRptNo IS NOT NULL
          AND fi.itemMnftrRptNo != ''
          AND fi.foodLv3Nm = ?
          AND fi.foodLv4Nm = ?
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
    params: list = [lv3, lv4]
    if limit is not None and limit > 0:
        sql += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [Product(item_rpt_no=r[0], food_name=r[1] or "", mfr_name=r[2] or "") for r in rows]


def fetch_product_by_report_no(conn: sqlite3.Connection, report_no: str) -> Product | None:
    """품목보고번호로 단일 상품 조회 (재시도용)."""
    row = conn.execute(
        """
        SELECT fi.itemMnftrRptNo, fi.foodNm, COALESCE(fi.mfrNm, '')
        FROM food_info fi
        WHERE fi.itemMnftrRptNo = ?
        ORDER BY fi.id
        LIMIT 1
        """,
        (report_no,),
    ).fetchone()
    if not row:
        return None
    return Product(item_rpt_no=row[0], food_name=row[1] or "", mfr_name=row[2] or "")


def get_priority_subcategories(conn: sqlite3.Connection) -> list[dict]:
    """중분류별 우선순위/진행현황 목록."""
    rows = conn.execute(
        """
        WITH base AS (
            SELECT DISTINCT
                fi.itemMnftrRptNo AS rpt_no,
                COALESCE(NULLIF(TRIM(fi.foodLv3Nm), ''), '미분류') AS lv3,
                COALESCE(NULLIF(TRIM(fi.foodLv4Nm), ''), '중분류 미지정') AS lv4
            FROM food_info fi
            WHERE fi.itemMnftrRptNo IS NOT NULL
              AND fi.itemMnftrRptNo != ''
        )
        SELECT
            b.lv3,
            b.lv4,
            COUNT(*) AS total_count,
            SUM(CASE WHEN ia.query_itemMnftrRptNo IS NOT NULL THEN 1 ELSE 0 END) AS attempted_count,
            SUM(CASE WHEN ii.itemMnftrRptNo IS NOT NULL THEN 1 ELSE 0 END) AS success_count
        FROM base b
        LEFT JOIN ingredient_attempts ia ON ia.query_itemMnftrRptNo = b.rpt_no
        LEFT JOIN ingredient_info ii ON ii.itemMnftrRptNo = b.rpt_no
        GROUP BY b.lv3, b.lv4
        """
    ).fetchall()

    result: list[dict] = []
    for lv3, lv4, total_count, attempted_count, success_count in rows:
        score = _priority_score(lv3, lv4)
        success_rate = (success_count / total_count * 100) if total_count else 0.0
        result.append(
            {
                "lv3": lv3,
                "lv4": lv4,
                "score": score,
                "priority": _priority_label(score),
                "total_count": total_count,
                "attempted_count": attempted_count,
                "success_count": success_count,
                "success_rate": success_rate,
            }
        )
    result.sort(
        key=lambda x: (
            -x["score"],
            -x["total_count"],
            x["lv3"],
            x["lv4"],
        )
    )
    return result


def process_product(
    conn: sqlite3.Connection,
    product: Product,
    api_key: str,
    analyzer: URLIngredientAnalyzer,
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
    reason_counter: Counter[str] = Counter()

    if verbose:
        print(f"  이미지 후보: {len(image_urls)}개")

    for rank, image_url in enumerate(image_urls, start=1):
        analyzed_count += 1
        cache_hit = False
        analysis = get_cached_image_analysis(conn, image_url)
        if analysis is None:
            try:
                analysis = analyzer.analyze(
                    image_url=image_url,
                    target_item_rpt_no=product.item_rpt_no,
                )
            except Exception as exc:  # pylint: disable=broad-except
                analysis = {
                    "itemMnftrRptNo": None,
                    "ingredients_text": None,
                    "note": f"analysis_error:{type(exc).__name__}",
                    "error": str(exc),
                }
            upsert_image_analysis_cache(conn, image_url, analysis)
        else:
            cache_hit = True
        extracted = analysis.get("itemMnftrRptNo")
        ingredients = analysis.get("ingredients_text")
        is_flat = analysis.get("is_flat")
        is_match = bool(
            extracted
            and _has_text(ingredients)
            and is_flat is True
            and extracted == product.item_rpt_no
        )
        outcome_code = _analysis_outcome_code(analysis, product.item_rpt_no)
        reason_counter[outcome_code] += 1

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

        if extracted and _has_text(ingredients) and is_flat is True:
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
            extracted_text = extracted or "없음"
            result_tag = "MATCH" if is_match else "NO-MATCH"
            cache_tag = "CACHE" if cache_hit else "ANALYZE"
            flat_text = "flat" if is_flat is True else ("non-flat" if is_flat is False else "unknown")
            status_label, reason_text = _diagnose_analysis(analysis, product.item_rpt_no)
            ingredients_preview = _short(ingredients, 76) if _has_text(ingredients) else "없음"
            print(
                f"    [{rank:02d}/{len(image_urls):02d}] {cache_tag} {result_tag} "
                f"번호={extracted_text} | 평면={flat_text} | url={image_url}"
            )
            print(f"      상태: {status_label} | 사유: {reason_text}")
            print(f"      원재료: {ingredients_preview}")
            print(f"      분류코드: {outcome_code}")
            if extracted and _has_text(ingredients) and is_flat is True:
                print(f"      저장: itemMnftrRptNo={extracted} (원재료 DB 반영)")

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
            print(f"  [완료] 매칭 성공 | source_url={matched_image_url}")
        return {
            "status": "matched",
            "images_requested": len(image_urls),
            "images_analyzed": analyzed_count,
            "matched_image_url": matched_image_url,
            "saved_records": saved_records,
            "query": query,
            "reason_counts": dict(reason_counter),
            "dominant_reason": reason_counter.most_common(1)[0][0] if reason_counter else None,
        }

    dominant_reason = reason_counter.most_common(1)[0][0] if reason_counter else None
    upsert_attempt(
        conn,
        product,
        status="unmatched",
        query=query,
        images_requested=len(image_urls),
        images_analyzed=analyzed_count,
        error_message=dominant_reason,
    )
    conn.commit()
    if verbose:
        print(f"  [완료] 미매칭 (주요 사유: {dominant_reason or 'unknown'})")
    return {
        "status": "unmatched",
        "images_requested": len(image_urls),
        "images_analyzed": analyzed_count,
        "matched_image_url": None,
        "saved_records": saved_records,
        "query": query,
        "reason_counts": dict(reason_counter),
        "dominant_reason": dominant_reason,
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


def run_enricher(
    limit: int = 20,
    db_path: str = DB_FILE,
    quiet: bool = False,
    lv3: str | None = None,
    lv4: str | None = None,
) -> None:
    api_key = os.getenv("SERPAPI_KEY")
    if not api_key:
        raise SystemExit("SERPAPI_KEY 환경변수를 설정해주세요.")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        raise SystemExit("OPENAI_API_KEY 환경변수를 설정해주세요.")

    conn = sqlite3.connect(db_path)
    init_ingredient_tables(conn)
    analyzer = URLIngredientAnalyzer(api_key=openai_api_key)

    if lv3 is not None and lv4 is not None:
        targets = fetch_target_products_by_category(conn, lv3=lv3, lv4=lv4, limit=limit)
    else:
        targets = fetch_target_products(conn, limit=limit)
    if not targets:
        print("처리할 대상이 없습니다. (이미 ingredient_info/ingredient_attempts에 존재)")
        summarize(conn)
        conn.close()
        return

    print("\n╔══════════════════════════════════════════════════════════════════╗")
    print("║                원재료 수집 실행 (URL 분석 모드)                ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    if lv3 is not None and lv4 is not None:
        print(f"  선택 카테고리: {lv3} > {lv4}")
    print(f"  처리 대상: {len(targets):,}건")
    print(f"  이미지 상한: 상품당 {TOP_IMAGES}개")

    started = time.time()
    stats = {"matched": 0, "unmatched": 0, "failed": 0}
    reason_totals: Counter[str] = Counter()

    for idx, product in enumerate(targets, start=1):
        result = process_product(
            conn,
            product,
            api_key=api_key,
            analyzer=analyzer,
            verbose=not quiet,
        )
        stats[result["status"]] += 1
        for k, v in (result.get("reason_counts") or {}).items():
            reason_totals[k] += int(v)

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
        if result.get("dominant_reason"):
            print(f"  주요 사유: {result['dominant_reason']}")
        if result["matched_image_url"]:
            print(f"  매칭 출처 URL: {result['matched_image_url']}")

    elapsed = time.time() - started
    print(f"\n실행 완료: {elapsed:.1f}초")
    print(
        f"- matched={stats['matched']:,}, unmatched={stats['unmatched']:,}, failed={stats['failed']:,}"
    )
    if reason_totals:
        print("- 이미지 결과 사유 집계(top 8):")
        for code, cnt in reason_totals.most_common(8):
            print(f"  • {code}: {cnt:,}")
    summarize(conn)
    conn.close()


def run_enricher_for_report_no(
    report_no: str,
    db_path: str = DB_FILE,
    quiet: bool = False,
) -> None:
    """특정 품목보고번호 1건만 원재료 분석 실행."""
    api_key = os.getenv("SERPAPI_KEY")
    if not api_key:
        raise SystemExit("SERPAPI_KEY 환경변수를 설정해주세요.")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        raise SystemExit("OPENAI_API_KEY 환경변수를 설정해주세요.")

    report_no = (report_no or "").strip()
    if not report_no:
        raise SystemExit("품목보고번호를 입력해주세요.")

    conn = sqlite3.connect(db_path)
    init_ingredient_tables(conn)
    analyzer = URLIngredientAnalyzer(api_key=openai_api_key)

    product = fetch_product_by_report_no(conn, report_no)
    if not product:
        print(f"대상 품목보고번호를 food_info에서 찾지 못했습니다: {report_no}")
        conn.close()
        return

    print("\n╔══════════════════════════════════════════════════════════════════╗")
    print("║              원재료 수집 실행 (품목보고번호 1건)              ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    result = process_product(
        conn,
        product=product,
        api_key=api_key,
        analyzer=analyzer,
        verbose=not quiet,
    )
    print("\n[1건 실행 결과]")
    print(
        f"- status={result['status']} | images={result['images_analyzed']}/{result['images_requested']} "
        f"| 저장={result['saved_records']}"
    )
    if result.get("matched_image_url"):
        print(f"- 매칭 출처 URL: {result['matched_image_url']}")
    summarize(conn)
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="원재료명 수집 파이프라인 (Gemini URL 분석)")
    parser.add_argument("--limit", type=int, default=20, help="이번 실행에서 처리할 최대 상품 수")
    parser.add_argument("--db", type=str, default=DB_FILE, help="SQLite DB 파일 경로")
    parser.add_argument("--quiet", action="store_true", help="이미지별 상세 로그 출력 생략")
    args = parser.parse_args()
    run_enricher(limit=args.limit, db_path=args.db, quiet=args.quiet)


if __name__ == "__main__":
    main()
