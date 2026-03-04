from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from typing import Any

import requests


NUTRITION_KEY_MAP_PUBLIC_DB = {
    "열량(kcal)": "kcal",
    "칼로리(kcal)": "kcal",
    "단백질(g)": "protein_g",
    "지방(g)": "fat_g",
    "탄수화물(g)": "carbohydrate_g",
    "당류(g)": "sugar_g",
    "나트륨(mg)": "sodium_mg",
    "콜레스테롤(mg)": "cholesterol_mg",
    "포화지방(g)": "saturated_fat_g",
    "트랜스지방(g)": "trans_fat_g",
    "식이섬유(g)": "dietary_fiber_g",
    "칼슘(mg)": "calcium_mg",
    "철(mg)": "iron_mg",
    "인(mg)": "phosphorus_mg",
    "칼륨(mg)": "potassium_mg",
}


NUTRITION_NAME_MAP_IMAGE_PASS4 = {
    "칼로리": "kcal",
    "열량": "kcal",
    "단백질": "protein_g",
    "지방": "fat_g",
    "탄수화물": "carbohydrate_g",
    "당류": "sugar_g",
    "나트륨": "sodium_mg",
    "콜레스테롤": "cholesterol_mg",
    "포화지방": "saturated_fat_g",
    "트랜스지방": "trans_fat_g",
    "식이섬유": "dietary_fiber_g",
    "칼슘": "calcium_mg",
    "철": "iron_mg",
    "인": "phosphorus_mg",
    "칼륨": "potassium_mg",
    "수분": "water_g",
}


@dataclass
class TransformResult:
    payload: dict[str, Any] | None
    error: str | None = None


MAX_PRODUCT_NAME_LEN = 200


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    text = text.replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def _load_json(text: str | None) -> dict[str, Any] | None:
    if text is None:
        return None
    raw = text.strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _validate_product_name(name: str) -> str | None:
    normalized = re.sub(r"\s+", " ", name).strip()
    if not normalized:
        return "missing_product_name"
    if len(normalized) > MAX_PRODUCT_NAME_LEN:
        return f"invalid_product_name_too_long:{len(normalized)}"
    # 대표적인 이상치: 같은 문자만 매우 길게 반복 (예: xxxxxxxxx...)
    if re.fullmatch(r"(.)\1{29,}", normalized):
        return "invalid_product_name_repeated_char"
    # 공백/기호 제외 실질 문자가 2개 미만이면 제품명으로 보기 어려움
    alnum = re.sub(r"[\W_]+", "", normalized, flags=re.UNICODE)
    if len(alnum) < 2:
        return "invalid_product_name_too_short"
    return None


def _normalize_ingredient_node(node: dict[str, Any], *, root: bool) -> dict[str, Any] | None:
    if root:
        name = str(node.get("ingredient_name") or "").strip()
    else:
        name = str(node.get("name") or "").strip()
    if not name:
        return None

    out: dict[str, Any] = {"ingredient_name" if root else "name": name}
    if node.get("origin") is not None:
        out["origin"] = node.get("origin")
    if node.get("origin_detail") is not None:
        out["origin_detail"] = node.get("origin_detail")
    if node.get("amount") is not None:
        out["amount"] = node.get("amount")

    raw_children = node.get("sub_ingredients")
    if isinstance(raw_children, list):
        normalized_children: list[dict[str, Any]] = []
        for child in raw_children:
            if not isinstance(child, dict):
                continue
            normalized_child = _normalize_ingredient_node(child, root=False)
            if normalized_child is not None:
                normalized_children.append(normalized_child)
        out["sub_ingredients"] = normalized_children

    return out


def _parse_ingredients(raw_ingredients_text: str | None) -> list[dict[str, Any]]:
    parsed = _load_json(raw_ingredients_text)
    if not parsed:
        return []

    raw_items = parsed.get("ingredients_items")
    if not isinstance(raw_items, list):
        return []

    normalized: list[dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        normalized_item = _normalize_ingredient_node(item, root=True)
        if normalized_item is not None:
            normalized.append(normalized_item)
    return normalized


def _parse_nutrition_public_db(parsed_nutrition: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    items = parsed_nutrition.get("items")
    if not isinstance(items, dict):
        return out

    for source_key, target_key in NUTRITION_KEY_MAP_PUBLIC_DB.items():
        number = _to_float(items.get(source_key))
        if number is not None:
            out[target_key] = number

    source_name = parsed_nutrition.get("source")
    if isinstance(source_name, str) and source_name.strip():
        out["source_name"] = source_name.strip()

    return out


def _normalize_nutrition_name(name: str) -> str:
    return name.replace(" ", "").replace("\t", "").replace("\n", "")


def _parse_nutrition_image_pass4(parsed_nutrition: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    nutrition_items = parsed_nutrition.get("nutrition_items")
    if not isinstance(nutrition_items, list):
        return out

    for row in nutrition_items:
        if not isinstance(row, dict):
            continue
        name = _normalize_nutrition_name(str(row.get("name") or ""))
        unit = str(row.get("unit") or "").strip().lower()
        value = _to_float(row.get("value"))
        if value is None:
            continue

        # 총 내용량은 backend의 food_weight_g로 보냄.
        if name in {"총내용량", "총내용량당"} and unit == "g":
            out["food_weight_g"] = value
            continue

        for key_prefix, target_key in NUTRITION_NAME_MAP_IMAGE_PASS4.items():
            if name.startswith(key_prefix):
                out[target_key] = value
                break

    return out


def _parse_nutrition(
    raw_nutrition_text: str | None,
    nutrition_source: str | None,
) -> dict[str, Any] | None:
    parsed = _load_json(raw_nutrition_text)
    if not parsed:
        return None

    source = (nutrition_source or "").strip()
    if source == "public_food_db":
        out = _parse_nutrition_public_db(parsed)
    elif source == "image_pass4":
        out = _parse_nutrition_image_pass4(parsed)
    else:
        # source 정보가 없으면 가능한 쪽부터 시도.
        out = _parse_nutrition_public_db(parsed)
        if not out:
            out = _parse_nutrition_image_pass4(parsed)

    return out if out else None


def transform_food_final_row(row: sqlite3.Row) -> TransformResult:
    product_name = str(row["product_name"] or "").strip()
    product_name_error = _validate_product_name(product_name)
    if product_name_error is not None:
        return TransformResult(payload=None, error=product_name_error)

    payload: dict[str, Any] = {
        "product": {
            "name_ko": product_name,
            "reportnum": (str(row["item_mnftr_rpt_no"]).strip() if row["item_mnftr_rpt_no"] else None),
        },
        "source": {
            "url": str(row["source_image_url"] or "").strip() or None,
        },
    }

    ingredients_items = _parse_ingredients(row["ingredients_text"])
    if ingredients_items:
        payload["ingredients"] = {
            "ingredients_items": ingredients_items,
            "allergens": [],
            "collected_at": row["created_at"],
        }

    nutrition = _parse_nutrition(row["nutrition_text"], row["nutrition_source"])
    if nutrition:
        nutrition["collected_at"] = row["created_at"]
        payload["nutrition"] = nutrition

    return TransformResult(payload=payload)


def transform_haccp_parsed_row(row: sqlite3.Row) -> TransformResult:
    product_name = str(row["product_name"] or "").strip()
    product_name_error = _validate_product_name(product_name)
    if product_name_error is not None:
        return TransformResult(payload=None, error=product_name_error)

    payload: dict[str, Any] = {
        "product": {
            "name_ko": product_name,
            "reportnum": (str(row["item_mnftr_rpt_no"]).strip() if row["item_mnftr_rpt_no"] else None),
        },
        "source": {
            "url": str(row["source_image_url"] or "").strip() or None,
        },
    }

    ingredients_items = _parse_ingredients(row["ingredients_text"])
    if ingredients_items:
        payload["ingredients"] = {
            "ingredients_items": ingredients_items,
            "allergens": [],
            "collected_at": row["created_at"],
        }

    nutrition = _parse_nutrition(row["nutrition_text"], row["nutrition_source"])
    if nutrition:
        nutrition["collected_at"] = row["created_at"]
        payload["nutrition"] = nutrition

    return TransformResult(payload=payload)


def fetch_food_final_rows(
    db_path: str,
    *,
    min_id: int | None,
    max_id: int | None,
    limit: int | None,
) -> list[sqlite3.Row]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        where_clauses: list[str] = []
        params: list[Any] = []

        where_clauses.append("COALESCE(TRIM(product_name), '') != ''")
        where_clauses.append("COALESCE(TRIM(source_image_url), '') != ''")

        if min_id is not None:
            where_clauses.append("id >= ?")
            params.append(min_id)
        if max_id is not None:
            where_clauses.append("id <= ?")
            params.append(max_id)

        where_sql = " AND ".join(where_clauses)
        sql = f"""
            SELECT
              id,
              product_name,
              item_mnftr_rpt_no,
              ingredients_text,
              nutrition_text,
              nutrition_source,
              source_image_url,
              created_at
            FROM food_final
            WHERE {where_sql}
            ORDER BY id ASC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        rows = conn.execute(sql, params).fetchall()
        return rows
    finally:
        conn.close()


def fetch_haccp_parsed_rows(
    db_path: str,
    *,
    min_id: int | None,
    max_id: int | None,
    limit: int | None,
    exclude_blocked: bool = True,
) -> list[sqlite3.Row]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        where_clauses: list[str] = []
        params: list[Any] = []

        where_clauses.append("p.parse_status = 'ok'")
        where_clauses.append("COALESCE(TRIM(h.prdlstNm), '') != ''")
        where_clauses.append(
            "(COALESCE(TRIM(p.ingredients_items_json), '') != '' OR COALESCE(TRIM(p.nutrition_items_json), '') != '')"
        )
        if exclude_blocked:
            where_clauses.append("b.prdlstReportNo IS NULL")
        if min_id is not None:
            where_clauses.append("p.id >= ?")
            params.append(min_id)
        if max_id is not None:
            where_clauses.append("p.id <= ?")
            params.append(max_id)

        where_sql = " AND ".join(where_clauses)
        sql = f"""
            SELECT
              p.id,
              p.prdlstReportNo AS item_mnftr_rpt_no,
              h.prdlstNm AS product_name,
              p.ingredients_items_json AS ingredients_text,
              p.nutrition_items_json AS nutrition_text,
              'image_pass4' AS nutrition_source,
              COALESCE(NULLIF(TRIM(h.imgurl1), ''), NULLIF(TRIM(h.imgurl2), '')) AS source_image_url,
              p.parsed_at AS created_at
            FROM haccp_parsed_cache p
            JOIN haccp_product_info h ON h.prdlstReportNo = p.prdlstReportNo
            LEFT JOIN haccp_parse_blocklist b
              ON b.prdlstReportNo = p.prdlstReportNo AND b.is_blocked = 1
            WHERE {where_sql}
            ORDER BY p.id ASC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return rows
    finally:
        conn.close()


def chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def send_batches(
    payloads: list[dict[str, Any]],
    *,
    import_url: str,
    batch_size: int,
    timeout_sec: int,
) -> tuple[int, int]:
    sent_count = 0
    error_count = 0

    for index, batch in enumerate(chunked(payloads, batch_size), start=1):
        try:
            resp = requests.post(import_url, json=batch, timeout=timeout_sec)
            if resp.status_code >= 400:
                error_count += len(batch)
                print(
                    f"[batch {index}] HTTP {resp.status_code} failed - size={len(batch)} "
                    f"body={resp.text[:500]}"
                )
                continue

            data = resp.json()
            created = int(data.get("created", 0))
            skipped = int(data.get("skipped", 0))
            errors = data.get("errors", [])
            print(
                f"[batch {index}] ok size={len(batch)} created={created} "
                f"skipped={skipped} row_errors={len(errors)}"
            )
            sent_count += len(batch)
        except Exception as exc:  # noqa: BLE001
            error_count += len(batch)
            print(f"[batch {index}] exception size={len(batch)} error={exc}")

    return sent_count, error_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="gathering.food_final -> backend /api/import 배치 전송"
    )
    parser.add_argument(
        "--db-path",
        default=os.environ.get("GATHERING_DB_PATH", "/Users/dmyeon/gathering/food_data.db"),
        help="SQLite food_data.db 경로",
    )
    parser.add_argument(
        "--import-url",
        default=os.environ.get("BACKEND_IMPORT_URL", "http://localhost:3000/api/import"),
        help="insidefood backend import endpoint",
    )
    parser.add_argument("--batch-size", type=int, default=50, help="배치 전송 크기")
    parser.add_argument("--timeout-sec", type=int, default=30, help="HTTP timeout(초)")
    parser.add_argument("--limit", type=int, default=None, help="최대 처리 건수")
    parser.add_argument("--min-id", type=int, default=None, help="시작 id (inclusive)")
    parser.add_argument("--max-id", type=int, default=None, help="끝 id (inclusive)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="전송 없이 변환만 수행",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    rows = fetch_food_final_rows(
        args.db_path,
        min_id=args.min_id,
        max_id=args.max_id,
        limit=args.limit,
    )
    print(f"loaded rows={len(rows)} from {args.db_path}")

    payloads: list[dict[str, Any]] = []
    transform_errors = 0

    for row in rows:
        transformed = transform_food_final_row(row)
        if transformed.payload is not None:
            payloads.append(transformed.payload)
        else:
            transform_errors += 1
            print(f"[row {row['id']}] skipped: {transformed.error}")

    print(f"transformed payloads={len(payloads)} transform_errors={transform_errors}")

    if args.dry_run:
        if payloads:
            print("--- dry-run first payload ---")
            print(json.dumps(payloads[0], ensure_ascii=False, indent=2))
        return

    if not payloads:
        print("nothing to send")
        return

    sent_count, send_errors = send_batches(
        payloads,
        import_url=args.import_url,
        batch_size=max(1, args.batch_size),
        timeout_sec=max(1, args.timeout_sec),
    )
    print(
        f"done payloads={len(payloads)} sent={sent_count} "
        f"send_errors={send_errors} transform_errors={transform_errors}"
    )


if __name__ == "__main__":
    main()
