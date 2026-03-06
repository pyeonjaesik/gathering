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
    "수분(g)": "water_g",
    "회분(g)": "ash_g",
    "비타민A(μg RAE)": "vitamin_a_rae_ug",
    "레티놀(μg)": "retinol_ug",
    "베타카로틴(μg)": "beta_carotene_ug",
    "비타민B1(mg)": "vitamin_b1_mg",
    "비타민B2(mg)": "vitamin_b2_mg",
    "나이아신(mg)": "niacin_mg",
    "비타민C(mg)": "vitamin_c_mg",
    "비타민D(μg)": "vitamin_d_ug",
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
    "회분": "ash_g",
    "비타민a": "vitamin_a_rae_ug",
    "레티놀": "retinol_ug",
    "베타카로틴": "beta_carotene_ug",
    "비타민b1": "vitamin_b1_mg",
    "비타민b2": "vitamin_b2_mg",
    "나이아신": "niacin_mg",
    "비타민c": "vitamin_c_mg",
    "비타민d": "vitamin_d_ug",
    "포화지방산": "saturated_fat_g",
    "트랜스지방산": "trans_fat_g",
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
    if isinstance(items, dict):
        for source_key, target_key in NUTRITION_KEY_MAP_PUBLIC_DB.items():
            number = _to_float(items.get(source_key))
            if number is not None:
                out[target_key] = number

    nutrition_items = parsed_nutrition.get("nutrition_items")
    if isinstance(nutrition_items, list):
        for row in nutrition_items:
            if not isinstance(row, dict):
                continue
            name = _normalize_nutrition_name(str(row.get("name") or "")).lower()
            unit = str(row.get("unit") or "").strip().lower()
            value_raw = row.get("value")
            value_num = _to_float(value_raw)

            if name in {"기준량", "영양성분기준량"}:
                txt = str(value_raw or "").strip()
                if txt:
                    out["basis_text"] = txt
                continue
            if name.startswith("1회제공량"):
                txt = str(value_raw or "").strip()
                if txt:
                    out["serving_size"] = txt
                if value_num is not None and unit in {"g", "ml"}:
                    out["serving_size_value"] = value_num
                    out["serving_size_unit"] = unit
                continue
            if name.startswith("총내용량"):
                txt = str(value_raw or "").strip()
                if txt:
                    out["total_content"] = txt
                if value_num is not None and unit in {"g", "ml"}:
                    out["food_weight_g"] = value_num if unit == "g" else None
                    out["food_weight_ml"] = value_num if unit == "ml" else None
                continue
            if ("회제공량" in name) and name.startswith("총"):
                txt = str(value_raw or "").strip()
                if txt:
                    out["servings_per_container"] = txt
                continue

            if value_num is None:
                continue
            for key_prefix, target_key in NUTRITION_NAME_MAP_IMAGE_PASS4.items():
                if name.startswith(key_prefix):
                    out[target_key] = value_num
                    break

    source_name = parsed_nutrition.get("source")
    if isinstance(source_name, str) and source_name.strip():
        out["source_name"] = source_name.strip()

    nutrition_basis = parsed_nutrition.get("nutrition_basis")
    if isinstance(nutrition_basis, dict):
        per_amount = _to_float(nutrition_basis.get("per_amount"))
        per_unit = str(nutrition_basis.get("per_unit") or "").strip().lower() or None
        basis_text = str(nutrition_basis.get("basis_text") or "").strip() or None
        if per_amount is not None:
            out["basis_amount"] = per_amount
        if per_unit:
            out["basis_unit"] = per_unit
        if basis_text and (not out.get("basis_text")):
            out["basis_text"] = basis_text

    serv_size = str(parsed_nutrition.get("serv_size") or "").strip()
    food_size = str(parsed_nutrition.get("food_size") or "").strip()
    if serv_size and (not out.get("serving_size")):
        out["serving_size"] = serv_size
    if food_size and (not out.get("total_content")):
        out["total_content"] = food_size

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
        value_raw = row.get("value")

        if name in {"기준량", "영양성분기준량"}:
            txt = str(value_raw or "").strip()
            if txt:
                out["basis_text"] = txt
            continue
        if name.startswith("1회제공량"):
            txt = str(value_raw or "").strip()
            if txt:
                out["serving_size"] = txt
        if name.startswith("총내용량"):
            txt = str(value_raw or "").strip()
            if txt:
                out["total_content"] = txt
        if ("회제공량" in name) and name.startswith("총"):
            txt = str(value_raw or "").strip()
            if txt:
                out["servings_per_container"] = txt

        value = _to_float(value_raw)
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
        if name.startswith("1회제공량") and unit in {"g", "ml"}:
            out["serving_size_value"] = value
            out["serving_size_unit"] = unit
        if name.startswith("총내용량") and unit in {"g", "ml"}:
            out["food_weight_g"] = value if unit == "g" else out.get("food_weight_g")
            out["food_weight_ml"] = value if unit == "ml" else out.get("food_weight_ml")

    nutrition_basis = parsed_nutrition.get("nutrition_basis")
    if isinstance(nutrition_basis, dict):
        per_amount = _to_float(nutrition_basis.get("per_amount"))
        per_unit = str(nutrition_basis.get("per_unit") or "").strip().lower() or None
        basis_text = str(nutrition_basis.get("basis_text") or "").strip() or None
        if per_amount is not None:
            out["basis_amount"] = per_amount
        if per_unit:
            out["basis_unit"] = per_unit
        if basis_text and (not out.get("basis_text")):
            out["basis_text"] = basis_text

    serv_size = str(parsed_nutrition.get("serv_size") or "").strip()
    food_size = str(parsed_nutrition.get("food_size") or "").strip()
    if serv_size and (not out.get("serving_size")):
        out["serving_size"] = serv_size
    if food_size and (not out.get("total_content")):
        out["total_content"] = food_size

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

    source_url = str(row["source_image_url"] or "").strip() or None
    payload: dict[str, Any] = {
        "product": {
            "name_ko": product_name,
            "reportnum": (str(row["item_mnftr_rpt_no"]).strip() if row["item_mnftr_rpt_no"] else None),
        },
        "source": {
            "url": source_url,
            "product": {
                "name": {
                    "source_type": "image_pass3",
                    "source_field": "food_final.product_name",
                    "url": source_url,
                },
                "reportnum": {
                    "source_type": "image_pass3",
                    "source_field": "food_final.item_mnftr_rpt_no",
                    "selected_from": (str(row["report_no_selected_from"] or "").strip() or None),
                    "candidates_json": (str(row["all_report_nos_json"] or "").strip() or None),
                    "url": source_url,
                },
            },
            "ingredients": {
                "source_type": "image_pass4_ing",
                "source_field": "food_final.ingredients_text",
                "value_format": "ingredients_items_json",
                "url": source_url,
            },
            "nutrition": {
                "source_type": (str(row["nutrition_source"] or "").strip() or "none"),
                "source_field": "food_final.nutrition_text",
                "value_format": "nutrition_json",
                "url": source_url,
            },
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
        basis_fallback = str(row["public_basis_text"] or "").strip()
        serv_fallback = str(row["public_serv_size"] or "").strip()
        food_fallback = str(row["public_food_size"] or "").strip()
        if basis_fallback and (not nutrition.get("basis_text")):
            nutrition["basis_text"] = basis_fallback
        if serv_fallback and (not nutrition.get("serving_size")):
            nutrition["serving_size"] = serv_fallback
        if food_fallback and (not nutrition.get("total_content")):
            nutrition["total_content"] = food_fallback
        nutrition["collected_at"] = row["created_at"]
        payload["nutrition"] = nutrition
    else:
        payload["source"]["nutrition"]["note"] = "nutrition_not_available"

    return TransformResult(payload=payload)


def transform_haccp_parsed_row(row: sqlite3.Row) -> TransformResult:
    product_name = str(row["product_name"] or "").strip()
    product_name_error = _validate_product_name(product_name)
    if product_name_error is not None:
        return TransformResult(payload=None, error=product_name_error)

    source_url = str(row["source_image_url"] or "").strip() or None
    parser_version = str(row["parser_version"] or "").strip() or None
    source_nutrient_raw = str(row["source_nutrient"] or "").strip() or None
    source_rawmtrl_raw = str(row["source_rawmtrl"] or "").strip() or None
    nutrition_source_type = "haccp_pass4_nut"
    if source_nutrient_raw and ("processed_food_info" in source_nutrient_raw):
        nutrition_source_type = "processed_food_db"

    payload: dict[str, Any] = {
        "product": {
            "name_ko": product_name,
            "reportnum": (str(row["item_mnftr_rpt_no"]).strip() if row["item_mnftr_rpt_no"] else None),
        },
        "source": {
            "url": source_url,
            "product": {
                "name": {
                    "source_type": "haccp_api",
                    "source_field": "haccp_product_info.prdlstNm",
                    "url": source_url,
                },
                "reportnum": {
                    "source_type": "haccp_api",
                    "source_field": "haccp_product_info.prdlstReportNo",
                    "url": source_url,
                },
            },
            "ingredients": {
                "source_type": "haccp_pass4_ing",
                "source_field": "haccp_parsed_cache.ingredients_items_json",
                "parser_version": parser_version,
                "raw_text": source_rawmtrl_raw,
                "url": source_url,
            },
            "nutrition": {
                "source_type": nutrition_source_type,
                "source_field": "haccp_parsed_cache.nutrition_items_json",
                "parser_version": parser_version,
                "raw_text": source_nutrient_raw,
                "url": source_url,
            },
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
    else:
        payload["source"]["nutrition"]["note"] = "nutrition_not_available"

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
              ff.id,
              ff.product_name,
              ff.item_mnftr_rpt_no,
              ff.ingredients_text,
              ff.nutrition_text,
              ff.nutrition_source,
              ff.source_image_url,
              ff.report_no_selected_from,
              ff.all_report_nos_json,
              ff.created_at,
              pf.nutConSrtrQua AS public_basis_text,
              pf.servSize AS public_serv_size,
              pf.foodSize AS public_food_size
            FROM food_final ff
            LEFT JOIN processed_food_info pf
              ON COALESCE(TRIM(pf.itemMnftrRptNo), '') = COALESCE(TRIM(ff.item_mnftr_rpt_no), '')
            WHERE {where_sql}
            ORDER BY ff.id ASC
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
              p.parser_version AS parser_version,
              p.source_rawmtrl AS source_rawmtrl,
              p.source_nutrient AS source_nutrient,
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
            if errors:
                preview = errors[:5]
                for err in preview:
                    product = str(err.get("product") or "-")
                    reportnum = str(err.get("reportnum") or "-")
                    message = str(err.get("error") or "unknown_error")
                    print(
                        f"  [row_error] product={product} reportnum={reportnum} error={message}"
                    )
                if len(errors) > len(preview):
                    print(f"  ... and {len(errors) - len(preview)} more row_errors")
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
