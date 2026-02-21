from __future__ import annotations
import re
from typing import Any


_PLACEHOLDER_KEYWORDS = (
    "예시", "샘플", "dummy", "test", "placeholder",
    "aaaa", "xxxx", "yyy", "zzz",
)


def _placeholder_reason(text: str | None, field: str) -> str | None:
    value = str(text or "").strip()
    if not value:
        return f"{field}_missing"

    lower = value.lower()
    if any(k in lower for k in _PLACEHOLDER_KEYWORDS):
        return f"{field}_contains_placeholder_keyword"

    compact = re.sub(r"\s+", "", value)
    if not compact:
        return f"{field}_empty"

    # ○●□■△▲▽▼, *, ^, X 등 마스킹/더미 기호 과다
    mask_chars = re.findall(r"[○●□■△▲▽▼◇◆*^xX]", compact)
    if mask_chars and (len(mask_chars) / max(1, len(compact))) >= 0.20:
        return f"{field}_masked_text"

    # 동일문자 반복 / 기호 반복
    if re.search(r"(.)\1{3,}", compact):
        return f"{field}_repeated_chars"
    if re.search(r"[\^*#=_\-]{3,}", compact):
        return f"{field}_repeated_symbols"

    # 의미 토큰 부족
    word_tokens = re.findall(r"[A-Za-z가-힣]+", value)
    unique_tokens = {w.lower() for w in word_tokens}
    if field in ("product_name", "ingredients"):
        if len(word_tokens) == 0:
            return f"{field}_no_meaningful_tokens"
        if len(unique_tokens) <= 1 and len(compact) >= 8:
            return f"{field}_low_token_diversity"

    # 기호 비율 과다
    symbol_cnt = len(re.findall(r"[^A-Za-z가-힣0-9\s]", value))
    if (symbol_cnt / max(1, len(value))) >= 0.45:
        return f"{field}_high_symbol_ratio"

    return None


def _normalize_sub_ingredient_node(node: Any) -> dict[str, Any] | None:
    if not isinstance(node, dict):
        return None
    # 하위 노드는 name 또는 ingredient_name 둘 다 허용
    raw_name = node.get("name")
    if raw_name is None:
        raw_name = node.get("ingredient_name")
    name = str(raw_name).strip() if raw_name else None
    origin = str(node.get("origin")).strip() if node.get("origin") else None
    origin_detail = str(node.get("origin_detail")).strip() if node.get("origin_detail") else None
    amount = str(node.get("amount")).strip() if node.get("amount") else None
    children: list[dict[str, Any]] = []
    raw_children = node.get("sub_ingredients") or []
    if isinstance(raw_children, list):
        for child in raw_children:
            n = _normalize_sub_ingredient_node(child)
            if n is not None:
                children.append(n)
    return {
        "name": name,
        "origin": origin,
        "origin_detail": origin_detail,
        "amount": amount,
        "sub_ingredients": children,
    }


def run_pass4_normalize(
    analyzer: Any,
    pass2_result: dict[str, Any],
    pass3_result: dict[str, Any] | None,
    target_item_rpt_no: str | None = None,
) -> dict[str, Any]:
    decision_raw = str(pass2_result.get("ai_decision") or "SKIP").upper()
    suitability_raw = str(pass2_result.get("ai_suitability") or ("적합" if decision_raw == "READ" else "부적합")).strip()
    decision_reason = str(pass2_result.get("ai_decision_reason") or "").strip()
    quality_score = int(pass2_result.get("quality_score") or 0)
    quality_fail_reasons = list(pass2_result.get("quality_fail_reasons") or [])
    pass2_flags = pass2_result.get("quality_flags") or {}

    if decision_raw != "READ":
        return {
            "itemMnftrRptNo": None,
            "ingredients_text": None,
            "allergen_text": None,
            "nutrition_text": None,
            "note": pass2_result.get("note") or "chatgpt(pass2_skip)",
            "is_flat": None,
            "is_table_format": False,
            "has_rect_ingredient_box": False,
            "has_report_label": False,
            "is_designed_graphic": None,
            "has_real_world_objects": None,
            "brand": None,
            "product_name_in_image": None,
            "manufacturer": None,
            "full_text": None,
            "has_ingredients": False,
            "quality_gate_pass": False,
            "quality_score": quality_score,
            "quality_fail_reasons": quality_fail_reasons,
            "quality_flags": {
                "is_real_world_photo": pass2_flags.get("is_real_world_photo"),
                "is_blurry_or_lowres": pass2_flags.get("is_blurry_or_lowres"),
                "is_wrinkled_or_distorted": pass2_flags.get("is_wrinkled_or_distorted"),
                "is_cropped_or_partial": pass2_flags.get("is_cropped_or_partial"),
                "ingredients_complete": False,
                "report_number_complete": False,
                "product_name_complete": False,
                "nutrition_complete": False,
            },
            "ai_decision": "SKIP",
            "ai_suitability": suitability_raw,
            "ai_decision_confidence": int(pass2_result.get("ai_decision_confidence") or 0),
            "ai_decision_reason": decision_reason or "pass2_skip",
            "raw_model_text": pass2_result.get("raw_model_text"),
            "raw_model_text_pass2": pass2_result.get("raw_model_text_pass2"),
            "raw_model_text_pass3": None,
            "source_model": analyzer.model,
            "ingredient_items": [],
            "ingredient_items_reason": "pass2_skip",
            "nutrition_items": [],
            "report_number_validation": {
                "is_valid": False,
                "normalized_report_number": None,
                "reason": "pass2_skip",
            },
            "raw_model_text_pass4": None,
        }

    if not pass3_result or pass3_result.get("error"):
        err = pass3_result.get("error") if pass3_result else "pass3_missing"
        if err:
            quality_fail_reasons.append(f"pass3_error:{err}")
        return {
            **analyzer._error_result(RuntimeError(err), pass3_result.get("raw_model_text") if pass3_result else None),
            "raw_model_text_pass2": pass2_result.get("raw_model_text_pass2"),
            "raw_model_text_pass3": pass3_result.get("raw_model_text_pass3") if pass3_result else None,
            "ingredient_items": [],
            "ingredient_items_reason": "pass3_error",
            "nutrition_items": [],
            "report_number_validation": {
                "is_valid": False,
                "normalized_report_number": None,
                "reason": f"pass3_error:{err}",
            },
            "raw_model_text_pass4": None,
        }

    report_no = analyzer._resolve_report_no(
        model_report_no=pass3_result.get("product_report_number"),
        full_text=pass3_result.get("full_text"),
        target_item_rpt_no=target_item_rpt_no,
    )
    ingredients_text = pass3_result.get("ingredients_text")
    if ingredients_text is not None:
        ingredients_text = str(ingredients_text).strip() or None
    allergen_text = pass3_result.get("allergen_text")
    if allergen_text is not None:
        allergen_text = str(allergen_text).strip() or None
    ingredients_text, extracted_allergen_text = analyzer._split_allergen_notice(ingredients_text)
    if not allergen_text:
        allergen_text = extracted_allergen_text
    elif extracted_allergen_text and extracted_allergen_text not in allergen_text:
        allergen_text = f"{allergen_text} | {extracted_allergen_text}"
    product_name = pass3_result.get("product_name_in_image")
    if product_name is not None:
        product_name = str(product_name).strip() or None
    nutrition_text = pass3_result.get("nutrition_text")
    if nutrition_text is not None:
        nutrition_text = str(nutrition_text).strip() or None

    has_report_label = bool(pass3_result.get("has_report_label"))
    if not analyzer._looks_like_ingredients_text(ingredients_text):
        ingredients_text = None
    if not analyzer._looks_like_nutrition_text(nutrition_text):
        nutrition_text = None

    ingredients_complete = bool(pass3_result.get("ingredients_complete"))
    report_complete = bool(pass3_result.get("report_number_complete"))
    product_complete = bool(pass3_result.get("product_name_complete"))
    nutrition_complete = bool(pass3_result.get("nutrition_complete"))

    if not report_no:
        report_complete = False
    if not ingredients_text:
        ingredients_complete = False
    if not product_name:
        product_complete = False
    if not nutrition_text:
        nutrition_complete = False

    quality_gate_pass = True
    if not report_no:
        quality_gate_pass = False
        suitability_raw = "부적합"
        decision_reason = (decision_reason + " | missing_report_number").strip(" |")
        quality_fail_reasons.append("missing_report_number")

    if analyzer.strict_mode and not quality_gate_pass:
        report_no = None
        ingredients_text = None
        allergen_text = None
        product_name = None
        nutrition_text = None
        report_complete = False
        ingredients_complete = False
        product_complete = False
        nutrition_complete = False

    ingredient_items: list[dict[str, Any]] = []
    nutrition_items: list[dict[str, Any]] = []
    ingredient_items_reason: str | None = None
    normalized_report_no = re.sub(r"[^0-9]", "", str(report_no or ""))
    local_report_valid = bool(normalized_report_no and re.fullmatch(r"\d{10,16}", normalized_report_no))
    if not normalized_report_no:
        report_reason = "missing_report_number"
    elif not re.fullmatch(r"\d{10,16}", normalized_report_no):
        report_reason = "invalid_report_number_format"
    else:
        report_reason = "valid_report_number_format"
    report_number_validation = {
        "is_valid": local_report_valid,
        "normalized_report_number": (normalized_report_no or None),
        "reason": report_reason,
    }

    # 로컬 후처리: 예시/더미/플레이스홀더 텍스트 차단
    product_placeholder = _placeholder_reason(product_name, "product_name")
    ingredients_placeholder = _placeholder_reason(ingredients_text, "ingredients")
    report_placeholder = None
    if normalized_report_no and re.fullmatch(r"(\d)\1{9,15}", normalized_report_no):
        report_placeholder = "report_number_repeated_digits"

    if product_placeholder:
        quality_gate_pass = False
        product_name = None
        product_complete = False
        quality_fail_reasons.append(product_placeholder)
    if ingredients_placeholder:
        quality_gate_pass = False
        ingredients_text = None
        ingredients_complete = False
        quality_fail_reasons.append(ingredients_placeholder)
    if report_placeholder:
        quality_gate_pass = False
        report_no = None
        report_complete = False
        report_number_validation["is_valid"] = False
        report_number_validation["reason"] = report_placeholder
        quality_fail_reasons.append(report_placeholder)

    if (product_placeholder or ingredients_placeholder or report_placeholder):
        suitability_raw = "부적합"
        extras = [x for x in (product_placeholder, ingredients_placeholder, report_placeholder) if x]
        decision_reason = (decision_reason + " | " + ",".join(extras)).strip(" |")
    raw_model_text_pass4_ingredients: str | None = None
    raw_model_text_pass4_nutrition: str | None = None
    raw_api_response_pass4_ingredients: str | None = None
    raw_api_response_pass4_nutrition: str | None = None
    raw_model_text_pass4: str | None = None
    raw_api_response_pass4: str | None = None
    pass4_ai_error: str | None = None
    # 요청사항: 품목번호/원재료명/제품명이 모두 추출된 경우(영양성분 무관) Pass-4 AI 구조화 실행
    if report_no and ingredients_text and product_name:
        try:
            prompt_ing = analyzer._build_prompt_pass4_ingredients(
                ingredients_text=ingredients_text,
            )
            raw_text_ing, parsed_ing, raw_api_ing = analyzer._call_text_model_openai(prompt_ing)
            raw_model_text_pass4_ingredients = raw_text_ing
            raw_api_response_pass4_ingredients = raw_api_ing
            items = parsed_ing.get("ingredients_items") or []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    ingredient_name = item.get("ingredient_name")
                    origin = item.get("origin")
                    origin_detail = item.get("origin_detail")
                    amount = item.get("amount")
                    sub_items: list[dict[str, Any]] = []
                    raw_sub_items = item.get("sub_ingredients") or []
                    if isinstance(raw_sub_items, list):
                        for s in raw_sub_items:
                            normalized = _normalize_sub_ingredient_node(s)
                            if normalized is not None:
                                sub_items.append(normalized)
                    ingredient_items.append(
                        {
                            "ingredient_name": (str(ingredient_name).strip() if ingredient_name else None),
                            "origin": (str(origin).strip() if origin else None),
                            "origin_detail": (str(origin_detail).strip() if origin_detail else None),
                            "amount": (str(amount).strip() if amount else None),
                            "sub_ingredients": sub_items,
                        }
                    )
            ingredient_items_reason = str(parsed_ing.get("reason") or "pass4_ingredients_structured")

            if nutrition_text:
                prompt_nut = analyzer._build_prompt_pass4_nutrition(
                    nutrition_text=nutrition_text,
                )
                raw_text_nut, parsed_nut, raw_api_nut = analyzer._call_text_model_openai(prompt_nut)
                raw_model_text_pass4_nutrition = raw_text_nut
                raw_api_response_pass4_nutrition = raw_api_nut
                n_items = parsed_nut.get("nutrition_items") or []
                if isinstance(n_items, list):
                    for n in n_items:
                        if not isinstance(n, dict):
                            continue
                        nutrition_items.append(
                            {
                                "name": (str(n.get("name")).strip() if n.get("name") else None),
                                "value": (str(n.get("value")).strip() if n.get("value") else None),
                                "unit": (str(n.get("unit")).strip() if n.get("unit") else None),
                                "daily_value": (str(n.get("daily_value")).strip() if n.get("daily_value") else None),
                            }
                        )

            # 호환용 통합 raw
            parts: list[str] = []
            if raw_model_text_pass4_ingredients:
                parts.append("[PASS4-INGREDIENTS]\n" + raw_model_text_pass4_ingredients)
            if raw_model_text_pass4_nutrition:
                parts.append("[PASS4-NUTRITION]\n" + raw_model_text_pass4_nutrition)
            raw_model_text_pass4 = "\n\n".join(parts) if parts else None
            api_parts: list[str] = []
            if raw_api_response_pass4_ingredients:
                api_parts.append("[PASS4-INGREDIENTS]\n" + raw_api_response_pass4_ingredients)
            if raw_api_response_pass4_nutrition:
                api_parts.append("[PASS4-NUTRITION]\n" + raw_api_response_pass4_nutrition)
            raw_api_response_pass4 = "\n\n".join(api_parts) if api_parts else None
        except Exception as exc:  # pylint: disable=broad-except
            pass4_ai_error = str(exc)
            ingredient_items_reason = f"pass4_structuring_failed:{exc}"
    else:
        ingredient_items_reason = "pass4_skipped_missing_required_fields"

    return {
        "itemMnftrRptNo": report_no,
        "ingredients_text": ingredients_text,
        "allergen_text": allergen_text,
        "nutrition_text": nutrition_text,
        "note": pass3_result.get("note") or "chatgpt(pass3)",
        "is_flat": None,
        "is_table_format": False,
        "has_rect_ingredient_box": False,
        "has_report_label": has_report_label,
        "is_designed_graphic": None,
        "has_real_world_objects": None,
        "brand": None,
        "product_name_in_image": product_name,
        "manufacturer": None,
        "full_text": pass3_result.get("full_text"),
        "has_ingredients": bool(ingredients_text),
        "quality_gate_pass": quality_gate_pass,
        "quality_score": quality_score,
        "quality_fail_reasons": quality_fail_reasons,
        "quality_flags": {
            "is_real_world_photo": pass2_flags.get("is_real_world_photo"),
            "is_blurry_or_lowres": pass2_flags.get("is_blurry_or_lowres"),
            "is_wrinkled_or_distorted": pass2_flags.get("is_wrinkled_or_distorted"),
            "is_cropped_or_partial": pass2_flags.get("is_cropped_or_partial"),
            "ingredients_complete": ingredients_complete,
            "report_number_complete": report_complete,
            "product_name_complete": product_complete,
            "nutrition_complete": nutrition_complete,
        },
        "ai_decision": "READ" if quality_gate_pass else "SKIP",
        "ai_suitability": suitability_raw,
        "ai_decision_confidence": int(pass2_result.get("ai_decision_confidence") or 0),
        "ai_decision_reason": decision_reason,
        "raw_model_text": pass3_result.get("raw_model_text"),
        "raw_model_text_pass2": pass2_result.get("raw_model_text_pass2"),
        "raw_model_text_pass3": pass3_result.get("raw_model_text_pass3"),
        "source_model": analyzer.model,
        "ingredient_items": ingredient_items,
        "ingredient_items_reason": ingredient_items_reason,
        "nutrition_items": nutrition_items,
        "report_number_validation": report_number_validation,
        "pass4_ai_error": pass4_ai_error,
        "raw_model_text_pass4": raw_model_text_pass4,
        "raw_api_response_pass4": raw_api_response_pass4,
        "raw_model_text_pass4_ingredients": raw_model_text_pass4_ingredients,
        "raw_model_text_pass4_nutrition": raw_model_text_pass4_nutrition,
        "raw_api_response_pass4_ingredients": raw_api_response_pass4_ingredients,
        "raw_api_response_pass4_nutrition": raw_api_response_pass4_nutrition,
    }
