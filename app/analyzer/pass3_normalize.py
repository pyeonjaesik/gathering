from __future__ import annotations

from typing import Any


def run_pass3_normalize(
    analyzer: Any,
    pass1_result: dict[str, Any],
    pass2_result: dict[str, Any] | None,
    target_item_rpt_no: str | None = None,
) -> dict[str, Any]:
    decision_raw = str(pass1_result.get("ai_decision") or "SKIP").upper()
    suitability_raw = str(pass1_result.get("ai_suitability") or ("적합" if decision_raw == "READ" else "부적합")).strip()
    decision_reason = str(pass1_result.get("ai_decision_reason") or "").strip()
    quality_score = int(pass1_result.get("quality_score") or 0)
    quality_fail_reasons = list(pass1_result.get("quality_fail_reasons") or [])
    pass1_flags = pass1_result.get("quality_flags") or {}

    if decision_raw != "READ":
        return {
            "itemMnftrRptNo": None,
            "ingredients_text": None,
            "nutrition_text": None,
            "note": pass1_result.get("note") or "chatgpt(pass1_skip)",
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
                "is_real_world_photo": pass1_flags.get("is_real_world_photo"),
                "is_blurry_or_lowres": pass1_flags.get("is_blurry_or_lowres"),
                "is_wrinkled_or_distorted": pass1_flags.get("is_wrinkled_or_distorted"),
                "is_cropped_or_partial": pass1_flags.get("is_cropped_or_partial"),
                "ingredients_complete": False,
                "report_number_complete": False,
                "product_name_complete": False,
                "nutrition_complete": False,
            },
            "ai_decision": "SKIP",
            "ai_suitability": suitability_raw,
            "ai_decision_confidence": int(pass1_result.get("ai_decision_confidence") or 0),
            "ai_decision_reason": decision_reason or "pass1_skip",
            "raw_model_text": pass1_result.get("raw_model_text"),
            "raw_model_text_pass1": pass1_result.get("raw_model_text_pass1"),
            "raw_model_text_pass2": None,
            "source_model": analyzer.model,
        }

    if not pass2_result or pass2_result.get("error"):
        err = pass2_result.get("error") if pass2_result else "pass2_missing"
        if err:
            quality_fail_reasons.append(f"pass2_error:{err}")
        return {
            **analyzer._error_result(RuntimeError(err), pass2_result.get("raw_model_text") if pass2_result else None),
            "raw_model_text_pass1": pass1_result.get("raw_model_text_pass1"),
            "raw_model_text_pass2": pass2_result.get("raw_model_text_pass2") if pass2_result else None,
        }

    report_no = analyzer._resolve_report_no(
        model_report_no=pass2_result.get("product_report_number"),
        full_text=pass2_result.get("full_text"),
        target_item_rpt_no=target_item_rpt_no,
    )
    ingredients_text = pass2_result.get("ingredients_text")
    if ingredients_text is not None:
        ingredients_text = str(ingredients_text).strip() or None
    ingredients_text = analyzer._remove_allergen_notice(ingredients_text)
    product_name = pass2_result.get("product_name_in_image")
    if product_name is not None:
        product_name = str(product_name).strip() or None
    nutrition_text = pass2_result.get("nutrition_text")
    if nutrition_text is not None:
        nutrition_text = str(nutrition_text).strip() or None

    has_report_label = bool(pass2_result.get("has_report_label"))
    if not analyzer._looks_like_ingredients_text(ingredients_text):
        ingredients_text = None
    if not analyzer._looks_like_nutrition_text(nutrition_text):
        nutrition_text = None

    ingredients_complete = bool(pass2_result.get("ingredients_complete"))
    report_complete = bool(pass2_result.get("report_number_complete"))
    product_complete = bool(pass2_result.get("product_name_complete"))
    nutrition_complete = bool(pass2_result.get("nutrition_complete"))

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
        product_name = None
        nutrition_text = None
        report_complete = False
        ingredients_complete = False
        product_complete = False
        nutrition_complete = False

    return {
        "itemMnftrRptNo": report_no,
        "ingredients_text": ingredients_text,
        "nutrition_text": nutrition_text,
        "note": pass2_result.get("note") or "chatgpt(pass2)",
        "is_flat": None,
        "is_table_format": False,
        "has_rect_ingredient_box": False,
        "has_report_label": has_report_label,
        "is_designed_graphic": None,
        "has_real_world_objects": None,
        "brand": None,
        "product_name_in_image": product_name,
        "manufacturer": None,
        "full_text": pass2_result.get("full_text"),
        "has_ingredients": bool(ingredients_text),
        "quality_gate_pass": quality_gate_pass,
        "quality_score": quality_score,
        "quality_fail_reasons": quality_fail_reasons,
        "quality_flags": {
            "is_real_world_photo": pass1_flags.get("is_real_world_photo"),
            "is_blurry_or_lowres": pass1_flags.get("is_blurry_or_lowres"),
            "is_wrinkled_or_distorted": pass1_flags.get("is_wrinkled_or_distorted"),
            "is_cropped_or_partial": pass1_flags.get("is_cropped_or_partial"),
            "ingredients_complete": ingredients_complete,
            "report_number_complete": report_complete,
            "product_name_complete": product_complete,
            "nutrition_complete": nutrition_complete,
        },
        "ai_decision": "READ" if quality_gate_pass else "SKIP",
        "ai_suitability": suitability_raw,
        "ai_decision_confidence": int(pass1_result.get("ai_decision_confidence") or 0),
        "ai_decision_reason": decision_reason,
        "raw_model_text": pass2_result.get("raw_model_text"),
        "raw_model_text_pass1": pass1_result.get("raw_model_text_pass1"),
        "raw_model_text_pass2": pass2_result.get("raw_model_text_pass2"),
        "source_model": analyzer.model,
    }
