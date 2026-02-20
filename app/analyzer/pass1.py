from __future__ import annotations

import time
from typing import Any


def run_pass1(analyzer: Any, image_bytes: bytes, mime_type: str, target_item_rpt_no: str | None = None) -> dict[str, Any]:
    prompt_pass1 = analyzer._build_prompt_pass1(target_item_rpt_no=target_item_rpt_no)
    analyzer._print_prompts_once(prompt_pass1, None)

    last_error: Exception | None = None
    last_raw_text: str | None = None
    for attempt in range(analyzer.model_retries + 1):
        try:
            raw_text_pass1, parsed_pass1, raw_api_response = analyzer._call_model(
                image_bytes=image_bytes,
                mime_type=mime_type,
                prompt=prompt_pass1,
            )
            last_raw_text = raw_text_pass1

            # v2 prompt: true == good 상태. (구버전 키도 역변환으로 호환)
            is_clear_text = parsed_pass1.get("is_clear_text")
            if is_clear_text is None:
                is_clear_text = not bool(parsed_pass1.get("is_blurry_or_lowres"))
            else:
                is_clear_text = bool(is_clear_text)

            is_full_frame = parsed_pass1.get("is_full_frame")
            if is_full_frame is None:
                is_full_frame = not bool(parsed_pass1.get("is_cropped_or_partial"))
            else:
                is_full_frame = bool(is_full_frame)

            is_flat_undistorted = parsed_pass1.get("is_flat_undistorted")
            if is_flat_undistorted is None:
                is_flat_undistorted = not bool(parsed_pass1.get("is_wrinkled_or_distorted"))
            else:
                is_flat_undistorted = bool(is_flat_undistorted)
            has_ingredients = bool(parsed_pass1.get("has_ingredients_section"))
            has_report_label = bool(parsed_pass1.get("has_report_number_label"))
            has_product_name = bool(parsed_pass1.get("has_product_name"))
            has_single_product = bool(parsed_pass1.get("has_single_product"))
            has_nutrition = bool(parsed_pass1.get("has_nutrition_section"))

            fail_checks: list[str] = []
            if not is_clear_text:
                fail_checks.append("not_clear_text")
            if not is_full_frame:
                fail_checks.append("not_full_frame")
            if not is_flat_undistorted:
                fail_checks.append("not_flat_undistorted")
            if not has_ingredients:
                fail_checks.append("missing_ingredients_section")
            if not has_report_label:
                fail_checks.append("missing_report_label")
            if not has_product_name:
                fail_checks.append("missing_product_name")
            if not has_single_product:
                fail_checks.append("not_single_product")

            decision_raw = "READ" if not fail_checks else "SKIP"
            suitability_raw = "적합" if decision_raw == "READ" else "부적합"
            total_checks = 7
            passed_checks = total_checks - len(fail_checks)
            quality_score = max(0, min(100, int((passed_checks / total_checks) * 100)))
            decision_conf = 100
            decision_reason = str(parsed_pass1.get("reason") or "").strip()
            if fail_checks:
                rule_reason = ",".join(fail_checks)
                decision_reason = f"{rule_reason} | {decision_reason}" if decision_reason else rule_reason
            elif not decision_reason:
                decision_reason = "all_checks_passed"

            quality_fail_reasons = fail_checks.copy()
            if decision_raw != "READ":
                quality_fail_reasons.append(f"ai_skip:{decision_reason or 'pass1_skip'}")

            return {
                "note": parsed_pass1.get("reason") or "chatgpt(pass1)",
                "quality_gate_pass": decision_raw == "READ",
                "quality_score": quality_score,
                "quality_fail_reasons": quality_fail_reasons,
                "quality_flags": {
                    "is_real_world_photo": bool(parsed_pass1.get("is_real_world_photo")),
                    # 하위호환: 기존 음수 키도 함께 제공
                    "is_blurry_or_lowres": (not is_clear_text),
                    "is_wrinkled_or_distorted": (not is_flat_undistorted),
                    "is_cropped_or_partial": (not is_full_frame),
                    # 신규 양수 의미 키
                    "is_clear_text": is_clear_text,
                    "is_full_frame": is_full_frame,
                    "is_flat_undistorted": is_flat_undistorted,
                    "has_ingredients_section": has_ingredients,
                    "has_report_number_label": has_report_label,
                    "has_product_name": has_product_name,
                    "has_single_product": has_single_product,
                    "has_nutrition_section": has_nutrition,
                },
                "ai_decision": decision_raw,
                "ai_suitability": suitability_raw,
                "ai_decision_confidence": decision_conf,
                "ai_decision_reason": decision_reason or ("pass1_read" if decision_raw == "READ" else "pass1_skip"),
                "raw_model_text": raw_text_pass1,
                "raw_model_text_pass1": raw_text_pass1,
                "raw_api_response": raw_api_response,
                "source_model": analyzer.model,
            }
        except Exception as exc:
            last_error = exc
            msg = str(exc).lower()
            retryable = any(k in msg for k in ("429", "resource_exhausted", "503", "deadline", "timeout", "temporarily unavailable"))
            if attempt < analyzer.model_retries:
                delay = analyzer.retry_backoff_sec * (attempt + 1)
                if retryable:
                    delay *= 2.5
                time.sleep(delay)
                continue
            break

    return analyzer._error_result(last_error, last_raw_text)
