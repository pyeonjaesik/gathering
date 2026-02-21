from __future__ import annotations

import time
from typing import Any


def _call_pass2_model_with_retry(
    analyzer: Any,
    image_bytes: bytes,
    mime_type: str,
    prompt: str,
    stage: str,
) -> tuple[str, dict[str, Any], str]:
    last_error: Exception | None = None
    last_raw_text: str | None = None
    for attempt in range(analyzer.model_retries + 1):
        try:
            if stage == "2a":
                raw_text, parsed, raw_api_response = analyzer._call_model_pass2a(
                    image_bytes=image_bytes, mime_type=mime_type, prompt=prompt
                )
            else:
                raw_text, parsed, raw_api_response = analyzer._call_model_pass2b(
                    image_bytes=image_bytes, mime_type=mime_type, prompt=prompt
                )
            last_raw_text = raw_text
            return raw_text, parsed, raw_api_response
        except Exception as exc:
            last_error = exc
            msg = str(exc).lower()
            retryable = any(
                k in msg
                for k in ("429", "resource_exhausted", "503", "deadline", "timeout", "temporarily unavailable")
            )
            if attempt < analyzer.model_retries:
                delay = analyzer.retry_backoff_sec * (attempt + 1)
                if retryable:
                    delay *= 2.5
                time.sleep(delay)
                continue
            raise RuntimeError(str(last_error or last_raw_text or "pass2_call_failed")) from exc
    raise RuntimeError(str(last_error or "pass2_call_failed"))


def run_pass2_gate(analyzer: Any, image_bytes: bytes, mime_type: str, target_item_rpt_no: str | None = None) -> dict[str, Any]:
    prompt_pass2a = analyzer._build_prompt_pass2a(target_item_rpt_no=target_item_rpt_no)
    prompt_pass2b = analyzer._build_prompt_pass2b(target_item_rpt_no=target_item_rpt_no)
    analyzer._print_prompts_once(prompt_pass2a, prompt_pass2b=prompt_pass2b)

    last_error: Exception | None = None
    last_raw_text: str | None = None
    try:
        raw_text_pass2a, parsed_pass2a, raw_api_response2a = _call_pass2_model_with_retry(
            analyzer=analyzer,
            image_bytes=image_bytes,
            mime_type=mime_type,
            prompt=prompt_pass2a,
            stage="2a",
        )
        last_raw_text = raw_text_pass2a

        # 2A: 품질/가림/왜곡/단일제품 게이트
        is_clear_text = parsed_pass2a.get("is_clear_text")
        if is_clear_text is None:
            is_clear_text = not bool(parsed_pass2a.get("is_blurry_or_lowres"))
        else:
            is_clear_text = bool(is_clear_text)

        is_full_frame = parsed_pass2a.get("is_full_frame")
        if is_full_frame is None:
            is_full_frame = not bool(parsed_pass2a.get("is_cropped_or_partial"))
        else:
            is_full_frame = bool(is_full_frame)

        is_flat_undistorted = parsed_pass2a.get("is_flat_undistorted")
        if is_flat_undistorted is None:
            is_flat_undistorted = not bool(parsed_pass2a.get("is_wrinkled_or_distorted"))
        else:
            is_flat_undistorted = bool(is_flat_undistorted)

        has_single_product = bool(parsed_pass2a.get("has_single_product"))

        key_fields_fully_visible = parsed_pass2a.get("key_fields_fully_visible")
        if key_fields_fully_visible is None:
            key_fields_fully_visible = bool(is_full_frame)
        else:
            key_fields_fully_visible = bool(key_fields_fully_visible)

        no_glare_on_key_fields = parsed_pass2a.get("no_glare_on_key_fields")
        if no_glare_on_key_fields is None:
            if parsed_pass2a.get("has_glare_on_key_fields") is not None:
                no_glare_on_key_fields = not bool(parsed_pass2a.get("has_glare_on_key_fields"))
            else:
                no_glare_on_key_fields = bool(is_clear_text)
        else:
            no_glare_on_key_fields = bool(no_glare_on_key_fields)

        no_object_occlusion_on_key_fields = parsed_pass2a.get("no_object_occlusion_on_key_fields")
        if no_object_occlusion_on_key_fields is None:
            if parsed_pass2a.get("has_object_occlusion_on_key_fields") is not None:
                no_object_occlusion_on_key_fields = not bool(parsed_pass2a.get("has_object_occlusion_on_key_fields"))
            else:
                no_object_occlusion_on_key_fields = bool(key_fields_fully_visible)
        else:
            no_object_occlusion_on_key_fields = bool(no_object_occlusion_on_key_fields)

        no_any_text_occlusion_on_key_fields = parsed_pass2a.get("no_any_text_occlusion_on_key_fields")
        if no_any_text_occlusion_on_key_fields is None:
            # 누락 시 기존 가림 계열 플래그를 모두 만족해야 true
            no_any_text_occlusion_on_key_fields = bool(
                no_object_occlusion_on_key_fields and key_fields_fully_visible
            )
        else:
            no_any_text_occlusion_on_key_fields = bool(no_any_text_occlusion_on_key_fields)

        no_glare_overlap_on_key_text = parsed_pass2a.get("no_glare_overlap_on_key_text")
        if no_glare_overlap_on_key_text is None:
            if parsed_pass2a.get("glare_overlap_on_key_text") is not None:
                no_glare_overlap_on_key_text = not bool(parsed_pass2a.get("glare_overlap_on_key_text"))
            else:
                no_glare_overlap_on_key_text = bool(no_glare_on_key_fields)
        else:
            no_glare_overlap_on_key_text = bool(no_glare_overlap_on_key_text)

        no_occlusion_overlap_on_key_text = parsed_pass2a.get("no_occlusion_overlap_on_key_text")
        if no_occlusion_overlap_on_key_text is None:
            if parsed_pass2a.get("occlusion_overlap_on_key_text") is not None:
                no_occlusion_overlap_on_key_text = not bool(parsed_pass2a.get("occlusion_overlap_on_key_text"))
            else:
                no_occlusion_overlap_on_key_text = bool(no_object_occlusion_on_key_fields)
        else:
            no_occlusion_overlap_on_key_text = bool(no_occlusion_overlap_on_key_text)

        no_white_circle_overlay_on_key_fields = parsed_pass2a.get("no_white_circle_overlay_on_key_fields")
        if no_white_circle_overlay_on_key_fields is None:
            # 명시 응답이 없으면 기존 가림 판정과 동일하게 보수 추론
            no_white_circle_overlay_on_key_fields = bool(no_object_occlusion_on_key_fields)
        else:
            no_white_circle_overlay_on_key_fields = bool(no_white_circle_overlay_on_key_fields)

        no_wrinkle_fold_occlusion_on_key_fields = parsed_pass2a.get("no_wrinkle_fold_occlusion_on_key_fields")
        if no_wrinkle_fold_occlusion_on_key_fields is None:
            # 명시 응답 누락 시 평면/왜곡 판정 기반으로 보수 추론
            no_wrinkle_fold_occlusion_on_key_fields = bool(is_flat_undistorted and key_fields_fully_visible)
        else:
            no_wrinkle_fold_occlusion_on_key_fields = bool(no_wrinkle_fold_occlusion_on_key_fields)

        fail_checks: list[str] = []
        if not is_clear_text:
            fail_checks.append("not_clear_text")
        if not is_full_frame:
            fail_checks.append("not_full_frame")
        if not is_flat_undistorted:
            fail_checks.append("not_flat_undistorted")
        if not has_single_product:
            fail_checks.append("not_single_product")
        if not key_fields_fully_visible:
            fail_checks.append("key_fields_not_fully_visible")
        if not no_glare_on_key_fields:
            fail_checks.append("glare_on_key_fields")
        if not no_object_occlusion_on_key_fields:
            fail_checks.append("object_occlusion_on_key_fields")
        if not no_any_text_occlusion_on_key_fields:
            fail_checks.append("any_text_occlusion_on_key_fields")
        if not no_glare_overlap_on_key_text:
            fail_checks.append("glare_overlap_on_key_text")
        if not no_occlusion_overlap_on_key_text:
            fail_checks.append("occlusion_overlap_on_key_text")
        if not no_white_circle_overlay_on_key_fields:
            fail_checks.append("white_circle_overlay_on_key_fields")
        if not no_wrinkle_fold_occlusion_on_key_fields:
            fail_checks.append("wrinkle_fold_occlusion_on_key_fields")

        # 2A 실패 시 2B 스킵
        pass2a_ok = len(fail_checks) == 0
        parsed_pass2b: dict[str, Any] = {}
        raw_text_pass2b: str | None = None
        raw_api_response2b: str | None = None
        if pass2a_ok:
            raw_text_pass2b, parsed_pass2b, raw_api_response2b = _call_pass2_model_with_retry(
                analyzer=analyzer,
                image_bytes=image_bytes,
                mime_type=mime_type,
                prompt=prompt_pass2b,
                stage="2b",
            )
        else:
            fail_checks.append("pass2b_skipped_by_pass2a_fail")

        # 2B: 정보 존재 판정
        has_ingredients = bool(parsed_pass2b.get("has_ingredients_section")) if pass2a_ok else False
        has_report_label = bool(parsed_pass2b.get("has_report_number_label")) if pass2a_ok else False
        has_product_name = bool(parsed_pass2b.get("has_product_name")) if pass2a_ok else False
        has_nutrition = bool(parsed_pass2b.get("has_nutrition_section")) if pass2a_ok else False

        if pass2a_ok and not has_ingredients:
            fail_checks.append("missing_ingredients_section")
        if pass2a_ok and not has_report_label:
            fail_checks.append("missing_report_label")
        if pass2a_ok and not has_product_name:
            fail_checks.append("missing_product_name")

        decision_raw = "READ" if not fail_checks else "SKIP"
        suitability_raw = "적합" if decision_raw == "READ" else "부적합"
        total_checks = 15
        passed_checks = total_checks - len([c for c in fail_checks if c != "pass2b_skipped_by_pass2a_fail"])
        quality_score = max(0, min(100, int((passed_checks / total_checks) * 100)))
        decision_conf = 100
        reason_a = str(parsed_pass2a.get("reason") or "").strip()
        reason_b = str(parsed_pass2b.get("reason") or "").strip() if pass2a_ok else "pass2b_skipped_by_pass2a_fail"
        decision_reason = " | ".join([x for x in [reason_a, reason_b] if x]).strip()
        if fail_checks:
            rule_reason = ",".join(fail_checks)
            decision_reason = f"{rule_reason} | {decision_reason}" if decision_reason else rule_reason
        elif not decision_reason:
            decision_reason = "all_checks_passed"

        quality_fail_reasons = fail_checks.copy()
        if decision_raw != "READ":
            quality_fail_reasons.append(f"ai_skip:{decision_reason or 'pass2_skip'}")

        parts_text = ["[PASS2-A]\n" + (raw_text_pass2a or "")]
        if raw_text_pass2b is not None:
            parts_text.append("[PASS2-B]\n" + (raw_text_pass2b or ""))
        else:
            parts_text.append("[PASS2-B]\n(skipped_by_pass2a_fail)")
        raw_model_text_pass2 = "\n\n".join(parts_text).strip()

        parts_api = ["[PASS2-A]\n" + (raw_api_response2a or "")]
        if raw_api_response2b is not None:
            parts_api.append("[PASS2-B]\n" + (raw_api_response2b or ""))
        else:
            parts_api.append("[PASS2-B]\n(skipped_by_pass2a_fail)")
        raw_api_response = "\n\n".join(parts_api).strip()

        return {
            "note": decision_reason or "pass2(2a+2b)",
            "quality_gate_pass": decision_raw == "READ",
            "quality_score": quality_score,
            "quality_fail_reasons": quality_fail_reasons,
            "quality_flags": {
                "is_real_world_photo": bool(parsed_pass2a.get("is_real_world_photo")),
                "is_blurry_or_lowres": (not is_clear_text),
                "is_wrinkled_or_distorted": (not is_flat_undistorted),
                "is_cropped_or_partial": (not is_full_frame),
                "is_clear_text": is_clear_text,
                "is_full_frame": is_full_frame,
                "is_flat_undistorted": is_flat_undistorted,
                "has_ingredients_section": has_ingredients,
                "has_report_number_label": has_report_label,
                "has_product_name": has_product_name,
                "has_single_product": has_single_product,
                "has_nutrition_section": has_nutrition,
                "key_fields_fully_visible": key_fields_fully_visible,
                "no_glare_on_key_fields": no_glare_on_key_fields,
                "no_object_occlusion_on_key_fields": no_object_occlusion_on_key_fields,
                "no_any_text_occlusion_on_key_fields": no_any_text_occlusion_on_key_fields,
                "no_glare_overlap_on_key_text": no_glare_overlap_on_key_text,
                "no_occlusion_overlap_on_key_text": no_occlusion_overlap_on_key_text,
                "no_white_circle_overlay_on_key_fields": no_white_circle_overlay_on_key_fields,
                "no_wrinkle_fold_occlusion_on_key_fields": no_wrinkle_fold_occlusion_on_key_fields,
                "glare_overlap_on_key_text": (not no_glare_overlap_on_key_text),
                "occlusion_overlap_on_key_text": (not no_occlusion_overlap_on_key_text),
                "any_text_occlusion_on_key_fields": (not no_any_text_occlusion_on_key_fields),
                "white_circle_overlay_on_key_fields": (not no_white_circle_overlay_on_key_fields),
                "wrinkle_fold_occlusion_on_key_fields": (not no_wrinkle_fold_occlusion_on_key_fields),
                "pass2a_ok": pass2a_ok,
                "pass2b_executed": pass2a_ok,
            },
            "ai_decision": decision_raw,
            "ai_suitability": suitability_raw,
            "ai_decision_confidence": decision_conf,
            "ai_decision_reason": decision_reason or ("pass2_read" if decision_raw == "READ" else "pass2_skip"),
            "raw_model_text": raw_model_text_pass2,
            "raw_model_text_pass2": raw_model_text_pass2,
            "raw_model_text_pass2a": raw_text_pass2a,
            "raw_model_text_pass2b": raw_text_pass2b,
            "raw_api_response": raw_api_response,
            "raw_api_response_pass2a": raw_api_response2a,
            "raw_api_response_pass2b": raw_api_response2b,
            "source_model_pass2a": (
                analyzer.pass2a_gemini_model
                if str(getattr(analyzer, "pass2a_provider", "")).lower() == "gemini"
                else getattr(analyzer, "pass2a_openai_model", analyzer.model)
            ),
            "source_model_pass2b": analyzer.model,
            "source_model": analyzer.model,
        }
    except Exception as exc:
        last_error = exc
        return analyzer._error_result(last_error, last_raw_text)
