from __future__ import annotations

import random
import time
from typing import Any


def _call_pass3_with_retry(
    analyzer: Any,
    image_bytes: bytes,
    mime_type: str,
    prompt: str,
) -> tuple[str, dict[str, Any], str]:
    last_error: Exception | None = None
    max_attempts = max(1, int(getattr(analyzer, "model_retries", 0)) + 1)
    attempt = 0
    while attempt < max_attempts:
        try:
            return analyzer._call_model_pass3(
                image_bytes=image_bytes,
                mime_type=mime_type,
                prompt=prompt,
            )
        except Exception as exc:
            last_error = exc
            msg = str(exc).lower()
            is_429 = ("429" in msg) or ("resource_exhausted" in msg)
            retryable = any(k in msg for k in ("429", "resource_exhausted", "503", "deadline", "timeout", "temporarily unavailable"))

            if is_429:
                max_429_attempts = max(
                    max_attempts,
                    int(getattr(analyzer, "pass3_retry_on_429_max_attempts", 6)),
                )
                if max_429_attempts > max_attempts:
                    max_attempts = max_429_attempts

            if attempt < (max_attempts - 1) and retryable:
                if is_429:
                    base = float(getattr(analyzer, "pass3_retry_on_429_base_sec", 2.0))
                    cap = float(getattr(analyzer, "pass3_retry_on_429_max_sec", 30.0))
                    delay = min(cap, base * (2 ** attempt))
                    delay += random.uniform(0.0, 0.8)
                else:
                    delay = analyzer.retry_backoff_sec * (attempt + 1)
                    delay *= 2.5
                time.sleep(delay)
                attempt += 1
                continue
            raise RuntimeError(str(last_error) if last_error else "unknown") from exc
    raise RuntimeError(str(last_error) if last_error else "unknown")


def run_pass3_extract(analyzer: Any, image_bytes: bytes, mime_type: str, target_item_rpt_no: str | None = None) -> dict[str, Any]:
    prompt_pass3_ing = analyzer._build_prompt_pass3_ingredients(target_item_rpt_no=target_item_rpt_no)
    include_nutrition = bool(getattr(analyzer, "_pass3_include_nutrition", True))
    prompt_pass3_nut = analyzer._build_prompt_pass3_nutrition(target_item_rpt_no=target_item_rpt_no) if include_nutrition else None
    analyzer._print_prompts_once(
        analyzer._build_prompt_pass2a(target_item_rpt_no),
        prompt_pass2b=analyzer._build_prompt_pass2b(target_item_rpt_no),
        prompt_pass3=prompt_pass3_ing,
        prompt_pass3_nutrition=(prompt_pass3_nut if include_nutrition else None),
    )

    last_raw_text: str | None = None
    try:
        raw_ing, parsed_ing, raw_api_ing = _call_pass3_with_retry(
            analyzer=analyzer,
            image_bytes=image_bytes,
            mime_type=mime_type,
            prompt=prompt_pass3_ing,
        )
        last_raw_text = raw_ing
        raw_nut: str | None = None
        parsed_nut: dict[str, Any] = {}
        raw_api_nut: str | None = None
        if include_nutrition and prompt_pass3_nut is not None:
            raw_nut, parsed_nut, raw_api_nut = _call_pass3_with_retry(
                analyzer=analyzer,
                image_bytes=image_bytes,
                mime_type=mime_type,
                prompt=prompt_pass3_nut,
            )
        parts_text = ["[PASS3-INGREDIENTS]\n" + (raw_ing or "")]
        if include_nutrition:
            parts_text.append("[PASS3-NUTRITION]\n" + (raw_nut or ""))
        else:
            parts_text.append("[PASS3-NUTRITION]\n(skipped_by_pass2_no_nutrition)")
        raw_model_text_pass3 = "\n\n".join(parts_text).strip()
        parts_api = ["[PASS3-INGREDIENTS]\n" + (raw_api_ing or "")]
        if include_nutrition:
            parts_api.append("[PASS3-NUTRITION]\n" + (raw_api_nut or ""))
        else:
            parts_api.append("[PASS3-NUTRITION]\n(skipped_by_pass2_no_nutrition)")
        raw_api_response = "\n\n".join(parts_api).strip()
        return {
            "note": parsed_ing.get("reason") or parsed_nut.get("reason") or "ai(pass3)",
            "product_report_number": parsed_ing.get("product_report_number"),
            "ingredients_text": parsed_ing.get("ingredients_text"),
            "allergen_text": parsed_ing.get("allergen_text"),
            "nutrition_text": (parsed_nut.get("nutrition_text") if include_nutrition else None),
            "product_name_in_image": parsed_ing.get("product_name_in_image"),
            "full_text": parsed_ing.get("full_text") or parsed_nut.get("full_text"),
            "has_report_label": bool(parsed_ing.get("has_report_label")),
            "ingredients_complete": bool(parsed_ing.get("ingredients_complete")),
            "report_number_complete": bool(parsed_ing.get("report_number_complete")),
            "product_name_complete": bool(parsed_ing.get("product_name_complete")),
            "nutrition_complete": (bool(parsed_nut.get("nutrition_complete")) if include_nutrition else False),
            "raw_model_text": raw_model_text_pass3,
            "raw_model_text_pass3": raw_model_text_pass3,
            "raw_model_text_pass3_ingredients": raw_ing,
            "raw_model_text_pass3_nutrition": (raw_nut if include_nutrition else None),
            "raw_api_response": raw_api_response,
            "raw_api_response_pass3_ingredients": raw_api_ing,
            "raw_api_response_pass3_nutrition": (raw_api_nut if include_nutrition else None),
            "source_model": analyzer._pass3_source_model(),
        }
    except Exception as exc:
        return {
            "error": str(exc) if exc else "unknown",
            "raw_model_text": last_raw_text,
            "raw_model_text_pass3": last_raw_text,
            "source_model": analyzer._pass3_source_model(),
        }
