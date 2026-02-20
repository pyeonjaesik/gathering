from __future__ import annotations

import time
from typing import Any


def run_pass2(analyzer: Any, image_bytes: bytes, mime_type: str, target_item_rpt_no: str | None = None) -> dict[str, Any]:
    prompt_pass2 = analyzer._build_prompt_pass2(target_item_rpt_no=target_item_rpt_no)
    analyzer._print_prompts_once(analyzer._build_prompt_pass1(target_item_rpt_no), prompt_pass2)

    last_error: Exception | None = None
    last_raw_text: str | None = None
    for attempt in range(analyzer.model_retries + 1):
        try:
            raw_text_pass2, parsed_pass2, raw_api_response = analyzer._call_model(
                image_bytes=image_bytes,
                mime_type=mime_type,
                prompt=prompt_pass2,
            )
            last_raw_text = raw_text_pass2
            return {
                "note": parsed_pass2.get("reason") or "chatgpt(pass2)",
                "product_report_number": parsed_pass2.get("product_report_number"),
                "ingredients_text": parsed_pass2.get("ingredients_text"),
                "nutrition_text": parsed_pass2.get("nutrition_text"),
                "product_name_in_image": parsed_pass2.get("product_name_in_image"),
                "full_text": parsed_pass2.get("full_text"),
                "has_report_label": bool(parsed_pass2.get("has_report_label")),
                "ingredients_complete": bool(parsed_pass2.get("ingredients_complete")),
                "report_number_complete": bool(parsed_pass2.get("report_number_complete")),
                "product_name_complete": bool(parsed_pass2.get("product_name_complete")),
                "nutrition_complete": bool(parsed_pass2.get("nutrition_complete")),
                "raw_model_text": raw_text_pass2,
                "raw_model_text_pass2": raw_text_pass2,
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
    return {
        "error": str(last_error) if last_error else "unknown",
        "raw_model_text": last_raw_text,
        "raw_model_text_pass2": last_raw_text,
        "source_model": analyzer.model,
    }
