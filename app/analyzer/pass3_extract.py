from __future__ import annotations

import random
import time
from typing import Any


def run_pass3_extract(analyzer: Any, image_bytes: bytes, mime_type: str, target_item_rpt_no: str | None = None) -> dict[str, Any]:
    prompt_pass3 = analyzer._build_prompt_pass3(target_item_rpt_no=target_item_rpt_no)
    analyzer._print_prompts_once(analyzer._build_prompt_pass2(target_item_rpt_no), prompt_pass3)

    last_error: Exception | None = None
    last_raw_text: str | None = None
    max_attempts = max(1, int(getattr(analyzer, "model_retries", 0)) + 1)
    attempt = 0
    while attempt < max_attempts:
        try:
            raw_text_pass3, parsed_pass3, raw_api_response = analyzer._call_model_pass3(
                image_bytes=image_bytes,
                mime_type=mime_type,
                prompt=prompt_pass3,
            )
            last_raw_text = raw_text_pass3
            return {
                "note": parsed_pass3.get("reason") or "ai(pass3)",
                "product_report_number": parsed_pass3.get("product_report_number"),
                "ingredients_text": parsed_pass3.get("ingredients_text"),
                "allergen_text": parsed_pass3.get("allergen_text"),
                "nutrition_text": parsed_pass3.get("nutrition_text"),
                "product_name_in_image": parsed_pass3.get("product_name_in_image"),
                "full_text": parsed_pass3.get("full_text"),
                "has_report_label": bool(parsed_pass3.get("has_report_label")),
                "ingredients_complete": bool(parsed_pass3.get("ingredients_complete")),
                "report_number_complete": bool(parsed_pass3.get("report_number_complete")),
                "product_name_complete": bool(parsed_pass3.get("product_name_complete")),
                "nutrition_complete": bool(parsed_pass3.get("nutrition_complete")),
                "raw_model_text": raw_text_pass3,
                "raw_model_text_pass3": raw_text_pass3,
                "raw_api_response": raw_api_response,
                "source_model": analyzer._pass3_source_model(),
            }
        except Exception as exc:
            last_error = exc
            msg = str(exc).lower()
            is_429 = ("429" in msg) or ("resource_exhausted" in msg)
            retryable = any(k in msg for k in ("429", "resource_exhausted", "503", "deadline", "timeout", "temporarily unavailable"))

            # Gemini 429는 짧은 재시도로는 회복이 잘 안 되므로 시도 횟수/대기시간을 확장한다.
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
                    # 2,4,8,16... + 지터
                    delay = min(cap, base * (2 ** attempt))
                    delay += random.uniform(0.0, 0.8)
                else:
                    delay = analyzer.retry_backoff_sec * (attempt + 1)
                    delay *= 2.5
                time.sleep(delay)
                attempt += 1
                continue
            break
    return {
        "error": str(last_error) if last_error else "unknown",
        "raw_model_text": last_raw_text,
        "raw_model_text_pass3": last_raw_text,
        "source_model": analyzer._pass3_source_model(),
    }
