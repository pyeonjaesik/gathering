from __future__ import annotations

from typing import Any


SUPPORTED_IMAGE_MIMES = {"image/png", "image/jpeg", "image/gif", "image/webp"}


def _normalize_mime(mime_type: str | None) -> str:
    m = str(mime_type or "").strip().lower()
    if m == "image/jpg":
        return "image/jpeg"
    return m


def _mime_matches_signature(mime_type: str, image_bytes: bytes) -> bool:
    if not image_bytes:
        return False
    if mime_type == "image/png":
        return image_bytes.startswith(b"\x89PNG")
    if mime_type == "image/jpeg":
        return image_bytes.startswith(b"\xff\xd8")
    if mime_type == "image/gif":
        return image_bytes.startswith(b"GIF87a") or image_bytes.startswith(b"GIF89a")
    if mime_type == "image/webp":
        return image_bytes.startswith(b"RIFF") and b"WEBP" in image_bytes[:32]
    return False


def run_pass1_precheck(analyzer: Any, image_bytes: bytes, mime_type: str, image_url: str | None = None) -> dict[str, Any]:
    normalized = _normalize_mime(mime_type)
    reasons: list[str] = []
    ok = True

    if normalized not in SUPPORTED_IMAGE_MIMES:
        ok = False
        reasons.append(f"unsupported_image_format:{normalized or 'unknown'}")

    if not image_bytes:
        ok = False
        reasons.append("empty_image_bytes")

    if ok and not _mime_matches_signature(normalized, image_bytes):
        ok = False
        reasons.append("mime_signature_mismatch")

    if ok:
        return {
            "precheck_pass": True,
            "precheck_reason": "ok",
            "precheck_mime_type": normalized,
            "quality_gate_pass": True,
            "ai_decision": "READ",
            "ai_suitability": "적합",
            "ai_decision_confidence": 100,
            "ai_decision_reason": "precheck_ok",
            "quality_fail_reasons": [],
            "quality_flags": {},
            "raw_model_text": "precheck_ok",
            "raw_api_response": "precheck_ok",
            "source_model": analyzer.model,
        }

    reason = ",".join(reasons) if reasons else "precheck_failed"
    if image_url:
        reason = f"{reason} | url={image_url}"
    return {
        "precheck_pass": False,
        "precheck_reason": reason,
        "precheck_mime_type": normalized,
        "itemMnftrRptNo": None,
        "ingredients_text": None,
        "nutrition_text": None,
        "note": f"precheck_skip: {reason}",
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
        "quality_score": 0,
        "quality_fail_reasons": [f"precheck:{r}" for r in reasons] or ["precheck:failed"],
        "quality_flags": {
            "is_real_world_photo": None,
            "is_blurry_or_lowres": None,
            "is_wrinkled_or_distorted": None,
            "is_cropped_or_partial": None,
            "ingredients_complete": None,
            "report_number_complete": None,
            "product_name_complete": None,
            "nutrition_complete": None,
        },
        "ai_decision": "SKIP",
        "ai_suitability": "부적합",
        "ai_decision_confidence": 100,
        "ai_decision_reason": reason,
        "raw_model_text": reason,
        "raw_model_text_pass1": reason,
        "raw_model_text_pass2": None,
        "raw_api_response": reason,
        "source_model": analyzer.model,
    }
