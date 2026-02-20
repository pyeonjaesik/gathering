"""
URL 기반 원재료 분석기.

- 이미지 URL을 직접 받아 Gemini로 분석한다.
- 네트워크/모델 실패 시 재시도한다.
- ingredient_enricher에서 기대하는 형태로 결과를 반환한다.
"""

from __future__ import annotations

import json
import base64
import mimetypes
import re
import time
import threading
import os
from dataclasses import dataclass
from typing import Any
from pathlib import Path
from urllib.parse import urlparse

import requests
from google import genai
from google.genai import types

RETRYABLE_HTTP_STATUS = {408, 425, 429, 500, 502, 503, 504}
DEFAULT_MODEL = "gemini-2.0-flash"
_PROMPT_PRINTED_ONCE = False
_PROMPT_PRINT_LOCK = threading.Lock()
DEFAULT_PROMPT_PASS1_FILE = Path(__file__).resolve().parent.parent / "prompts" / "analyze_pass1_prompt.txt"
DEFAULT_PROMPT_PASS2_FILE = Path(__file__).resolve().parent.parent / "prompts" / "analyze_pass2_prompt.txt"


def _strip_code_fence(text: str) -> str:
    value = (text or "").strip()
    if value.startswith("```"):
        value = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", value)
        value = re.sub(r"\n?```$", "", value)
    return value.strip()


def _extract_first_json_object(text: str) -> dict[str, Any]:
    cleaned = _strip_code_fence(text)
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if not match:
        raise ValueError("JSON object not found in model response")
    parsed = json.loads(match.group(0))
    if not isinstance(parsed, dict):
        raise ValueError("JSON response is not an object")
    return parsed


def _normalize_report_no(value: Any, min_digits: int = 8, max_digits: int = 20) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if len(digits) < min_digits or len(digits) > max_digits:
        return None
    return digits


def _extract_report_no_from_text(
    text: str | None,
    min_digits: int = 8,
    max_digits: int = 20,
) -> str | None:
    if not text:
        return None
    match = re.search(rf"\b\d{{{min_digits},{max_digits}}}\b", text)
    return match.group(0) if match else None


@dataclass
class URLIngredientAnalyzer:
    api_key: str
    model: str = DEFAULT_MODEL
    request_timeout_sec: int = 60
    download_timeout_sec: int = 20
    download_retries: int = 2
    model_retries: int = 3
    retry_backoff_sec: float = 0.8
    max_image_bytes: int = 8 * 1024 * 1024
    strict_mode: bool = True
    min_ingredients_len: int = 10
    min_report_digits: int = 10
    max_report_digits: int = 16
    show_prompt_once: bool = True
    prompt_file_pass1: str | None = None
    prompt_file_pass2: str | None = None

    def __post_init__(self) -> None:
        self.client = genai.Client(api_key=self.api_key)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )
        self.prompt_template_pass1, self.prompt_template_pass1_path = self._load_prompt_template(
            candidate=self.prompt_file_pass1 or os.getenv("ANALYZE_PROMPT_FILE_PASS1"),
            default_path=DEFAULT_PROMPT_PASS1_FILE,
            fallback=self._default_prompt_template_pass1(),
        )
        self.prompt_template_pass2, self.prompt_template_pass2_path = self._load_prompt_template(
            candidate=self.prompt_file_pass2 or os.getenv("ANALYZE_PROMPT_FILE_PASS2"),
            default_path=DEFAULT_PROMPT_PASS2_FILE,
            fallback=self._default_prompt_template_pass2(),
        )

    def _load_prompt_template(
        self,
        candidate: str | None,
        default_path: Path,
        fallback: str,
    ) -> tuple[str, str]:
        path = Path(candidate) if candidate else default_path
        try:
            if path.exists():
                return (path.read_text(encoding="utf-8"), str(path))
        except Exception:  # pylint: disable=broad-except
            pass
        return (fallback, str(path))

    def _default_prompt_template_pass1(self) -> str:
        return """이 이미지를 판독 대상으로 사용할지 판단하세요.

타깃 품목제조보고번호: __TARGET_ITEM_RPT_NO__

판정 기준:
- image_suitability=적합: 원재료명 이미지로 판독/추출 가능한 상태
- image_suitability=부적합: 흐림, 잘림, 왜곡, 정보 누락, 다중 제품 혼재 등으로 추출 대상에서 제외

응답 JSON 스키마:
{
  "image_suitability": "적합 또는 부적합",
  "decision_confidence": 0~100 정수,
  "decision_reason": "적합/부적합 근거(핵심 사유)",
  "is_real_world_photo": true/false,
  "is_blurry_or_lowres": true/false,
  "is_wrinkled_or_distorted": true/false,
  "is_cropped_or_partial": true/false,
  "quality_score": 0~100 정수,
  "quality_fail_reasons": ["사유1", "사유2", ...],
  "reason": "간단한 판정 근거"
}"""

    def _default_prompt_template_pass2(self) -> str:
        return """이 이미지를 OCR 기반으로 상세 추출해주세요.

타깃 품목제조보고번호: __TARGET_ITEM_RPT_NO__

목표:
- 품목보고번호(product_report_number)
- 원재료명(ingredients_text)
- 제품명(product_name_in_image)
- 영양성분표 텍스트(nutrition_text)

규칙:
- 이미지 내 텍스트를 최대한 읽고 full_text에 담으세요.
- 품목보고번호 라벨이 보이고 번호가 읽히면 product_report_number에 넣으세요.
- 번호를 못 읽으면 null.
- 원재료명/원재료/INGREDIENTS 영역 텍스트를 ingredients_text에 넣으세요.
- 제품명을 읽으면 product_name_in_image에 넣으세요.
- 영양성분표를 읽으면 nutrition_text에 넣으세요.
- 각 항목을 일부만 읽었거나 잘렸다면 *_complete를 false로 두세요.
- JSON 외 텍스트를 출력하지 마세요.

응답 JSON 스키마:
{
  "product_report_number": "문자열 또는 null",
  "ingredients_text": "문자열 또는 null",
  "product_name_in_image": "문자열 또는 null",
  "nutrition_text": "문자열 또는 null",
  "ingredients_complete": true/false,
  "report_number_complete": true/false,
  "product_name_complete": true/false,
  "nutrition_complete": true/false,
  "has_report_label": true/false,
  "full_text": "문자열 또는 null",
  "reason": "간단한 추출 근거"
}"""

    def _looks_like_ingredients_text(self, text: str | None) -> bool:
        value = (text or "").strip()
        if len(value) < self.min_ingredients_len:
            return False
        # 원재료 열거 특성(구분자/키워드) 기반 최소 검증
        has_separator = any(sep in value for sep in (",", "·", "/", "，"))
        has_keyword = any(
            kw in value.lower()
            for kw in ("원재료", "ingredients", "함량")
        )
        return has_separator or has_keyword

    def _looks_like_nutrition_text(self, text: str | None) -> bool:
        value = (text or "").strip().lower()
        if len(value) < 8:
            return False
        keywords = ("영양성분", "영양정보", "나트륨", "탄수화물", "단백질", "지방", "calories", "nutrition")
        return any(kw in value for kw in keywords)

    def _remove_allergen_notice(self, text: str | None) -> str | None:
        """원재료명 뒤에 붙는 알레르기 안내를 제거하되, 실제 원재료는 최대한 보존한다."""
        value = (text or "").strip()
        if not value:
            return None

        # 문장형 알레르기 안내 키워드가 있을 때만 이후를 잘라낸다.
        cut_markers = [
            "알레르기",
            "알레르기 유발",
            "알레르기유발",
            "알레르겐",
            "이 제품은",
            "본 제품은",
            "같은 제조시설",
            "교차오염",
            "함유되어",
            "함유되어있",
        ]
        lower = value.lower()
        cut_idx = -1
        for marker in cut_markers:
            idx = lower.find(marker.lower())
            if idx >= 0:
                cut_idx = idx if cut_idx < 0 else min(cut_idx, idx)
        if cut_idx >= 0:
            value = value[:cut_idx].rstrip(" ,.;:/")

        # 끝에 붙는 "..., 대두, 우유 함유/포함" 형태만 제거한다.
        # 주의: 구분자(, /) 없이 원재료 단어에 붙은 경우(예: 효소스테비아대두...)는 자르지 않는다.
        allergen_words = (
            "메밀|밀|대두|호두|땅콩|잣|계란|난류|우유|토마토|새우|게|오징어|"
            "고등어|조개류|굴|전복|홍합|복숭아|돼지고기|쇠고기|닭고기|아황산류"
        )
        tail_pattern = rf"(?:^|[,/]\s*)(?:{allergen_words})(?:\s*[,/]\s*(?:{allergen_words}))*\s*(?:함유|포함)\s*$"
        m = re.search(tail_pattern, value)
        if m:
            value = value[: m.start()].rstrip(" ,.;:/")

        # 보수적 추가 정리: "함유/포함" 토큰은 '알레르기 재료명만'으로 구성된 경우에만 제거.
        parts = re.split(r"[,/]", value)
        kept: list[str] = []
        allergen_set = {
            "메밀", "밀", "대두", "호두", "땅콩", "잣", "계란", "난류", "우유", "토마토", "새우",
            "게", "오징어", "고등어", "조개류", "굴", "전복", "홍합", "복숭아", "돼지고기", "쇠고기",
            "닭고기", "아황산류",
        }
        for part in parts:
            token = part.strip()
            if not token:
                continue
            t = token.replace(" ", "")
            if ("함유" in t or "포함" in t):
                core = re.sub(r"(함유|포함)", "", t)
                # 숫자/비율/괄호가 있으면 실제 원재료일 가능성이 커서 보존
                if any(ch in t for ch in ("%","(",")")) or re.search(r"\d", t):
                    kept.append(token)
                    continue
                # 알레르기 재료명만으로 구성된 경우에만 제거
                core_parts = [x for x in re.split(r"[·,\s]+", core) if x]
                if core_parts and all(x in allergen_set for x in core_parts):
                    continue
            kept.append(token)

        cleaned = ", ".join(kept).strip(" ,")
        return cleaned or None

    def _guess_mime_type(
        self,
        image_url: str,
        content_type: str | None,
        image_bytes: bytes,
    ) -> str:
        if content_type:
            guessed = content_type.split(";")[0].strip().lower()
            if guessed.startswith("image/"):
                return guessed

        parsed = urlparse(image_url)
        ext_type = mimetypes.guess_type(parsed.path)[0]
        if ext_type and ext_type.startswith("image/"):
            return ext_type

        if image_bytes.startswith(b"\x89PNG"):
            return "image/png"
        if image_bytes.startswith(b"\xff\xd8"):
            return "image/jpeg"
        if image_bytes.startswith(b"GIF87a") or image_bytes.startswith(b"GIF89a"):
            return "image/gif"
        if image_bytes.startswith(b"RIFF") and b"WEBP" in image_bytes[:32]:
            return "image/webp"
        return "image/jpeg"

    def _download_image(self, image_url: str) -> tuple[bytes, str]:
        if image_url.startswith("data:image/"):
            return self._decode_data_url(image_url)

        last_error: Exception | None = None
        for attempt in range(self.download_retries + 1):
            try:
                response = self.session.get(
                    image_url,
                    timeout=self.download_timeout_sec,
                    allow_redirects=True,
                    stream=True,
                )
                status = response.status_code
                if status >= 400:
                    if status in RETRYABLE_HTTP_STATUS and attempt < self.download_retries:
                        time.sleep(self.retry_backoff_sec * (attempt + 1))
                        continue
                    raise RuntimeError(f"image download failed: http={status}")

                chunks: list[bytes] = []
                total_size = 0
                for chunk in response.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    total_size += len(chunk)
                    if total_size > self.max_image_bytes:
                        raise RuntimeError(
                            f"image too large: {total_size} bytes > {self.max_image_bytes}"
                        )
                    chunks.append(chunk)
                image_bytes = b"".join(chunks)
                if not image_bytes:
                    raise RuntimeError("empty image bytes")
                mime_type = self._guess_mime_type(
                    image_url=image_url,
                    content_type=response.headers.get("Content-Type"),
                    image_bytes=image_bytes,
                )
                return image_bytes, mime_type
            except Exception as exc:  # pylint: disable=broad-except
                last_error = exc
                if attempt < self.download_retries:
                    time.sleep(self.retry_backoff_sec * (attempt + 1))
                    continue
                break
        raise RuntimeError(f"image download error: {last_error}")

    def _decode_data_url(self, image_url: str) -> tuple[bytes, str]:
        """data:image/...;base64,... 포맷을 디코딩한다."""
        try:
            header, encoded = image_url.split(",", 1)
        except ValueError as exc:
            raise RuntimeError("invalid data URL format") from exc

        m = re.match(r"^data:(image/[a-zA-Z0-9.+-]+);base64$", header.strip())
        if not m:
            raise RuntimeError("unsupported data URL header")
        mime_type = m.group(1).lower()

        try:
            image_bytes = base64.b64decode(encoded, validate=True)
        except Exception as exc:  # pylint: disable=broad-except
            raise RuntimeError(f"invalid base64 image data: {exc}") from exc

        if not image_bytes:
            raise RuntimeError("empty image bytes from data URL")
        if len(image_bytes) > self.max_image_bytes:
            raise RuntimeError(
                f"image too large: {len(image_bytes)} bytes > {self.max_image_bytes}"
            )
        return image_bytes, mime_type

    def _build_prompt_pass1(self, target_item_rpt_no: str | None) -> str:
        target_value = target_item_rpt_no if target_item_rpt_no else "없음"
        return self.prompt_template_pass1.replace("__TARGET_ITEM_RPT_NO__", str(target_value))

    def _build_prompt_pass2(self, target_item_rpt_no: str | None) -> str:
        target_value = target_item_rpt_no if target_item_rpt_no else "없음"
        return self.prompt_template_pass2.replace("__TARGET_ITEM_RPT_NO__", str(target_value))

    def _call_model(
        self,
        image_bytes: bytes,
        mime_type: str,
        prompt: str,
    ) -> tuple[str, dict[str, Any]]:
        response = self.client.models.generate_content(
            model=self.model,
            contents=[
                types.Part.from_bytes(data=image_bytes, mime_type=mime_type),
                prompt,
            ],
            config=types.GenerateContentConfig(
                http_options=types.HttpOptions(timeout=self.request_timeout_sec * 1000)
            ),
        )
        raw_text = (response.text or "").strip()
        parsed = _extract_first_json_object(raw_text)
        return raw_text, parsed

    def _resolve_report_no(
        self,
        model_report_no: Any,
        full_text: str | None,
        target_item_rpt_no: str | None,
    ) -> str | None:
        target_norm = _normalize_report_no(
            target_item_rpt_no,
            min_digits=self.min_report_digits,
            max_digits=24,
        )
        report_no = _normalize_report_no(
            model_report_no,
            min_digits=self.min_report_digits,
            max_digits=24,
        )

        # OCR이 숫자를 붙여 읽은 경우 target 부분 일치면 target으로 보정.
        if report_no and target_norm and report_no != target_norm and target_norm in report_no:
            return target_norm

        if not report_no:
            report_no = _extract_report_no_from_text(
                full_text,
                min_digits=self.min_report_digits,
                max_digits=self.max_report_digits,
            )
            report_no = _normalize_report_no(
                report_no,
                min_digits=self.min_report_digits,
                max_digits=self.max_report_digits,
            )

        if report_no and target_norm and report_no != target_norm and target_norm in report_no:
            return target_norm

        return _normalize_report_no(
            report_no,
            min_digits=self.min_report_digits,
            max_digits=self.max_report_digits,
        )

    def analyze(self, image_url: str, target_item_rpt_no: str | None = None) -> dict[str, Any]:
        global _PROMPT_PRINTED_ONCE  # pylint: disable=global-statement
        image_bytes, mime_type = self._download_image(image_url)
        prompt_pass1 = self._build_prompt_pass1(target_item_rpt_no=target_item_rpt_no)
        prompt_pass2 = self._build_prompt_pass2(target_item_rpt_no=target_item_rpt_no)
        if self.show_prompt_once:
            with _PROMPT_PRINT_LOCK:
                if not _PROMPT_PRINTED_ONCE:
                    print("\n" + "=" * 88)
                    print("[analyze 프롬프트 1회 출력]")
                    print(f"[pass-1 prompt file] {self.prompt_template_pass1_path}")
                    print(prompt_pass1)
                    print("-" * 88)
                    print(f"[pass-2 prompt file] {self.prompt_template_pass2_path}")
                    print(prompt_pass2)
                    print("=" * 88 + "\n")
                    _PROMPT_PRINTED_ONCE = True

        last_error: Exception | None = None
        last_raw_text: str | None = None
        for attempt in range(self.model_retries + 1):
            try:
                raw_text_pass1, parsed_pass1 = self._call_model(
                    image_bytes=image_bytes,
                    mime_type=mime_type,
                    prompt=prompt_pass1,
                )
                last_raw_text = raw_text_pass1

                suitability_raw = str(parsed_pass1.get("image_suitability") or "").strip()
                decision_raw = str(parsed_pass1.get("decision") or "").strip().upper()
                if suitability_raw in ("적합", "부적합"):
                    decision_raw = "READ" if suitability_raw == "적합" else "SKIP"
                elif decision_raw in ("READ", "SKIP"):
                    suitability_raw = "적합" if decision_raw == "READ" else "부적합"
                else:
                    decision_raw = "SKIP"
                    suitability_raw = "부적합"

                decision_conf_raw = parsed_pass1.get("decision_confidence")
                try:
                    decision_conf = int(decision_conf_raw)
                except Exception:  # pylint: disable=broad-except
                    decision_conf = 0
                decision_conf = max(0, min(100, decision_conf))

                decision_reason = str(parsed_pass1.get("decision_reason") or "").strip()
                if not decision_reason:
                    decision_reason = str(parsed_pass1.get("reason") or "").strip()

                is_real_world_photo = bool(parsed_pass1.get("is_real_world_photo"))
                is_blurry = bool(parsed_pass1.get("is_blurry_or_lowres"))
                is_wrinkled = bool(parsed_pass1.get("is_wrinkled_or_distorted"))
                is_cropped = bool(parsed_pass1.get("is_cropped_or_partial"))
                quality_score_raw = parsed_pass1.get("quality_score")
                try:
                    quality_score = int(quality_score_raw)
                except Exception:  # pylint: disable=broad-except
                    quality_score = 0
                quality_score = max(0, min(100, quality_score))
                quality_fail_reasons = parsed_pass1.get("quality_fail_reasons")
                if not isinstance(quality_fail_reasons, list):
                    quality_fail_reasons = []
                quality_fail_reasons = [str(x).strip() for x in quality_fail_reasons if str(x).strip()]
                if decision_raw != "READ":
                    if decision_reason:
                        quality_fail_reasons.append(f"ai_skip:{decision_reason}")
                    else:
                        quality_fail_reasons.append("ai_skip")
                    return {
                        "itemMnftrRptNo": None,
                        "ingredients_text": None,
                        "nutrition_text": None,
                        "note": parsed_pass1.get("reason") or "gemini(pass1_skip)",
                        "is_flat": None,
                        "is_table_format": False,
                        "has_rect_ingredient_box": False,
                        "has_report_label": False,
                        "is_designed_graphic": None,
                        "has_real_world_objects": None,
                        "brand": None,
                        "product_name_in_image": None,
                        "manufacturer": None,
                        "full_text": parsed_pass1.get("full_text"),
                        "has_ingredients": False,
                        "quality_gate_pass": False,
                        "quality_score": quality_score,
                        "quality_fail_reasons": quality_fail_reasons,
                        "quality_flags": {
                            "is_real_world_photo": is_real_world_photo,
                            "is_blurry_or_lowres": is_blurry,
                            "is_wrinkled_or_distorted": is_wrinkled,
                            "is_cropped_or_partial": is_cropped,
                            "ingredients_complete": False,
                            "report_number_complete": False,
                            "product_name_complete": False,
                            "nutrition_complete": False,
                        },
                        "ai_decision": "SKIP",
                        "ai_suitability": suitability_raw,
                        "ai_decision_confidence": decision_conf,
                        "ai_decision_reason": decision_reason or "pass1_skip",
                        "raw_model_text": raw_text_pass1,
                        "raw_model_text_pass1": raw_text_pass1,
                        "raw_model_text_pass2": None,
                        "source_model": self.model,
                    }

                raw_text_pass2, parsed_pass2 = self._call_model(
                    image_bytes=image_bytes,
                    mime_type=mime_type,
                    prompt=prompt_pass2,
                )
                last_raw_text = raw_text_pass2

                report_no = self._resolve_report_no(
                    model_report_no=parsed_pass2.get("product_report_number"),
                    full_text=parsed_pass2.get("full_text"),
                    target_item_rpt_no=target_item_rpt_no,
                )

                ingredients_text = parsed_pass2.get("ingredients_text")
                if ingredients_text is not None:
                    ingredients_text = str(ingredients_text).strip() or None
                ingredients_text = self._remove_allergen_notice(ingredients_text)
                product_name = parsed_pass2.get("product_name_in_image")
                if product_name is not None:
                    product_name = str(product_name).strip() or None
                nutrition_text = parsed_pass2.get("nutrition_text")
                if nutrition_text is not None:
                    nutrition_text = str(nutrition_text).strip() or None

                has_report_label = bool(parsed_pass2.get("has_report_label"))
                if not self._looks_like_ingredients_text(ingredients_text):
                    ingredients_text = None
                if not self._looks_like_nutrition_text(nutrition_text):
                    nutrition_text = None

                ingredients_complete = bool(parsed_pass2.get("ingredients_complete"))
                report_complete = bool(parsed_pass2.get("report_number_complete"))
                product_complete = bool(parsed_pass2.get("product_name_complete"))
                nutrition_complete = bool(parsed_pass2.get("nutrition_complete"))

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
                    if decision_reason:
                        decision_reason = f"{decision_reason} | missing_report_number"
                    else:
                        decision_reason = "missing_report_number"
                    quality_fail_reasons.append("missing_report_number")

                if self.strict_mode and not quality_gate_pass:
                    report_no = None
                    ingredients_text = None
                    product_name = None
                    nutrition_text = None
                    report_complete = False
                    ingredients_complete = False
                    product_complete = False
                    nutrition_complete = False

                note_text = parsed_pass2.get("reason") or "gemini(pass2)"
                return {
                    "itemMnftrRptNo": report_no,
                    "ingredients_text": ingredients_text,
                    "nutrition_text": nutrition_text,
                    "note": note_text,
                    "is_flat": None,
                    "is_table_format": False,
                    "has_rect_ingredient_box": False,
                    "has_report_label": has_report_label,
                    "is_designed_graphic": None,
                    "has_real_world_objects": None,
                    "brand": None,
                    "product_name_in_image": product_name,
                    "manufacturer": None,
                    "full_text": parsed_pass2.get("full_text"),
                    "has_ingredients": bool(ingredients_text),
                    "quality_gate_pass": quality_gate_pass,
                    "quality_score": quality_score,
                    "quality_fail_reasons": quality_fail_reasons,
                    "quality_flags": {
                        "is_real_world_photo": is_real_world_photo,
                        "is_blurry_or_lowres": is_blurry,
                        "is_wrinkled_or_distorted": is_wrinkled,
                        "is_cropped_or_partial": is_cropped,
                        "ingredients_complete": ingredients_complete,
                        "report_number_complete": report_complete,
                        "product_name_complete": product_complete,
                        "nutrition_complete": nutrition_complete,
                    },
                    "ai_decision": "READ" if quality_gate_pass else "SKIP",
                    "ai_suitability": suitability_raw,
                    "ai_decision_confidence": decision_conf,
                    "ai_decision_reason": decision_reason,
                    "raw_model_text": raw_text_pass2,
                    "raw_model_text_pass1": raw_text_pass1,
                    "raw_model_text_pass2": raw_text_pass2,
                    "source_model": self.model,
                }
            except Exception as exc:  # pylint: disable=broad-except
                last_error = exc
                msg = str(exc).lower()
                retryable = (
                    ("429" in msg)
                    or ("resource_exhausted" in msg)
                    or ("503" in msg)
                    or ("deadline" in msg)
                    or ("timeout" in msg)
                    or ("temporarily unavailable" in msg)
                )
                if attempt < self.model_retries:
                    delay = self.retry_backoff_sec * (attempt + 1)
                    if retryable:
                        delay *= 2.5
                    time.sleep(delay)
                    continue
                break

        # 최종 실패 시 예외를 다시 던지지 않고, SKIP 결과를 반환한다.
        err_text = (
            f"gemini analyze error: {last_error} "
            "(hint: 429/quota, timeout, 또는 일시적 서비스 오류 가능)"
        )
        return {
            "itemMnftrRptNo": None,
            "ingredients_text": None,
            "nutrition_text": None,
            "note": err_text,
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
            "quality_fail_reasons": [f"runtime_error:{type(last_error).__name__ if last_error else 'unknown'}"],
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
            "ai_decision_confidence": 0,
            "ai_decision_reason": err_text,
            "raw_model_text": last_raw_text,
            "source_model": self.model,
        }
