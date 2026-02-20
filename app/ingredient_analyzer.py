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
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests
from google import genai
from google.genai import types

RETRYABLE_HTTP_STATUS = {408, 425, 429, 500, 502, 503, 504}
DEFAULT_MODEL = "gemini-2.0-flash"


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

    def _build_prompt(self, target_item_rpt_no: str | None) -> str:
        target_line = (
            f"타깃 품목제조보고번호: {target_item_rpt_no}"
            if target_item_rpt_no
            else "타깃 품목제조보고번호: 없음"
        )
        return f"""이 이미지를 OCR 기반으로 분석해주세요.

{target_line}

목표는 2개만 추출하는 것입니다:
1) 품목보고번호 또는 품목제조보고번호 (product_report_number)
2) 원재료명 텍스트 (ingredients_text)

추출 규칙:
- 이미지 내 텍스트를 최대한 읽고 full_text에 담으세요.
- 품목보고번호 라벨이 보이고 번호가 읽히면 product_report_number에 넣으세요.
- 번호를 못 읽으면 null.
- 원재료명/원재료/INGREDIENTS 영역 텍스트를 ingredients_text에 넣으세요.
- 원재료를 못 읽으면 null.
- JSON 외 텍스트를 출력하지 마세요.

응답 JSON 스키마:
{{
  "product_report_number": "문자열 또는 null",
  "ingredients_text": "문자열 또는 null",
  "has_report_label": true/false,
  "full_text": "문자열 또는 null",
  "reason": "간단한 추출 근거"
}}"""

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
        image_bytes, mime_type = self._download_image(image_url)
        prompt = self._build_prompt(target_item_rpt_no=target_item_rpt_no)

        last_error: Exception | None = None
        for attempt in range(self.model_retries + 1):
            try:
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

                report_no = self._resolve_report_no(
                    model_report_no=parsed.get("product_report_number"),
                    full_text=parsed.get("full_text"),
                    target_item_rpt_no=target_item_rpt_no,
                )

                ingredients_text = parsed.get("ingredients_text")
                if ingredients_text is not None:
                    ingredients_text = str(ingredients_text).strip() or None

                has_report_label = bool(parsed.get("has_report_label"))
                # 추출 중심 모드: 너무 짧은 텍스트만 최소한으로 제거.
                if not self._looks_like_ingredients_text(ingredients_text):
                    ingredients_text = None

                note_text = parsed.get("reason") or "gemini"

                return {
                    "itemMnftrRptNo": report_no,
                    "ingredients_text": ingredients_text,
                    "note": note_text,
                    "is_flat": None,
                    "is_table_format": False,
                    "has_rect_ingredient_box": False,
                    "has_report_label": has_report_label,
                    "is_designed_graphic": None,
                    "has_real_world_objects": None,
                    "brand": None,
                    "product_name_in_image": None,
                    "manufacturer": None,
                    "full_text": parsed.get("full_text"),
                    "has_ingredients": bool(ingredients_text),
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

        raise RuntimeError(
            f"gemini analyze error: {last_error} "
            "(hint: 429/quota, timeout, 또는 일시적 서비스 오류 가능)"
        )
