"""
URL 기반 원재료 분석기.

- 이미지 URL을 직접 받아 Gemini로 분석한다.
- 네트워크/모델 실패 시 재시도한다.
- ingredient_enricher에서 기대하는 형태로 결과를 반환한다.
"""

from __future__ import annotations

import json
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


def _normalize_report_no(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if len(digits) < 8:
        return None
    return digits


def _extract_report_no_from_text(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"\b\d{8,}\b", text)
    return match.group(0) if match else None


@dataclass
class URLIngredientAnalyzer:
    api_key: str
    model: str = DEFAULT_MODEL
    request_timeout_sec: int = 60
    download_timeout_sec: int = 20
    download_retries: int = 2
    model_retries: int = 1
    retry_backoff_sec: float = 0.8
    max_image_bytes: int = 8 * 1024 * 1024

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

    def _build_prompt(self, target_item_rpt_no: str | None) -> str:
        target_line = (
            f"타깃 품목제조보고번호: {target_item_rpt_no}"
            if target_item_rpt_no
            else "타깃 품목제조보고번호: 없음"
        )
        return f"""이미지에서 식품 라벨을 읽고 다음 항목을 JSON으로만 반환하세요.

{target_line}

규칙:
1) 원재료명/원재료/INGREDIENTS 영역만 ingredients_text로 추출 (영양성분표 제외)
2) 품목보고번호/품목제조보고번호가 보이면 product_report_number에 넣기
3) 숫자 확신이 없으면 추정하지 말고 null
4) 결과 외의 텍스트를 출력하지 말 것

응답 JSON 스키마:
{{
  "product_report_number": "문자열 또는 null",
  "ingredients_text": "문자열 또는 null",
  "product_name_in_image": "문자열 또는 null",
  "manufacturer": "문자열 또는 null",
  "full_text": "문자열 또는 null",
  "has_ingredients": true/false,
  "reason": "간단한 판단 근거"
}}"""

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

                report_no = _normalize_report_no(parsed.get("product_report_number"))
                if not report_no:
                    report_no = _extract_report_no_from_text(parsed.get("full_text"))
                    report_no = _normalize_report_no(report_no)

                ingredients_text = parsed.get("ingredients_text")
                if ingredients_text is not None:
                    ingredients_text = str(ingredients_text).strip() or None

                return {
                    "itemMnftrRptNo": report_no,
                    "ingredients_text": ingredients_text,
                    "note": parsed.get("reason") or "gemini",
                    "product_name_in_image": parsed.get("product_name_in_image"),
                    "manufacturer": parsed.get("manufacturer"),
                    "full_text": parsed.get("full_text"),
                    "has_ingredients": bool(parsed.get("has_ingredients")),
                    "source_model": self.model,
                }
            except Exception as exc:  # pylint: disable=broad-except
                last_error = exc
                if attempt < self.model_retries:
                    time.sleep(self.retry_backoff_sec * (attempt + 1))
                    continue
                break

        raise RuntimeError(f"gemini analyze error: {last_error}")
