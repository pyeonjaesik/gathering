"""
URL 기반 원재료 분석기.

- 이미지 URL을 직접 받아 ChatGPT API(OpenAI)로 분석한다.
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

from app.analyzer.pass1_precheck import run_pass1_precheck
from app.analyzer.pass2_gate import run_pass2_gate
from app.analyzer.pass3_extract import run_pass3_extract
from app.analyzer.pass4_normalize import run_pass4_normalize

RETRYABLE_HTTP_STATUS = {408, 425, 429, 500, 502, 503, 504}
DEFAULT_MODEL = "gpt-4.1-mini"
_PROMPT_PRINTED_ONCE = False
_PROMPT_PRINT_LOCK = threading.Lock()
DEFAULT_PROMPT_PASS2_FILE = Path(__file__).resolve().parent / "prompts" / "analyze_pass2_prompt.txt"
DEFAULT_PROMPT_PASS2A_FILE = Path(__file__).resolve().parent / "prompts" / "analyze_pass2a_prompt.txt"
DEFAULT_PROMPT_PASS2B_FILE = Path(__file__).resolve().parent / "prompts" / "analyze_pass2b_prompt.txt"
DEFAULT_PROMPT_PASS3_FILE = Path(__file__).resolve().parent / "prompts" / "analyze_pass3_prompt.txt"
DEFAULT_PROMPT_PASS3_INGREDIENTS_FILE = Path(__file__).resolve().parent / "prompts" / "analyze_pass3_ingredients_prompt.txt"
DEFAULT_PROMPT_PASS3_NUTRITION_FILE = Path(__file__).resolve().parent / "prompts" / "analyze_pass3_nutrition_prompt.txt"
DEFAULT_PROMPT_PASS4_FILE = Path(__file__).resolve().parent / "prompts" / "analyze_pass4_prompt.txt"
DEFAULT_PROMPT_PASS4_INGREDIENTS_FILE = Path(__file__).resolve().parent / "prompts" / "analyze_pass4_ingredients_prompt.txt"
DEFAULT_PROMPT_PASS4_NUTRITION_FILE = Path(__file__).resolve().parent / "prompts" / "analyze_pass4_nutrition_prompt.txt"


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
    # 모델이 JSON 앞/뒤에 설명문을 붙이거나 JSON 여러 개를 연달아 내는 경우가 있어
    # 첫 번째 JSON 객체만 raw_decode로 안전하게 추출한다.
    decoder = json.JSONDecoder()
    idx = 0
    length = len(cleaned)
    while idx < length:
        start = cleaned.find("{", idx)
        if start < 0:
            break
        try:
            parsed, _end = decoder.raw_decode(cleaned, start)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            idx = start + 1
            continue
        idx = start + 1
    raise ValueError("JSON object not found in model response")


def _extract_openai_text(payload: dict[str, Any]) -> str:
    """OpenAI chat.completions 응답에서 텍스트를 추출한다."""
    choices = payload.get("choices") or []
    if not choices:
        return ""
    msg = (choices[0] or {}).get("message") or {}
    content = msg.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: list[str] = []
        for part in content:
            if isinstance(part, dict):
                text = part.get("text")
                if text:
                    chunks.append(str(text))
        return "\n".join(chunks).strip()
    return ""


def _extract_gemini_text(payload: dict[str, Any]) -> str:
    """Gemini generateContent 응답에서 텍스트를 추출한다."""
    candidates = payload.get("candidates") or []
    if not candidates:
        return ""
    content = (candidates[0] or {}).get("content") or {}
    parts = content.get("parts") or []
    chunks: list[str] = []
    for part in parts:
        if isinstance(part, dict):
            text = part.get("text")
            if text:
                chunks.append(str(text))
    return "\n".join(chunks).strip()


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
    prompt_file_pass2: str | None = None
    prompt_file_pass2a: str | None = None
    prompt_file_pass2b: str | None = None
    prompt_file_pass3: str | None = None
    prompt_file_pass3_ingredients: str | None = None
    prompt_file_pass3_nutrition: str | None = None
    prompt_file_pass4: str | None = None
    prompt_file_pass4_ingredients: str | None = None
    prompt_file_pass4_nutrition: str | None = None
    pass2a_provider: str = "openai"
    pass2a_openai_model: str = "gpt-4o-mini"
    pass2a_gemini_model: str = "gemini-2.0-flash"
    pass2a_gemini_api_key: str | None = None
    pass3_provider: str = "gemini"
    pass3_gemini_model: str = "gemini-2.0-flash"
    pass3_gemini_api_key: str | None = None
    pass3_retry_on_429_max_attempts: int = 6
    pass3_retry_on_429_base_sec: float = 2.0
    pass3_retry_on_429_max_sec: float = 30.0

    def __post_init__(self) -> None:
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
        self.prompt_template_pass2, self.prompt_template_pass2_path = self._load_prompt_template(
            candidate=self.prompt_file_pass2 or os.getenv("ANALYZE_PROMPT_FILE_PASS2") or os.getenv("ANALYZE_PROMPT_FILE_PASS1"),
            default_path=DEFAULT_PROMPT_PASS2_FILE,
        )
        self.prompt_template_pass2a, self.prompt_template_pass2a_path = self._load_prompt_template(
            candidate=(
                self.prompt_file_pass2a
                or os.getenv("ANALYZE_PROMPT_FILE_PASS2A")
                or self.prompt_file_pass2
                or os.getenv("ANALYZE_PROMPT_FILE_PASS2")
                or os.getenv("ANALYZE_PROMPT_FILE_PASS1")
            ),
            default_path=DEFAULT_PROMPT_PASS2A_FILE if DEFAULT_PROMPT_PASS2A_FILE.exists() else DEFAULT_PROMPT_PASS2_FILE,
        )
        self.prompt_template_pass2b, self.prompt_template_pass2b_path = self._load_prompt_template(
            candidate=(
                self.prompt_file_pass2b
                or os.getenv("ANALYZE_PROMPT_FILE_PASS2B")
                or self.prompt_file_pass2
                or os.getenv("ANALYZE_PROMPT_FILE_PASS2")
                or os.getenv("ANALYZE_PROMPT_FILE_PASS1")
            ),
            default_path=DEFAULT_PROMPT_PASS2B_FILE if DEFAULT_PROMPT_PASS2B_FILE.exists() else DEFAULT_PROMPT_PASS2_FILE,
        )
        self.prompt_template_pass3_ingredients, self.prompt_template_pass3_ingredients_path = self._load_prompt_template(
            candidate=(
                self.prompt_file_pass3_ingredients
                or os.getenv("ANALYZE_PROMPT_FILE_PASS3_INGREDIENTS")
                or self.prompt_file_pass3
                or os.getenv("ANALYZE_PROMPT_FILE_PASS3")
                or os.getenv("ANALYZE_PROMPT_FILE_PASS2")
            ),
            default_path=DEFAULT_PROMPT_PASS3_INGREDIENTS_FILE if DEFAULT_PROMPT_PASS3_INGREDIENTS_FILE.exists() else DEFAULT_PROMPT_PASS3_FILE,
        )
        self.prompt_template_pass3_nutrition, self.prompt_template_pass3_nutrition_path = self._load_prompt_template(
            candidate=(
                self.prompt_file_pass3_nutrition
                or os.getenv("ANALYZE_PROMPT_FILE_PASS3_NUTRITION")
                or self.prompt_file_pass3
                or os.getenv("ANALYZE_PROMPT_FILE_PASS3")
                or os.getenv("ANALYZE_PROMPT_FILE_PASS2")
            ),
            default_path=DEFAULT_PROMPT_PASS3_NUTRITION_FILE if DEFAULT_PROMPT_PASS3_NUTRITION_FILE.exists() else DEFAULT_PROMPT_PASS3_FILE,
        )
        # Backward compatibility: if split prompt is not set, fallback to legacy pass4 file.
        self.prompt_template_pass4_ingredients, self.prompt_template_pass4_ingredients_path = self._load_prompt_template(
            candidate=(
                self.prompt_file_pass4_ingredients
                or os.getenv("ANALYZE_PROMPT_FILE_PASS4_INGREDIENTS")
                or self.prompt_file_pass4
                or os.getenv("ANALYZE_PROMPT_FILE_PASS4")
            ),
            default_path=DEFAULT_PROMPT_PASS4_INGREDIENTS_FILE if DEFAULT_PROMPT_PASS4_INGREDIENTS_FILE.exists() else DEFAULT_PROMPT_PASS4_FILE,
        )
        self.prompt_template_pass4_nutrition, self.prompt_template_pass4_nutrition_path = self._load_prompt_template(
            candidate=(
                self.prompt_file_pass4_nutrition
                or os.getenv("ANALYZE_PROMPT_FILE_PASS4_NUTRITION")
                or self.prompt_file_pass4
                or os.getenv("ANALYZE_PROMPT_FILE_PASS4")
            ),
            default_path=DEFAULT_PROMPT_PASS4_NUTRITION_FILE if DEFAULT_PROMPT_PASS4_NUTRITION_FILE.exists() else DEFAULT_PROMPT_PASS4_FILE,
        )
        self.pass3_provider = str(self.pass3_provider or "gemini").strip().lower()
        self.pass2a_provider = str(self.pass2a_provider or "openai").strip().lower()
        self.pass2a_openai_model = (
            str(
                self.pass2a_openai_model
                or os.getenv("PASS2A_OPENAI_MODEL")
                or "gpt-4o-mini"
            )
            .strip()
        )
        self.pass2a_gemini_api_key = (
            self.pass2a_gemini_api_key
            or os.getenv("PASS2A_GEMINI_API_KEY")
            or os.getenv("GEMINI_API_KEY")
            or os.getenv("GOOGLE_API_KEY")
        )
        self.pass3_gemini_api_key = (
            self.pass3_gemini_api_key
            or os.getenv("GEMINI_API_KEY")
            or os.getenv("GOOGLE_API_KEY")
        )

    def _load_prompt_template(
        self,
        candidate: str | None,
        default_path: Path,
    ) -> tuple[str, str]:
        path = Path(candidate) if candidate else default_path
        if not path.exists():
            raise FileNotFoundError(f"프롬프트 파일이 없습니다: {path}")
        try:
            return (path.read_text(encoding="utf-8"), str(path))
        except Exception as exc:  # pylint: disable=broad-except
            raise RuntimeError(f"프롬프트 파일을 읽을 수 없습니다: {path} ({exc})") from exc

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

    def _split_allergen_notice(self, text: str | None) -> tuple[str | None, str | None]:
        """원재료 문자열에서 알레르기 안내 문구를 분리한다.

        Returns:
            (clean_ingredients_text, allergen_text)
        """
        value = (text or "").strip()
        if not value:
            return (None, None)

        original = value

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
        marker_removed: str | None = None
        if cut_idx >= 0:
            marker_removed = value[cut_idx:].strip(" ,.;:/")
            value = value[:cut_idx].rstrip(" ,.;:/")

        # 끝에 붙는 "..., 대두, 우유 함유/포함" 형태만 제거한다.
        # 주의: 구분자(, /) 없이 원재료 단어에 붙은 경우(예: 효소스테비아대두...)는 자르지 않는다.
        allergen_words = (
            "메밀|밀|대두|호두|땅콩|잣|계란|난류|우유|토마토|새우|게|오징어|"
            "고등어|조개류|굴|전복|홍합|복숭아|돼지고기|쇠고기|닭고기|아황산류"
        )
        tail_pattern = rf"(?:^|[,/]\s*)(?:{allergen_words})(?:\s*[,/]\s*(?:{allergen_words}))*\s*(?:함유|포함)\s*$"
        m = re.search(tail_pattern, value)
        tail_removed: str | None = None
        if m:
            tail_removed = value[m.start():].strip(" ,.;:/")
            value = value[: m.start()].rstrip(" ,.;:/")

        # 보수적 추가 정리: "함유/포함" 토큰은 '알레르기 재료명만'으로 구성된 경우에만 제거.
        parts = re.split(r"[,/]", value)
        kept: list[str] = []
        removed_tokens: list[str] = []
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
                    removed_tokens.append(token)
                    continue
            kept.append(token)

        cleaned = ", ".join(kept).strip(" ,")
        allergen_bits = [x for x in (marker_removed, tail_removed, ", ".join(removed_tokens).strip(" ,")) if x]
        allergen_text = " | ".join(allergen_bits).strip(" |")
        if not allergen_text and cleaned != original:
            diff = original.replace(cleaned, "").strip(" ,.;:/")
            allergen_text = diff or None
        return (cleaned or None, allergen_text or None)

    def _remove_allergen_notice(self, text: str | None) -> str | None:
        """원재료명 뒤에 붙는 알레르기 안내를 제거하되, 실제 원재료는 최대한 보존한다."""
        cleaned, _ = self._split_allergen_notice(text)
        return cleaned

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
        return "application/octet-stream"

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

    def _build_prompt_pass2(self, target_item_rpt_no: str | None) -> str:
        # backward-compatible alias: pass2a 사용
        return self._build_prompt_pass2a(target_item_rpt_no=target_item_rpt_no)

    def _build_prompt_pass2a(self, target_item_rpt_no: str | None) -> str:
        target_value = target_item_rpt_no if target_item_rpt_no else "없음"
        return self.prompt_template_pass2a.replace("__TARGET_ITEM_RPT_NO__", str(target_value))

    def _build_prompt_pass2b(self, target_item_rpt_no: str | None) -> str:
        target_value = target_item_rpt_no if target_item_rpt_no else "없음"
        return self.prompt_template_pass2b.replace("__TARGET_ITEM_RPT_NO__", str(target_value))

    def _build_prompt_pass3(self, target_item_rpt_no: str | None) -> str:
        # backward-compatible alias (ingredients track)
        return self._build_prompt_pass3_ingredients(target_item_rpt_no=target_item_rpt_no)

    def _build_prompt_pass3_ingredients(self, target_item_rpt_no: str | None) -> str:
        target_value = target_item_rpt_no if target_item_rpt_no else "없음"
        return self.prompt_template_pass3_ingredients.replace("__TARGET_ITEM_RPT_NO__", str(target_value))

    def _build_prompt_pass3_nutrition(self, target_item_rpt_no: str | None) -> str:
        target_value = target_item_rpt_no if target_item_rpt_no else "없음"
        return self.prompt_template_pass3_nutrition.replace("__TARGET_ITEM_RPT_NO__", str(target_value))

    def _build_prompt_pass4_ingredients(
        self,
        ingredients_text: str,
    ) -> str:
        prompt = self.prompt_template_pass4_ingredients.replace("__INGREDIENTS_TEXT__", str(ingredients_text or ""))
        return prompt

    def _build_prompt_pass4_nutrition(
        self,
        nutrition_text: str | None = None,
    ) -> str:
        prompt = self.prompt_template_pass4_nutrition.replace("__NUTRITION_TEXT__", str(nutrition_text or "null"))
        return prompt

    def _call_model(
        self,
        image_bytes: bytes,
        mime_type: str,
        prompt: str,
    ) -> tuple[str, dict[str, Any], str]:
        return self._call_model_openai(
            image_bytes=image_bytes,
            mime_type=mime_type,
            prompt=prompt,
            model_name=self.model,
        )

    def _call_model_openai(
        self,
        image_bytes: bytes,
        mime_type: str,
        prompt: str,
        *,
        model_name: str | None = None,
    ) -> tuple[str, dict[str, Any], str]:
        data_url = f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('ascii')}"
        model = str(model_name or self.model).strip()
        body = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        resp = self.session.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            data=json.dumps(body, ensure_ascii=False),
            timeout=self.request_timeout_sec,
        )
        raw_api_response = resp.text
        if resp.status_code >= 400:
            raise RuntimeError(f"openai_http_{resp.status_code}: {raw_api_response[:1200]}")
        payload = resp.json()
        raw_text = _extract_openai_text(payload)
        if not raw_text:
            raise RuntimeError(f"empty_model_response: id={payload.get('id')} finish={((payload.get('choices') or [{}])[0] or {}).get('finish_reason')}")
        parsed = _extract_first_json_object(raw_text)
        return raw_text, parsed, raw_api_response

    def _call_model_gemini(
        self,
        image_bytes: bytes,
        mime_type: str,
        prompt: str,
        *,
        model_name: str | None = None,
        api_key_value: str | None = None,
    ) -> tuple[str, dict[str, Any], str]:
        api_key = (api_key_value or self.pass3_gemini_api_key or "").strip()
        if not api_key:
            raise RuntimeError("gemini_api_key_missing (set GEMINI_API_KEY or GOOGLE_API_KEY)")

        model = str(model_name or self.pass3_gemini_model).strip()
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{model}:generateContent?key={api_key}"
        )
        body = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt},
                        {
                            "inline_data": {
                                "mime_type": mime_type,
                                "data": base64.b64encode(image_bytes).decode("ascii"),
                            }
                        },
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0,
                "responseMimeType": "application/json",
            },
        }
        resp = self.session.post(
            url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(body, ensure_ascii=False),
            timeout=self.request_timeout_sec,
        )
        raw_api_response = resp.text
        if resp.status_code >= 400:
            raise RuntimeError(f"gemini_http_{resp.status_code}: {raw_api_response[:1200]}")
        payload = resp.json()
        raw_text = _extract_gemini_text(payload)
        if not raw_text:
            raise RuntimeError("empty_gemini_response")
        parsed = _extract_first_json_object(raw_text)
        return raw_text, parsed, raw_api_response

    def _call_model_pass3(
        self,
        image_bytes: bytes,
        mime_type: str,
        prompt: str,
    ) -> tuple[str, dict[str, Any], str]:
        if self.pass3_provider == "gemini":
            return self._call_model_gemini(image_bytes=image_bytes, mime_type=mime_type, prompt=prompt)
        return self._call_model(image_bytes=image_bytes, mime_type=mime_type, prompt=prompt)

    def _call_model_pass2a(
        self,
        image_bytes: bytes,
        mime_type: str,
        prompt: str,
    ) -> tuple[str, dict[str, Any], str]:
        if self.pass2a_provider == "gemini":
            return self._call_model_gemini(
                image_bytes=image_bytes,
                mime_type=mime_type,
                prompt=prompt,
                model_name=self.pass2a_gemini_model,
                api_key_value=self.pass2a_gemini_api_key,
            )
        return self._call_model_openai(
            image_bytes=image_bytes,
            mime_type=mime_type,
            prompt=prompt,
            model_name=self.pass2a_openai_model,
        )

    def _call_model_pass2b(
        self,
        image_bytes: bytes,
        mime_type: str,
        prompt: str,
    ) -> tuple[str, dict[str, Any], str]:
        # Pass2B는 기존 OpenAI 경로 유지
        return self._call_model(image_bytes=image_bytes, mime_type=mime_type, prompt=prompt)

    def _pass3_source_model(self) -> str:
        if self.pass3_provider == "gemini":
            return self.pass3_gemini_model
        return self.model

    def _call_text_model_openai(
        self,
        prompt: str,
    ) -> tuple[str, dict[str, Any], str]:
        body = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        resp = self.session.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            data=json.dumps(body, ensure_ascii=False),
            timeout=self.request_timeout_sec,
        )
        raw_api_response = resp.text
        if resp.status_code >= 400:
            raise RuntimeError(f"openai_http_{resp.status_code}: {raw_api_response[:1200]}")
        payload = resp.json()
        raw_text = _extract_openai_text(payload)
        if not raw_text:
            raise RuntimeError(f"empty_model_response: id={payload.get('id')} finish={((payload.get('choices') or [{}])[0] or {}).get('finish_reason')}")
        parsed = _extract_first_json_object(raw_text)
        return raw_text, parsed, raw_api_response

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

    def _print_prompts_once(
        self,
        prompt_pass2: str,
        prompt_pass2b: str | None = None,
        prompt_pass3: str | None = None,
        prompt_pass3_nutrition: str | None = None,
    ) -> None:
        global _PROMPT_PRINTED_ONCE  # pylint: disable=global-statement
        if not self.show_prompt_once:
            return
        with _PROMPT_PRINT_LOCK:
            if _PROMPT_PRINTED_ONCE:
                return
            print("\n" + "=" * 88)
            print("[analyze 프롬프트 1회 출력]")
            print(f"[pass-2a prompt file] {self.prompt_template_pass2a_path}")
            print(prompt_pass2)
            if prompt_pass2b is not None:
                print("-" * 88)
                print(f"[pass-2b prompt file] {self.prompt_template_pass2b_path}")
                print(prompt_pass2b)
            if prompt_pass3 is not None:
                print("-" * 88)
                print(f"[pass-3 ingredients prompt file] {self.prompt_template_pass3_ingredients_path}")
                print(prompt_pass3)
            if prompt_pass3_nutrition is not None:
                print("-" * 88)
                print(f"[pass-3 nutrition prompt file] {self.prompt_template_pass3_nutrition_path}")
                print(prompt_pass3_nutrition)
            print("=" * 88 + "\n")
            _PROMPT_PRINTED_ONCE = True

    def _error_result(self, last_error: Exception | None, last_raw_text: str | None = None) -> dict[str, Any]:
        err_text = (
            f"chatgpt analyze error: {last_error} "
            "(hint: 429/quota, timeout, 또는 일시적 서비스 오류 가능)"
        )
        fallback_raw = (last_raw_text or "").strip() or str(last_error or "")
        return {
            "itemMnftrRptNo": None,
            "ingredients_text": None,
            "allergen_text": None,
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
            "raw_model_text": fallback_raw,
            "raw_model_text_pass1": fallback_raw,
            "raw_model_text_pass2": None,
            "raw_api_response": fallback_raw,
            "source_model": self.model,
        }

    def analyze_pass1_precheck(self, image_url: str) -> dict[str, Any]:
        try:
            image_bytes, mime_type = self._download_image(image_url)
        except Exception as exc:  # pylint: disable=broad-except
            return self._error_result(RuntimeError(f"image_download_failed: {exc}"))
        return self.analyze_pass1_precheck_from_bytes(image_bytes, mime_type, image_url=image_url)

    def analyze_pass1_precheck_from_bytes(
        self,
        image_bytes: bytes,
        mime_type: str,
        image_url: str | None = None,
    ) -> dict[str, Any]:
        return run_pass1_precheck(self, image_bytes, mime_type, image_url=image_url)

    def analyze_pass1(self, image_url: str, target_item_rpt_no: str | None = None) -> dict[str, Any]:
        # Backward compatibility: legacy name now maps to stage-2 (AI 적합성 판정)
        return self.analyze_pass2(image_url=image_url, target_item_rpt_no=target_item_rpt_no)

    def analyze_pass2(self, image_url: str, target_item_rpt_no: str | None = None) -> dict[str, Any]:
        try:
            image_bytes, mime_type = self._download_image(image_url)
        except Exception as exc:  # pylint: disable=broad-except
            return self._error_result(RuntimeError(f"image_download_failed: {exc}"))
        pre = self.analyze_pass1_precheck_from_bytes(image_bytes, mime_type, image_url=image_url)
        if not pre.get("precheck_pass"):
            return pre
        return self.analyze_pass2_from_bytes(image_bytes, mime_type, target_item_rpt_no)

    def analyze_pass2_from_bytes(
        self,
        image_bytes: bytes,
        mime_type: str,
        target_item_rpt_no: str | None = None,
    ) -> dict[str, Any]:
        return run_pass2_gate(self, image_bytes, mime_type, target_item_rpt_no)

    def analyze_pass3(
        self,
        image_url: str,
        target_item_rpt_no: str | None = None,
        include_nutrition: bool | None = None,
    ) -> dict[str, Any]:
        try:
            image_bytes, mime_type = self._download_image(image_url)
        except Exception as exc:  # pylint: disable=broad-except
            return {
                "error": f"image_download_failed: {exc}",
                "raw_model_text": None,
                "raw_model_text_pass3": None,
                "source_model": self.model,
            }
        pre = self.analyze_pass1_precheck_from_bytes(image_bytes, mime_type, image_url=image_url)
        if not pre.get("precheck_pass"):
            return {"error": pre.get("ai_decision_reason") or "precheck_failed"}
        pass2 = self.analyze_pass2_from_bytes(image_bytes, mime_type, target_item_rpt_no)
        if str(pass2.get("ai_decision") or "").upper() != "READ":
            return {"error": pass2.get("ai_decision_reason") or "pass2_skip"}
        qf = pass2.get("quality_flags") or {}
        include_nut = bool(qf.get("has_nutrition_section")) if include_nutrition is None else bool(include_nutrition)
        return self.analyze_pass3_from_bytes(
            image_bytes,
            mime_type,
            target_item_rpt_no,
            include_nutrition=include_nut,
        )

    def analyze_pass3_from_bytes(
        self,
        image_bytes: bytes,
        mime_type: str,
        target_item_rpt_no: str | None = None,
        include_nutrition: bool = True,
    ) -> dict[str, Any]:
        # run_pass3_extract 시그니처를 단순하게 유지하기 위해 인스턴스 임시 플래그 사용
        setattr(self, "_pass3_include_nutrition", bool(include_nutrition))
        return run_pass3_extract(self, image_bytes, mime_type, target_item_rpt_no)

    def analyze_pass4_normalize(
        self,
        pass2_result: dict[str, Any],
        pass3_result: dict[str, Any] | None,
        target_item_rpt_no: str | None = None,
    ) -> dict[str, Any]:
        return run_pass4_normalize(self, pass2_result, pass3_result, target_item_rpt_no)

    # backward-compatible alias
    def normalize_analysis_result(
        self,
        pass1_result: dict[str, Any],
        pass2_result: dict[str, Any] | None,
        target_item_rpt_no: str | None = None,
    ) -> dict[str, Any]:
        return self.analyze_pass4_normalize(pass1_result, pass2_result, target_item_rpt_no)

    def analyze(self, image_url: str, target_item_rpt_no: str | None = None) -> dict[str, Any]:
        try:
            image_bytes, mime_type = self._download_image(image_url)
        except Exception as exc:  # pylint: disable=broad-except
            return self._error_result(RuntimeError(f"image_download_failed: {exc}"))
        # Stage-1: 포맷/무결성 사전검증 (AI 호출 없음)
        pass1_result = self.analyze_pass1_precheck_from_bytes(image_bytes, mime_type, image_url=image_url)
        if not pass1_result.get("precheck_pass"):
            return pass1_result
        # Stage-2: 적합성 판정
        pass2_result = self.analyze_pass2_from_bytes(image_bytes, mime_type, target_item_rpt_no)
        # Stage-3: 상세 추출
        pass3_result = None
        if str(pass2_result.get("ai_decision") or "").upper() == "READ":
            qf = pass2_result.get("quality_flags") or {}
            include_nut = bool(qf.get("has_nutrition_section"))
            pass3_result = self.analyze_pass3_from_bytes(
                image_bytes,
                mime_type,
                target_item_rpt_no,
                include_nutrition=include_nut,
            )
        # Stage-4: 정규화
        return self.analyze_pass4_normalize(pass2_result, pass3_result, target_item_rpt_no)
