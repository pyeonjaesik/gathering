"""
검색어 기반 이미지 벤치마크.

- 검색어 1개를 받아 SerpAPI(google_images)로 이미지를 가능한 한 많이 수집
- 각 이미지 URL을 analyze로 분석
- 원재료/품목보고번호/성분표/제품명 검출 여부와 근거를 터미널에 계층형으로 출력
"""

from __future__ import annotations

import os
import re
import time
import threading
import json
import html
import webbrowser
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urljoin
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any
from collections import Counter, defaultdict

import requests

from app.analyzer import URLIngredientAnalyzer
from app.config import DB_FILE
from app import config as app_config

SERPAPI_URL = "https://serpapi.com/search.json"
NAVER_OPENAPI_IMAGE_URL = "https://openapi.naver.com/v1/search/image.json"
NAVER_OPENAPI_SHOP_URL = "https://openapi.naver.com/v1/search/shop.json"
SERPAPI_TIMEOUT = 40
SERPAPI_CONNECT_TIMEOUT = 8
SERPAPI_RETRIES = 4
SERPAPI_RETRY_BACKOFF = 1.0
SERP_REPORT_DIR = Path("reports/serp_batch")


@dataclass
class ImageCandidate:
    url: str
    title: str | None
    source: str | None
    page_no: int
    rank_in_page: int


def _compact(text: Any) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip()


def _short(text: Any, max_len: int = 42) -> str:
    value = _compact(text)
    if not value:
        return "null"
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"


def _extract_assistant_content(raw_api_response: Any, raw_model_text: Any) -> str:
    text = str(raw_api_response or "").strip()
    if text:
        try:
            payload = json.loads(text)
            choices = payload.get("choices") or []
            if choices:
                msg = (choices[0] or {}).get("message") or {}
                content = msg.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()
            # Gemini 응답 호환
            candidates = payload.get("candidates") or []
            if candidates:
                content = (candidates[0] or {}).get("content") or {}
                parts = content.get("parts") or []
                chunks: list[str] = []
                for part in parts:
                    if isinstance(part, dict):
                        t = part.get("text")
                        if t:
                            chunks.append(str(t))
                if chunks:
                    return "\n".join(chunks).strip()
        except Exception:  # pylint: disable=broad-except
            pass
    fallback = str(raw_model_text or "").strip()
    return fallback


def _is_precheck_skip(result: dict[str, Any]) -> bool:
    reason = str(result.get("ai_decision_reason") or "").lower()
    if "precheck" in reason:
        return True
    for code in (result.get("quality_fail_reasons") or []):
        if str(code).lower().startswith("precheck:"):
            return True
    return False


def _is_api_failure(result: dict[str, Any], err: str | None) -> bool:
    if err:
        return True
    reason = str(result.get("ai_decision_reason") or "").lower()
    note = str(result.get("note") or "").lower()
    text = " ".join([reason, note])
    failure_keys = (
        "openai_http_",
        "empty_model_response",
        "chatgpt analyze error",
        "resource_exhausted",
        "timeout",
        "insufficient_quota",
    )
    return any(k in text for k in failure_keys)


def _all_true_flags(result: dict[str, Any], keys: list[str]) -> bool:
    qf = result.get("quality_flags") or {}
    return all(qf.get(k) is True for k in keys)


def _is_pass2_extractable(result: dict[str, Any]) -> bool:
    if str(result.get("ai_decision") or "").upper() != "READ":
        return False
    if not bool(result.get("quality_gate_pass")):
        return False
    qf = result.get("quality_flags") or {}
    # 방어적으로 핵심 플래그까지 재검증
    required_true = [
        "is_clear_text",
        "is_full_frame",
        "is_flat_undistorted",
        "no_curved_surface_text_distortion",
        "has_ingredients_section",
        "has_report_number_label",
        "has_single_product",
        "key_fields_fully_visible",
        "no_glare_on_key_fields",
        "no_object_occlusion_on_key_fields",
        "no_any_text_occlusion_on_key_fields",
        "no_occlusion_overlap_on_key_text",
        "no_wrinkle_fold_occlusion_on_key_fields",
    ]
    if not all(qf.get(k) is True for k in required_true):
        return False
    return True


def _is_transient_error(err: str | None) -> bool:
    low = str(err or "").lower()
    return any(
        k in low
        for k in (
            "429",
            "resource_exhausted",
            "timeout",
            "timed out",
            "deadline",
            "temporarily unavailable",
            "503",
            "502",
        )
    )


def _contains_nutrition(text: str | None) -> tuple[bool, str]:
    value = str(text or "")
    kws = ["영양정보", "영양성분", "나트륨", "탄수화물", "단백질", "지방", "calories", "nutrition"]
    found = [kw for kw in kws if kw.lower() in value.lower()]
    if not found:
        return (False, "영양성분 키워드 미검출")
    return (True, f"영양성분 키워드 검출: {', '.join(found[:6])}")


def _mark_report(result: dict[str, Any]) -> str:
    raw = result.get("itemMnftrRptNo")
    compact = _compact(raw)
    if not compact:
        return "❌"
    qf = result.get("quality_flags") or {}
    digits = re.sub(r"[^0-9]", "", str(compact))
    if 10 <= len(digits) <= 16 and qf.get("report_number_complete") is True:
        return "✅"
    return "🔺"


def _mark_ingredients(result: dict[str, Any]) -> str:
    ing = _compact(result.get("ingredients_text"))
    if not ing:
        return "❌"
    qf = result.get("quality_flags") or {}
    if qf.get("ingredients_complete") is True:
        return "✅"
    if len(ing) >= 20 and ("," in ing or "원재료" in ing.lower() or "ingredients" in ing.lower()):
        return "✅"
    return "🔺"


def _mark_product(result: dict[str, Any], title: str | None) -> tuple[str, str | None]:
    name = result.get("product_name_in_image")
    value = _compact(name)
    if not value:
        return ("❌", None)
    qf = result.get("quality_flags") or {}
    if qf.get("product_name_complete") is True:
        return ("✅", value)
    if result.get("product_name_in_image") and len(value) >= 2:
        return ("✅", value)
    return ("🔺", value)


def _mark_nutrition(full_text: str | None) -> tuple[str, str]:
    has_nutri, how = _contains_nutrition(full_text)
    if not has_nutri:
        return ("❌", how)
    low = str(full_text or "").lower()
    hits = 0
    for kw in ("영양정보", "영양성분", "나트륨", "탄수화물", "단백질", "지방", "calories", "nutrition"):
        if kw.lower() in low:
            hits += 1
    if hits >= 2:
        return ("✅", how)
    return ("🔺", how)


def _mark_nutrition_from_result(result: dict[str, Any]) -> tuple[str, str]:
    txt = result.get("nutrition_text")
    if txt:
        compact = _compact(txt)
        if not compact:
            return ("❌", "nutrition_text empty")
        qf = result.get("quality_flags") or {}
        if qf.get("nutrition_complete") is True:
            return ("✅", "nutrition_complete=true")
        if len(compact) >= 12:
            return ("🔺", "nutrition_text partial")
        return ("🔺", "nutrition_text short")
    return ("❌", "nutrition_text null")


def _provider_from_model(model_name: Any) -> str:
    m = str(model_name or "").strip().lower()
    if not m:
        return "unknown"
    if "gemini" in m:
        return "gemini"
    if "gpt" in m or "o1" in m or "o3" in m or "o4" in m:
        return "openai"
    return "unknown"


def _raw_api_meta(raw_api_text: Any) -> str:
    text = str(raw_api_text or "").strip()
    if not text:
        return "raw_api=none"
    size = len(text)
    try:
        payload = json.loads(text)
    except Exception:
        return f"raw_api=text_only bytes={size}"
    model = payload.get("model") or payload.get("modelVersion")
    req_id = payload.get("id")
    err = payload.get("error")
    keys = []
    if req_id:
        keys.append(f"id={req_id}")
    if model:
        keys.append(f"model={model}")
    if err:
        keys.append("error=yes")
    else:
        keys.append("error=no")
    if "choices" in payload:
        keys.append("format=openai")
    elif "candidates" in payload:
        keys.append("format=gemini")
    else:
        keys.append("format=unknown_json")
    keys.append(f"bytes={size}")
    return " ".join(keys)


def _search_images_all(
    query: str,
    api_key: str,
    max_pages: int = 20,
    per_page: int = 100,
    provider: str = "google",
    debug_serp: bool = False,
) -> list[ImageCandidate]:
    seen: set[str] = set()
    out: list[ImageCandidate] = []
    provider_norm = str(provider or "google").strip().lower()
    if provider_norm not in ("google", "naver_official", "naver_blog", "naver_shop"):
        provider_norm = "google"
    consecutive_failures = 0

    for page_no in range(max_pages):
        request_url = SERPAPI_URL
        request_headers: dict[str, str] | None = None
        if provider_norm == "naver_official":
            page_size = min(per_page, 100)
            naver_client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
            naver_client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
            if not (naver_client_id and naver_client_secret):
                raise RuntimeError("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 필요합니다.")
            params = {
                "query": query,
                "display": page_size,
                "start": page_no * page_size + 1,
                "sort": "sim",
            }
            request_url = NAVER_OPENAPI_IMAGE_URL
            request_headers = {
                "X-Naver-Client-Id": naver_client_id,
                "X-Naver-Client-Secret": naver_client_secret,
            }
        elif provider_norm == "naver_blog":
            page_size = min(per_page, 100)
            naver_client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
            naver_client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
            if not (naver_client_id and naver_client_secret):
                raise RuntimeError("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 필요합니다.")
            params = {
                "query": query,
                "display": page_size,
                "start": page_no * page_size + 1,
                "sort": "sim",
            }
            # 요청사항: 블로그 글 URL이 아니라 "이미지 결과 중 블로그 출처"만 수집
            request_url = NAVER_OPENAPI_IMAGE_URL
            request_headers = {
                "X-Naver-Client-Id": naver_client_id,
                "X-Naver-Client-Secret": naver_client_secret,
            }
        elif provider_norm == "naver_shop":
            page_size = min(per_page, 100)
            naver_client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
            naver_client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
            if not (naver_client_id and naver_client_secret):
                raise RuntimeError("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 필요합니다.")
            params = {
                "query": query,
                "display": page_size,
                "start": page_no * page_size + 1,
                "sort": "sim",
            }
            request_url = NAVER_OPENAPI_SHOP_URL
            request_headers = {
                "X-Naver-Client-Id": naver_client_id,
                "X-Naver-Client-Secret": naver_client_secret,
            }
        else:
            # SerpAPI Google Images
            page_size = per_page
            params = {
                "engine": "google_images",
                "q": query,
                "hl": "ko",
                "gl": "kr",
                "num": page_size,
                "ijn": page_no,
                "api_key": api_key,
                "no_cache": "true",
            }

        data: dict[str, Any] | None = None
        last_error = None
        for attempt in range(SERPAPI_RETRIES + 1):
            try:
                if debug_serp:
                    safe_params = {k: v for k, v in params.items() if k != "api_key"}
                    print(f"  [SERP][page={page_no} attempt={attempt+1}/{SERPAPI_RETRIES+1}] 요청 시작")
                    print(f"    endpoint={request_url}")
                    print(f"    params={safe_params}")
                t0 = time.time()
                resp = requests.get(
                    request_url,
                    params=params,
                    headers=request_headers,
                    timeout=(SERPAPI_CONNECT_TIMEOUT, SERPAPI_TIMEOUT),
                )
                elapsed = time.time() - t0
                raw_text = resp.text or ""
                try:
                    data = resp.json()
                except Exception as json_exc:  # pylint: disable=broad-except
                    data = None
                    last_error = f"json_parse_error:{type(json_exc).__name__}:{json_exc}"
                    if debug_serp:
                        print(f"  [SERP][page={page_no}] JSON 파싱 실패")
                        print(f"    status={resp.status_code} elapsed={elapsed:.2f}s")
                        print(f"    raw(앞 1200자)={raw_text[:1200]}")
                    if attempt < SERPAPI_RETRIES:
                        time.sleep(SERPAPI_RETRY_BACKOFF * (2 ** attempt))
                        continue
                    break

                api_err = data.get("error")
                if provider_norm in ("naver_official", "naver_blog", "naver_shop") and resp.status_code >= 400:
                    api_err = data.get("errorMessage") or data.get("message") or data.get("errorCode") or api_err
                if debug_serp:
                    print(f"  [SERP][page={page_no}] 응답 수신 status={resp.status_code} elapsed={elapsed:.2f}s")
                    print(f"    api_error={api_err}")
                    if provider_norm in ("naver_official", "naver_blog", "naver_shop"):
                        imgs = data.get("items") or []
                    else:
                        imgs = data.get("images_results") or []
                    print(f"    images_results={len(imgs)}")
                    print(f"    raw(앞 1200자)={raw_text[:1200]}")
                # SerpAPI가 "결과 없음"을 error 문자열로 주는 경우가 있어,
                # 실패가 아니라 페이징 종료 신호로 처리한다.
                if resp.status_code == 200 and isinstance(api_err, str):
                    api_err_low = api_err.lower()
                    if ("hasn't returned any results" in api_err_low) or ("no results" in api_err_low):
                        data = {"images_results": []}
                        break
                if resp.status_code == 200 and api_err is None:
                    break
                last_error = f"http={resp.status_code} api_error={api_err}"
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < SERPAPI_RETRIES:
                    time.sleep(SERPAPI_RETRY_BACKOFF * (2 ** attempt))
                    continue
                raise RuntimeError(last_error)
            except Exception as exc:  # pylint: disable=broad-except
                last_error = str(exc)
                if debug_serp:
                    print(f"  [SERP][page={page_no}] 예외: {type(exc).__name__}: {exc}")
                # Naver 이미지에서 응답 페이로드가 커 타임아웃이 잦은 경우,
                # display를 줄여 같은 페이지를 더 가볍게 재시도한다.
                if attempt < SERPAPI_RETRIES:
                    time.sleep(SERPAPI_RETRY_BACKOFF * (2 ** attempt))
                    continue
                break

        if data is None:
            consecutive_failures += 1
            print(f"  ⚠️ SerpAPI 페이지 스킵(page={page_no}): {last_error}")
            if consecutive_failures >= 3:
                print("  ⚠️ SerpAPI 연속 실패 3회로 수집을 중단합니다.")
                break
            continue

        consecutive_failures = 0
        if provider_norm in ("naver_official", "naver_blog", "naver_shop"):
            images = data.get("items") or []
        else:
            images = data.get("images_results") or []
        if not images:
            break

        added = 0
        for rank, item in enumerate(images, start=1):
            if provider_norm == "naver_official":
                url = item.get("link") or item.get("thumbnail")
                source = urlparse(str(url)).netloc if url else "naver_openapi"
            elif provider_norm == "naver_blog":
                url = item.get("link") or item.get("thumbnail")
                thumb = str(item.get("thumbnail") or "").lower()
                link = str(item.get("link") or "").lower()
                # "출처가 블로그"에 해당하는 케이스만 수집
                is_blog = any(
                    key in link or key in thumb
                    for key in (
                        "blog.naver.com",
                        "blogfiles.pstatic.net",
                        "postfiles.pstatic.net",
                        "mblogthumb-phinf.pstatic.net",
                        "phinf.pstatic.net",
                        "blog",  # 네이버 이미지 URL 내 blog 타입 힌트 대응
                    )
                )
                if not is_blog:
                    continue
                source = urlparse(str(url)).netloc if url else "naver_blog_image"
            elif provider_norm == "naver_shop":
                product_url = str(item.get("link") or "").strip()
                product_title = str(item.get("title") or "").strip()
                mall_name = str(item.get("mallName") or "").strip()
                if not product_url:
                    continue
                shop_imgs = _extract_images_from_shop_detail(
                    product_url,
                    timeout=(SERPAPI_CONNECT_TIMEOUT, SERPAPI_TIMEOUT),
                    debug=debug_serp,
                )
                if not shop_imgs:
                    continue
                for img_url in shop_imgs:
                    if not img_url or img_url in seen:
                        continue
                    seen.add(img_url)
                    out.append(
                        ImageCandidate(
                            url=img_url,
                            title=product_title or item.get("title"),
                            source=mall_name or urlparse(product_url).netloc or "naver_shop",
                            page_no=page_no,
                            rank_in_page=rank,
                        )
                    )
                    added += 1
                continue
            else:
                url = item.get("original") or item.get("thumbnail")
                source = item.get("source")
            if not url or url in seen:
                continue
            seen.add(url)
            out.append(
                ImageCandidate(
                    url=url,
                    title=item.get("title"),
                    source=source,
                    page_no=page_no,
                    rank_in_page=rank,
                )
            )
            added += 1

        if added == 0:
            break

    return out


def _extract_images_from_shop_detail(product_url: str, timeout: tuple[int, int], debug: bool = False) -> list[str]:
    # 1) Playwright 렌더링 기반 추출 우선
    pw_images = _extract_images_from_shop_detail_playwright(product_url, timeout=timeout, debug=debug)
    if pw_images:
        return pw_images

    # 2) requests 정적 HTML 추출 fallback
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    }
    try:
        resp = requests.get(product_url, headers=headers, timeout=timeout, allow_redirects=True)
        html_text = resp.text or ""
        final_url = resp.url or product_url
    except Exception as exc:  # pylint: disable=broad-except
        if debug:
            print(f"    [SHOP] detail fetch 실패: {product_url} | {type(exc).__name__}: {exc}")
        return []

    found: list[tuple[str, int]] = []
    seen: set[str] = set()

    # 메타 이미지
    for m in re.finditer(
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        html_text,
        re.IGNORECASE,
    ):
        u = html.unescape(m.group(1).strip())
        if u.startswith("//"):
            u = "https:" + u
        u = urljoin(final_url, u)
        sc = _score_shop_image_candidate(u, meta_text="og_image", width=None, height=None)
        if _looks_like_image_for_shop(u) and u not in seen and sc >= 1:
            seen.add(u)
            found.append((u, sc))

    # 본문 이미지 (src / data-src)
    for pat in (
        r'<img[^>]+src=["\']([^"\']+)["\']',
        r'<img[^>]+data-src=["\']([^"\']+)["\']',
        r'"image"\s*:\s*"([^"]+)"',
    ):
        for m in re.finditer(pat, html_text, re.IGNORECASE):
            u = html.unescape(m.group(1).strip().replace("\\/", "/"))
            if u.startswith("//"):
                u = "https:" + u
            u = urljoin(final_url, u)
            sc = _score_shop_image_candidate(u, meta_text="", width=None, height=None)
            if _looks_like_image_for_shop(u) and u not in seen and sc >= 1:
                seen.add(u)
                found.append((u, sc))

    # 너무 많으면 상위만 사용
    found.sort(key=lambda x: x[1], reverse=True)
    return [u for u, _ in found[:20]]


def _extract_images_from_shop_detail_playwright(
    product_url: str,
    timeout: tuple[int, int],
    debug: bool = False,
) -> list[str]:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception as exc:  # pylint: disable=broad-except
        if debug:
            print(f"    [SHOP][PW] playwright import 실패: {type(exc).__name__}: {exc}")
        return []

    nav_timeout_ms = max(5000, int(timeout[1] * 1000))
    found: list[tuple[str, int]] = []
    seen: set[str] = set()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 1900},
            )
            page = context.new_page()
            page.goto(product_url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            page.wait_for_timeout(1800)
            # 지연 로딩 대응: 스크롤하며 이미지 로드 유도
            for _ in range(6):
                page.mouse.wheel(0, 2400)
                page.wait_for_timeout(450)

            img_objs: list[dict[str, Any]] = page.eval_on_selector_all(
                "img",
                (
                    "els => els.map(e => ({"
                    "src: (e.currentSrc || e.src || e.getAttribute('data-src') || e.getAttribute('data-original') || ''),"
                    "srcset: (e.getAttribute('srcset') || ''),"
                    "alt: (e.getAttribute('alt') || ''),"
                    "cls: (e.getAttribute('class') || ''),"
                    "id: (e.getAttribute('id') || ''),"
                    "w: (e.naturalWidth || 0),"
                    "h: (e.naturalHeight || 0)"
                    "}));"
                ),
            )
            bg_urls: list[str] = page.eval_on_selector_all(
                "[style*='background-image']",
                (
                    "els => els.map(e => getComputedStyle(e).backgroundImage || '')"
                ),
            )
            html_text = page.content()
            for obj in img_objs:
                raw_s = str((obj or {}).get("src") or "").strip()
                # srcset 케이스 분리
                srcset = str((obj or {}).get("srcset") or "").strip()
                if srcset:
                    srcset_cand = [x.strip().split(" ")[0] for x in srcset.split(",")]
                else:
                    srcset_cand = []
                cand = ([x.strip().split(" ")[0] for x in raw_s.split(",")] if "," in raw_s else [raw_s]) + srcset_cand
                meta_text = " ".join(
                    [
                        str((obj or {}).get("alt") or ""),
                        str((obj or {}).get("cls") or ""),
                        str((obj or {}).get("id") or ""),
                    ]
                )
                w = int((obj or {}).get("w") or 0)
                h = int((obj or {}).get("h") or 0)
                for c in cand:
                    u = c
                    if u.startswith("//"):
                        u = "https:" + u
                    sc = _score_shop_image_candidate(u, meta_text=meta_text, width=w, height=h)
                    if _looks_like_image_for_shop(u) and u not in seen and sc >= 2:
                        seen.add(u)
                        found.append((u, sc))
            for b in bg_urls:
                bs = str(b or "")
                for m in re.finditer(r'url\(["\']?([^"\')]+)', bs, re.IGNORECASE):
                    u = m.group(1).strip()
                    if u.startswith("//"):
                        u = "https:" + u
                    sc = _score_shop_image_candidate(u, meta_text="background-image", width=None, height=None)
                    if _looks_like_image_for_shop(u) and u not in seen and sc >= 2:
                        seen.add(u)
                        found.append((u, sc))
            # DOM 외 포함 텍스트에서 이미지 URL 패턴 추가 탐지
            for m in re.finditer(r'https?://[^\s"\\\']+\.(?:jpg|jpeg|png|webp|gif|bmp)(?:\?[^\s"\\\']*)?', html_text, re.IGNORECASE):
                u = m.group(0).strip()
                if u.startswith("//"):
                    u = "https:" + u
                sc = _score_shop_image_candidate(u, meta_text="html_text", width=None, height=None)
                if _looks_like_image_for_shop(u) and u not in seen and sc >= 2:
                    seen.add(u)
                    found.append((u, sc))

            browser.close()
    except Exception as exc:  # pylint: disable=broad-except
        if debug:
            print(f"    [SHOP][PW] 추출 실패: {type(exc).__name__}: {exc}")
        return []

    found.sort(key=lambda x: x[1], reverse=True)
    if debug:
        print(f"    [SHOP][PW] 추출 성공(필터후): {len(found)}")
        print(f"    [SHOP][PW] 상위 점수: {[s for _, s in found[:5]]}")
    return [u for u, _ in found[:20]]


def _looks_like_image_for_shop(url: str) -> bool:
    low = str(url or "").lower()
    if not (low.startswith("http://") or low.startswith("https://")):
        return False
    if any(x in low for x in ("sprite", "icon", "logo", "badge", "blank", "loading")):
        return False
    return any(ext in low for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")) or ("type=w" in low)


def _score_shop_image_candidate(
    url: str,
    *,
    meta_text: str,
    width: int | None,
    height: int | None,
) -> int:
    low = str(url or "").lower()
    meta = str(meta_text or "").lower()
    score = 0

    positive = (
        "detail", "product", "goods", "desc", "content", "editor",
        "ingredient", "nutrition", "원재료", "성분", "영양", "상세",
    )
    negative = (
        "banner", "event", "promo", "review", "qna", "logo", "icon",
        "sprite", "sns", "youtube", "profile", "thumb", "main", "brandstory",
        "recommend", "coupon",
    )

    if any(k in low for k in positive) or any(k in meta for k in positive):
        score += 3
    if any(k in low for k in negative) or any(k in meta for k in negative):
        score -= 4

    w = int(width or 0)
    h = int(height or 0)
    if w > 0 and h > 0:
        if w >= 600 or h >= 600:
            score += 1
        if w < 220 and h < 220:
            score -= 2
    return score


def _safe_filename(text: str, max_len: int = 80) -> str:
    value = re.sub(r"\s+", "_", str(text or "").strip())
    value = re.sub(r"[^0-9A-Za-z가-힣._-]", "_", value)
    value = re.sub(r"_+", "_", value).strip("._-")
    if not value:
        value = "query"
    return value[:max_len]


def run_query_image_benchmark(
    query: str,
    max_pages: int = 20,
    per_page: int = 20,
    delay_sec: float = 0.0,
    max_concurrency: int = 5,
    adaptive: bool = True,
    auto_open_report: bool = True,
    provider: str = "google",
    debug_serp: bool = False,
    images_only: bool = False,
    debug_pass_trace: bool = True,
) -> None:
    # 인터페이스를 오래 켜둔 경우를 대비해 매 실행 시 .env를 다시 로드
    app_config.reload_dotenv()
    provider_norm = str(provider or "google").strip().lower()
    serp_key = os.getenv("SERPAPI_KEY")
    if provider_norm not in ("naver_official", "naver_blog", "naver_shop") and not serp_key:
        raise SystemExit("SERPAPI_KEY 환경변수를 설정해주세요.")
    if provider_norm in ("naver_official", "naver_blog", "naver_shop"):
        naver_client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
        naver_client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
        if not (naver_client_id and naver_client_secret):
            raise SystemExit("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수를 설정해주세요.")
    print("\n=== 검색어 기반 이미지 벤치마크 ===")
    print(f"- 검색어: {query}")
    print(f"- 검색엔진: {provider}")
    print(f"- 최대 페이지: {max_pages}")
    print(f"- 페이지당 수집 개수: {per_page}")
    print("- SerpAPI에서 이미지 수집 중...")
    images = _search_images_all(
        query=query,
        api_key=serp_key or "",
        max_pages=max_pages,
        per_page=max(1, min(100, int(per_page))),
        provider=provider,
        debug_serp=debug_serp,
    )
    print(f"- 수집된 이미지: {len(images)}개")
    print("- 상태 기준: ✅ 추출 가능 | ❌ 추출 불가")
    if not images:
        return
    if images_only:
        _save_images_only_report(
            query=query,
            provider=provider,
            images=images,
            auto_open_report=auto_open_report,
        )
        return

    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        raise SystemExit("OPENAI_API_KEY 환경변수를 설정해주세요.")

    max_concurrency = max(1, min(200, int(max_concurrency)))
    print(f"- analyze 병렬 처리: 최대 {max_concurrency}개 동시 실행")
    print(f"- adaptive 모드: {'ON' if adaptive else 'OFF'}")
    thread_local = threading.local()

    def _get_analyzer() -> URLIngredientAnalyzer:
        analyzer = getattr(thread_local, "analyzer", None)
        if analyzer is None:
            # 벤치마크는 체감 속도를 위해 timeout/retry를 보수적으로 낮춘다.
            analyzer = URLIngredientAnalyzer(
                api_key=openai_key,
                strict_mode=False,
                request_timeout_sec=35,
                download_timeout_sec=12,
                download_retries=1,
                model_retries=1,
            )
            thread_local.analyzer = analyzer
        return analyzer

    def _analyze_one(
        idx: int,
        img: ImageCandidate,
    ) -> tuple[
        int,
        ImageCandidate,
        dict[str, Any],
        str | None,
        dict[str, Any] | None,
        str | None,
        dict[str, Any] | None,
        dict[str, Any],
    ]:
        trace: dict[str, Any] = {
            "pass2_ms": None,
            "pass3_ms": None,
            "pass4_ms": None,
            "pass3_requested": False,
            "pass4_requested": False,
        }
        try:
            analyzer = _get_analyzer()
            t_pass2 = time.time()
            result = analyzer.analyze_pass2(image_url=img.url, target_item_rpt_no=None)
            trace["pass2_ms"] = int((time.time() - t_pass2) * 1000)
            qf = result.get("quality_flags") or {}
            should_run_pass3 = _is_pass2_extractable(result)
            trace["pass3_requested"] = bool(should_run_pass3)
            pass3_result: dict[str, Any] | None = None
            pass3_err: str | None = None
            if should_run_pass3:
                include_nutrition = bool(qf.get("has_nutrition_section"))
                t_pass3 = time.time()
                pass3_result = analyzer.analyze_pass3(
                    image_url=img.url,
                    target_item_rpt_no=None,
                    include_nutrition=include_nutrition,
                )
                trace["pass3_ms"] = int((time.time() - t_pass3) * 1000)
                if pass3_result.get("error"):
                    pass3_err = str(pass3_result.get("error"))
            pass4_result: dict[str, Any] | None = None
            if pass3_result and not pass3_err:
                has_required = bool(
                    (pass3_result.get("product_report_number"))
                    and (pass3_result.get("ingredients_text"))
                )
                if has_required:
                    trace["pass4_requested"] = True
                    t_pass4 = time.time()
                    pass4_result = analyzer.analyze_pass4_normalize(
                        pass2_result=result,
                        pass3_result=pass3_result,
                        target_item_rpt_no=None,
                    )
                    trace["pass4_ms"] = int((time.time() - t_pass4) * 1000)
            return (idx, img, result, None, pass3_result, pass3_err, pass4_result, trace)
        except Exception as exc:  # pylint: disable=broad-except
            result = {
                "itemMnftrRptNo": None,
                "ingredients_text": None,
                "full_text": None,
                "note": f"analysis_error:{type(exc).__name__}",
            }
            return (idx, img, result, str(exc), None, None, None, trace)

    extractable_cnt = 0
    precheck_skip_cnt = 0
    api_fail_cnt = 0
    api_success_skip_cnt = 0
    api_success_read_cnt = 0
    all_true_except_ing_cnt = 0
    all_true_with_ing_cnt = 0
    all_true_except_nutrition_rows: list[tuple[int, str, bool | None]] = []
    pass3_triggered_cnt = 0
    pass3_success_cnt = 0
    pass3_failed_cnt = 0
    pass3_success_rows: list[dict[str, Any]] = []
    pass3_failed_rows: list[dict[str, Any]] = []
    pass4_run_cnt = 0
    pass4_ok_cnt = 0
    pass4_fail_cnt = 0
    pass4_rows: list[dict[str, Any]] = []
    pass2_pass_rows: list[dict[str, Any]] = []
    pass2_all_rows: list[dict[str, Any]] = []
    pass2_fail_reason_counter: Counter[str] = Counter()
    pass2_fail_reason_samples: dict[str, list[str]] = defaultdict(list)
    gate_read_cnt = 0
    gate_read_but_not_extractable_cnt = 0
    pass2a_pass_cnt = 0
    pass2b_executed_cnt = 0
    pass2b_pass_cnt = 0
    with ThreadPoolExecutor(max_workers=max_concurrency) as ex:
        pending: dict[Any, tuple[int, ImageCandidate]] = {}
        next_i = 0
        current_limit = min(3, max_concurrency) if adaptive else max_concurrency
        done_count = 0
        stable_success = 0
        last_heartbeat = time.time()
        while pending or next_i < len(images):
            while next_i < len(images) and len(pending) < current_limit:
                idx = next_i + 1
                img = images[next_i]
                fut = ex.submit(_analyze_one, idx, img)
                pending[fut] = (idx, img)
                next_i += 1

            if not pending:
                continue

            done_set, _ = wait(set(pending.keys()), timeout=2.0, return_when=FIRST_COMPLETED)
            now = time.time()
            if not done_set and (now - last_heartbeat) >= 2.0:
                print(
                    f"  ...분석 진행중 ({done_count}/{len(images)} 완료)"
                    f" | in_flight={len(pending)} | limit={current_limit}"
                )
                last_heartbeat = now
                continue

            for fut in done_set:
                idx, img = pending.pop(fut)
                done_count += 1
                idx, img, result, err, pass3_result, pass3_err, pass4_result, trace = fut.result()
                gate_result = "READ" if (str(result.get("ai_decision") or "").upper() == "READ") else "SKIP"
                is_extractable = _is_pass2_extractable(result)
                if gate_result == "READ":
                    gate_read_cnt += 1
                    if not is_extractable:
                        gate_read_but_not_extractable_cnt += 1
                if is_extractable and err is None:
                    extractable_cnt += 1

                # 통계 분류
                is_precheck = _is_precheck_skip(result)
                is_api_fail = _is_api_failure(result, err)
                if is_precheck:
                    precheck_skip_cnt += 1
                elif is_api_fail:
                    api_fail_cnt += 1
                else:
                    if gate_result == "READ":
                        api_success_read_cnt += 1
                    else:
                        api_success_skip_cnt += 1
                    if not is_extractable:
                        reasons = []
                        for code in (result.get("quality_fail_reasons") or []):
                            c = str(code or "").strip()
                            if not c:
                                continue
                            if c.startswith("ai_skip:"):
                                continue
                            reasons.append(c)
                        if not reasons:
                            reasons = ["unknown_pass2_fail_reason"]
                        for r in reasons:
                            pass2_fail_reason_counter[r] += 1
                            if len(pass2_fail_reason_samples[r]) < 5:
                                pass2_fail_reason_samples[r].append(img.url)

                # 품질 플래그 통계
                # READ 판정 기준과 동일한 핵심 키(영양성분은 선택 항목)
                relaxed_keys = [
                    "is_clear_text",
                    "is_full_frame",
                    "is_flat_undistorted",
                    "no_curved_surface_text_distortion",
                    "has_report_number_label",
                    "has_product_name",
                    "has_single_product",
                    "key_fields_fully_visible",
                    "no_glare_on_key_fields",
                    "no_object_occlusion_on_key_fields",
                    "no_occlusion_overlap_on_key_text",
                ]
                strict_keys = relaxed_keys + ["has_ingredients_section"]
                qf = result.get("quality_flags") or {}
                overlap_safe = (qf.get("no_occlusion_overlap_on_key_text") is True)
                if _all_true_flags(result, relaxed_keys) and overlap_safe:
                    all_true_except_ing_cnt += 1
                if _all_true_flags(result, strict_keys) and overlap_safe:
                    all_true_with_ing_cnt += 1

                # nutrition 제외, 나머지 핵심 지표 모두 true인 목록 수집
                # 기준: strict_keys (nutrition만 제외)
                if is_extractable:
                    nutri_flag = qf.get("has_nutrition_section")
                    all_true_except_nutrition_rows.append((idx, img.url, nutri_flag if isinstance(nutri_flag, bool) else None))
                    pass3_triggered_cnt += 1
                    p3_has_required = False
                    p3_raw = None
                    p4_raw = None
                    if pass3_result and not pass3_err:
                        pass3_success_cnt += 1
                        p3_raw = _extract_assistant_content(
                            raw_api_response=pass3_result.get("raw_api_response"),
                            raw_model_text=pass3_result.get("raw_model_text"),
                        )
                        p3_ing_executed = bool(pass3_result.get("raw_model_text_pass3_ingredients"))
                        p3_nut_executed = bool(pass3_result.get("raw_model_text_pass3_nutrition"))
                        p3_nut_expected = bool(nutri_flag)
                        p3_has_required = bool(
                            (pass3_result.get("product_report_number"))
                            and (pass3_result.get("ingredients_text"))
                        )
                        p3_nut_pass = bool(pass3_result.get("nutrition_text"))
                        p4_items = []
                        p4_err = None
                        p4_reason = None
                        p4_executed = False
                        p4_ing_executed = False
                        p4_nut_executed = False
                        p4_nut_pass = False
                        p4_report_valid = None
                        p4_report_reason = None
                        p4_nut_items = []
                        if pass4_result:
                            pass4_run_cnt += 1
                            p4_items = list(pass4_result.get("ingredient_items") or [])
                            p4_err = pass4_result.get("pass4_ai_error")
                            p4_reason = pass4_result.get("ingredient_items_reason")
                            p4_executed = bool(
                                pass4_result.get("raw_model_text_pass4")
                                or pass4_result.get("raw_api_response_pass4")
                            )
                            p4_ing_executed = bool(
                                pass4_result.get("raw_model_text_pass4_ingredients")
                                or pass4_result.get("raw_api_response_pass4_ingredients")
                            )
                            p4_nut_executed = bool(
                                pass4_result.get("raw_model_text_pass4_nutrition")
                                or pass4_result.get("raw_api_response_pass4_nutrition")
                            )
                            rv = pass4_result.get("report_number_validation") or {}
                            p4_report_valid = rv.get("is_valid")
                            p4_report_reason = rv.get("reason")
                            p4_nut_items = list(pass4_result.get("nutrition_items") or [])
                            p4_nut_pass = len(p4_nut_items) > 0
                            if p4_err:
                                pass4_fail_cnt += 1
                            else:
                                pass4_ok_cnt += 1
                            p4_raw = _extract_assistant_content(
                                raw_api_response=pass4_result.get("raw_api_response_pass4"),
                                raw_model_text=pass4_result.get("raw_model_text_pass4"),
                            )
                            pass4_rows.append(
                                {
                                    "no": idx,
                                    "url": img.url,
                                    "product_name": pass3_result.get("product_name_in_image"),
                                    "report_no": pass3_result.get("product_report_number"),
                                    "report_valid": p4_report_valid,
                                    "report_reason": p4_report_reason,
                                    "ingredient_items": p4_items,
                                    "nutrition_items": p4_nut_items,
                                    "pass4_error": p4_err,
                                    "pass4_raw": p4_raw,
                                }
                            )
                        pass3_success_rows.append(
                            {
                                "no": idx,
                                "url": img.url,
                                "product_name": pass3_result.get("product_name_in_image"),
                                "report_no": pass3_result.get("product_report_number"),
                                "ingredients": pass3_result.get("ingredients_text"),
                                "nutrition": pass3_result.get("nutrition_text"),
                                "ingredient_items_count": len(p4_items),
                                "nutrition_items_count": len(p4_nut_items),
                                "report_valid": p4_report_valid,
                                "report_reason": p4_report_reason,
                                "pass4_reason": p4_reason,
                                "pass4_executed": p4_executed,
                                "pass3_ing_executed": p3_ing_executed,
                                "pass3_nut_expected": p3_nut_expected,
                                "pass3_nut_executed": p3_nut_executed,
                                "pass3_nut_pass": p3_nut_pass,
                                "pass4_ing_executed": p4_ing_executed,
                                "pass4_nut_executed": p4_nut_executed,
                                "pass4_nut_pass": p4_nut_pass,
                                "pass4_error": p4_err,
                            }
                        )
                    else:
                        pass3_failed_cnt += 1
                        raw_pass3 = _extract_assistant_content(
                            raw_api_response=(pass3_result or {}).get("raw_api_response"),
                            raw_model_text=(pass3_result or {}).get("raw_model_text"),
                        )
                        p3_raw = raw_pass3 or pass3_err or "null"
                        pass3_failed_rows.append(
                            {
                                "no": idx,
                                "url": img.url,
                                "error": pass3_err or (pass3_result or {}).get("error") or "unknown",
                                "raw": raw_pass3 or "null",
                            }
                        )
                    pass2_pass_rows.append(
                        {
                            "no": idx,
                            "url": img.url,
                            "pass3_ok": p3_has_required,
                            "pass3_error": pass3_err,
                            "pass3_product_name": (pass3_result or {}).get("product_name_in_image") if pass3_result else None,
                                "pass3_report_no": (pass3_result or {}).get("product_report_number") if pass3_result else None,
                                "pass3_ingredients": (pass3_result or {}).get("ingredients_text") if pass3_result else None,
                                "pass3_nutrition": (pass3_result or {}).get("nutrition_text") if pass3_result else None,
                                "pass3_raw": p3_raw,
                            "pass3_ing_executed": bool((pass3_result or {}).get("raw_model_text_pass3_ingredients")) if pass3_result else False,
                            "pass3_nut_expected": bool(nutri_flag),
                            "pass3_nut_executed": bool((pass3_result or {}).get("raw_model_text_pass3_nutrition")) if pass3_result else False,
                            "pass3_nut_pass": bool((pass3_result or {}).get("nutrition_text")) if pass3_result else False,
                            "pass4_exists": bool(pass4_result),
                            "pass4_executed": bool(
                                (pass4_result or {}).get("raw_model_text_pass4")
                                or (pass4_result or {}).get("raw_api_response_pass4")
                            ) if pass4_result else False,
                            "pass4_ing_executed": bool(
                                (pass4_result or {}).get("raw_model_text_pass4_ingredients")
                                or (pass4_result or {}).get("raw_api_response_pass4_ingredients")
                            ) if pass4_result else False,
                            "pass4_nut_executed": bool(
                                (pass4_result or {}).get("raw_model_text_pass4_nutrition")
                                or (pass4_result or {}).get("raw_api_response_pass4_nutrition")
                            ) if pass4_result else False,
                            "pass4_nut_pass": bool((pass4_result or {}).get("nutrition_items")) if pass4_result else False,
                                "pass4_reason": (pass4_result or {}).get("ingredient_items_reason") if pass4_result else None,
                                "pass4_error": (pass4_result or {}).get("pass4_ai_error") if pass4_result else None,
                            "pass4_raw": p4_raw,
                            "pass4_ingredient_items": (pass4_result or {}).get("ingredient_items") if pass4_result else None,
                            "pass4_nutrition_items": (pass4_result or {}).get("nutrition_items") if pass4_result else None,
                            "pass4_ing_items_count": len((pass4_result or {}).get("ingredient_items") or []) if pass4_result else 0,
                            "pass4_ok": bool(
                                bool(pass4_result)
                                and (not (pass4_result or {}).get("pass4_ai_error"))
                                and len((pass4_result or {}).get("ingredient_items") or []) > 0
                            ),
                            }
                        )

                pass2_raw = None
                qf = result.get("quality_flags") or {}
                p2a_ok = bool(qf.get("pass2a_ok"))
                p2b_executed = bool(qf.get("pass2b_executed"))
                p2b_pass = bool(
                    p2b_executed
                    and qf.get("has_ingredients_section") is True
                    and qf.get("has_report_number_label") is True
                )
                if p2a_ok:
                    pass2a_pass_cnt += 1
                if p2b_executed:
                    pass2b_executed_cnt += 1
                if p2b_pass:
                    pass2b_pass_cnt += 1
                if err:
                    pass2_raw = f"(호출 실패) {err}"
                else:
                    content = _extract_assistant_content(
                        raw_api_response=result.get("raw_api_response"),
                        raw_model_text=result.get("raw_model_text"),
                    )
                    if not content:
                        content = result.get("ai_decision_reason") or result.get("note") or "(원문 없음)"
                    pass2_raw = content
                pass2_all_rows.append(
                    {
                        "no": idx,
                        "url": img.url,
                        "raw": pass2_raw or "(원문 없음)",
                        "raw_pass2a": result.get("raw_model_text_pass2a") or "(원문 없음)",
                        "raw_pass2b": result.get("raw_model_text_pass2b") or "(미실행)",
                        "pass2a_ok": p2a_ok,
                        "pass2b_executed": p2b_executed,
                        "pass2b_pass": p2b_pass,
                        "pass2_decision": gate_result,
                    }
                )
                print(f"\n[{idx:03d}/{len(images):03d}] URL: {img.url}")
                if debug_pass_trace:
                    p2a_model = result.get("source_model_pass2a")
                    p2b_model = result.get("source_model_pass2b")
                    p3_model = (pass3_result or {}).get("source_model")
                    p4_model = (pass4_result or {}).get("source_model")
                    qf = result.get("quality_flags") or {}
                    print("  [PASS TRACE]")
                    print(
                        "  PASS2-A"
                        f" | provider={_provider_from_model(p2a_model)}"
                        f" | model={p2a_model or 'unknown'}"
                        f" | took={trace.get('pass2_ms') if trace.get('pass2_ms') is not None else '-'}ms"
                        f" | api={_raw_api_meta(result.get('raw_api_response_pass2a'))}"
                    )
                    print(
                        "  PASS2-B"
                        f" | executed={'yes' if qf.get('pass2b_executed') else 'no'}"
                        f" | provider={_provider_from_model(p2b_model)}"
                        f" | model={p2b_model or 'unknown'}"
                        f" | api={_raw_api_meta(result.get('raw_api_response_pass2b'))}"
                    )
                    print(
                        "  PASS3-ING"
                        f" | requested={'yes' if trace.get('pass3_requested') else 'no'}"
                        f" | executed={'yes' if bool((pass3_result or {}).get('raw_model_text_pass3_ingredients')) else 'no'}"
                        f" | provider={_provider_from_model(p3_model)}"
                        f" | model={p3_model or 'unknown'}"
                        f" | took={trace.get('pass3_ms') if trace.get('pass3_ms') is not None else '-'}ms"
                        f" | api={_raw_api_meta((pass3_result or {}).get('raw_api_response_pass3_ingredients'))}"
                    )
                    print(
                        "  PASS3-NUT"
                        f" | expected={'yes' if qf.get('has_nutrition_section') else 'no'}"
                        f" | executed={'yes' if bool((pass3_result or {}).get('raw_model_text_pass3_nutrition')) else 'no'}"
                        f" | provider={_provider_from_model(p3_model)}"
                        f" | model={p3_model or 'unknown'}"
                        f" | api={_raw_api_meta((pass3_result or {}).get('raw_api_response_pass3_nutrition'))}"
                    )
                    print(
                        "  PASS4-ING"
                        f" | requested={'yes' if trace.get('pass4_requested') else 'no'}"
                        f" | executed={'yes' if bool((pass4_result or {}).get('raw_model_text_pass4_ingredients')) else 'no'}"
                        f" | provider={_provider_from_model(p4_model)}"
                        f" | model={p4_model or 'unknown'}"
                        f" | took={trace.get('pass4_ms') if trace.get('pass4_ms') is not None else '-'}ms"
                        f" | api={_raw_api_meta((pass4_result or {}).get('raw_api_response_pass4_ingredients'))}"
                    )
                    print(
                        "  PASS4-NUT"
                        f" | executed={'yes' if bool((pass4_result or {}).get('raw_model_text_pass4_nutrition')) else 'no'}"
                        f" | provider={_provider_from_model(p4_model)}"
                        f" | model={p4_model or 'unknown'}"
                        f" | api={_raw_api_meta((pass4_result or {}).get('raw_api_response_pass4_nutrition'))}"
                    )
                    if err:
                        print(f"  TRACE-ERROR | analyzer_error={err}")
                    if pass3_err:
                        print(f"  TRACE-ERROR | pass3_error={pass3_err}")
                    p4_err = (pass4_result or {}).get("pass4_ai_error")
                    if p4_err:
                        print(f"  TRACE-ERROR | pass4_error={p4_err}")

                if adaptive:
                    if _is_transient_error(err):
                        prev = current_limit
                        current_limit = max(1, current_limit - 1)
                        stable_success = 0
                        if current_limit != prev:
                            print(f"  ⚙️ adaptive: 일시 오류 감지 -> 동시성 {prev} -> {current_limit}")
                    else:
                        stable_success += 1
                        if stable_success >= 8 and current_limit < max_concurrency:
                            prev = current_limit
                            current_limit += 1
                            stable_success = 0
                            print(f"  ⚙️ adaptive: 안정 구간 -> 동시성 {prev} -> {current_limit}")

                if delay_sec > 0:
                    time.sleep(delay_sec)

    final_lines: list[str] = []

    def _emit_final(line: str = "") -> None:
        final_lines.append(line)

    pass4_success_rows = [r for r in pass2_pass_rows if bool(r.get("pass3_ok")) and bool(r.get("pass4_ok"))]

    _emit_final("=" * 90)
    _emit_final("PASS4 최종 통과 결과 (Pass4 raw 포함)")
    _emit_final("=" * 90)
    if not pass4_success_rows:
        _emit_final("- 없음")
    else:
        for row in sorted(pass4_success_rows, key=lambda x: x["no"]):
            _emit_final(f"[{row['no']:03d}] URL: {row['url']}")
            _emit_final(f"  제품명: {row.get('pass3_product_name') or 'null'}")
            _emit_final(f"  품목보고번호: {row.get('pass3_report_no') or 'null'}")
            _emit_final(f"  원재료명: {row.get('pass3_ingredients') or 'null'}")
            _emit_final("  [PASS4 RAW]")
            _emit_final(f"  {row.get('pass4_raw') or '(원문 없음)'}")
            _emit_final("-" * 90)

    total_cnt = len(images)
    pass1_pass_cnt = max(0, total_cnt - precheck_skip_cnt)
    pass2_pass_cnt = len(pass2_pass_rows)
    pass3_pass_cnt = sum(1 for r in pass2_pass_rows if bool(r.get("pass3_ok")))
    pass4_pass_cnt = sum(
        1
        for r in pass2_pass_rows
        if bool(r.get("pass3_ok")) and bool(r.get("pass4_ok"))
    )
    pass3_ing_pass_cnt = sum(1 for r in pass2_pass_rows if bool(r.get("pass3_ok")))
    # 비교 가능하도록 영양 트랙 대상/통과는 "pass3 원재료 통과 집합" 기준으로 집계
    pass3_nut_target_cnt = sum(
        1 for r in pass2_pass_rows
        if bool(r.get("pass3_ok")) and bool(r.get("pass3_nut_expected"))
    )
    pass3_nut_pass_cnt = sum(
        1 for r in pass2_pass_rows
        if bool(r.get("pass3_ok")) and bool(r.get("pass3_nut_expected")) and bool(r.get("pass3_nut_pass"))
    )
    pass4_ing_pass_cnt = sum(
        1
        for r in pass2_pass_rows
        if bool(r.get("pass3_ok")) and bool(r.get("pass4_ok"))
    )
    # Pass4 영양도 동일하게 pass4 원재료 트랙 실행 건 기준으로 집계
    pass4_nut_target_cnt = sum(
        1 for r in pass2_pass_rows
        if bool(r.get("pass3_ok")) and bool(r.get("pass4_ing_executed")) and bool(r.get("pass4_nut_executed"))
    )
    pass4_nut_pass_cnt = sum(
        1 for r in pass2_pass_rows
        if bool(r.get("pass3_ok")) and bool(r.get("pass4_ing_executed")) and bool(r.get("pass4_nut_executed")) and bool(r.get("pass4_nut_pass"))
    )

    # Pass2 이후 브랜치 집계
    branch_ing_only = [r for r in pass2_pass_rows if not bool(r.get("pass3_nut_expected"))]
    branch_ing_nut = [r for r in pass2_pass_rows if bool(r.get("pass3_nut_expected"))]

    b1_total = len(branch_ing_only)
    b1_p3_ing = sum(1 for r in branch_ing_only if bool(r.get("pass3_ok")))
    b1_p4_ing = sum(
        1 for r in branch_ing_only
        if bool(r.get("pass3_ok")) and bool(r.get("pass4_ok"))
    )

    b2_total = len(branch_ing_nut)
    b2_p3_ing = sum(1 for r in branch_ing_nut if bool(r.get("pass3_ok")))
    b2_p3_nut = sum(1 for r in branch_ing_nut if bool(r.get("pass3_ok")) and bool(r.get("pass3_nut_pass")))
    b2_p4_ing = sum(
        1 for r in branch_ing_nut
        if bool(r.get("pass3_ok")) and bool(r.get("pass4_ok"))
    )
    b2_p4_nut = sum(
        1 for r in branch_ing_nut
        if bool(r.get("pass3_ok")) and bool(r.get("pass4_nut_executed")) and bool(r.get("pass4_nut_pass")) and (not r.get("pass4_error"))
    )

    _emit_final("")
    _emit_final("[Funnel]")
    _emit_final(f"- 전체: {total_cnt}")
    _emit_final(f"- Pass1 통과: {pass1_pass_cnt}")
    _emit_final(f"- Pass2 통과: {pass2_pass_cnt}")
    _emit_final(f"- Pass3 통과: {pass3_pass_cnt}")
    _emit_final(f"- Pass4 통과: {pass4_pass_cnt}")
    _emit_final(f"- Pass3-원재료 통과: {pass3_ing_pass_cnt}")
    _emit_final(f"- Pass3-영양 대상/통과: {pass3_nut_target_cnt}/{pass3_nut_pass_cnt}")
    _emit_final(f"- Pass4-원재료 통과: {pass4_ing_pass_cnt}")
    _emit_final(f"- Pass4-영양 대상/통과: {pass4_nut_target_cnt}/{pass4_nut_pass_cnt}")

    try:
        SERP_REPORT_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        safe_query = _safe_filename(query)
        safe_provider = _safe_filename(provider)
        report_path = SERP_REPORT_DIR / f"{safe_provider}_{safe_query}_{date_str}.txt"
        report_path.write_text("\n".join(final_lines) + "\n", encoding="utf-8")
        html_report_path = SERP_REPORT_DIR / f"{safe_provider}_{safe_query}_{date_str}.html"

        html_parts: list[str] = []
        html_parts.append("<!doctype html>")
        html_parts.append("<html lang='ko'><head><meta charset='utf-8'>")
        html_parts.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
        html_parts.append(f"<title>SERP 배치 결과 - {html.escape(query)}</title>")
        html_parts.append(
            "<style>"
            "body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px;background:#f7f7f8;color:#111;}"
            ".wrap{max-width:1100px;margin:0 auto;}"
            ".card{background:#fff;border:1px solid #e3e3e6;border-radius:12px;padding:16px;margin-bottom:16px;}"
            ".meta{font-size:13px;color:#555;margin-bottom:8px;}"
            ".funnel{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin:12px 0 18px 0;}"
            ".fcard{background:#fff;border:1px solid #e3e3e6;border-radius:10px;padding:12px;}"
            ".fstep{font-size:12px;color:#666;}.fnum{font-size:22px;font-weight:800;line-height:1.1;margin-top:4px;}"
            ".frate{font-size:12px;color:#444;margin-top:2px;}"
            ".branch-wrap{display:grid;grid-template-columns:1fr;gap:10px;margin:10px 0 18px 0;}"
            ".branch{background:#fff;border:1px solid #e3e3e6;border-radius:10px;padding:12px;}"
            ".branch-title{font-weight:800;margin-bottom:8px;}"
            ".branch-flow{display:flex;flex-wrap:wrap;gap:8px;align-items:center;font-size:13px;}"
            ".chip{background:#f3f4f6;border:1px solid #e5e7eb;border-radius:999px;padding:4px 10px;}"
            ".arrow{color:#888;font-weight:700;}"
            ".grid{display:grid;grid-template-columns:360px 1fr;gap:16px;}"
            ".imgbox{background:#fafafa;border:1px solid #eee;border-radius:10px;padding:8px;}"
            ".imgbox img{width:100%;height:auto;border-radius:8px;display:block;}"
            ".lbl{font-weight:700;margin-top:8px;}"
            ".tree{margin:8px 0 0 0;padding-left:18px;}"
            ".tree li{margin:4px 0;line-height:1.45;}"
            ".tag{display:inline-block;background:#eef2ff;border:1px solid #dbe3ff;border-radius:999px;padding:1px 8px;font-size:12px;margin-right:6px;}"
            ".rawbox{background:#0f172a;color:#e5e7eb;border-radius:10px;padding:10px;max-height:360px;overflow:auto;}"
            "details>summary{cursor:pointer;font-weight:700;margin-top:8px;}"
            "pre{white-space:pre-wrap;word-break:break-word;background:#f4f5f7;border:1px solid #e5e7eb;border-radius:8px;padding:10px;}"
            "a{color:#0b57d0;text-decoration:none;}a:hover{text-decoration:underline;}"
            ".ok{color:#0a7f2e;font-weight:700;}.bad{color:#c21f39;font-weight:700;}"
            "@media (max-width: 900px){.grid{grid-template-columns:1fr;}}"
            "</style>"
        )
        html_parts.append("</head><body><div class='wrap'>")
        html_parts.append(f"<h1>SERP 배치 결과</h1><div class='meta'>검색어: <b>{html.escape(query)}</b> | 날짜: {date_str}</div>")
        html_parts.append("<div class='funnel'>")
        steps = [
            ("전체", total_cnt),
            ("Pass1 통과", pass1_pass_cnt),
            ("Pass2-A 통과", pass2a_pass_cnt),
            ("Pass2-B 통과", pass2b_pass_cnt),
            ("Pass3 통과", pass3_pass_cnt),
            ("Pass4 통과", pass4_pass_cnt),
        ]
        total_base = total_cnt
        prev_count = total_cnt
        for name, count in steps:
            prev_rate = (count / prev_count * 100.0) if prev_count > 0 else 0.0
            total_rate = (count / total_base * 100.0) if total_base > 0 else 0.0
            html_parts.append("<div class='fcard'>")
            html_parts.append(f"<div class='fstep'>{html.escape(name)}</div>")
            html_parts.append(f"<div class='fnum'>{count:,}</div>")
            html_parts.append(f"<div class='frate'>이전단계 대비 {prev_rate:.1f}%</div>")
            html_parts.append(f"<div class='frate'>전체 대비 {total_rate:.1f}%</div>")
            html_parts.append("</div>")
            prev_count = count
        html_parts.append("</div>")
        html_parts.append("<div class='branch-wrap'>")
        html_parts.append("<div class='branch'>")
        html_parts.append("<div class='branch-title'>가지 A: Pass2 통과 후 원재료만(영양성분 대상 아님)</div>")
        html_parts.append("<div class='branch-flow'>")
        html_parts.append(f"<span class='chip'>시작 {b1_total}</span><span class='arrow'>→</span>")
        html_parts.append(f"<span class='chip'>Pass3-원재료 통과 {b1_p3_ing}</span><span class='arrow'>→</span>")
        html_parts.append(f"<span class='chip'>Pass4-원재료 통과 {b1_p4_ing}</span>")
        html_parts.append("</div></div>")
        html_parts.append("<div class='branch'>")
        html_parts.append("<div class='branch-title'>가지 B: Pass2 통과 후 원재료+영양성분(영양성분 대상)</div>")
        html_parts.append("<div class='branch-flow'>")
        html_parts.append(f"<span class='chip'>시작 {b2_total}</span><span class='arrow'>→</span>")
        html_parts.append(f"<span class='chip'>Pass3-원재료 통과 {b2_p3_ing}</span><span class='arrow'>→</span>")
        html_parts.append(f"<span class='chip'>Pass3-영양 통과 {b2_p3_nut}</span><span class='arrow'>→</span>")
        html_parts.append(f"<span class='chip'>Pass4-원재료 통과 {b2_p4_ing}</span><span class='arrow'>→</span>")
        html_parts.append(f"<span class='chip'>Pass4-영양 통과 {b2_p4_nut}</span>")
        html_parts.append("</div></div>")
        html_parts.append("<div class='branch'>")
        html_parts.append("<div class='branch-title'>총합(가지 A + 가지 B)</div>")
        html_parts.append("<div class='branch-flow'>")
        html_parts.append(f"<span class='chip'>Pass2 통과 총합 {b1_total + b2_total}</span><span class='arrow'>|</span>")
        html_parts.append(f"<span class='chip'>Pass3-원재료 통과 총합 {b1_p3_ing + b2_p3_ing}</span><span class='arrow'>|</span>")
        html_parts.append(f"<span class='chip'>Pass3-영양 통과 총합 {b2_p3_nut}</span><span class='arrow'>|</span>")
        html_parts.append(f"<span class='chip'>Pass4-원재료 통과 총합 {b1_p4_ing + b2_p4_ing}</span><span class='arrow'>|</span>")
        html_parts.append(f"<span class='chip'>Pass4-영양 통과 총합 {b2_p4_nut}</span>")
        html_parts.append("</div></div>")
        html_parts.append("</div>")
        def _render_sub_ingredients(items: Any) -> str:
            if not isinstance(items, list) or not items:
                return "<div class='meta'>파싱된 원재료 없음</div>"

            def _node_to_html(node: Any) -> str:
                if not isinstance(node, dict):
                    return ""
                name = node.get("ingredient_name")
                if name is None:
                    name = node.get("name")
                origin = node.get("origin")
                origin_detail = node.get("origin_detail")
                amount = node.get("amount")
                children = node.get("sub_ingredients") or []
                chunks: list[str] = []
                chunks.append("<li>")
                chunks.append(f"<span class='tag'>{html.escape(str(name or 'null'))}</span>")
                if amount:
                    chunks.append(f"함량={html.escape(str(amount))} ")
                if origin:
                    chunks.append(f"원산지={html.escape(str(origin))} ")
                if origin_detail:
                    chunks.append(f"원산지상세={html.escape(str(origin_detail))} ")
                if isinstance(children, list) and children:
                    chunks.append("<ul class='tree'>")
                    for child in children:
                        chunks.append(_node_to_html(child))
                    chunks.append("</ul>")
                chunks.append("</li>")
                return "".join(chunks)

            parts: list[str] = ["<ul class='tree'>"]
            for item in items:
                parts.append(_node_to_html(item))
            parts.append("</ul>")
            return "".join(parts)

        pass3_success_view_rows = sorted(
            [r for r in pass2_pass_rows if bool(r.get("pass3_ok"))],
            key=lambda x: x["no"],
        )
        if not pass3_success_view_rows:
            html_parts.append("<div class='card'><div class='meta'>Pass3 통과 결과가 없습니다.</div></div>")
        else:
            html_parts.append("<div class='card'><div class='meta'>Pass3 통과 결과 전체 (Pass4 상태 포함)</div></div>")
            for row in pass3_success_view_rows:
                no = int(row.get("no") or 0)
                url = str(row.get("url") or "")
                report_no = str(row.get("pass3_report_no") or "null")
                product_name = str(row.get("pass3_product_name") or "").strip()
                ingredients_text = str(row.get("pass3_ingredients") or "null")
                ing_items = row.get("pass4_ingredient_items") or []
                pass4_raw = str(row.get("pass4_raw") or "(원문 없음)")
                pass4_error = str(row.get("pass4_error") or "").strip()
                ing_items_count = int(row.get("pass4_ing_items_count") or 0)
                pass4_ok = bool(row.get("pass4_ok"))

                html_parts.append("<div class='card'>")
                html_parts.append(f"<div class='meta'>[{no:03d}] <a href='{html.escape(url)}' target='_blank' rel='noopener'>{html.escape(url)}</a></div>")
                html_parts.append("<div class='grid'>")
                html_parts.append("<div class='imgbox'>")
                html_parts.append(f"<img src='{html.escape(url)}' loading='lazy' referrerpolicy='no-referrer' onerror=\"this.style.display='none'; this.nextElementSibling.style.display='block';\">")
                html_parts.append("<div style='display:none;color:#888;font-size:13px;'>이미지 로드 실패</div>")
                html_parts.append("</div>")
                html_parts.append("<div>")
                if pass4_ok:
                    html_parts.append("<div><span class='lbl'>Pass4 상태:</span> <span class='ok'>✅ 통과</span></div>")
                else:
                    html_parts.append("<div><span class='lbl'>Pass4 상태:</span> <span class='bad'>❌ 실패</span></div>")
                    fail_reason = pass4_error or ("pass4_no_structured_ingredients" if ing_items_count == 0 else "pass4_not_executed_or_unknown")
                    html_parts.append(f"<div><span class='lbl'>실패 사유:</span> {html.escape(fail_reason)}</div>")
                html_parts.append(f"<div><span class='lbl'>품목보고번호:</span> {html.escape(report_no)}</div>")
                if product_name:
                    html_parts.append(f"<div><span class='lbl'>제품명:</span> {html.escape(product_name)}</div>")
                html_parts.append(f"<div><span class='lbl'>원재료 원문:</span> {html.escape(ingredients_text)}</div>")
                html_parts.append(f"<div><span class='lbl'>원재료 구조화:</span> {len(ing_items) if isinstance(ing_items, list) else 0}개</div>")
                html_parts.append(_render_sub_ingredients(ing_items))
                html_parts.append("<details>")
                html_parts.append("<summary>Pass4 RAW 보기</summary>")
                html_parts.append(f"<pre class='rawbox'>{html.escape(pass4_raw)}</pre>")
                html_parts.append("</details>")
                html_parts.append("</div>")
                html_parts.append("</div>")
                html_parts.append("</div>")

        html_parts.append("</div></body></html>")
        html_report_path.write_text("\n".join(html_parts), encoding="utf-8")

        print(f"\n📁 마지막 결과 저장(txt): {report_path}")
        print(f"🌐 마지막 결과 저장(html): {html_report_path}")
        if auto_open_report:
            try:
                webbrowser.open(html_report_path.resolve().as_uri())
                print("🖥️ 브라우저 자동 열기 완료")
            except Exception as open_exc:  # pylint: disable=broad-except
                print(f"⚠️ 브라우저 자동 열기 실패: {open_exc}")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"\n⚠️ 마지막 결과 파일 저장 실패: {exc}")


def _save_images_only_report(
    *,
    query: str,
    provider: str,
    images: list[ImageCandidate],
    auto_open_report: bool,
) -> None:
    try:
        SERP_REPORT_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        safe_query = _safe_filename(query)
        safe_provider = _safe_filename(provider)
        txt_path = SERP_REPORT_DIR / f"{safe_provider}_{safe_query}_{date_str}_images_only.txt"
        html_path = SERP_REPORT_DIR / f"{safe_provider}_{safe_query}_{date_str}_images_only.html"

        lines: list[str] = []
        lines.append("=== IMAGE ONLY RESULT ===")
        lines.append(f"query={query}")
        lines.append(f"provider={provider}")
        lines.append(f"count={len(images)}")
        lines.append("")
        for i, img in enumerate(images, start=1):
            lines.append(f"[{i:04d}] {img.url}")
            if img.title:
                lines.append(f"  title={img.title}")
            if img.source:
                lines.append(f"  source={img.source}")
        txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        html_parts: list[str] = []
        html_parts.append("<!doctype html><html lang='ko'><head><meta charset='utf-8'>")
        html_parts.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
        html_parts.append(f"<title>Image Only - {html.escape(query)}</title>")
        html_parts.append(
            "<style>"
            "body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px;background:#f7f7f8;color:#111;}"
            ".wrap{max-width:1200px;margin:0 auto;}.card{background:#fff;border:1px solid #e3e3e6;border-radius:12px;padding:14px;margin-bottom:12px;}"
            ".meta{font-size:13px;color:#555;margin-bottom:8px;} .grid{display:grid;grid-template-columns:280px 1fr;gap:12px;}"
            ".imgbox{background:#fafafa;border:1px solid #eee;border-radius:10px;padding:8px;}"
            ".imgbox img{width:100%;height:auto;border-radius:8px;display:block;}"
            "pre{white-space:pre-wrap;word-break:break-word;background:#f4f5f7;border:1px solid #e5e7eb;border-radius:8px;padding:10px;}"
            "a{color:#0b57d0;text-decoration:none;}a:hover{text-decoration:underline;}"
            "@media (max-width: 900px){.grid{grid-template-columns:1fr;}}"
            "</style></head><body><div class='wrap'>"
        )
        html_parts.append(f"<h1>이미지 수집 전용 결과</h1><div class='meta'>검색어: <b>{html.escape(query)}</b> | provider={html.escape(provider)} | count={len(images):,}</div>")
        for i, img in enumerate(images, start=1):
            url = str(img.url or "")
            html_parts.append("<div class='card'>")
            html_parts.append(f"<div class='meta'>[{i:04d}] <a href='{html.escape(url)}' target='_blank' rel='noopener'>{html.escape(url)}</a></div>")
            html_parts.append("<div class='grid'>")
            html_parts.append("<div class='imgbox'>")
            html_parts.append(f"<img src='{html.escape(url)}' loading='lazy' referrerpolicy='no-referrer' onerror=\"this.style.display='none'; this.nextElementSibling.style.display='block';\">")
            html_parts.append("<div style='display:none;color:#888;font-size:13px;'>이미지 로드 실패</div>")
            html_parts.append("</div>")
            html_parts.append("<div>")
            html_parts.append(f"<pre>title={html.escape(str(img.title or ''))}\nsource={html.escape(str(img.source or ''))}</pre>")
            html_parts.append("</div></div></div>")
        html_parts.append("</div></body></html>")
        html_path.write_text("\n".join(html_parts), encoding="utf-8")

        print(f"\n📁 이미지 전용 결과(txt): {txt_path}")
        print(f"🌐 이미지 전용 결과(html): {html_path}")
        if auto_open_report:
            try:
                webbrowser.open(html_path.resolve().as_uri())
                print("🖥️ 브라우저 자동 열기 완료")
            except Exception as open_exc:  # pylint: disable=broad-except
                print(f"⚠️ 브라우저 자동 열기 실패: {open_exc}")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"⚠️ 이미지 전용 결과 저장 실패: {exc}")


def run_query_image_benchmark_interactive() -> None:
    print("\n  🔎 [검색어 기반 이미지 벤치마크]")
    print("  🔹 이미지 검색 엔진 선택")
    print("    [1] Google Images")
    print("    [2] Naver Images (Official OpenAPI)")
    print("    [3] Naver Images (Blog source only)")
    print("    [4] Naver Shop Detail Images")
    raw_provider = input("  선택 > ").strip()
    if raw_provider == "2":
        provider = "naver_official"
    elif raw_provider == "3":
        provider = "naver_blog"
    elif raw_provider == "4":
        provider = "naver_shop"
    else:
        provider = "google"
    query = _choose_benchmark_query_interactive()
    if not query:
        return

    raw_pages = input("  🔹 최대 페이지 수 [기본 20]: ").strip()
    max_pages = 20
    if raw_pages:
        try:
            v = int(raw_pages)
            if v > 0:
                max_pages = v
        except ValueError:
            pass

    # 요청사항: API 종류와 무관하게 페이지당 최대치 고정(100)
    per_page = 100

    delay_sec = 0.0

    raw_conc = input("  🔹 최대 동시 요청 수 [기본 5, 최대 200]: ").strip()
    max_concurrency = 5
    if raw_conc:
        try:
            v = int(raw_conc)
            if v > 0:
                max_concurrency = v
        except ValueError:
            pass

    # 요청사항: adaptive 자동 감속 기능 OFF 고정
    adaptive = False
    raw_open = input("  🔹 실행 후 HTML 자동 열기? [Y/n]: ").strip().lower()
    auto_open_report = not (raw_open in ("n", "no"))
    raw_debug_serp = input("  🔹 SERP 요청/응답 raw 디버그 출력? [y/N]: ").strip().lower()
    debug_serp = raw_debug_serp in ("y", "yes")
    raw_debug_pass = input("  🔹 이미지별 Pass/API 호출 추적 로그 출력? [Y/n]: ").strip().lower()
    debug_pass_trace = not (raw_debug_pass in ("n", "no"))
    print("\n  🚀 실행합니다. 결과는 이미지별로 순차 출력됩니다.")
    run_query_image_benchmark(
        query=query,
        max_pages=max_pages,
        per_page=per_page,
        delay_sec=delay_sec,
        max_concurrency=max_concurrency,
        adaptive=adaptive,
        auto_open_report=auto_open_report,
        provider=provider,
        debug_serp=debug_serp,
        images_only=False,
        debug_pass_trace=debug_pass_trace,
    )


def _choose_benchmark_query_interactive() -> str | None:
    print("  🔹 검색어 선택 방식")
    print("    [1] 직접 입력")
    print("    [2] 검색어풀에서 선택(우선순위 높은 순)")
    mode = input("  선택 > ").strip() or "1"

    if mode == "1":
        query = input("  🔹 검색어 입력: ").strip()
        if not query:
            print("  ⚠️ 검색어를 입력해주세요.")
            return None
        return query

    if mode != "2":
        print("  ⚠️ 올바른 번호를 선택해주세요.")
        return None

    raw_limit = input("  🔹 검색어풀 표시 개수 [기본 30]: ").strip()
    limit = 30
    if raw_limit:
        try:
            v = int(raw_limit)
            if v > 0:
                limit = v
        except ValueError:
            pass

    rows = _load_query_pool_rows(limit=limit)
    if not rows:
        print("  ⚠️ 검색어풀이 비어있거나(query_pool), 아직 초기화되지 않았습니다.")
        return None

    print("\n  📚 [검색어풀 - 우선순위 높은 순]")
    print("  No   점수   상태      실행수  query_id  검색어")
    print("  ─────────────────────────────────────────────────────────────────────────")
    for idx, row in enumerate(rows, start=1):
        score = float(row["priority_score"] or 0.0)
        status = str(row["status"] or "-")
        run_count = int(row["run_count"] or 0)
        qid = int(row["id"])
        text = str(row["query_text"] or "")
        print(f"  {idx:>2}  {score:>6.1f}  {status:<8}  {run_count:>6}  {qid:>8}  {text}")

    raw_no = input("\n  🔹 실행할 번호 선택: ").strip()
    try:
        picked = int(raw_no)
    except ValueError:
        print("  ⚠️ 숫자로 입력해주세요.")
        return None
    if picked < 1 or picked > len(rows):
        print("  ⚠️ 범위를 벗어난 번호입니다.")
        return None

    chosen = str(rows[picked - 1]["query_text"] or "").strip()
    if not chosen:
        print("  ⚠️ 선택한 검색어가 비어있습니다.")
        return None
    print(f"  ✅ 선택된 검색어: {chosen}")
    return chosen


def _load_query_pool_rows(limit: int = 30) -> list[sqlite3.Row]:
    db_path = Path(DB_FILE)
    if not db_path.exists():
        return []

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT id, query_text, priority_score, status, run_count
            FROM query_pool
            ORDER BY priority_score DESC, id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return list(rows)
    except sqlite3.Error:
        return []
    finally:
        conn.close()
