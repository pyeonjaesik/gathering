"""
아주 단순한 SerpAPI 이미지 호출 디버그 스크립트.

예시:
  python3 -m app.serp_simple_debug --provider google --query "성분표 원재료명" --pages 1 --per-page 20
  python3 -m app.serp_simple_debug --provider naver_serpapi --query "성분표 원재료명" --pages 1 --per-page 20
  python3 -m app.serp_simple_debug --provider naver_official --query "성분표 원재료명" --pages 1 --per-page 20
"""

from __future__ import annotations

import argparse
import os
from typing import Any
from urllib.parse import urlparse, urljoin
from collections import Counter
from pathlib import Path
from datetime import datetime
import webbrowser
import html as html_lib
import re

import requests

from app import config as _config

URL = "https://serpapi.com/search.json"
NAVER_OPENAPI_IMAGE_URL = "https://openapi.naver.com/v1/search/image.json"
NAVER_OPENAPI_SHOP_URL = "https://openapi.naver.com/v1/search/shop.json"
SERP_REPORT_DIR = Path("reports/serp_batch")


def build_request(provider: str, query: str, page: int, per_page: int, api_key: str) -> tuple[str, dict[str, Any], dict[str, str] | None]:
    if provider == "naver_serpapi":
        return (
            URL,
            {
                "engine": "naver",
                "where": "image",
                "query": query,
                "num": per_page,
                "page": page + 1,
                "no_cache": "true",
                "api_key": api_key,
            },
            None,
        )
    if provider in ("naver_official", "naver_blog", "naver_shop"):
        client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
        client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
        if not (client_id and client_secret):
            raise RuntimeError("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 필요")
        if provider == "naver_blog":
            return (
                NAVER_OPENAPI_IMAGE_URL,
                {
                    "query": query,
                    "display": per_page,
                    "start": page * per_page + 1,
                    "sort": "sim",
                },
                {
                    "X-Naver-Client-Id": client_id,
                    "X-Naver-Client-Secret": client_secret,
                },
            )
        if provider == "naver_shop":
            return (
                NAVER_OPENAPI_SHOP_URL,
                {
                    "query": query,
                    "display": per_page,
                    "start": page * per_page + 1,
                    "sort": "sim",
                },
                {
                    "X-Naver-Client-Id": client_id,
                    "X-Naver-Client-Secret": client_secret,
                },
            )
        return (
            NAVER_OPENAPI_IMAGE_URL,
            {
                "query": query,
                "display": per_page,
                "start": page * per_page + 1,
                "sort": "sim",
            },
            {
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
            },
        )
    return (URL, {
        "engine": "google_images",
        "q": query,
        "num": per_page,
        "ijn": page,
        "hl": "ko",
        "gl": "kr",
        "no_cache": "true",
        "api_key": api_key,
    }, None)


def _extract_images_from_shop_detail(product_url: str, timeout: int) -> list[str]:
    pw_images = _extract_images_from_shop_detail_playwright(product_url, timeout=timeout)
    if pw_images:
        return pw_images

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
    except Exception:
        return []

    found: list[tuple[str, int]] = []
    seen: set[str] = set()
    for pat in (
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<img[^>]+src=["\']([^"\']+)["\']',
        r'<img[^>]+data-src=["\']([^"\']+)["\']',
        r'"image"\s*:\s*"([^"]+)"',
    ):
        for m in re.finditer(pat, html_text, re.IGNORECASE):
            u = (m.group(1) or "").replace("\\/", "/").strip()
            if u.startswith("//"):
                u = "https:" + u
            u = urljoin(final_url, u)
            low = u.lower()
            if not (low.startswith("http://") or low.startswith("https://")):
                continue
            if any(x in low for x in ("sprite", "icon", "logo", "badge", "blank", "loading")):
                continue
            sc = _score_shop_image_candidate(u, meta_text=pat, width=None, height=None)
            if u not in seen and sc >= 1:
                seen.add(u)
                found.append((u, sc))
    found.sort(key=lambda x: x[1], reverse=True)
    return [u for u, _ in found[:20]]


def _extract_images_from_shop_detail_playwright(product_url: str, timeout: int) -> list[str]:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return []

    timeout_ms = max(5000, int(timeout * 1000))
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
            page.goto(product_url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1800)
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
                "els => els.map(e => getComputedStyle(e).backgroundImage || '')",
            )
            html_text = page.content()
            for obj in img_objs:
                raw_s = str((obj or {}).get("src") or "").strip()
                srcset = str((obj or {}).get("srcset") or "").strip()
                srcset_cand = [x.strip().split(" ")[0] for x in srcset.split(",")] if srcset else []
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
                    u = str(c or "").strip()
                    if u.startswith("//"):
                        u = "https:" + u
                    low = u.lower()
                    if not (low.startswith("http://") or low.startswith("https://")):
                        continue
                    if any(x in low for x in ("sprite", "icon", "logo", "badge", "blank", "loading")):
                        continue
                    sc = _score_shop_image_candidate(u, meta_text=meta_text, width=w, height=h)
                    if u not in seen and sc >= 2:
                        seen.add(u)
                        found.append((u, sc))
            for b in bg_urls:
                bs = str(b or "")
                for m in re.finditer(r'url\(["\']?([^"\')]+)', bs, re.IGNORECASE):
                    u = m.group(1).strip()
                    if u.startswith("//"):
                        u = "https:" + u
                    low = u.lower()
                    if not (low.startswith("http://") or low.startswith("https://")):
                        continue
                    if any(x in low for x in ("sprite", "icon", "logo", "badge", "blank", "loading")):
                        continue
                    sc = _score_shop_image_candidate(u, meta_text="background-image", width=None, height=None)
                    if u not in seen and sc >= 2:
                        seen.add(u)
                        found.append((u, sc))
            for m in re.finditer(r'https?://[^\s"\\\']+\.(?:jpg|jpeg|png|webp|gif|bmp)(?:\?[^\s"\\\']*)?', html_text, re.IGNORECASE):
                u = m.group(0).strip()
                low = u.lower()
                if any(x in low for x in ("sprite", "icon", "logo", "badge", "blank", "loading")):
                    continue
                sc = _score_shop_image_candidate(u, meta_text="html_text", width=None, height=None)
                if u not in seen and sc >= 2:
                    seen.add(u)
                    found.append((u, sc))
            browser.close()
    except Exception:
        return []
    found.sort(key=lambda x: x[1], reverse=True)
    return [u for u, _ in found[:20]]


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


def run_serp_simple_debug_interactive() -> None:
    print("\n  🧩 [초간단 SERP 디버그]")
    print("    [1] Google Images (SerpAPI)")
    print("    [2] Naver Images (SerpAPI)")
    print("    [3] Naver Images (Official OpenAPI)")
    print("    [4] Naver Images (Blog source only)")
    print("    [5] Naver Shop Detail Images")
    raw_provider = input("  🔹 검색엔진 선택: ").strip()
    if raw_provider == "2":
        provider = "naver_serpapi"
    elif raw_provider == "3":
        provider = "naver_official"
    elif raw_provider == "4":
        provider = "naver_blog"
    elif raw_provider == "5":
        provider = "naver_shop"
    else:
        provider = "google"

    query = input("  🔹 검색어: ").strip()
    if not query:
        print("  ⚠️ 검색어를 입력해주세요.")
        return

    raw_pages = input("  🔹 최대 페이지 [기본 1]: ").strip()
    raw_per_page = input("  🔹 페이지당 개수 [기본 20]: ").strip()
    raw_timeout = input("  🔹 timeout(초) [기본 40]: ").strip()
    raw_open = input("  🔹 실행 후 브라우저로 결과 열기? [Y/n]: ").strip().lower()

    pages = int(raw_pages) if raw_pages.isdigit() else 1
    per_page = int(raw_per_page) if raw_per_page.isdigit() else 20
    timeout = int(raw_timeout) if raw_timeout.isdigit() else 40

    _config.reload_dotenv()
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if provider not in ("naver_official", "naver_blog", "naver_shop") and not api_key:
        print("  ❌ SERPAPI_KEY가 없습니다.")
        return

    pages = max(1, pages)
    per_page = max(1, min(100, per_page))
    timeout = max(5, timeout)
    auto_open = not (raw_open in ("n", "no"))

    print("\n=== serp_simple_debug ===")
    print(f"provider={provider} query={query}")
    print(f"pages={pages} per_page={per_page} timeout={timeout}")
    reject_domain_counter: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []

    for page in range(pages):
        try:
            req_url, params, headers = build_request(provider, query, page, per_page, api_key)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[PAGE {page}] request_prepare_error={type(exc).__name__}: {exc}")
            return
        print("\n----------------------------------------")
        print(f"[PAGE {page}]")

        try:
            resp = requests.get(req_url, params=params, headers=headers, timeout=timeout)
            data = resp.json()
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[PAGE {page}] request_error={type(exc).__name__}: {exc}")
            continue

        if provider == "naver_official":
            api_error = data.get("errorMessage") or data.get("message") or data.get("errorCode")
            images = data.get("items") or []
        elif provider == "naver_blog":
            api_error = data.get("errorMessage") or data.get("message") or data.get("errorCode")
            all_items = data.get("items") or []
            images = []
            for it in all_items:
                link = str(it.get("link") or "").lower()
                thumb = str(it.get("thumbnail") or "").lower()
                is_blog = any(
                    key in link or key in thumb
                    for key in (
                        "blog.naver.com",
                        "blogfiles.pstatic.net",
                        "postfiles.pstatic.net",
                        "mblogthumb-phinf.pstatic.net",
                        "phinf.pstatic.net",
                        "blog",
                    )
                )
                if is_blog:
                    images.append(it)
                else:
                    dom = urlparse(str(it.get("link") or "")).netloc or "(unknown)"
                    reject_domain_counter[dom] += 1
        elif provider == "naver_shop":
            api_error = data.get("errorMessage") or data.get("message") or data.get("errorCode")
            images = data.get("items") or []
        else:
            api_error = data.get("error")
            images = data.get("images_results") or []
        if api_error:
            print(f"  api_error={api_error}")
            rows.append(
                {
                    "page": page,
                    "api_error": str(api_error),
                    "raw_items_count": len(all_items) if provider == "naver_blog" else None,
                    "filtered_count": 0,
                    "items": [],
                }
            )
            continue
        if provider == "naver_blog":
            print(f"  raw_items_count={len(all_items)}")
            print(f"  blog_filtered_count={len(images)}")
        print(f"  images_results_count={len(images)}")

        page_items: list[dict[str, Any]] = []
        for i, item in enumerate(images, start=1):
            if provider == "naver_official":
                url = item.get("link") or item.get("thumbnail")
                source = urlparse(str(url)).netloc if url else None
                thumb = item.get("thumbnail")
                width = item.get("sizewidth")
                height = item.get("sizeheight")
            elif provider == "naver_blog":
                url = item.get("link")
                source = urlparse(str(url)).netloc if url else None
                thumb = item.get("thumbnail")
                width = item.get("sizewidth")
                height = item.get("sizeheight")
            elif provider == "naver_shop":
                detail_url = item.get("link")
                source = str(item.get("mallName") or "") or (urlparse(str(detail_url)).netloc if detail_url else None)
                detail_images = _extract_images_from_shop_detail(str(detail_url or ""), timeout=timeout)
                url = detail_url
                thumb = item.get("image")
                width = None
                height = None
                title = item.get("title")
                print(f"  - #{i} url={url}")
                print(f"      title={title}")
                print(f"      source={source}")
                print(f"      thumbnail={thumb}")
                print(f"      detail_images={len(detail_images)}")
                page_items.append(
                    {
                        "idx": i,
                        "url": str(url or ""),
                        "title": str(title or ""),
                        "source": str(source or ""),
                        "thumbnail": str(thumb or ""),
                        "size": f"{width}x{height}",
                        "detail_images": [str(x) for x in detail_images],
                    }
                )
                continue
            elif provider == "naver_serpapi":
                url = item.get("original") or item.get("thumbnail") or item.get("link")
                source = item.get("source") or (urlparse(str(item.get("link") or "")).netloc if item.get("link") else None)
                thumb = item.get("thumbnail")
                width = item.get("original_width") or item.get("width")
                height = item.get("original_height") or item.get("height")
            else:
                url = item.get("original") or item.get("thumbnail")
                source = item.get("source")
                thumb = item.get("thumbnail")
                width = item.get("original_width") or item.get("width")
                height = item.get("original_height") or item.get("height")
            title = item.get("title")
            print(f"  - #{i} url={url}")
            print(f"      title={title}")
            print(f"      source={source}")
            print(f"      thumbnail={thumb}")
            print(f"      size={width}x{height}")
            page_items.append(
                {
                    "idx": i,
                    "url": str(url or ""),
                    "title": str(title or ""),
                    "source": str(source or ""),
                    "thumbnail": str(thumb or ""),
                    "size": f"{width}x{height}",
                }
            )
        rows.append(
            {
                "page": page,
                "api_error": None,
                "raw_items_count": len(all_items) if provider == "naver_blog" else None,
                "filtered_count": len(images),
                "items": page_items,
            }
        )

    if provider == "naver_blog":
        print("\n[blog filter debug] 탈락 도메인 상위")
        if not reject_domain_counter:
            print("  - 없음")
        else:
            for dom, cnt in reject_domain_counter.most_common(10):
                print(f"  - {dom}: {cnt}")
    _write_debug_html_report(
        provider=provider,
        query=query,
        pages=pages,
        per_page=per_page,
        rows=rows,
        reject_domain_counter=reject_domain_counter,
        auto_open=auto_open,
    )


def _write_debug_html_report(
    *,
    provider: str,
    query: str,
    pages: int,
    per_page: int,
    rows: list[dict[str, Any]],
    reject_domain_counter: Counter[str],
    auto_open: bool,
) -> None:
    SERP_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_q = "".join(c if c.isalnum() or c in ("_", "-", ".") else "_" for c in query)[:80] or "query"
    out_path = SERP_REPORT_DIR / f"debug_{provider}_{safe_q}_{date_str}.html"

    total_items = sum(len(r.get("items") or []) for r in rows if not r.get("api_error"))
    page_ok = sum(1 for r in rows if not r.get("api_error"))

    parts: list[str] = []
    parts.append("<!doctype html><html lang='ko'><head><meta charset='utf-8'>")
    parts.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    parts.append("<title>SERP Debug Report</title>")
    parts.append(
        "<style>"
        "body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f6f7fb;color:#111;margin:0;}"
        ".wrap{max-width:1200px;margin:0 auto;padding:20px;}"
        ".hero{background:#fff;border:1px solid #e4e7ef;border-radius:14px;padding:16px;margin-bottom:16px;}"
        ".kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-top:10px;}"
        ".kpi{background:#f8fafc;border:1px solid #e5e7eb;border-radius:10px;padding:10px;}"
        ".kpi .n{font-size:24px;font-weight:800;}"
        ".card{background:#fff;border:1px solid #e4e7ef;border-radius:12px;padding:14px;margin-bottom:12px;}"
        ".bad{color:#b42318;font-weight:700;}.ok{color:#067647;font-weight:700;}"
        ".meta{font-size:13px;color:#667085;}"
        ".grid{display:grid;grid-template-columns:200px 1fr;gap:12px;margin-top:10px;}"
        ".imgbox{border:1px solid #eaecf0;border-radius:10px;padding:8px;background:#fcfcfd;}"
        ".imgbox img{width:100%;height:auto;border-radius:8px;display:block;}"
        "pre{white-space:pre-wrap;word-break:break-word;background:#f9fafb;border:1px solid #eaecf0;border-radius:8px;padding:8px;}"
        "a{color:#175cd3;text-decoration:none;} a:hover{text-decoration:underline;}"
        "</style></head><body><div class='wrap'>"
    )
    parts.append("<div class='hero'>")
    parts.append(f"<h1 style='margin:0 0 6px 0;'>SERP 디버그 리포트</h1>")
    parts.append(
        f"<div class='meta'>provider=<b>{html_lib.escape(provider)}</b> | "
        f"query=<b>{html_lib.escape(query)}</b> | pages={pages} | per_page={per_page}</div>"
    )
    parts.append("<div class='kpis'>")
    parts.append(f"<div class='kpi'><div class='meta'>정상 페이지</div><div class='n'>{page_ok}</div></div>")
    parts.append(f"<div class='kpi'><div class='meta'>총 이미지</div><div class='n'>{total_items}</div></div>")
    parts.append(f"<div class='kpi'><div class='meta'>에러 페이지</div><div class='n'>{len(rows)-page_ok}</div></div>")
    parts.append("</div></div>")

    if provider == "naver_blog":
        parts.append("<div class='card'><h3 style='margin-top:0;'>Blog 필터 탈락 도메인 TOP 10</h3>")
        if reject_domain_counter:
            for dom, cnt in reject_domain_counter.most_common(10):
                parts.append(f"<div class='meta'>- {html_lib.escape(dom)}: {cnt}</div>")
        else:
            parts.append("<div class='meta'>- 없음</div>")
        parts.append("</div>")

    for row in rows:
        page = int(row.get("page", 0))
        err = row.get("api_error")
        items = row.get("items") or []
        parts.append("<div class='card'>")
        parts.append(f"<h3 style='margin:0 0 4px 0;'>Page {page}</h3>")
        if err:
            parts.append(f"<div class='bad'>API Error: {html_lib.escape(str(err))}</div>")
            parts.append("</div>")
            continue
        if provider == "naver_blog":
            parts.append(
                f"<div class='meta'>raw_items_count={row.get('raw_items_count')} | "
                f"blog_filtered_count={row.get('filtered_count')}</div>"
            )
        parts.append(f"<div class='ok'>images_results_count={len(items)}</div>")
        for it in items:
            u = str(it.get("url") or "")
            t = str(it.get("title") or "")
            s = str(it.get("source") or "")
            th = str(it.get("thumbnail") or "")
            sz = str(it.get("size") or "")
            detail_images = it.get("detail_images") or []
            parts.append("<div class='grid'>")
            parts.append("<div class='imgbox'>")
            if th:
                parts.append(
                    f"<img src='{html_lib.escape(th)}' loading='lazy' referrerpolicy='no-referrer' "
                    f"onerror=\"this.style.display='none'; this.nextElementSibling.style.display='block';\">"
                )
                parts.append("<div style='display:none' class='meta'>썸네일 로드 실패</div>")
            else:
                parts.append("<div class='meta'>썸네일 없음</div>")
            parts.append("</div>")
            parts.append(
                "<pre>"
                f"url: {html_lib.escape(u)}\n"
                f"title: {html_lib.escape(t)}\n"
                f"source: {html_lib.escape(s)}\n"
                f"thumbnail: {html_lib.escape(th)}\n"
                f"size: {html_lib.escape(sz)}\n"
                "</pre>"
            )
            if provider == "naver_shop":
                parts.append(
                    f"<div class='meta' style='grid-column:1 / -1;margin-top:6px;'>"
                    f"상세페이지 내부 이미지 추출 개수: <b>{len(detail_images)}</b>"
                    f"</div>"
                )
            if detail_images:
                parts.append("<div style='grid-column:1 / -1;'>")
                parts.append("<div class='meta' style='font-weight:700;margin-bottom:6px;'>상세페이지 내부 이미지</div>")
                parts.append("<div style='display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:8px;'>")
                for durl in detail_images:
                    d = str(durl or "")
                    if not d:
                        continue
                    parts.append("<div class='imgbox'>")
                    parts.append(f"<a href='{html_lib.escape(d)}' target='_blank' rel='noopener'>")
                    parts.append(
                        f"<img src='{html_lib.escape(d)}' loading='lazy' referrerpolicy='no-referrer' "
                        "onerror=\"this.style.display='none'; this.nextElementSibling.style.display='block';\">"
                    )
                    parts.append("<div style='display:none' class='meta'>이미지 로드 실패</div>")
                    parts.append("</a>")
                    parts.append(f"<div class='meta' style='margin-top:4px;word-break:break-all;'><a href='{html_lib.escape(d)}' target='_blank' rel='noopener'>{html_lib.escape(d)}</a></div>")
                    parts.append("</div>")
                parts.append("</div></div>")
            elif provider == "naver_shop":
                parts.append(
                    "<div class='meta' style='grid-column:1 / -1;margin-top:6px;'>"
                    "상세페이지 내부 이미지를 추출하지 못했습니다. "
                    "현재 페이지가 JS 렌더링/접근제한일 수 있습니다."
                    "</div>"
                )
            parts.append("</div>")
        parts.append("</div>")

    parts.append("</div></body></html>")
    out_path.write_text("\n".join(parts), encoding="utf-8")
    print(f"\n🌐 디버그 리포트(html): {out_path}")
    if auto_open:
        try:
            webbrowser.open(out_path.resolve().as_uri())
            print("🖥️ 브라우저 자동 열기 완료")
        except Exception as exc:  # pylint: disable=broad-except
            print(f"⚠️ 브라우저 자동 열기 실패: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="SerpAPI 초간단 디버그")
    parser.add_argument("--provider", choices=["google", "naver_serpapi", "naver_official", "naver_blog", "naver_shop"], default="google")
    parser.add_argument("--query", required=True)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--per-page", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=40)
    args = parser.parse_args()

    _config.reload_dotenv()
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if args.provider not in ("naver_official", "naver_blog", "naver_shop") and not api_key:
        raise SystemExit("SERPAPI_KEY가 없습니다.")

    pages = max(1, int(args.pages))
    per_page = max(1, min(100, int(args.per_page)))
    timeout = max(5, int(args.timeout))

    print("=== serp_simple_debug ===")
    print(f"provider={args.provider} query={args.query}")
    print(f"pages={pages} per_page={per_page} timeout={timeout}")
    reject_domain_counter: Counter[str] = Counter()

    for page in range(pages):
        req_url, params, headers = build_request(args.provider, args.query, page, per_page, api_key)
        print("\n----------------------------------------")
        print(f"[PAGE {page}]")

        try:
            resp = requests.get(req_url, params=params, headers=headers, timeout=timeout)
            data = resp.json()
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[PAGE {page}] request_error={type(exc).__name__}: {exc}")
            continue

        if args.provider in ("naver_official", "naver_blog", "naver_shop"):
            api_error = data.get("errorMessage") or data.get("message") or data.get("errorCode")
            if args.provider == "naver_blog":
                all_items = data.get("items") or []
                images = []
                for it in all_items:
                    link = str(it.get("link") or "").lower()
                    thumb = str(it.get("thumbnail") or "").lower()
                    is_blog = any(
                        key in link or key in thumb
                        for key in (
                            "blog.naver.com",
                            "blogfiles.pstatic.net",
                            "postfiles.pstatic.net",
                            "mblogthumb-phinf.pstatic.net",
                            "phinf.pstatic.net",
                            "blog",
                        )
                    )
                    if is_blog:
                        images.append(it)
                    else:
                        dom = urlparse(str(it.get("link") or "")).netloc or "(unknown)"
                        reject_domain_counter[dom] += 1
            else:
                images = data.get("items") or []
        else:
            api_error = data.get("error")
            images = data.get("images_results") or []
        if api_error:
            print(f"  api_error={api_error}")
            continue
        if args.provider == "naver_blog":
            print(f"  raw_items_count={len(all_items)}")
            print(f"  blog_filtered_count={len(images)}")
        print(f"  images_results_count={len(images)}")

        for i, item in enumerate(images, start=1):
            if args.provider == "naver_official":
                url = item.get("link") or item.get("thumbnail")
                source = urlparse(str(url)).netloc if url else None
                thumb = item.get("thumbnail")
                width = item.get("sizewidth")
                height = item.get("sizeheight")
            elif args.provider == "naver_blog":
                url = item.get("link")
                source = urlparse(str(url)).netloc if url else None
                thumb = item.get("thumbnail")
                width = item.get("sizewidth")
                height = item.get("sizeheight")
            elif args.provider == "naver_shop":
                detail_url = item.get("link")
                source = str(item.get("mallName") or "") or (urlparse(str(detail_url)).netloc if detail_url else None)
                detail_images = _extract_images_from_shop_detail(str(detail_url or ""), timeout=timeout)
                if detail_images:
                    for j, durl in enumerate(detail_images, start=1):
                        title = f"{item.get('title') or ''} (detail #{j})"
                        print(f"  - #{i}.{j} url={durl}")
                        print(f"      title={title}")
                        print(f"      source={source}")
                        print(f"      thumbnail={durl}")
                        print("      size=NonexNone")
                    continue
                url = detail_url
                thumb = item.get("image")
                width = None
                height = None
            elif args.provider == "naver_serpapi":
                url = item.get("original") or item.get("thumbnail") or item.get("link")
                source = item.get("source")
                thumb = item.get("thumbnail")
                width = item.get("original_width") or item.get("width")
                height = item.get("original_height") or item.get("height")
            else:
                url = item.get("original") or item.get("thumbnail")
                source = item.get("source")
                thumb = item.get("thumbnail")
                width = item.get("original_width") or item.get("width")
                height = item.get("original_height") or item.get("height")
            title = item.get("title")
            print(f"  - #{i} url={url}")
            print(f"      title={title}")
            print(f"      source={source}")
            print(f"      thumbnail={thumb}")
            print(f"      size={width}x{height}")

    if args.provider == "naver_blog":
        print("\n[blog filter debug] 탈락 도메인 상위")
        if not reject_domain_counter:
            print("  - 없음")
        else:
            for dom, cnt in reject_domain_counter.most_common(10):
                print(f"  - {dom}: {cnt}")


if __name__ == "__main__":
    main()
