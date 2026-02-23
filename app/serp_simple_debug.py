"""
아주 단순한 SerpAPI 이미지 호출 디버그 스크립트.

예시:
  python3 -m app.serp_simple_debug --provider naver --query "성분표 원재료명" --pages 1 --per-page 20
  python3 -m app.serp_simple_debug --provider google --query "성분표 원재료명" --pages 1 --per-page 20 --show-raw
"""

from __future__ import annotations

import argparse
import os
from typing import Any
from urllib.parse import urlparse

import requests

from app import config as _config

URL = "https://serpapi.com/search.json"
NAVER_OPENAPI_IMAGE_URL = "https://openapi.naver.com/v1/search/image.json"


def build_request(provider: str, query: str, page: int, per_page: int, api_key: str) -> tuple[str, dict[str, Any], dict[str, str] | None]:
    if provider == "naver_official":
        client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
        client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
        if not (client_id and client_secret):
            raise RuntimeError("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 필요")
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
    if provider == "naver":
        return (URL, {
            "engine": "naver",
            "where": "image",
            "query": query,
            "display": per_page,
            "start": page * per_page + 1,
            "no_cache": "true",
            "api_key": api_key,
        }, None)
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


def run_serp_simple_debug_interactive() -> None:
    print("\n  🧩 [초간단 SERP 디버그]")
    print("    [1] Google Images (SerpAPI)")
    print("    [2] Naver Images (SerpAPI)")
    print("    [3] Naver Images (Official OpenAPI)")
    raw_provider = input("  🔹 검색엔진 선택: ").strip()
    if raw_provider == "2":
        provider = "naver"
    elif raw_provider == "3":
        provider = "naver_official"
    else:
        provider = "google"

    query = input("  🔹 검색어: ").strip()
    if not query:
        print("  ⚠️ 검색어를 입력해주세요.")
        return

    raw_pages = input("  🔹 최대 페이지 [기본 1]: ").strip()
    raw_per_page = input("  🔹 페이지당 개수 [기본 20]: ").strip()
    raw_timeout = input("  🔹 timeout(초) [기본 40]: ").strip()
    raw_show = input("  🔹 raw JSON 일부 출력? [y/N]: ").strip().lower()

    pages = int(raw_pages) if raw_pages.isdigit() else 1
    per_page = int(raw_per_page) if raw_per_page.isdigit() else 20
    timeout = int(raw_timeout) if raw_timeout.isdigit() else 40
    show_raw = raw_show in ("y", "yes")

    _config.reload_dotenv()
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if provider != "naver_official" and not api_key:
        print("  ❌ SERPAPI_KEY가 없습니다.")
        return

    pages = max(1, pages)
    per_page = max(1, min(100, per_page))
    timeout = max(5, timeout)

    print("\n=== serp_simple_debug ===")
    print(f"provider={provider} query={query}")
    print(f"pages={pages} per_page={per_page} timeout={timeout}")

    for page in range(pages):
        try:
            req_url, params, headers = build_request(provider, query, page, per_page, api_key)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[PAGE {page}] request_prepare_error={type(exc).__name__}: {exc}")
            return
        safe_params = {k: v for k, v in params.items() if k != "api_key"}
        print("\n----------------------------------------")
        print(f"[PAGE {page}] request params:")
        print(f"endpoint={req_url}")
        print(safe_params)

        try:
            resp = requests.get(req_url, params=params, headers=headers, timeout=timeout)
            print(f"[PAGE {page}] http_status={resp.status_code}")
            data = resp.json()
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[PAGE {page}] request_error={type(exc).__name__}: {exc}")
            continue

        if provider == "naver_official":
            api_error = data.get("errorMessage") or data.get("message") or data.get("errorCode")
            images = data.get("items") or []
        else:
            api_error = data.get("error")
            images = data.get("images_results") or []
        print(f"[PAGE {page}] api_error={api_error}")
        print(f"[PAGE {page}] images_results_count={len(images)}")

        if show_raw:
            raw = json.dumps(data, ensure_ascii=False)[:3000]
            print(f"[PAGE {page}] raw_head:\n{raw}")

        for i, item in enumerate(images[:5], start=1):
            if provider == "naver_official":
                url = item.get("link") or item.get("thumbnail")
                source = urlparse(str(url)).netloc if url else None
            else:
                url = item.get("original") or item.get("thumbnail")
                source = item.get("source")
            title = item.get("title")
            print(f"  - #{i} url={url}")
            print(f"      title={title}")
            print(f"      source={source}")


def main() -> None:
    parser = argparse.ArgumentParser(description="SerpAPI 초간단 디버그")
    parser.add_argument("--provider", choices=["google", "naver", "naver_official"], default="google")
    parser.add_argument("--query", required=True)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--per-page", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=40)
    args = parser.parse_args()

    _config.reload_dotenv()
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if args.provider != "naver_official" and not api_key:
        raise SystemExit("SERPAPI_KEY가 없습니다.")

    pages = max(1, int(args.pages))
    per_page = max(1, min(100, int(args.per_page)))
    timeout = max(5, int(args.timeout))

    print("=== serp_simple_debug ===")
    print(f"provider={args.provider} query={args.query}")
    print(f"pages={pages} per_page={per_page} timeout={timeout}")

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

        if args.provider == "naver_official":
            api_error = data.get("errorMessage") or data.get("message") or data.get("errorCode")
            images = data.get("items") or []
        else:
            api_error = data.get("error")
            images = data.get("images_results") or []
        if api_error:
            print(f"  api_error={api_error}")
            continue
        print(f"  images_results_count={len(images)}")

        for i, item in enumerate(images, start=1):
            if args.provider == "naver_official":
                url = item.get("link") or item.get("thumbnail")
                source = urlparse(str(url)).netloc if url else None
                thumb = item.get("thumbnail")
                width = item.get("sizewidth")
                height = item.get("sizeheight")
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


if __name__ == "__main__":
    main()
