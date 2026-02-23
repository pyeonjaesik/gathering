"""
SerpAPI 이미지 수집만 단독으로 점검하는 테스트 도구.

용도:
- analyzer/pass 로직 없이 이미지 수집 API 호출 자체가 정상인지 확인
- provider(google/naver_official/naver_blog/naver_shop)별 응답 구조/오류를 빠르게 확인
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any
from urllib.parse import urlparse

import requests

# app.config import 시 프로젝트 .env가 자동 로드됨
from app import config as _app_config

SERPAPI_URL = "https://serpapi.com/search.json"
NAVER_OPENAPI_IMAGE_URL = "https://openapi.naver.com/v1/search/image.json"
NAVER_OPENAPI_SHOP_URL = "https://openapi.naver.com/v1/search/shop.json"


def build_request(provider: str, query: str, page: int, per_page: int, api_key: str) -> tuple[str, dict[str, Any], dict[str, str] | None]:
    if provider in ("naver_official", "naver_blog", "naver_shop"):
        client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
        client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
        if not (client_id and client_secret):
            raise RuntimeError("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 필요합니다.")
        if provider == "naver_blog":
            params = {
                "query": query,
                "display": per_page,
                "start": page * per_page + 1,
                "sort": "sim",
            }
            headers = {
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
            }
            return (NAVER_OPENAPI_IMAGE_URL, params, headers)
        if provider == "naver_shop":
            params = {
                "query": query,
                "display": per_page,
                "start": page * per_page + 1,
                "sort": "sim",
            }
            headers = {
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
            }
            return (NAVER_OPENAPI_SHOP_URL, params, headers)
        params = {
            "query": query,
            "display": per_page,
            "start": page * per_page + 1,
            "sort": "sim",
        }
        headers = {
            "X-Naver-Client-Id": client_id,
            "X-Naver-Client-Secret": client_secret,
        }
        return (NAVER_OPENAPI_IMAGE_URL, params, headers)

    return (SERPAPI_URL, {
        "engine": "google_images",
        "q": query,
        "hl": "ko",
        "gl": "kr",
        "num": per_page,
        "ijn": page,
        "api_key": api_key,
        "no_cache": "true",
    }, None)


def run_serp_test(
    *,
    provider: str,
    query: str,
    max_pages: int,
    per_page: int,
    timeout: int,
    retries: int,
    print_raw: bool,
) -> int:
    _app_config.reload_dotenv()
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if provider not in ("naver_official", "naver_blog", "naver_shop") and not api_key:
        print("❌ SERPAPI_KEY 환경변수가 필요합니다.")
        return 1

    print("=== Serp Only Test ===")
    print(f"- provider: {provider}")
    print(f"- query   : {query}")
    print(f"- pages   : {max_pages}")
    print(f"- per_page: {per_page}")
    print(f"- timeout : {timeout}s")
    print(f"- retries : {retries}")

    total = 0
    seen: set[str] = set()
    for page in range(max_pages):
        try:
            req_url, params, headers = build_request(provider, query, page, per_page, api_key)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"\n[page={page}] request_prepare_error={type(exc).__name__}: {exc}")
            return 1
        last_err: str | None = None
        payload: dict[str, Any] | None = None
        status_code: int | None = None

        for attempt in range(retries + 1):
            try:
                resp = requests.get(req_url, params=params, headers=headers, timeout=timeout)
                status_code = resp.status_code
                payload = resp.json()
                if provider in ("naver_official", "naver_blog", "naver_shop"):
                    api_error = payload.get("errorMessage") or payload.get("message") or payload.get("errorCode")
                else:
                    api_error = payload.get("error")
                if status_code == 200 and api_error is None:
                    break
                last_err = f"http={status_code} api_error={api_error}"
                if attempt < retries:
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                break
            except Exception as exc:  # pylint: disable=broad-except
                last_err = f"{type(exc).__name__}: {exc}"
                if attempt < retries:
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                break

        print(f"\n[page={page}] status={status_code} err={last_err or '-'}")

        if print_raw and payload is not None:
            raw = json.dumps(payload, ensure_ascii=False)[:2500]
            print("[raw]")
            print(raw)

        if payload is None:
            print("  - payload 없음(네트워크/파싱 실패)")
            continue

        if status_code != 200 or payload.get("error"):
            err = str(payload.get("error") or last_err or "unknown_error")
            # 결과 없음은 정상 종료로 취급
            if "hasn't returned any results" in err.lower():
                print("  - 더 이상 결과 없음(정상 종료)")
                break
            print("  - 실패 페이지, 다음 페이지로 진행")
            continue

        if provider == "naver_official":
            images = payload.get("items") or []
        elif provider == "naver_blog":
            all_items = payload.get("items") or []
            images = []
            for it in all_items:
                link = str(it.get("link") or "").lower()
                thumb = str(it.get("thumbnail") or "").lower()
                if any(
                    key in link or key in thumb
                    for key in (
                        "blog.naver.com",
                        "blogfiles.pstatic.net",
                        "postfiles.pstatic.net",
                        "mblogthumb-phinf.pstatic.net",
                        "phinf.pstatic.net",
                        "blog",
                    )
                ):
                    images.append(it)
        elif provider == "naver_shop":
            images = payload.get("items") or []
        else:
            images = payload.get("images_results") or []
        if not images:
            print("  - images_results 비어있음(종료)")
            break

        page_new = 0
        for i, item in enumerate(images, start=1):
            if provider == "naver_official":
                url = item.get("link") or item.get("thumbnail")
            elif provider == "naver_blog":
                url = item.get("link")
            elif provider == "naver_shop":
                url = item.get("link")
            else:
                url = item.get("original") or item.get("thumbnail")
            if not url or url in seen:
                continue
            seen.add(url)
            page_new += 1
            if i <= 5:
                title = str(item.get("title") or "").strip()
                if provider == "naver_official":
                    source = urlparse(str(url)).netloc if url else ""
                elif provider == "naver_blog":
                    source = urlparse(str(url)).netloc if url else ""
                elif provider == "naver_shop":
                    source = str(item.get("mallName") or "") or (urlparse(str(url)).netloc if url else "")
                else:
                    source = str(item.get("source") or "").strip()
                print(f"  - #{i:02d} {url}")
                if title:
                    print(f"      title : {title}")
                if source:
                    print(f"      source: {source}")
        total += page_new
        print(f"  - 새 URL: {page_new} (누적 {total})")
        if page_new == 0:
            print("  - 신규 URL이 없어 종료")
            break

    print("\n=== 요약 ===")
    print(f"- 총 고유 이미지 URL: {total}")
    return 0


def run_serp_test_interactive() -> None:
    print("\n  🌐 [SERP 단독 테스트]")
    print("    [1] Google Images")
    print("    [2] Naver Images (Official OpenAPI)")
    print("    [3] Naver Images (Blog source only)")
    print("    [4] Naver Shop Detail Images")
    raw_provider = input("  🔹 검색엔진 선택: ").strip()
    if raw_provider == "2":
        provider = "naver_official"
    elif raw_provider == "3":
        provider = "naver_blog"
    elif raw_provider == "4":
        provider = "naver_shop"
    else:
        provider = "google"

    query = input("  🔹 검색어 입력: ").strip()
    if not query:
        print("  ⚠️ 검색어를 입력해주세요.")
        return

    raw_pages = input("  🔹 최대 페이지 수 [기본 3]: ").strip()
    raw_per_page = input("  🔹 페이지당 개수 [기본 20, 최대 100]: ").strip()
    raw_timeout = input("  🔹 read timeout(초) [기본 40]: ").strip()
    raw_retries = input("  🔹 재시도 횟수 [기본 3]: ").strip()
    raw_raw = input("  🔹 raw JSON 일부 출력? [y/N]: ").strip().lower()

    max_pages = int(raw_pages) if raw_pages.isdigit() else 3
    per_page = int(raw_per_page) if raw_per_page.isdigit() else 20
    timeout = int(raw_timeout) if raw_timeout.isdigit() else 40
    retries = int(raw_retries) if raw_retries.isdigit() else 3
    print_raw = raw_raw in ("y", "yes")

    run_serp_test(
        provider=provider,
        query=query,
        max_pages=max(1, max_pages),
        per_page=max(1, min(100, per_page)),
        timeout=max(5, timeout),
        retries=max(0, retries),
        print_raw=print_raw,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SerpAPI 이미지 수집 단독 테스트")
    parser.add_argument("--provider", choices=["google", "naver_official", "naver_blog", "naver_shop"], default="google")
    parser.add_argument("--query", required=True)
    parser.add_argument("--max-pages", type=int, default=3)
    parser.add_argument("--per-page", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=40)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--raw", action="store_true", help="페이지별 raw JSON 일부 출력")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    code = run_serp_test(
        provider=args.provider,
        query=args.query,
        max_pages=max(1, int(args.max_pages)),
        per_page=max(1, min(100, int(args.per_page))),
        timeout=max(5, int(args.timeout)),
        retries=max(0, int(args.retries)),
        print_raw=bool(args.raw),
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()
