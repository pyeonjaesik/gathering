"""
SerpAPI 이미지 수집만 단독으로 점검하는 테스트 도구.

용도:
- analyzer/pass 로직 없이 SerpAPI 호출 자체가 정상인지 확인
- provider(google/naver)별 응답 구조/오류를 빠르게 확인
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

import requests

# app.config import 시 프로젝트 .env가 자동 로드됨
from app import config as _app_config  # noqa: F401

SERPAPI_URL = "https://serpapi.com/search.json"


def build_params(provider: str, query: str, page: int, per_page: int, api_key: str) -> dict[str, Any]:
    if provider == "naver":
        return {
            "engine": "naver",
            "where": "image",
            "query": query,
            "display": per_page,
            "start": page * per_page + 1,
            "api_key": api_key,
            "no_cache": "true",
        }
    return {
        "engine": "google_images",
        "q": query,
        "hl": "ko",
        "gl": "kr",
        "num": per_page,
        "ijn": page,
        "api_key": api_key,
        "no_cache": "true",
    }


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
    api_key = os.getenv("SERPAPI_KEY", "").strip()
    if not api_key:
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
        params = build_params(provider, query, page, per_page, api_key)
        last_err: str | None = None
        payload: dict[str, Any] | None = None
        status_code: int | None = None

        for attempt in range(retries + 1):
            try:
                resp = requests.get(SERPAPI_URL, params=params, timeout=timeout)
                status_code = resp.status_code
                payload = resp.json()
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

        images = payload.get("images_results") or []
        if not images:
            print("  - images_results 비어있음(종료)")
            break

        page_new = 0
        for i, item in enumerate(images, start=1):
            url = item.get("original") or item.get("thumbnail")
            if not url or url in seen:
                continue
            seen.add(url)
            page_new += 1
            if i <= 5:
                title = str(item.get("title") or "").strip()
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SerpAPI 이미지 수집 단독 테스트")
    parser.add_argument("--provider", choices=["google", "naver"], default="google")
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
