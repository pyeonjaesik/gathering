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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

import requests

from app.ingredient_analyzer import URLIngredientAnalyzer

SERPAPI_URL = "https://serpapi.com/search.json"
SERPAPI_TIMEOUT = 25
SERPAPI_RETRIES = 2
SERPAPI_RETRY_BACKOFF = 0.7


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


def _search_images_all(query: str, api_key: str, max_pages: int = 20, per_page: int = 100) -> list[ImageCandidate]:
    seen: set[str] = set()
    out: list[ImageCandidate] = []

    for page_no in range(max_pages):
        params = {
            "engine": "google_images",
            "q": query,
            "hl": "ko",
            "gl": "kr",
            "num": per_page,
            "ijn": page_no,
            "api_key": api_key,
            "no_cache": "true",
        }

        data: dict[str, Any] | None = None
        last_error = None
        for attempt in range(SERPAPI_RETRIES + 1):
            try:
                resp = requests.get(SERPAPI_URL, params=params, timeout=SERPAPI_TIMEOUT)
                data = resp.json()
                api_err = data.get("error")
                if resp.status_code == 200 and api_err is None:
                    break
                last_error = f"http={resp.status_code} api_error={api_err}"
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < SERPAPI_RETRIES:
                    time.sleep(SERPAPI_RETRY_BACKOFF * (attempt + 1))
                    continue
                raise RuntimeError(last_error)
            except Exception as exc:  # pylint: disable=broad-except
                last_error = str(exc)
                if attempt < SERPAPI_RETRIES:
                    time.sleep(SERPAPI_RETRY_BACKOFF * (attempt + 1))
                    continue
                raise RuntimeError(f"SerpAPI 검색 실패(page={page_no}): {last_error}") from exc

        if data is None:
            break
        images = data.get("images_results") or []
        if not images:
            break

        added = 0
        for rank, item in enumerate(images, start=1):
            url = item.get("original") or item.get("thumbnail")
            if not url or url in seen:
                continue
            seen.add(url)
            out.append(
                ImageCandidate(
                    url=url,
                    title=item.get("title"),
                    source=item.get("source"),
                    page_no=page_no,
                    rank_in_page=rank,
                )
            )
            added += 1

        if added == 0:
            break

    return out


def run_query_image_benchmark(
    query: str,
    max_pages: int = 20,
    delay_sec: float = 0.0,
    max_concurrency: int = 5,
    adaptive: bool = True,
) -> None:
    serp_key = os.getenv("SERPAPI_KEY")
    if not serp_key:
        raise SystemExit("SERPAPI_KEY 환경변수를 설정해주세요.")
    gemini_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not gemini_key:
        raise SystemExit("GEMINI_API_KEY(또는 GOOGLE_API_KEY) 환경변수를 설정해주세요.")

    print("\n=== 검색어 기반 이미지 벤치마크 ===")
    print(f"- 검색어: {query}")
    print(f"- 최대 페이지: {max_pages}")
    print("- SerpAPI에서 이미지 수집 중...")
    images = _search_images_all(query=query, api_key=serp_key, max_pages=max_pages, per_page=100)
    print(f"- 수집된 이미지: {len(images)}개")
    print("- 상태 기준: ✅ 온전 검출 | 🔺 부분/불확실 | ❌ 미검출")
    if not images:
        return

    max_concurrency = max(1, min(10, int(max_concurrency)))
    print(f"- analyze 병렬 처리: 최대 {max_concurrency}개 동시 실행")
    print(f"- adaptive 모드: {'ON' if adaptive else 'OFF'}")
    thread_local = threading.local()

    def _get_analyzer() -> URLIngredientAnalyzer:
        analyzer = getattr(thread_local, "analyzer", None)
        if analyzer is None:
            # 벤치마크는 체감 속도를 위해 timeout/retry를 보수적으로 낮춘다.
            analyzer = URLIngredientAnalyzer(
                api_key=gemini_key,
                strict_mode=False,
                request_timeout_sec=35,
                download_timeout_sec=12,
                download_retries=1,
                model_retries=1,
            )
            thread_local.analyzer = analyzer
        return analyzer

    def _analyze_one(idx: int, img: ImageCandidate) -> tuple[int, ImageCandidate, dict[str, Any], str | None]:
        try:
            analyzer = _get_analyzer()
            result = analyzer.analyze(image_url=img.url, target_item_rpt_no=None)
            return (idx, img, result, None)
        except Exception as exc:  # pylint: disable=broad-except
            result = {
                "itemMnftrRptNo": None,
                "ingredients_text": None,
                "full_text": None,
                "note": f"analysis_error:{type(exc).__name__}",
            }
            return (idx, img, result, str(exc))

    ok_cnt = 0
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
                idx, img, result, err = fut.result()
                rpt = result.get("itemMnftrRptNo")
                ing = result.get("ingredients_text")
                full_text = result.get("full_text")
                mark_report = _mark_report(result)
                mark_ing = _mark_ingredients(result)
                mark_prod, product_name = _mark_product(result, img.title)
                mark_nutri, nutri_how = _mark_nutrition_from_result(result)
                gate_pass = bool(result.get("quality_gate_pass"))
                gate_score = result.get("quality_score")
                gate_fail = result.get("quality_fail_reasons") or []
                gate_result = "READ" if (str(result.get("ai_decision") or "").upper() == "READ") else "SKIP"
                suitability = str(result.get("ai_suitability") or "").strip()
                if suitability not in ("적합", "부적합"):
                    suitability = "적합" if gate_result == "READ" else "부적합"
                decision_conf = result.get("ai_decision_confidence")
                decision_reason = result.get("ai_decision_reason")
                raw_model_text = result.get("raw_model_text")

                got_any = any(m != "❌" for m in (mark_report, mark_ing, mark_prod, mark_nutri))
                if got_any and err is None:
                    ok_cnt += 1

                print(f"\n[{idx:03d}/{len(images):03d}] URL: {img.url}")
                print("  [AI 원문 응답]")
                print(f"  {raw_model_text or '(원문 없음)'}")

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

    print("\n" + "=" * 90)
    print("요약")
    print(f"- 총 이미지: {len(images)}")
    print(f"- 의미 있는 추출(번호/원재료/성분표/제품명 중 하나 이상): {ok_cnt}")
    print(f"- 미검출/오류 중심 이미지: {len(images) - ok_cnt}")


def run_query_image_benchmark_interactive() -> None:
    print("\n  🔎 [검색어 기반 이미지 벤치마크]")
    query = input("  🔹 검색어 입력: ").strip()
    if not query:
        print("  ⚠️ 검색어를 입력해주세요.")
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

    raw_delay = input("  🔹 이미지 간 대기(초) [기본 0]: ").strip()
    delay_sec = 0.0
    if raw_delay:
        try:
            d = float(raw_delay)
            if d >= 0:
                delay_sec = d
        except ValueError:
            pass

    raw_conc = input("  🔹 최대 동시 요청 수 [기본 5]: ").strip()
    max_concurrency = 5
    if raw_conc:
        try:
            v = int(raw_conc)
            if v > 0:
                max_concurrency = v
        except ValueError:
            pass

    raw_adapt = input("  🔹 adaptive 자동 감속 사용? [Y/n]: ").strip().lower()
    adaptive = (raw_adapt != "n")

    print("\n  🚀 실행합니다. 결과는 이미지별로 순차 출력됩니다.")
    run_query_image_benchmark(
        query=query,
        max_pages=max_pages,
        delay_sec=delay_sec,
        max_concurrency=max_concurrency,
        adaptive=adaptive,
    )
