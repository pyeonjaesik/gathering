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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

import requests

from app.analyzer import URLIngredientAnalyzer

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
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        raise SystemExit("OPENAI_API_KEY 환경변수를 설정해주세요.")

    print("\n=== 검색어 기반 이미지 벤치마크 ===")
    print(f"- 검색어: {query}")
    print(f"- 최대 페이지: {max_pages}")
    print("- SerpAPI에서 이미지 수집 중...")
    images = _search_images_all(query=query, api_key=serp_key, max_pages=max_pages, per_page=100)
    print(f"- 수집된 이미지: {len(images)}개")
    print("- 상태 기준: ✅ 추출 가능 | ❌ 추출 불가")
    if not images:
        return

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
    ) -> tuple[int, ImageCandidate, dict[str, Any], str | None, dict[str, Any] | None, str | None, dict[str, Any] | None]:
        try:
            analyzer = _get_analyzer()
            result = analyzer.analyze_pass2(image_url=img.url, target_item_rpt_no=None)
            qf = result.get("quality_flags") or {}
            pass3_trigger_keys = [
                "is_clear_text",
                "is_full_frame",
                "is_flat_undistorted",
                "has_report_number_label",
                "has_product_name",
                "has_single_product",
                "has_ingredients_section",
            ]
            should_run_pass3 = all(qf.get(k) is True for k in pass3_trigger_keys)
            pass3_result: dict[str, Any] | None = None
            pass3_err: str | None = None
            if should_run_pass3:
                pass3_result = analyzer.analyze_pass3(image_url=img.url, target_item_rpt_no=None)
                if pass3_result.get("error"):
                    pass3_err = str(pass3_result.get("error"))
            pass4_result: dict[str, Any] | None = None
            if pass3_result and not pass3_err:
                has_required = bool(
                    (pass3_result.get("product_report_number"))
                    and (pass3_result.get("ingredients_text"))
                    and (pass3_result.get("product_name_in_image"))
                )
                if has_required:
                    pass4_result = analyzer.analyze_pass4_normalize(
                        pass2_result=result,
                        pass3_result=pass3_result,
                        target_item_rpt_no=None,
                    )
            return (idx, img, result, None, pass3_result, pass3_err, pass4_result)
        except Exception as exc:  # pylint: disable=broad-except
            result = {
                "itemMnftrRptNo": None,
                "ingredients_text": None,
                "full_text": None,
                "note": f"analysis_error:{type(exc).__name__}",
            }
            return (idx, img, result, str(exc), None, None, None)

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
                idx, img, result, err, pass3_result, pass3_err, pass4_result = fut.result()
                gate_pass = bool(result.get("quality_gate_pass"))
                gate_result = "READ" if (str(result.get("ai_decision") or "").upper() == "READ") else "SKIP"
                is_extractable = gate_pass and (gate_result == "READ")
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

                # 품질 플래그 통계
                # READ 판정 기준과 동일한 핵심 키(영양성분은 선택 항목)
                relaxed_keys = [
                    "is_clear_text",
                    "is_full_frame",
                    "is_flat_undistorted",
                    "has_report_number_label",
                    "has_product_name",
                    "has_single_product",
                ]
                strict_keys = relaxed_keys + ["has_ingredients_section"]
                if _all_true_flags(result, relaxed_keys):
                    all_true_except_ing_cnt += 1
                if _all_true_flags(result, strict_keys):
                    all_true_with_ing_cnt += 1

                # nutrition 제외, 나머지 핵심 지표 모두 true인 목록 수집
                # 기준: strict_keys (nutrition만 제외)
                if _all_true_flags(result, strict_keys):
                    qf = result.get("quality_flags") or {}
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
                        p3_has_required = bool(
                            (pass3_result.get("product_report_number"))
                            and (pass3_result.get("ingredients_text"))
                            and (pass3_result.get("product_name_in_image"))
                        )
                        p4_items = []
                        p4_err = None
                        p4_report_valid = None
                        p4_report_reason = None
                        p4_nut_items = []
                        if pass4_result:
                            pass4_run_cnt += 1
                            p4_items = list(pass4_result.get("ingredient_items") or [])
                            p4_err = pass4_result.get("pass4_ai_error")
                            rv = pass4_result.get("report_number_validation") or {}
                            p4_report_valid = rv.get("is_valid")
                            p4_report_reason = rv.get("reason")
                            p4_nut_items = list(pass4_result.get("nutrition_items") or [])
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
                            "pass3_raw": p3_raw,
                            "pass4_exists": bool(pass4_result),
                            "pass4_error": (pass4_result or {}).get("pass4_ai_error") if pass4_result else None,
                            "pass4_raw": p4_raw,
                        }
                    )

                print(f"\n[{idx:03d}/{len(images):03d}] URL: {img.url}")
                print("  [AI 원문 응답]")
                if err:
                    print(f"  (호출 실패) {err}")
                else:
                    content = _extract_assistant_content(
                        raw_api_response=result.get("raw_api_response"),
                        raw_model_text=result.get("raw_model_text"),
                    )
                    if not content:
                        content = result.get("ai_decision_reason") or result.get("note") or "(원문 없음)"
                    print(f"  {content}")
                if pass3_result is not None:
                    print("  [Pass-3 추출 결과]")
                    if pass3_err:
                        print(f"  - 상태: 실패 ({pass3_err})")
                        raw_pass3 = _extract_assistant_content(
                            raw_api_response=pass3_result.get("raw_api_response"),
                            raw_model_text=pass3_result.get("raw_model_text"),
                        )
                        print("  - [AI 원문 raw]")
                        print(f"  {raw_pass3 or pass3_err or '(원문 없음)'}")
                    else:
                        rpt = pass3_result.get("product_report_number")
                        ing = (pass3_result.get("ingredients_text") or "").strip()
                        prod = pass3_result.get("product_name_in_image")
                        nut = (pass3_result.get("nutrition_text") or "").strip()
                        ing_preview = ing if len(ing) <= 120 else ing[:120] + "..."
                        print("  - 상태: 성공")
                        print(f"  - 제품명: {prod or 'null'}")
                        print(f"  - 품목보고번호: {rpt or 'null'}")
                        print(f"  - 원재료명: {ing_preview or 'null'}")
                        print(f"  - 영양성분 존재: {'true' if nut else 'false'}")
                        if pass4_result is not None:
                            items_cnt = len(list(pass4_result.get("ingredient_items") or []))
                            nut_cnt = len(list(pass4_result.get("nutrition_items") or []))
                            rv = pass4_result.get("report_number_validation") or {}
                            rv_txt = "true" if rv.get("is_valid") is True else ("false" if rv.get("is_valid") is False else "null")
                            p4_err = pass4_result.get("pass4_ai_error")
                            if p4_err:
                                print(f"  - Pass-4 구조화: 실패 ({p4_err})")
                            else:
                                print(f"  - Pass-4 구조화 항목수: {items_cnt}")
                                print(f"  - Pass-4 영양성분 항목수: {nut_cnt}")
                                print(f"  - Pass-4 품목번호 적합성: {rv_txt}")

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
    print("Pass-2 통과 결과 (nutrition 무관, 핵심값 모두 true)")
    print("=" * 90)
    if not pass2_pass_rows:
        print("- 없음")
    else:
        for row in sorted(pass2_pass_rows, key=lambda x: x["no"]):
            print(f"[{row['no']:03d}] URL: {row['url']}")
            print("  [Pass-3]")
            if row.get("pass3_ok"):
                print("  - 상태: 통과")
            else:
                print("  - 상태: 미통과")
            if row.get("pass3_error"):
                print(f"  - 실패사유: {row.get('pass3_error')}")
            print(f"  - 제품명: {row.get('pass3_product_name') or 'null'}")
            print(f"  - 품목보고번호: {row.get('pass3_report_no') or 'null'}")
            print(f"  - 원재료명: {row.get('pass3_ingredients') or 'null'}")
            print("  - raw:")
            print(f"  {row.get('pass3_raw') or 'null'}")

            if row.get("pass3_ok"):
                print("  [Pass-4]")
                if row.get("pass4_exists"):
                    if row.get("pass4_error"):
                        print(f"  - 상태: 실패 ({row.get('pass4_error')})")
                    else:
                        print("  - 상태: 완료")
                    print("  - raw:")
                    print(f"  {row.get('pass4_raw') or 'null'}")
                else:
                    print("  - 상태: 미실행")
            print("-" * 90)


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

    print("\n  🚀 실행합니다. 결과는 이미지별로 순차 출력됩니다.")
    run_query_image_benchmark(
        query=query,
        max_pages=max_pages,
        delay_sec=delay_sec,
        max_concurrency=max_concurrency,
        adaptive=adaptive,
    )
