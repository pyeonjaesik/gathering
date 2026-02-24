"""
통합 실행 허브
- 데이터 조회 뷰어
- 원재료명 추출 파이프라인
- 공공 API 수집
"""

import os
import json
import html
import socket
import subprocess
import sys
import time
import re
import shutil
import webbrowser
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

import sqlite3
from app import collector, viewer
from app.backup_tools import create_backup, list_backups, read_backup_metadata, restore_backup, verify_backup
from app.config import DB_FILE
from app.dedupe_tools import (
    duplicate_conditions,
    get_duplicate_samples,
    get_duplicate_stats,
    run_dedupe,
)
from app.database import ensure_processed_food_table
from app.ingredient_enricher import (
    diagnose_analysis,
    get_priority_subcategories,
    run_enricher,
    run_enricher_for_report_no,
)
from app.analyzer import URLIngredientAnalyzer
from app.analyzer.pass1_precheck import run_pass1_precheck
from app.query_image_benchmark import run_query_image_benchmark_interactive
from app.serp_simple_debug import run_serp_simple_debug_interactive
from app.query_pipeline import (
    cache_serp_images,
    finish_query_run,
    get_image_analysis_cache,
    get_provider_max_page_done,
    init_query_pipeline_tables,
    list_next_queries,
    list_recent_runs,
    start_query_run,
    upsert_provider_max_page_done,
    upsert_food_final,
    upsert_image_analysis_cache,
    upsert_query,
)

W = 68
WEB_UI_PORT = 8501
WEB_UI_URL = f"http://localhost:{WEB_UI_PORT}"
QUERY_WEB_UI_PORT = 8502
QUERY_WEB_UI_URL = f"http://localhost:{QUERY_WEB_UI_PORT}"
ADMIN_WEB_UI_PORT = 8503
ADMIN_WEB_UI_URL = f"http://localhost:{ADMIN_WEB_UI_PORT}"


def _normalize_report_no(value: str | None) -> str | None:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    if len(digits) < 10:
        return None
    return digits


def _build_public_food_index(conn: sqlite3.Connection) -> dict[str, dict]:
    cur = conn.execute(
        """
        SELECT itemMnftrRptNo, foodNm, enerc, prot, fatce, chocdf, sugar, nat
        FROM processed_food_info
        WHERE COALESCE(itemMnftrRptNo, '') != ''
        """
    )
    out: dict[str, dict] = {}
    for row in cur.fetchall():
        item_no = row[0]
        norm = _normalize_report_no(item_no)
        if not norm:
            continue
        product_name = (row[1] or "").strip()
        fields = {
            "열량(kcal)": row[2],
            "단백질(g)": row[3],
            "지방(g)": row[4],
            "탄수화물(g)": row[5],
            "당류(g)": row[6],
            "나트륨(mg)": row[7],
        }
        nutrition_payload: dict[str, str] = {}
        filled_cnt = 0
        for k, v in fields.items():
            txt = (str(v).strip() if v is not None else "")
            if txt:
                nutrition_payload[k] = txt
                filled_cnt += 1
        nutrition_text = json.dumps(
            {"source": "public_food_db", "items": nutrition_payload},
            ensure_ascii=False,
        ) if nutrition_payload else None
        candidate = {
            "item_mnftr_rpt_no": str(item_no),
            "product_name": product_name or None,
            "nutrition_text": nutrition_text,
            "has_name": bool(product_name),
            "has_nutrition": bool(nutrition_text),
            "nutrition_fields_count": filled_cnt,
        }
        prev = out.get(norm)
        if prev is None:
            out[norm] = candidate
            continue
        prev_score = int(prev.get("has_name", False)) * 100 + int(prev.get("nutrition_fields_count", 0))
        new_score = int(candidate.get("has_name", False)) * 100 + int(candidate.get("nutrition_fields_count", 0))
        if new_score > prev_score:
            out[norm] = candidate
    return out


def _write_query_execution_html_report(reports: list[dict]) -> Path:
    reports_dir = Path(__file__).resolve().parent.parent / "reports" / "query_runs"
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = reports_dir / f"query_run_{ts}.html"

    total_queries = len(reports)
    total_images = sum(int(r.get("total_images") or 0) for r in reports)
    total_analyzed = sum(int(r.get("analyzed_images") or 0) for r in reports)
    total_saved = sum(int(r.get("final_saved_count") or 0) for r in reports)

    def _yn(v: bool | None) -> str:
        if v is True:
            return "✅"
        if v is False:
            return "❌"
        return "-"

    html_parts: list[str] = []
    html_parts.append("<!doctype html><html lang='ko'><head><meta charset='utf-8'>")
    html_parts.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    html_parts.append("<title>검색어 실행 결과 리포트</title>")
    html_parts.append(
        "<style>"
        "body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f6f7fb;color:#111;margin:24px;}"
        ".wrap{max-width:1280px;margin:0 auto;}"
        ".top{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:14px;margin-bottom:14px;}"
        ".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;}"
        ".kpi{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:10px;}"
        ".kpi .v{font-size:24px;font-weight:800;}"
        ".q{background:#fff;border:1px solid #dbe3ff;border-radius:12px;padding:14px;margin:14px 0;}"
        ".q h3{margin:0 0 8px 0;font-size:18px;}"
        ".meta{font-size:13px;color:#555;margin:2px 0;}"
        ".ok{color:#0a7f2e;font-weight:700;}.bad{color:#c21f39;font-weight:700;}.skip{color:#6b7280;font-weight:700;}"
        "table{width:100%;border-collapse:collapse;margin-top:10px;font-size:13px;background:#fff;}"
        "th,td{border:1px solid #e5e7eb;padding:8px;vertical-align:top;text-align:left;}"
        "th{background:#f3f4f6;position:sticky;top:0;}"
        "code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;word-break:break-word;}"
        ".img{max-width:220px;border:1px solid #e5e7eb;border-radius:8px;background:#fafafa;padding:4px;}"
        ".small{font-size:12px;color:#555;}"
        "</style>"
    )
    html_parts.append("</head><body><div class='wrap'>")
    html_parts.append("<div class='top'>")
    html_parts.append("<h2 style='margin:0 0 8px 0;'>검색어 실행 결과 리포트</h2>")
    html_parts.append(f"<div class='meta'>생성시각: {html.escape(time.strftime('%Y-%m-%d %H:%M:%S'))}</div>")
    html_parts.append("</div>")
    html_parts.append("<div class='grid'>")
    html_parts.append(f"<div class='kpi'><div>검색어</div><div class='v'>{total_queries:,}</div></div>")
    html_parts.append(f"<div class='kpi'><div>수집 이미지</div><div class='v'>{total_images:,}</div></div>")
    html_parts.append(f"<div class='kpi'><div>분석 시도</div><div class='v'>{total_analyzed:,}</div></div>")
    html_parts.append(f"<div class='kpi'><div>최종 저장</div><div class='v'>{total_saved:,}</div></div>")
    html_parts.append("</div>")

    for q in reports:
        qtext = str(q.get("query_text") or "")
        qstatus = str(q.get("status") or "")
        qreason = str(q.get("reason") or "")
        qclass = "ok" if qstatus == "done" else ("skip" if qstatus.startswith("skipped") else "bad")
        html_parts.append("<div class='q'>")
        html_parts.append(f"<h3>{html.escape(qtext)}</h3>")
        html_parts.append(f"<div class='meta'>provider={html.escape(str(q.get('provider') or ''))} | run_id={html.escape(str(q.get('run_id') or '-'))}</div>")
        html_parts.append(
            f"<div class='meta'>상태: <span class='{qclass}'>{html.escape(qstatus)}</span>"
            f"{' | 사유: ' + html.escape(qreason) if qreason else ''}</div>"
        )
        html_parts.append(
            "<div class='meta'>"
            f"total={int(q.get('total_images') or 0)}, analyzed={int(q.get('analyzed_images') or 0)}, "
            f"saved={int(q.get('final_saved_count') or 0)}, api_calls={int(q.get('api_calls') or 0)}"
            "</div>"
        )

        rows = list(q.get("images") or [])
        if not rows:
            html_parts.append("<div class='small' style='margin-top:8px;'>이미지 항목 없음</div>")
            html_parts.append("</div>")
            continue

        html_parts.append("<table>")
        html_parts.append(
            "<tr>"
            "<th>No</th><th>이미지</th><th>URL</th><th>처리결과</th><th>사유</th>"
            "<th>Pass 시도</th><th>Pass 성공</th><th>경로/출처</th><th>데이터</th>"
            "</tr>"
        )
        for r in rows:
            status = str(r.get("status") or "")
            fail_reason = str(r.get("fail_reason") or "")
            data_source_path = str(r.get("data_source_path") or "-")
            nutrition_source = str(r.get("nutrition_data_source") or "-")
            public_hit = _yn(bool(r.get("public_food_matched")))
            pass_attempts = (
                f"P1:{_yn(r.get('pass1_attempted'))} "
                f"P2A:{_yn(r.get('pass2a_attempted'))} "
                f"P2B:{_yn(r.get('pass2b_attempted'))} "
                f"P3I:{_yn(r.get('pass3_ing_attempted'))} "
                f"P3N:{_yn(r.get('pass3_nut_attempted'))} "
                f"P4I:{_yn(r.get('pass4_ing_attempted'))} "
                f"P4N:{_yn(r.get('pass4_nut_attempted'))}"
            )
            pass_ok = (
                f"P1:{_yn(r.get('pass1_ok'))} "
                f"P2A:{_yn(r.get('p2a_ok'))} "
                f"P2B:{_yn(r.get('p2b_ok'))} "
                f"P3:{_yn(r.get('pass3_ok'))} "
                f"P4:{_yn(r.get('pass4_ok'))}"
            )
            data_text = (
                f"report_no={html.escape(str(r.get('report_no') or ''))}<br>"
                f"product={html.escape(str(r.get('product_name') or ''))}<br>"
                f"ingredients(raw)={'있음' if r.get('ingredients_text') else '없음'}<br>"
                f"nutrition(raw)={'있음' if r.get('nutrition_text') else '없음'}"
            )
            url = str(r.get("url") or "")
            html_parts.append("<tr>")
            html_parts.append(f"<td>{int(r.get('idx') or 0)}</td>")
            html_parts.append(
                f"<td><img class='img' src='{html.escape(url)}' loading='lazy' referrerpolicy='no-referrer'></td>"
            )
            html_parts.append(f"<td><a href='{html.escape(url)}' target='_blank' rel='noopener'>{html.escape(url)}</a></td>")
            html_parts.append(f"<td>{html.escape(status)}</td>")
            html_parts.append(f"<td><code>{html.escape(fail_reason)}</code></td>")
            html_parts.append(f"<td>{html.escape(pass_attempts)}</td>")
            html_parts.append(f"<td>{html.escape(pass_ok)}</td>")
            html_parts.append(
                f"<td>data_source_path={html.escape(data_source_path)}<br>"
                f"nutrition_data_source={html.escape(nutrition_source)}<br>"
                f"public_food_matched={public_hit}</td>"
            )
            html_parts.append(f"<td>{data_text}</td>")
            html_parts.append("</tr>")
        html_parts.append("</table>")
        html_parts.append("</div>")

    html_parts.append("</div></body></html>")
    out_path.write_text("\n".join(html_parts), encoding="utf-8")
    return out_path


def _bar(char: str = "─") -> str:
    return "  " + char * (W - 4)


def _display_width(text: str) -> int:
    return sum(2 if ord(c) > 127 else 1 for c in text or "")


def _trunc_display(text: str, max_w: int) -> str:
    result = []
    width = 0
    for c in text or "":
        cw = 2 if ord(c) > 127 else 1
        if width + cw > max_w:
            break
        result.append(c)
        width += cw
    return "".join(result)


def _fixed_display(text: str, max_w: int) -> str:
    t = _trunc_display(text, max_w)
    return t + " " * (max_w - _display_width(t))


def print_header() -> None:
    title = "🍽️ 식품 데이터 통합 실행기"
    inner = W - 2
    pad_left = (inner - len(title)) // 2
    pad_right = inner - pad_left - len(title)
    print()
    print("╔" + "═" * inner + "╗")
    print("║" + " " * pad_left + title + " " * pad_right + "║")
    print("╚" + "═" * inner + "╝")
    print()


def run_data_viewer() -> None:
    print("\n  👀 [실행] 데이터 조회 뷰어를 시작합니다.\n")
    viewer.main()


def _is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _run_streamlit_app(*, app_path: str, port: int, url: str, log_name: str) -> None:
    if _is_port_open(port):
        print(f"\n  🌐 웹 UI가 이미 실행 중입니다. 브라우저를 엽니다: {url}")
        webbrowser.open_new_tab(url)
        return

    project_root = Path(__file__).resolve().parent.parent
    log_path = project_root / log_name
    env = os.environ.copy()
    env.setdefault("UV_CACHE_DIR", "/tmp/uv-cache")

    cmd = [
        "uv",
        "run",
        "streamlit",
        "run",
        app_path,
        "--server.port",
        str(port),
        "--server.headless",
        "true",
    ]

    print("\n  🚀 웹 UI 서버를 시작합니다...")
    print(f"  - URL: {url}")
    print(f"  - 로그: {log_path}")

    try:
        with open(log_path, "a", encoding="utf-8") as logf:
            subprocess.Popen(  # noqa: S603
                cmd,
                cwd=str(project_root),
                env=env,
                stdout=logf,
                stderr=logf,
                start_new_session=True,
            )
    except FileNotFoundError:
        print("  ❌ uv 명령을 찾지 못했습니다. `uv` 설치 상태를 확인해주세요.")
        return
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  ❌ 웹 UI 실행 실패: {exc}")
        return

    for _ in range(20):
        if _is_port_open(port):
            print("  ✅ 웹 UI 준비 완료. 브라우저를 엽니다.")
            webbrowser.open_new_tab(url)
            return
        time.sleep(0.5)

    print("  ⚠️ 서버 시작이 지연되고 있습니다. 수동으로 URL을 열어주세요.")
    print(f"  👉 {url}")
    print(f"  💡 문제 확인: {log_path}")


def run_web_monitor() -> None:
    _run_streamlit_app(
        app_path="app/web_ui.py",
        port=WEB_UI_PORT,
        url=WEB_UI_URL,
        log_name="streamlit_web_ui.log",
    )


def run_query_web_monitor() -> None:
    _run_streamlit_app(
        app_path="app/web_query_pipeline_ui.py",
        port=QUERY_WEB_UI_PORT,
        url=QUERY_WEB_UI_URL,
        log_name="streamlit_query_web_ui.log",
    )

def run_admin_web() -> None:
    _run_streamlit_app(
        app_path="app/web_admin.py",
        port=ADMIN_WEB_UI_PORT,
        url=ADMIN_WEB_UI_URL,
        log_name="streamlit_admin_web_ui.log",
    )


def run_image_analyzer_test() -> None:
    print("\n  🧪 [이미지 URL analyze 테스트]")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        print("  ❌ OPENAI_API_KEY 환경변수가 필요합니다.")
        return

    print("  🔹 입력 방법:")
    print("    - 일반 URL 직접 입력")
    print("    - data URL은 길어서 `paste` 모드 권장")
    print("    - 파일에서 읽기: @/path/to/data_url.txt")
    raw_input = input("  🔹 이미지 입력(URL / paste / @파일): ").strip()
    image_url = raw_input
    if raw_input.lower() == "paste":
        print("  📋 data URL을 붙여넣고 마지막 줄에 END 입력:")
        lines: list[str] = []
        while True:
            line = input()
            if line.strip() == "END":
                break
            lines.append(line.strip())
        image_url = "".join(lines).strip()
    elif raw_input.startswith("@"):
        p = Path(raw_input[1:]).expanduser()
        if not p.exists():
            print(f"  ❌ 파일을 찾지 못했습니다: {p}")
            return
        image_url = p.read_text(encoding="utf-8").strip()

    # data URL은 복붙 시 공백/줄바꿈이 섞일 수 있어 제거
    if image_url.startswith("data:image/"):
        image_url = re.sub(r"\s+", "", image_url)

    if not image_url:
        print("  ⚠️ URL을 입력해주세요.")
        return

    target_no = input("  🔹 타깃 품목보고번호(선택, Enter 생략): ").strip()
    target_no = target_no or None

    analyzer = URLIngredientAnalyzer(api_key=openai_api_key)
    print("\n  🔍 분석 중...")
    try:
        result = analyzer.analyze(image_url=image_url, target_item_rpt_no=target_no)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  ❌ 분석 실패: {exc}")
        return

    print("\n  ✅ [analyze 결과]")
    print(f"  - itemMnftrRptNo : {result.get('itemMnftrRptNo') or '없음'}")
    print(f"  - is_flat        : {result.get('is_flat')}")
    print(f"  - is_table_format: {result.get('is_table_format')}")
    print(f"  - has_ingredients: {result.get('has_ingredients')}")
    print(f"  - has_rect_box   : {result.get('has_rect_ingredient_box')}")
    print(f"  - has_report_lbl : {result.get('has_report_label')}")
    print(f"  - product_name   : {result.get('product_name_in_image') or '없음'}")
    print(f"  - brand          : {result.get('brand') or '없음'}")
    print(f"  - manufacturer   : {result.get('manufacturer') or '없음'}")
    print(f"  - note           : {result.get('note') or '없음'}")

    ingredients = (result.get("ingredients_text") or "").strip()
    if ingredients:
        preview = ingredients if len(ingredients) <= 240 else ingredients[:240] + "..."
        print(f"  - ingredients    : {preview}")
    else:
        print("  - ingredients    : 없음")

    if target_no:
        status, reason = diagnose_analysis(result, target_no)
        print("\n  📌 [타깃 기준 진단]")
        print(f"  - target         : {target_no}")
        print(f"  - status         : {status}")
        print(f"  - reason         : {reason}")

    print("\n  🧾 [원본 JSON]")
    print(json.dumps(result, ensure_ascii=False, indent=2))


def _latest_benchmark_summary_path() -> Path | None:
    root = Path(__file__).resolve().parent.parent / "validation_reports"
    if not root.exists():
        return None
    candidates = [p / "summary.json" for p in root.glob("benchmark_*") if (p / "summary.json").exists()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def run_benchmark_menu() -> None:
    while True:
        print("\n  📊 [벤치마크]")
        print("    [1] 검색어 기반 이미지 벤치마크 (SERP + Pass 분석)")
        print("    [2] SERP 테스트/디버그 통합 (브라우저 리포트)")
        print("    [b] 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            try:
                run_query_image_benchmark_interactive()
            except Exception as exc:  # pylint: disable=broad-except
                print(f"  ❌ 실행 실패: {exc}")
        elif sub == "2":
            try:
                run_serp_simple_debug_interactive()
            except Exception as exc:  # pylint: disable=broad-except
                print(f"  ❌ 실행 실패: {exc}")

        elif sub == "b":
            return
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_ingredient_menu() -> None:
    if not os.getenv("SERPAPI_KEY"):
        print("\n  ❌ 오류: SERPAPI_KEY 환경변수가 필요합니다.")
        print('  💡 예) export SERPAPI_KEY="YOUR_KEY"')
        return

    print("\n  🧪 [원재료명 추출 방식 선택]")
    print("    [1] 우선순위 중분류에서 선택")
    print("    [2] 품목보고번호 직접 입력 (1건)")
    print("    [b] 취소")
    mode = input("  👉 선택 : ").strip().lower()

    if mode == "b":
        print("  ↩️ 원재료 추출을 취소했습니다.")
        return

    if mode == "2":
        report_no = input("  🔹 품목보고번호 입력: ").strip()
        if not report_no:
            print("  ⚠️ 품목보고번호를 입력해주세요.")
            return
        raw_quiet = input("  🔹 이미지별 상세 로그 생략? [y/N]: ").strip().lower()
        quiet = raw_quiet == "y"
        print("\n  🚀 [실행] 지정한 품목보고번호 1건 분석을 시작합니다.\n")
        run_enricher_for_report_no(report_no=report_no, quiet=quiet)
        return

    if mode != "1":
        print("  ⚠️ 올바른 번호를 입력해주세요.")
        return

    print("\n  🧪 [원재료명 추출 대상 선택: 중분류]")
    with sqlite3.connect(DB_FILE) as conn:
        categories = get_priority_subcategories(conn)

    if not categories:
        print("  ⚠️ 대상 카테고리를 찾지 못했습니다.")
        return

    col_no = 4
    col_pr = 4
    col_cat = 38
    col_total = 8
    col_attempt = 8
    col_success = 8
    col_rate = 7

    header = (
        f"  {_fixed_display('No', col_no)}  "
        f"{_fixed_display('우선', col_pr)}  "
        f"{_fixed_display('대분류 > 중분류', col_cat)}  "
        f"{_fixed_display('총상품', col_total)}  "
        f"{_fixed_display('시도완료', col_attempt)}  "
        f"{_fixed_display('성공수집', col_success)}  "
        f"{_fixed_display('수집률', col_rate)}"
    )
    print(_bar())
    print(header)
    print(_bar())
    for idx, row in enumerate(categories, 1):
        label = f"{row['lv3']} > {row['lv4']}"
        label = _trunc_display(label, col_cat)
        total_txt = f"{row['total_count']:,}"
        attempted_txt = f"{row['attempted_count']:,}"
        success_txt = f"{row['success_count']:,}"
        rate_txt = f"{row['success_rate']:.1f}%"
        line = (
            f"  {_fixed_display(str(idx), col_no)}  "
            f"{_fixed_display(str(row['priority']), col_pr)}  "
            f"{_fixed_display(label, col_cat)}  "
            f"{_fixed_display(total_txt, col_total)}  "
            f"{_fixed_display(attempted_txt, col_attempt)}  "
            f"{_fixed_display(success_txt, col_success)}  "
            f"{_fixed_display(rate_txt, col_rate)}"
        )
        print(line)
    print(_bar())

    raw_pick = input("  👉 실행할 번호 선택 (b: 취소): ").strip().lower()
    if raw_pick == "b":
        print("  ↩️ 원재료 추출을 취소했습니다.")
        return
    if not raw_pick.isdigit():
        print("  ⚠️ 숫자로 입력해주세요.")
        return

    pick = int(raw_pick)
    if pick < 1 or pick > len(categories):
        print("  ⚠️ 범위를 벗어난 번호입니다.")
        return

    selected = categories[pick - 1]
    print("\n  ⚙️ [실행 옵션]")
    raw_limit = input("  🔹 처리 수 입력 (0 또는 '전체' = 전체, 숫자 = 일부) [기본 20]: ").strip()
    raw_quiet = input("  🔹 이미지별 상세 로그 생략? [y/N]: ").strip().lower()

    limit = 20
    if raw_limit:
        normalized = raw_limit.strip().lower()
        if normalized in ("전체", "all"):
            limit = 0
        else:
            try:
                limit = int(raw_limit)
                if limit < 0:
                    print("  ⚠️ 음수는 사용할 수 없습니다. 기본값 20으로 진행합니다.")
                    limit = 20
            except ValueError:
                print("  ⚠️ 잘못된 limit 입력입니다. 기본값 20으로 진행합니다.")

    quiet = raw_quiet == "y"

    print("\n  🚀 [실행] 선택한 중분류의 원재료 수집을 시작합니다.")
    print(f"  🎯 대상: {selected['lv3']} > {selected['lv4']}")
    print(
        f"  📦 현황: 총 {selected['total_count']:,} / "
        f"시도 {selected['attempted_count']:,} / 성공 {selected['success_count']:,} "
        f"({selected['success_rate']:.1f}%)"
    )
    if limit == 0:
        print("  🧭 실행 범위: 전체 대상 처리")
    else:
        print(f"  🧭 실행 범위: 최대 {limit:,}건 처리")
    print()
    run_enricher(
        limit=limit,
        quiet=quiet,
        lv3=selected["lv3"],
        lv4=selected["lv4"],
    )


def run_public_api_collection() -> None:
    print("\n  🌐 [공공 API 수집 설정]")
    raw = input("  🔹 저장할 데이터 개수 입력 (0 또는 '전체' = 전체 수집): ").strip()
    if not raw:
        print("  ⚠️ 입력이 비어 있어 수집을 취소합니다.")
        return

    # collector.main()은 sys.argv를 읽으므로 일시적으로 주입
    argv_backup = sys.argv[:]
    try:
        sys.argv = ["collector.py", raw]
        collector.main()
    finally:
        sys.argv = argv_backup


def run_public_api_menu() -> None:
    while True:
        print("\n  🌐 [공공 API 하위 메뉴]")
        print("    [1] 가공식품 데이터 수집")
        print("    [2] 가공식품 중복 데이터 점검/삭제")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            run_public_api_collection()
        elif sub == "2":
            run_duplicate_menu()
        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def _print_duplicate_stats(stats: dict[str, int]) -> None:
    print("  📊 [중복 현황]")
    print(f"    총 레코드                : {stats['total_rows']:,}")
    print(f"    A(foodCd) 그룹/초과행    : {stats['foodCd_groups']:,} / {stats['foodCd_extra']:,}")
    print(f"    B(이름+용량+카테고리)    : {stats['h1_groups']:,} / {stats['h1_extra']:,}")
    print(f"    C(이름+영양+카테고리)    : {stats['h2_groups']:,} / {stats['h2_extra']:,}")
    print(f"    D(이름+카테고리)         : {stats['h3_groups']:,} / {stats['h3_extra']:,}")


def run_duplicate_menu() -> None:
    while True:
        print("\n  🧹 [중복 관리]")
        print("    [1] 🔍 중복 조건/현황 보기")
        print("    [2] 🗑️ 중복 삭제 실행")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            print("\n  📐 [중복 판정 조건]")
            for condition in duplicate_conditions():
                print(f"    • {condition}")
            with sqlite3.connect(DB_FILE) as conn:
                stats = get_duplicate_stats(conn)
                _print_duplicate_stats(stats)
                print("\n  🧾 [중복 의심 샘플 10개]")
                samples = get_duplicate_samples(conn, limit=10)
                if not samples:
                    print("    ✅ 없음")
                else:
                    for row in samples:
                        food_nm, food_size, serv_size, lv3, lv4, cnt, foodcd_cnt = row
                        print(
                            f"    - {food_nm} | cnt={cnt} foodCd={foodcd_cnt} | "
                            f"size={food_size}, serv={serv_size}, cat={lv3}>{lv4}"
                        )
        elif sub == "2":
            print("\n  ⚠️ [삭제 실행 전 안내]")
            for condition in duplicate_conditions():
                print(f"    • {condition}")
            with sqlite3.connect(DB_FILE) as conn:
                before = get_duplicate_stats(conn)
            print("\n  📌 [실행 전 통계]")
            _print_duplicate_stats(before)
            confirm = input("\n  ❓ 위 조건으로 중복 삭제를 실행할까요? [y/N]: ").strip().lower()
            if confirm != "y":
                print("  🛑 삭제를 취소했습니다.")
                continue

            try:
                backup_path = create_backup(DB_FILE, label="pre_dedupe")
                print(f"\n  💾 안전 백업 생성 완료: {backup_path}")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"\n  ❌ 백업 생성 실패: {exc}")
                continue

            with sqlite3.connect(DB_FILE) as conn:
                result = run_dedupe(conn)
                after = get_duplicate_stats(conn)

            print("\n  ✅ [삭제 결과]")
            print(f"    - 규칙 A 삭제: {result['removed_a']:,}건")
            print(f"    - 규칙 B 삭제: {result['removed_b']:,}건")
            print(f"    - 규칙 C 삭제: {result['removed_c']:,}건")
            print(f"    - 규칙 D 삭제: {result['removed_d']:,}건")
            print(f"    - 총 삭제   : {result['removed_total']:,}건")
            print(f"    - 삭제 목록 CSV : {result['csv_path']}")

            print("\n  📌 [실행 후 통계]")
            _print_duplicate_stats(after)
        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_backup_menu() -> None:
    while True:
        print("\n  💾 [백업/복원 관리]")
        print("    [1] 백업 생성")
        print("    [2] 백업 목록 보기")
        print("    [3] 백업 복원")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            label = input("  🔹 백업 라벨 [기본 manual]: ").strip() or "manual"
            try:
                path = create_backup(DB_FILE, label=label)
                print(f"  ✅ 백업 생성 완료: {path}")
                drive_dir = os.getenv("GOOGLE_DRIVE_BACKUP_DIR", "").strip()
                if drive_dir:
                    print(f"  ☁️ Google Drive 복사 완료: {drive_dir}")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"  ❌ 백업 생성 실패: {exc}")

        elif sub == "2":
            backups = list_backups(DB_FILE)
            print("\n  📚 [백업 목록]")
            if not backups:
                print("    (백업 파일 없음)")
            else:
                for idx, path in enumerate(backups, 1):
                    meta = read_backup_metadata(path)
                    if meta:
                        size_mb = (meta.get("backup_size_bytes") or 0) / (1024 * 1024)
                        mtime = meta.get("backup_mtime") or "-"
                        print(f"    [{idx}] {path}")
                        print(f"         size={size_mb:.1f}MB | mtime={mtime} | meta=있음")
                    else:
                        print(f"    [{idx}] {path}  (meta 없음)")

        elif sub == "3":
            backups = list_backups(DB_FILE)
            if not backups:
                print("  ⚠️ 복원 가능한 백업이 없습니다.")
                continue
            print("\n  📚 [복원 대상 선택]")
            for idx, path in enumerate(backups, 1):
                print(f"    [{idx}] {path}")
            raw = input("  👉 복원할 번호 입력 (b: 취소): ").strip().lower()
            if raw == "b":
                continue
            if not raw.isdigit():
                print("  ⚠️ 숫자로 입력해주세요.")
                continue
            pick = int(raw)
            if pick < 1 or pick > len(backups):
                print("  ⚠️ 범위를 벗어난 번호입니다.")
                continue

            target = backups[pick - 1]
            confirm = input(
                "  ❗ 현재 DB를 해당 백업으로 덮어씁니다. 계속할까요? [y/N]: "
            ).strip().lower()
            if confirm != "y":
                print("  🛑 복원을 취소했습니다.")
                continue

            try:
                check = verify_backup(target)
                print(f"  🔎 백업 검증: integrity={check['sqlite_integrity_ok']} checksum={check['checksum_match']}")
                restored = restore_backup(
                    target,
                    DB_FILE,
                    keep_current_snapshot=True,
                    verify_before_restore=True,
                )
                print(f"  ✅ 복원 완료: {restored}")
                print("  💾 기존 DB는 pre_restore 라벨로 자동 백업되었습니다.")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"  ❌ 복원 실패: {exc}")

        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_query_pipeline_menu() -> None:
    while True:
        print("\n  🧩 [검색어 파이프라인]")
        print("    [1] ➕ 검색어 직접 추가")
        print("    [2] 🌐 검색어 풀 브라우저 보기")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            query_text = input("  🔹 검색어 입력: ").strip()
            if not query_text:
                print("  ⚠️ 검색어가 비어 있습니다.")
                continue
            raw_pri = input("  🔹 score(점수) [기본 0]: ").strip()
            notes = input("  🔹 메모(선택): ").strip() or None
            try:
                pri = float(raw_pri) if raw_pri else 0.0
            except ValueError:
                print("  ⚠️ 점수는 숫자여야 합니다.")
                continue

            with sqlite3.connect(DB_FILE) as conn:
                init_query_pipeline_tables(conn)
                query_id = upsert_query(
                    conn,
                    query_text,
                    source="manual",
                    priority_score=pri,
                    target_segment_score=0.0,
                    status="pending",
                    notes=notes,
                )
            print(f"  ✅ 저장 완료: query_id={query_id}")

        elif sub == "2":
            run_query_pool_browser_view()

        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_query_pool_browser_view() -> None:
    with sqlite3.connect(DB_FILE) as conn:
        out_path = viewer.open_query_pool_browser_report(conn)
    print(f"\n  ✅ 검색어 풀 브라우저 리포트 생성: {out_path}")


def _query_exec_log(logger: Callable[[str], None] | None, text: str) -> None:
    if logger is not None:
        logger(text)
    else:
        print(text)


def execute_query_pipeline_run(
    *,
    mode: str = "1",
    provider: str = "google",
    query_limit: int = 3,
    max_pages: int = 1,
    max_images: int = 0,
    pass_workers: int = 5,
    direct_query: str | None = None,
    open_browser_report: bool = True,
    logger: Callable[[str], None] | None = None,
) -> str | None:
    from app import config as app_config
    app_config.reload_dotenv()

    serp_key = os.getenv("SERPAPI_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not openai_key:
        _query_exec_log(logger, "  ❌ OPENAI_API_KEY가 필요합니다.")
        return None

    if provider not in ("naver_official", "naver_blog", "naver_shop") and not serp_key:
        _query_exec_log(logger, "  ❌ SERPAPI_KEY가 필요합니다.")
        return None
    if provider in ("naver_official", "naver_blog", "naver_shop"):
        naver_client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
        naver_client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
        if not (naver_client_id and naver_client_secret):
            _query_exec_log(logger, "  ❌ NAVER_CLIENT_ID / NAVER_CLIENT_SECRET이 필요합니다.")
            return None

    mode = (mode or "1").strip()
    provider = (provider or "google").strip()
    query_limit = int(query_limit)
    max_pages = int(max_pages)
    max_images = int(max_images)
    pass_workers = int(pass_workers)
    query_limit = max(1, query_limit)
    max_pages = max(1, min(20, max_pages))
    max_images = max(0, max_images)
    pass_workers = max(1, min(50, pass_workers))

    from app.query_image_benchmark import _search_images_all

    reports: list[dict] = []
    with sqlite3.connect(DB_FILE) as conn:
        init_query_pipeline_tables(conn)
        conn.row_factory = sqlite3.Row
        public_food_index = _build_public_food_index(conn)
        _query_exec_log(logger, "\n  📋 [실행 설정]")
        _query_exec_log(logger, f"    - provider           : {provider}")
        _query_exec_log(logger, f"    - query limit        : {query_limit}")
        _query_exec_log(logger, f"    - max pages          : {max_pages} (항상 1페이지부터 수집)")
        _query_exec_log(logger, f"    - max images/query   : {max_images if max_images > 0 else '전체'}")
        _query_exec_log(logger, f"    - pass workers       : {pass_workers}")
        _query_exec_log(logger, f"    - 공공DB 번호 인덱스    : {len(public_food_index):,}개")
        queries = []
        if mode == "2":
            if not str(direct_query or "").strip():
                _query_exec_log(logger, "  ⚠️ 직접 입력 모드인데 검색어가 비어 있습니다.")
                return None
            qid = upsert_query(
                conn,
                str(direct_query).strip(),
                source="manual_direct",
                priority_score=1000.0,
                target_segment_score=0.0,
                status="pending",
                notes="direct_run",
            )
            queries = conn.execute(
                "SELECT id, query_text, query_norm, priority_score, status FROM query_pool WHERE id=?",
                (qid,),
            ).fetchall()
        else:
            queries = conn.execute(
                """
                SELECT id, query_text, query_norm, priority_score, status
                FROM query_pool
                WHERE status IN ('pending', 'done', 'failed')
                ORDER BY priority_score DESC, id ASC
                LIMIT ?
                """,
                (query_limit,),
            ).fetchall()

        if not queries:
            _query_exec_log(logger, "  ⚠️ 실행할 검색어가 없습니다.")
            return None

        _query_exec_log(logger, f"\n  🚀 실행 시작: 대상 {len(queries)}개")

        for q in queries:
            query_id = int(q["id"])
            query_text = str(q["query_text"] or "").strip()
            query_norm = str(q["query_norm"] or "").strip()
            query_report: dict = {
                "query_id": query_id,
                "query_text": query_text,
                "provider": provider,
                "run_id": None,
                "status": "running",
                "reason": "",
                "total_images": 0,
                "analyzed_images": 0,
                "final_saved_count": 0,
                "api_calls": 0,
                "images": [],
            }
            max_page_done = get_provider_max_page_done(
                conn,
                query_norm=query_norm,
                provider=provider,
            )
            if max_pages <= max_page_done:
                _query_exec_log(logger, f"\n  ⏭️ query_id={query_id} 스킵")
                _query_exec_log(logger, f"    q={query_text}")
                _query_exec_log(
                    logger,
                    f"    사유: provider={provider} 기준 기존 max_page_done={max_page_done} "
                    f">= 요청 max_pages={max_pages}"
                )
                query_report["status"] = "skipped_by_provider_max_page"
                query_report["reason"] = (
                    f"provider={provider}, max_page_done={max_page_done}, requested={max_pages}"
                )
                reports.append(query_report)
                continue
            run_id = start_query_run(conn, query_id=query_id)
            _query_exec_log(logger, f"\n  ▶ query_id={query_id} run_id={run_id}")
            _query_exec_log(logger, f"    q={query_text}")
            query_report["run_id"] = run_id

            analyzed_images = 0
            pass2b_pass_count = 0
            pass4_pass_count = 0
            final_saved_count = 0
            api_calls = 0
            total_images = 0

            try:
                images = _search_images_all(
                    query=query_text,
                    api_key=serp_key,
                    max_pages=max_pages,
                    per_page=100,
                    provider=provider,
                )
                if max_images > 0 and len(images) > max_images:
                    images = images[:max_images]
                total_images = len(images)

                # SERP 캐시 저장
                page_map: dict[int, list[dict]] = {}
                for img in images:
                    page_map.setdefault(int(img.page_no), []).append(
                        {
                            "image_url": img.url,
                            "title": img.title,
                            "source": img.source,
                            "rank_in_page": img.rank_in_page,
                        }
                    )
                for page_no, items in page_map.items():
                    cache_serp_images(
                        conn,
                        query_id=query_id,
                        page=page_no,
                        page_size=100,
                        images=items,
                        run_id=run_id,
                    )

                _query_exec_log(logger, f"    수집 이미지: {total_images}개 | pass 동시호출: {pass_workers}")
                query_report["total_images"] = total_images

                to_process: list[tuple[int, object]] = []
                for idx, img in enumerate(images, 1):
                    cached = get_image_analysis_cache(conn, img.url)
                    if cached:
                        fail_stage = str(cached["fail_stage"] or "").strip() if "fail_stage" in cached.keys() else ""
                        stage_txt = fail_stage or ("pass4" if int(cached["pass4_ok"] or 0) == 1 else "attempted")
                        _query_exec_log(logger, f"    [{idx}/{total_images}] 기존 분석이력 스킵 (stage={stage_txt})")
                        query_report["images"].append(
                            {
                                "idx": idx,
                                "url": img.url,
                                "status": "skipped_by_existing_history",
                                "fail_reason": f"existing_history(stage={stage_txt})",
                                "pass1_attempted": False,
                                "pass2a_attempted": False,
                                "pass2b_attempted": False,
                                "pass3_ing_attempted": False,
                                "pass3_nut_attempted": False,
                                "pass4_ing_attempted": False,
                                "pass4_nut_attempted": False,
                                "pass1_ok": None,
                                "p2a_ok": None,
                                "p2b_ok": None,
                                "pass3_ok": None,
                                "pass4_ok": None,
                                "data_source_path": None,
                                "nutrition_data_source": "none",
                                "public_food_matched": False,
                                "report_no": None,
                                "product_name": None,
                                "ingredients_text": None,
                                "nutrition_text": None,
                            }
                        )
                        continue
                    to_process.append((idx, img))

                thread_local = threading.local()

                def _get_analyzer() -> URLIngredientAnalyzer:
                    az = getattr(thread_local, "analyzer", None)
                    if az is None:
                        az = URLIngredientAnalyzer(api_key=openai_key)
                        thread_local.analyzer = az
                    return az

                def _analyze_one(idx: int, img_obj: object) -> dict:
                    img_url = str(getattr(img_obj, "url"))
                    az = _get_analyzer()
                    result: dict = {
                        "idx": idx,
                        "url": img_url,
                        "api_calls": 0,
                        "pass1_ok": False,
                        "pass2_ok": False,
                        "pass3_ok": False,
                        "pass4_ok": False,
                        "fail_stage": None,
                        "fail_reason": None,
                        "p2a_ok": False,
                        "p2b_ok": False,
                        "raw_pass2a": None,
                        "raw_pass2b": None,
                        "raw_pass3": None,
                        "raw_pass4": None,
                        "product_name": None,
                        "report_no": None,
                        "ingredients_text": None,
                        "nutrition_text": None,
                        "data_source_path": None,
                        "nutrition_data_source": "none",
                        "public_food_matched": False,
                        "pass1_attempted": False,
                        "pass2a_attempted": False,
                        "pass2b_attempted": False,
                        "pass3_ing_attempted": False,
                        "pass3_nut_attempted": False,
                        "pass4_ing_attempted": False,
                        "pass4_nut_attempted": False,
                    }

                    try:
                        image_bytes, mime_type = az._download_image(img_url)  # pylint: disable=protected-access
                    except Exception as exc:  # pylint: disable=broad-except
                        result["fail_stage"] = "download"
                        result["fail_reason"] = str(exc) or "image_download_failed"
                        return result

                    result["pass1_attempted"] = True
                    pass1 = run_pass1_precheck(az, image_bytes=image_bytes, mime_type=mime_type, image_url=img_url)
                    pass1_ok = bool(pass1.get("precheck_pass"))
                    result["pass1_ok"] = pass1_ok
                    if not pass1_ok:
                        result["fail_stage"] = "pass1"
                        result["fail_reason"] = str(pass1.get("precheck_reason") or "pass1_precheck_failed")
                        return result

                    result["pass2a_attempted"] = True
                    pass2 = az.analyze_pass2_from_bytes(image_bytes, mime_type, target_item_rpt_no=None)
                    result["api_calls"] += 1
                    qf = pass2.get("quality_flags") or {}
                    p2a_ok = bool(qf.get("pass2a_ok"))
                    p2b_ok = bool(qf.get("pass2b_pass"))
                    p2b_executed = bool(qf.get("pass2b_executed"))
                    gate_ok = bool(pass2.get("quality_gate_pass"))
                    decision = str(pass2.get("ai_decision") or "").upper()
                    pass2_ok = gate_ok and p2a_ok and p2b_ok and decision == "READ"
                    result["p2a_ok"] = p2a_ok
                    result["p2b_ok"] = p2b_ok
                    result["pass2b_attempted"] = p2b_executed
                    result["raw_pass2a"] = pass2.get("raw_model_text_pass2a")
                    result["raw_pass2b"] = pass2.get("raw_model_text_pass2b")

                    if not pass2_ok:
                        result["fail_stage"] = "pass2"
                        result["fail_reason"] = (
                            "|".join(str(x) for x in (pass2.get("quality_fail_reasons") or []))
                            or str(pass2.get("ai_decision_reason") or "pass2_fail")
                        )
                        return result

                    result["pass2_ok"] = True
                    result["pass3_ing_attempted"] = True
                    result["pass3_nut_attempted"] = False
                    pass3_ing = az.analyze_pass3_from_bytes(
                        image_bytes=image_bytes,
                        mime_type=mime_type,
                        target_item_rpt_no=None,
                        include_nutrition=False,
                    )
                    result["api_calls"] += 1
                    raw_pass3_ing = pass3_ing.get("raw_model_text_pass3")
                    pass3_err = str(pass3_ing.get("error") or "").strip()
                    if pass3_err:
                        result["fail_stage"] = "pass3"
                        result["fail_reason"] = pass3_err
                        return result

                    product_name = (pass3_ing.get("product_name_in_image") or "").strip()
                    report_no = (pass3_ing.get("product_report_number") or "").strip()
                    ingredients_text = (pass3_ing.get("ingredients_text") or "").strip()
                    nutrition_text = None
                    if not (report_no and ingredients_text):
                        result["fail_stage"] = "pass3"
                        result["fail_reason"] = "required_fields_missing(report_no_or_ingredients)"
                        return result
                    report_no_norm = _normalize_report_no(report_no)
                    public_row = public_food_index.get(report_no_norm or "") if report_no_norm else None
                    public_complete = bool(
                        public_row
                        and public_row.get("has_name")
                        and public_row.get("has_nutrition")
                    )
                    result["public_food_matched"] = bool(public_row)
                    result["data_source_path"] = "public_food_enriched" if public_complete else "image_full_extraction"

                    # Case B: Pass3 번호 기준 공공DB 미완전이면, Pass2B에서 제품명/영양성분 존재가 둘 다 필요
                    if not public_complete:
                        has_product_in_p2b = bool(qf.get("has_product_name"))
                        has_nutri_in_p2b = bool(qf.get("has_nutrition_section"))
                        if not (has_product_in_p2b and has_nutri_in_p2b):
                            result["fail_stage"] = "pass2b"
                            result["fail_reason"] = "case_b_requires_product_and_nutrition_at_pass2b"
                            return result
                        if not product_name:
                            result["fail_stage"] = "pass3"
                            result["fail_reason"] = "case_b_requires_product_name_at_pass3_ingredients"
                            return result
                        # Case B에서만 Pass3-Nutrition 별도 호출
                        result["pass3_nut_attempted"] = True
                        prompt_nut = az._build_prompt_pass3_nutrition(target_item_rpt_no=None)  # pylint: disable=protected-access
                        raw_nut = None
                        parsed_nut = {}
                        last_nut_err = None
                        max_attempts = max(1, int(getattr(az, "model_retries", 0)) + 1)
                        for attempt in range(max_attempts):
                            try:
                                raw_nut, parsed_nut, _raw_api_nut = az._call_model_pass3(  # pylint: disable=protected-access
                                    image_bytes=image_bytes,
                                    mime_type=mime_type,
                                    prompt=prompt_nut,
                                )
                                break
                            except Exception as exc:  # pylint: disable=broad-except
                                last_nut_err = exc
                                if attempt < (max_attempts - 1):
                                    time.sleep(float(getattr(az, "retry_backoff_sec", 0.8)) * (attempt + 1))
                                    continue
                        if raw_nut is None and parsed_nut == {}:
                            result["fail_stage"] = "pass3"
                            result["fail_reason"] = f"pass3_nutrition_error:{last_nut_err}"
                            return result
                        result["api_calls"] += 1
                        nutrition_text = (parsed_nut.get("nutrition_text") or "").strip() or None
                        if not nutrition_text:
                            result["fail_stage"] = "pass3"
                            result["fail_reason"] = "case_b_requires_nutrition_at_pass3_nutrition"
                            return result
                        result["raw_pass3"] = (raw_pass3_ing or "") + "\n\n[PASS3-NUTRITION]\n" + (raw_nut or "")
                    else:
                        result["raw_pass3"] = raw_pass3_ing

                    result["pass3_ok"] = True
                    if public_complete and public_row:
                        result["product_name"] = str(public_row.get("product_name") or "").strip() or product_name
                        result["nutrition_text"] = str(public_row.get("nutrition_text") or "").strip() or None
                        result["nutrition_data_source"] = "public_food_db"
                    else:
                        result["product_name"] = product_name
                        result["nutrition_text"] = nutrition_text
                        result["nutrition_data_source"] = "image_pass4"
                    result["report_no"] = report_no
                    result["ingredients_text"] = ingredients_text

                    result["pass4_ing_attempted"] = True
                    result["pass4_nut_attempted"] = bool((not public_complete) and nutrition_text)
                    pass3_for_pass4 = dict(pass3_ing)
                    if public_complete:
                        # Case A: 영양성분은 공공DB 사용, Pass4 영양 파싱 호출 차단
                        pass3_for_pass4["nutrition_text"] = None
                        pass3_for_pass4["nutrition_complete"] = False
                        result["pass4_nut_attempted"] = False
                    else:
                        pass3_for_pass4["nutrition_text"] = nutrition_text
                        pass3_for_pass4["nutrition_complete"] = True if nutrition_text else False
                    pass4 = az.analyze_pass4_normalize(
                        pass2_result=pass2,
                        pass3_result=pass3_for_pass4,
                        target_item_rpt_no=None,
                    )
                    result["api_calls"] += 1
                    result["raw_pass4"] = pass4.get("raw_model_text_pass4")
                    pass4_err = str(pass4.get("pass4_ai_error") or "").strip()
                    if pass4_err:
                        result["fail_stage"] = "pass4"
                        result["fail_reason"] = pass4_err or "pass4_fail"
                        return result

                    ingredient_items = pass4.get("ingredient_items") or []
                    if not isinstance(ingredient_items, list) or len(ingredient_items) == 0:
                        result["fail_stage"] = "pass4"
                        result["fail_reason"] = "pass4_no_structured_ingredients"
                        return result

                    raw_pass4_ing = str(pass4.get("raw_model_text_pass4_ingredients") or "").strip()
                    if raw_pass4_ing:
                        result["ingredients_text"] = raw_pass4_ing
                    else:
                        result["ingredients_text"] = json.dumps(
                            {"ingredients_items": ingredient_items},
                            ensure_ascii=False,
                        )

                    if not public_complete:
                        raw_pass4_nut = str(pass4.get("raw_model_text_pass4_nutrition") or "").strip()
                        if raw_pass4_nut:
                            result["nutrition_text"] = raw_pass4_nut
                        else:
                            nut_items = pass4.get("nutrition_items") or []
                            if isinstance(nut_items, list) and nut_items:
                                result["nutrition_text"] = json.dumps(
                                    {"nutrition_items": nut_items},
                                    ensure_ascii=False,
                                )
                            else:
                                result["nutrition_text"] = None

                    result["pass4_ok"] = True
                    return result

                done_count = 0
                with ThreadPoolExecutor(max_workers=pass_workers) as ex:
                    fut_map = {ex.submit(_analyze_one, idx, img): idx for idx, img in to_process}
                    for fut in as_completed(fut_map):
                        res = fut.result()
                        idx = int(res["idx"])
                        done_count += 1
                        analyzed_images += 1
                        api_calls += int(res.get("api_calls", 0))
                        _query_exec_log(logger, f"    [{idx}/{total_images}] 완료 ({done_count}/{len(to_process)})")
                        image_status = "saved" if bool(res.get("pass4_ok")) else "failed"
                        query_report["images"].append(
                            {
                                "idx": idx,
                                "url": str(res.get("url") or ""),
                                "status": image_status,
                                "fail_reason": str(res.get("fail_reason") or ""),
                                "pass1_attempted": bool(res.get("pass1_attempted")),
                                "pass2a_attempted": bool(res.get("pass2a_attempted")),
                                "pass2b_attempted": bool(res.get("pass2b_attempted")),
                                "pass3_ing_attempted": bool(res.get("pass3_ing_attempted")),
                                "pass3_nut_attempted": bool(res.get("pass3_nut_attempted")),
                                "pass4_ing_attempted": bool(res.get("pass4_ing_attempted")),
                                "pass4_nut_attempted": bool(res.get("pass4_nut_attempted")),
                                "pass1_ok": res.get("pass1_ok"),
                                "p2a_ok": res.get("p2a_ok"),
                                "p2b_ok": res.get("p2b_ok"),
                                "pass3_ok": res.get("pass3_ok"),
                                "pass4_ok": res.get("pass4_ok"),
                                "data_source_path": res.get("data_source_path"),
                                "nutrition_data_source": res.get("nutrition_data_source"),
                                "public_food_matched": bool(res.get("public_food_matched")),
                                "report_no": res.get("report_no"),
                                "product_name": res.get("product_name"),
                                "ingredients_text": res.get("ingredients_text"),
                                "nutrition_text": res.get("nutrition_text"),
                            }
                        )

                        if not bool(res.get("pass2_ok")):
                            upsert_image_analysis_cache(
                                conn,
                                image_url=str(res["url"]),
                                run_id=run_id,
                                pass1_ok=bool(res.get("pass1_ok")),
                                pass2a_ok=bool(res.get("p2a_ok")),
                                pass2b_ok=bool(res.get("p2b_ok")),
                                pass3_ok=False,
                                pass4_ok=False,
                                fail_stage=str(res.get("fail_stage") or "pass2"),
                                fail_reason=str(res.get("fail_reason") or "pass2_fail"),
                                raw_pass2a=res.get("raw_pass2a"),
                                raw_pass2b=res.get("raw_pass2b"),
                                pass1_attempted=bool(res.get("pass1_attempted")),
                                pass2a_attempted=bool(res.get("pass2a_attempted")),
                                pass2b_attempted=bool(res.get("pass2b_attempted")),
                                pass3_ing_attempted=False,
                                pass3_nut_attempted=False,
                                pass4_ing_attempted=False,
                                pass4_nut_attempted=False,
                                data_source_path=res.get("data_source_path"),
                                nutrition_data_source=res.get("nutrition_data_source"),
                                public_food_matched=bool(res.get("public_food_matched")),
                            )
                            continue

                        pass2b_pass_count += 1

                        if not bool(res.get("pass3_ok")):
                            upsert_image_analysis_cache(
                                conn,
                                image_url=str(res["url"]),
                                run_id=run_id,
                                pass1_ok=bool(res.get("pass1_ok")),
                                pass2a_ok=bool(res.get("p2a_ok")),
                                pass2b_ok=bool(res.get("p2b_ok")),
                                pass3_ok=False,
                                pass4_ok=False,
                                fail_stage=str(res.get("fail_stage") or "pass3"),
                                fail_reason=str(res.get("fail_reason") or "pass3_fail"),
                                raw_pass2a=res.get("raw_pass2a"),
                                raw_pass2b=res.get("raw_pass2b"),
                                raw_pass3=res.get("raw_pass3"),
                                pass1_attempted=bool(res.get("pass1_attempted")),
                                pass2a_attempted=bool(res.get("pass2a_attempted")),
                                pass2b_attempted=bool(res.get("pass2b_attempted")),
                                pass3_ing_attempted=bool(res.get("pass3_ing_attempted")),
                                pass3_nut_attempted=bool(res.get("pass3_nut_attempted")),
                                pass4_ing_attempted=False,
                                pass4_nut_attempted=False,
                                data_source_path=res.get("data_source_path"),
                                nutrition_data_source=res.get("nutrition_data_source"),
                                public_food_matched=bool(res.get("public_food_matched")),
                            )
                            continue

                        if not bool(res.get("pass4_ok")):
                            upsert_image_analysis_cache(
                                conn,
                                image_url=str(res["url"]),
                                run_id=run_id,
                                pass1_ok=bool(res.get("pass1_ok")),
                                pass2a_ok=bool(res.get("p2a_ok")),
                                pass2b_ok=bool(res.get("p2b_ok")),
                                pass3_ok=True,
                                pass4_ok=False,
                                fail_stage=str(res.get("fail_stage") or "pass4"),
                                fail_reason=str(res.get("fail_reason") or "pass4_fail"),
                                raw_pass2a=res.get("raw_pass2a"),
                                raw_pass2b=res.get("raw_pass2b"),
                                raw_pass3=res.get("raw_pass3"),
                                raw_pass4=res.get("raw_pass4"),
                                pass1_attempted=bool(res.get("pass1_attempted")),
                                pass2a_attempted=bool(res.get("pass2a_attempted")),
                                pass2b_attempted=bool(res.get("pass2b_attempted")),
                                pass3_ing_attempted=bool(res.get("pass3_ing_attempted")),
                                pass3_nut_attempted=bool(res.get("pass3_nut_attempted")),
                                pass4_ing_attempted=bool(res.get("pass4_ing_attempted")),
                                pass4_nut_attempted=bool(res.get("pass4_nut_attempted")),
                                data_source_path=res.get("data_source_path"),
                                nutrition_data_source=res.get("nutrition_data_source"),
                                public_food_matched=bool(res.get("public_food_matched")),
                            )
                            continue

                        pass4_pass_count += 1
                        upsert_food_final(
                            conn,
                            product_name=str(res.get("product_name") or ""),
                            item_mnftr_rpt_no=str(res.get("report_no") or ""),
                            ingredients_text=str(res.get("ingredients_text") or ""),
                            nutrition_text=res.get("nutrition_text"),
                            nutrition_source=str(res.get("nutrition_data_source") or "none"),
                            source_image_url=str(res["url"]),
                            source_query_id=query_id,
                            source_run_id=run_id,
                        )
                        final_saved_count += 1

                        upsert_image_analysis_cache(
                            conn,
                            image_url=str(res["url"]),
                            run_id=run_id,
                            pass1_ok=bool(res.get("pass1_ok")),
                            pass2a_ok=bool(res.get("p2a_ok")),
                            pass2b_ok=bool(res.get("p2b_ok")),
                            pass3_ok=True,
                            pass4_ok=True,
                            fail_stage=None,
                            fail_reason=None,
                            raw_pass2a=res.get("raw_pass2a"),
                            raw_pass2b=res.get("raw_pass2b"),
                            raw_pass3=res.get("raw_pass3"),
                            raw_pass4=res.get("raw_pass4"),
                            pass1_attempted=bool(res.get("pass1_attempted")),
                            pass2a_attempted=bool(res.get("pass2a_attempted")),
                            pass2b_attempted=bool(res.get("pass2b_attempted")),
                            pass3_ing_attempted=bool(res.get("pass3_ing_attempted")),
                            pass3_nut_attempted=bool(res.get("pass3_nut_attempted")),
                            pass4_ing_attempted=bool(res.get("pass4_ing_attempted")),
                            pass4_nut_attempted=bool(res.get("pass4_nut_attempted")),
                            data_source_path=res.get("data_source_path"),
                            nutrition_data_source=res.get("nutrition_data_source"),
                            public_food_matched=bool(res.get("public_food_matched")),
                        )

                if total_images > 0:
                    upsert_provider_max_page_done(
                        conn,
                        query_norm=query_norm,
                        provider=provider,
                        max_page_done=max_pages,
                    )
                else:
                    _query_exec_log(logger, "    ℹ️ 수집 0건으로 provider max_page_done은 갱신하지 않음")
                finish_query_run(
                    conn,
                    run_id=run_id,
                    status="done",
                    total_images=total_images,
                    analyzed_images=analyzed_images,
                    pass2b_pass_count=pass2b_pass_count,
                    pass4_pass_count=pass4_pass_count,
                    final_saved_count=final_saved_count,
                    api_calls=api_calls,
                )
                _query_exec_log(
                    logger,
                    f"    ✅ 완료: analyzed={analyzed_images}, pass2b={pass2b_pass_count}, "
                    f"pass4={pass4_pass_count}, saved={final_saved_count}"
                )
                query_report["status"] = "done"
                query_report["analyzed_images"] = analyzed_images
                query_report["final_saved_count"] = final_saved_count
                query_report["api_calls"] = api_calls
                reports.append(query_report)
            except Exception as exc:  # pylint: disable=broad-except
                finish_query_run(
                    conn,
                    run_id=run_id,
                    status="failed",
                    total_images=total_images,
                    analyzed_images=analyzed_images,
                    pass2b_pass_count=pass2b_pass_count,
                    pass4_pass_count=pass4_pass_count,
                    final_saved_count=final_saved_count,
                    api_calls=api_calls,
                    error_message=str(exc),
                )
                _query_exec_log(logger, f"    ❌ 실패: {exc}")
                query_report["status"] = "failed"
                query_report["reason"] = str(exc)
                query_report["analyzed_images"] = analyzed_images
                query_report["final_saved_count"] = final_saved_count
                query_report["api_calls"] = api_calls
                reports.append(query_report)

    if not reports:
        return None

    report_path = _write_query_execution_html_report(reports)
    _query_exec_log(logger, f"\n  🌐 실행 결과 브라우저 리포트: {report_path}")
    if open_browser_report:
        try:
            webbrowser.open(report_path.resolve().as_uri())
            _query_exec_log(logger, "  🖥️ 브라우저 자동 열기 완료")
        except Exception as exc:  # pylint: disable=broad-except
            _query_exec_log(logger, f"  ⚠️ 브라우저 자동 열기 실패: {exc}")
    return str(report_path)


def run_query_pipeline_execute() -> None:
    print("\n  🔹 실행 대상 선택")
    print("    [1] 검색어 풀에서 선택 실행")
    print("    [2] 직접 검색어 입력 실행")
    mode = input("  선택 > ").strip() or "1"

    raw_q = input("  🔹 실행할 검색어 개수 [기본 3]: ").strip()
    raw_pages = input("  🔹 최대 페이지 수 [기본 1]: ").strip()
    raw_max_images = input("  🔹 검색어당 최대 이미지 수 [기본 전체=0]: ").strip()
    raw_workers = input("  🔹 Pass 동시호출 수 [기본 5]: ").strip()
    print("  🔹 이미지 검색 엔진")
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

    query_limit = int(raw_q) if raw_q.isdigit() else 3
    max_pages = int(raw_pages) if raw_pages.isdigit() else 1
    max_images = int(raw_max_images) if raw_max_images.isdigit() else 0
    pass_workers = int(raw_workers) if raw_workers.isdigit() else 5

    direct_query = None
    if mode == "2":
        direct_query = input("  🔹 직접 실행할 검색어 입력: ").strip()

    execute_query_pipeline_run(
        mode=mode,
        provider=provider,
        query_limit=query_limit,
        max_pages=max_pages,
        max_images=max_images,
        pass_workers=pass_workers,
        direct_query=direct_query,
        open_browser_report=True,
        logger=None,
    )


def main() -> None:
    try:
        with sqlite3.connect(DB_FILE) as _conn:
            ensure_processed_food_table(_conn)
    except sqlite3.Error:
        pass

    while True:
        print_header()
        print(_bar())
        print("  🎛️ [ 메인 메뉴 ]")
        print("    [1] 👀 데이터 조회/탐색 (신규 viewer)")
        print("    [2] 🌐 공공 API 관리 (가공식품)")
        print("    [3] 💾 백업/복원 관리")
        print("    [4] 📊 analyze 벤치마크 도우미")
        print("    [5] 🧩 검색어 관리")
        print("    [6] 🚀 검색어 실행 (수집 → 분석 → 최종저장)")
        print("    [7] 🧭 브라우저 어드민 실행")
        print("    [q] 🚪 종료")
        print(_bar())
        choice = input("  👉 선택 : ").strip().lower()

        if choice == "1":
            run_data_viewer()
        elif choice == "2":
            run_public_api_menu()
        elif choice == "3":
            run_backup_menu()
        elif choice == "4":
            run_benchmark_menu()
        elif choice == "5":
            run_query_pipeline_menu()
        elif choice == "6":
            print("\n  🚀 [검색어 실행]")
            print("    [1] 터미널에서 실행")
            print("    [2] 브라우저 실행 UI 열기 (실시간)")
            sub = input("  👉 선택 : ").strip().lower()
            if sub == "1":
                run_query_pipeline_execute()
            elif sub == "2":
                run_query_web_monitor()
            else:
                print("  ⚠️ 올바른 번호를 입력해주세요.")
        elif choice == "7":
            run_admin_web()
        elif choice == "q":
            print("\n  👋 실행기를 종료합니다.\n")
            break
        else:
            print("\n  ⚠️ 올바른 메뉴 번호를 입력해주세요.\n")


if __name__ == "__main__":
    main()
