"""
통합 실행 허브
- 데이터 조회 뷰어
- 원재료명 추출 파이프라인
- 공공 API 수집
"""

import os
import json
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
from app.query_image_benchmark import run_query_image_benchmark_interactive
from app.serp_simple_debug import run_serp_simple_debug_interactive
from app.query_pipeline import (
    cache_serp_images,
    finish_query_run,
    get_image_analysis_cache,
    init_query_pipeline_tables,
    list_next_queries,
    list_recent_runs,
    start_query_run,
    upsert_food_final,
    upsert_image_analysis_cache,
    upsert_query,
)

W = 68
WEB_UI_PORT = 8501
WEB_UI_URL = f"http://localhost:{WEB_UI_PORT}"


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


def run_web_monitor() -> None:
    if _is_port_open(WEB_UI_PORT):
        print(f"\n  🌐 웹 모니터가 이미 실행 중입니다. 브라우저를 엽니다: {WEB_UI_URL}")
        webbrowser.open_new_tab(WEB_UI_URL)
        return

    project_root = Path(__file__).resolve().parent.parent
    log_path = project_root / "streamlit_web_ui.log"
    env = os.environ.copy()
    env.setdefault("UV_CACHE_DIR", "/tmp/uv-cache")

    cmd = [
        "uv",
        "run",
        "streamlit",
        "run",
        "app/web_ui.py",
        "--server.port",
        str(WEB_UI_PORT),
        "--server.headless",
        "true",
    ]

    print("\n  🚀 웹 모니터 서버를 시작합니다...")
    print(f"  - URL: {WEB_UI_URL}")
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
        print(f"  ❌ 웹 모니터 실행 실패: {exc}")
        return

    for _ in range(20):
        if _is_port_open(WEB_UI_PORT):
            print("  ✅ 웹 모니터 준비 완료. 브라우저를 엽니다.")
            webbrowser.open_new_tab(WEB_UI_URL)
            return
        time.sleep(0.5)

    print("  ⚠️ 서버 시작이 지연되고 있습니다. 수동으로 URL을 열어주세요.")
    print(f"  👉 {WEB_UI_URL}")
    print(f"  💡 문제 확인: {log_path}")


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
        print("\n  🧩 [검색어 파이프라인 관리]")
        print("    [1] 검색어 직접 추가")
        print("    [2] 우선순위 대기 검색어 보기")
        print("    [3] 최근 실행 기록 보기")
        print("    [4] 검색어 실행 (SERP -> 분석 -> 최종저장)")
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

        elif sub == "3":
            raw = input("  🔹 조회 개수 [기본 20]: ").strip()
            limit = 20
            if raw:
                try:
                    limit = max(1, int(raw))
                except ValueError:
                    print("  ⚠️ 숫자 입력이 아니어서 기본 20을 사용합니다.")
                    limit = 20
            with sqlite3.connect(DB_FILE) as conn:
                init_query_pipeline_tables(conn)
                rows = list_recent_runs(conn, limit=limit)
            print("\n  🕘 [최근 실행]")
            if not rows:
                print("    (없음)")
            else:
                for row in rows:
                    print(
                        f"    - run={row['id']} | query_id={row['query_id']} | status={row['status']} "
                        f"| images={row['analyzed_images']}/{row['total_images']} "
                        f"| saved={row['final_saved_count']} | score={row['overall_score']:.1f}"
                    )
                    print(f"      q={row['query_text']}")

        elif sub == "4":
            run_query_pipeline_execute()

        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_query_pool_browser_view() -> None:
    with sqlite3.connect(DB_FILE) as conn:
        out_path = viewer.open_query_pool_browser_report(conn)
    print(f"\n  ✅ 검색어 풀 브라우저 리포트 생성: {out_path}")


def run_query_pipeline_execute() -> None:
    from app import config as app_config
    app_config.reload_dotenv()

    serp_key = os.getenv("SERPAPI_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not openai_key:
        print("  ❌ OPENAI_API_KEY가 필요합니다.")
        return

    raw_q = input("  🔹 실행할 pending 검색어 개수 [기본 3]: ").strip()
    raw_pages = input("  🔹 검색 페이지 수 [기본 1]: ").strip()
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

    if provider not in ("naver_official", "naver_blog", "naver_shop") and not serp_key:
        print("  ❌ SERPAPI_KEY가 필요합니다.")
        return
    if provider in ("naver_official", "naver_blog", "naver_shop"):
        naver_client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
        naver_client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
        if not (naver_client_id and naver_client_secret):
            print("  ❌ NAVER_CLIENT_ID / NAVER_CLIENT_SECRET이 필요합니다.")
            return
    query_limit = int(raw_q) if raw_q.isdigit() else 3
    max_pages = int(raw_pages) if raw_pages.isdigit() else 1
    max_images = int(raw_max_images) if raw_max_images.isdigit() else 0
    pass_workers = int(raw_workers) if raw_workers.isdigit() else 5
    query_limit = max(1, query_limit)
    max_pages = max(1, min(20, max_pages))
    max_images = max(0, max_images)
    pass_workers = max(1, min(50, pass_workers))

    from app.query_image_benchmark import _search_images_all

    with sqlite3.connect(DB_FILE) as conn:
        init_query_pipeline_tables(conn)
        conn.row_factory = sqlite3.Row
        queries = conn.execute(
            """
            SELECT id, query_text, priority_score, status
            FROM query_pool
            WHERE status='pending'
            ORDER BY priority_score DESC, id ASC
            LIMIT ?
            """,
            (query_limit,),
        ).fetchall()

        if not queries:
            print("  ⚠️ 실행할 pending 검색어가 없습니다.")
            return

        print(f"\n  🚀 실행 시작: pending {len(queries)}개")
        print(f"    검색엔진: {provider}")

        for q in queries:
            query_id = int(q["id"])
            query_text = str(q["query_text"] or "").strip()
            run_id = start_query_run(conn, query_id=query_id)
            print(f"\n  ▶ query_id={query_id} run_id={run_id}")
            print(f"    q={query_text}")

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

                print(f"    수집 이미지: {total_images}개 | pass 동시호출: {pass_workers}")

                to_process: list[tuple[int, object]] = []
                for idx, img in enumerate(images, 1):
                    cached = get_image_analysis_cache(conn, img.url)
                    if cached and int(cached["pass4_ok"] or 0) == 1:
                        print(f"    [{idx}/{total_images}] 캐시통과 스킵")
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
                    }

                    pass2 = az.analyze_pass2(image_url=img_url, target_item_rpt_no=None)
                    result["api_calls"] += 1
                    qf = pass2.get("quality_flags") or {}
                    p2a_ok = bool(qf.get("pass2a_ok"))
                    p2b_ok = bool(qf.get("pass2b_pass"))
                    gate_ok = bool(pass2.get("quality_gate_pass"))
                    decision = str(pass2.get("ai_decision") or "").upper()
                    pass2_ok = gate_ok and p2a_ok and p2b_ok and decision == "READ"
                    result["p2a_ok"] = p2a_ok
                    result["p2b_ok"] = p2b_ok
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
                    pass3 = az.analyze_pass3(
                        image_url=img_url,
                        target_item_rpt_no=None,
                        include_nutrition=bool(qf.get("has_nutrition_section")),
                    )
                    result["api_calls"] += 1
                    result["raw_pass3"] = pass3.get("raw_model_text_pass3_ingredients")
                    pass3_err = str(pass3.get("error") or "").strip()
                    if pass3_err:
                        result["fail_stage"] = "pass3"
                        result["fail_reason"] = pass3_err
                        return result

                    product_name = (pass3.get("product_name_in_image") or "").strip()
                    report_no = (pass3.get("product_report_number") or "").strip()
                    ingredients_text = (pass3.get("ingredients_text") or "").strip()
                    nutrition_text = (pass3.get("nutrition_text") or "").strip() or None
                    if not (report_no and ingredients_text):
                        result["fail_stage"] = "pass3"
                        result["fail_reason"] = "required_fields_missing"
                        return result

                    result["pass3_ok"] = True
                    result["product_name"] = product_name
                    result["report_no"] = report_no
                    result["ingredients_text"] = ingredients_text
                    result["nutrition_text"] = nutrition_text

                    pass4 = az.analyze_pass4_normalize(
                        pass2_result=pass2,
                        pass3_result=pass3,
                        target_item_rpt_no=None,
                    )
                    result["api_calls"] += 1
                    result["raw_pass4"] = pass4.get("raw_model_text_pass4_ingredients")
                    pass4_err = str(pass4.get("pass4_ai_error") or "").strip()
                    if pass4_err:
                        result["fail_stage"] = "pass4"
                        result["fail_reason"] = pass4_err or "pass4_fail"
                        return result

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
                        print(f"    [{idx}/{total_images}] 완료 ({done_count}/{len(to_process)})")

                        if not bool(res.get("pass2_ok")):
                            upsert_image_analysis_cache(
                                conn,
                                image_url=str(res["url"]),
                                run_id=run_id,
                                pass1_ok=None,
                                pass2a_ok=bool(res.get("p2a_ok")),
                                pass2b_ok=bool(res.get("p2b_ok")),
                                pass3_ok=False,
                                pass4_ok=False,
                                fail_stage=str(res.get("fail_stage") or "pass2"),
                                fail_reason=str(res.get("fail_reason") or "pass2_fail"),
                                raw_pass2a=res.get("raw_pass2a"),
                                raw_pass2b=res.get("raw_pass2b"),
                            )
                            continue

                        pass2b_pass_count += 1

                        if not bool(res.get("pass3_ok")):
                            upsert_image_analysis_cache(
                                conn,
                                image_url=str(res["url"]),
                                run_id=run_id,
                                pass1_ok=None,
                                pass2a_ok=bool(res.get("p2a_ok")),
                                pass2b_ok=bool(res.get("p2b_ok")),
                                pass3_ok=False,
                                pass4_ok=False,
                                fail_stage=str(res.get("fail_stage") or "pass3"),
                                fail_reason=str(res.get("fail_reason") or "pass3_fail"),
                                raw_pass2a=res.get("raw_pass2a"),
                                raw_pass2b=res.get("raw_pass2b"),
                                raw_pass3=res.get("raw_pass3"),
                            )
                            continue

                        if not bool(res.get("pass4_ok")):
                            upsert_image_analysis_cache(
                                conn,
                                image_url=str(res["url"]),
                                run_id=run_id,
                                pass1_ok=None,
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
                            )
                            continue

                        pass4_pass_count += 1
                        upsert_food_final(
                            conn,
                            product_name=str(res.get("product_name") or ""),
                            item_mnftr_rpt_no=str(res.get("report_no") or ""),
                            ingredients_text=str(res.get("ingredients_text") or ""),
                            nutrition_text=res.get("nutrition_text"),
                            nutrition_source="pass3",
                            source_image_url=str(res["url"]),
                            source_query_id=query_id,
                            source_run_id=run_id,
                        )
                        final_saved_count += 1

                        upsert_image_analysis_cache(
                            conn,
                            image_url=str(res["url"]),
                            run_id=run_id,
                            pass1_ok=None,
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
                        )

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
                print(
                    f"    ✅ 완료: analyzed={analyzed_images}, pass2b={pass2b_pass_count}, "
                    f"pass4={pass4_pass_count}, saved={final_saved_count}"
                )
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
                print(f"    ❌ 실패: {exc}")


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
        print("    [5] 🧩 검색어 파이프라인 관리")
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
        elif choice == "q":
            print("\n  👋 실행기를 종료합니다.\n")
            break
        else:
            print("\n  ⚠️ 올바른 메뉴 번호를 입력해주세요.\n")


if __name__ == "__main__":
    main()
