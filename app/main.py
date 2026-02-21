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
from pathlib import Path
from datetime import datetime

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
from app.query_pipeline import (
    init_query_pipeline_tables,
    list_next_queries,
    list_recent_runs,
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
        print("    [1] 검색어 기반 이미지 벤치마크 (SerpAPI)")
        print("    [b] 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            try:
                run_query_image_benchmark_interactive()
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
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            query_text = input("  🔹 검색어 입력: ").strip()
            if not query_text:
                print("  ⚠️ 검색어가 비어 있습니다.")
                continue
            raw_pri = input("  🔹 priority_score [기본 0]: ").strip()
            raw_seg = input("  🔹 target_segment_score [기본 0]: ").strip()
            notes = input("  🔹 메모(선택): ").strip() or None
            try:
                pri = float(raw_pri) if raw_pri else 0.0
                seg = float(raw_seg) if raw_seg else 0.0
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
                    target_segment_score=seg,
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

        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def _build_query_pool_html(rows: list[sqlite3.Row]) -> str:
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for r in rows:
        status = str(r["status"] or "unknown")
        source = str(r["source"] or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        source_counts[source] = source_counts.get(source, 0) + 1

    status_badges = " ".join(
        f"<span class='badge'>{html.escape(k)}: {v:,}</span>"
        for k, v in sorted(status_counts.items(), key=lambda x: (-x[1], x[0]))
    )
    source_badges = " ".join(
        f"<span class='badge'>{html.escape(k)}: {v:,}</span>"
        for k, v in sorted(source_counts.items(), key=lambda x: (-x[1], x[0]))
    )

    table_rows: list[str] = []
    for r in rows:
        table_rows.append(
            "<tr>"
            f"<td>{int(r['id'])}</td>"
            f"<td>{html.escape(str(r['status'] or ''))}</td>"
            f"<td>{html.escape(str(r['source'] or ''))}</td>"
            f"<td class='num'>{float(r['priority_score'] or 0.0):.1f}</td>"
            f"<td class='num'>{float(r['target_segment_score'] or 0.0):.1f}</td>"
            f"<td class='num'>{int(r['run_count'] or 0)}</td>"
            f"<td>{html.escape(str(r['last_run_at'] or '-'))}</td>"
            f"<td class='query'>{html.escape(str(r['query_text'] or ''))}</td>"
            "</tr>"
        )
    tbody = "\n".join(table_rows)

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>검색어 풀 조회</title>
  <style>
    :root {{
      --bg: #f6f8fb;
      --panel: #ffffff;
      --line: #d9e0ea;
      --text: #1f2937;
      --muted: #6b7280;
      --accent: #1f6feb;
      --badge: #eef4ff;
    }}
    body {{
      margin: 0;
      font-family: 'Apple SD Gothic Neo', 'Noto Sans KR', 'Malgun Gothic', sans-serif;
      color: var(--text);
      background: linear-gradient(180deg, #f9fbff 0%, var(--bg) 100%);
    }}
    .wrap {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px 18px;
      margin-bottom: 14px;
      box-shadow: 0 2px 10px rgba(31,41,55,0.04);
    }}
    h1 {{ margin: 0 0 6px; font-size: 24px; }}
    .sub {{ color: var(--muted); font-size: 14px; margin-bottom: 10px; }}
    .badge {{
      display: inline-block;
      margin: 4px 6px 0 0;
      padding: 4px 10px;
      border-radius: 999px;
      background: var(--badge);
      border: 1px solid #dbe7ff;
      font-size: 12px;
      color: #1e3a8a;
    }}
    .controls {{
      display: grid;
      grid-template-columns: 1fr 220px;
      gap: 10px;
      align-items: center;
    }}
    input, select {{
      width: 100%;
      font-size: 14px;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      background: #fff;
      box-sizing: border-box;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    thead th {{
      position: sticky; top: 0; z-index: 1;
      background: #eef3fb;
      border-bottom: 1px solid var(--line);
      text-align: left;
      padding: 10px 8px;
      white-space: nowrap;
    }}
    tbody td {{
      border-bottom: 1px solid #edf1f7;
      padding: 8px;
      vertical-align: top;
    }}
    tbody tr:hover {{ background: #f8fbff; }}
    .num {{ text-align: right; white-space: nowrap; }}
    .query {{ min-width: 420px; }}
    .small {{ color: var(--muted); font-size: 12px; margin-top: 8px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>검색어 풀 조회</h1>
      <div class="sub">query_pool 전체를 브라우저에서 가독성 높게 조회합니다.</div>
      <div><strong>총 검색어:</strong> {len(rows):,}</div>
      <div style="margin-top:8px;"><strong>상태 분포</strong><br>{status_badges or "-"}</div>
      <div style="margin-top:8px;"><strong>소스 분포</strong><br>{source_badges or "-"}</div>
    </div>

    <div class="card">
      <div class="controls">
        <input id="q" type="text" placeholder="검색어/소스/상태/카테고리 텍스트 검색" />
        <select id="statusFilter">
          <option value="">전체 상태</option>
          <option value="pending">pending</option>
          <option value="paused">paused</option>
          <option value="running">running</option>
          <option value="done">done</option>
          <option value="failed">failed</option>
        </select>
      </div>
      <div class="small">필터는 실시간 적용됩니다.</div>
    </div>

    <div class="card" style="padding:0; overflow:auto; max-height:70vh;">
      <table id="tbl">
        <thead>
          <tr>
            <th>ID</th>
            <th>상태</th>
            <th>소스</th>
            <th>우선점수</th>
            <th>세그점수</th>
            <th>run</th>
            <th>마지막 실행</th>
            <th>검색어</th>
          </tr>
        </thead>
        <tbody>
          {tbody}
        </tbody>
      </table>
    </div>
  </div>

  <script>
    const q = document.getElementById('q');
    const sf = document.getElementById('statusFilter');
    const rows = Array.from(document.querySelectorAll('#tbl tbody tr'));
    function applyFilter() {{
      const text = (q.value || '').toLowerCase();
      const st = (sf.value || '').toLowerCase();
      rows.forEach((tr) => {{
        const t = tr.textContent.toLowerCase();
        const statusCell = (tr.children[1]?.textContent || '').toLowerCase().trim();
        const matchText = !text || t.includes(text);
        const matchStatus = !st || statusCell === st;
        tr.style.display = (matchText && matchStatus) ? '' : 'none';
      }});
    }}
    q.addEventListener('input', applyFilter);
    sf.addEventListener('change', applyFilter);
  </script>
</body>
</html>
"""


def run_query_pool_browser_view() -> None:
    with sqlite3.connect(DB_FILE) as conn:
        init_query_pipeline_tables(conn)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, query_text, source, status, priority_score, target_segment_score, run_count, last_run_at
            FROM query_pool
            ORDER BY priority_score DESC, target_segment_score DESC, id ASC
            """
        ).fetchall()

    reports_dir = Path(__file__).resolve().parent.parent / "reports" / "query_pool"
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = reports_dir / f"query_pool_{ts}.html"
    out_path.write_text(_build_query_pool_html(rows), encoding="utf-8")

    print(f"\n  ✅ 검색어 풀 브라우저 리포트 생성: {out_path}")
    webbrowser.open_new_tab(out_path.resolve().as_uri())


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
