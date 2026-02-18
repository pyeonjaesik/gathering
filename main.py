"""
통합 실행 허브
- 데이터 조회 뷰어
- 원재료명 추출 파이프라인
- 공공 API 수집
"""

import os
import sys

import collector
import sqlite3
import viewer
from config import DB_FILE
from dedupe_tools import (
    duplicate_conditions,
    get_duplicate_samples,
    get_duplicate_stats,
    run_dedupe,
)
from ingredient_enricher import run_enricher
from ingredient_enricher import get_priority_subcategories

W = 68


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


def run_ingredient_menu() -> None:
    if not os.getenv("SERPAPI_KEY"):
        print("\n  ❌ 오류: SERPAPI_KEY 환경변수가 필요합니다.")
        print('  💡 예) export SERPAPI_KEY="YOUR_KEY"')
        return

    print("\n  🧪 [원재료명 추출 대상 선택]")
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
    raw_limit = input("  🔹 처리할 최대 상품 수 [기본 20]: ").strip()
    raw_seed = input("  🔹 랜덤 시드(모의 분석용) [기본 7]: ").strip()
    raw_quiet = input("  🔹 이미지별 상세 로그 생략? [y/N]: ").strip().lower()

    limit = 20
    seed = 7
    if raw_limit:
        try:
            limit = int(raw_limit)
        except ValueError:
            print("  ⚠️ 잘못된 limit 입력입니다. 기본값 20으로 진행합니다.")
    if raw_seed:
        try:
            seed = int(raw_seed)
        except ValueError:
            print("  ⚠️ 잘못된 seed 입력입니다. 기본값 7로 진행합니다.")

    quiet = raw_quiet == "y"

    print("\n  🚀 [실행] 선택한 중분류의 원재료 수집을 시작합니다.")
    print(f"  🎯 대상: {selected['lv3']} > {selected['lv4']}")
    print(
        f"  📦 현황: 총 {selected['total_count']:,} / "
        f"시도 {selected['attempted_count']:,} / 성공 {selected['success_count']:,} "
        f"({selected['success_rate']:.1f}%)"
    )
    print()
    run_enricher(
        limit=limit,
        seed=seed,
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


def _print_duplicate_stats(stats: dict[str, int]) -> None:
    print("  📊 [중복 현황]")
    print(f"    총 레코드                : {stats['total_rows']:,}")
    print(f"    A(foodCd) 그룹/초과행    : {stats['foodCd_groups']:,} / {stats['foodCd_extra']:,}")
    print(f"    B(이름+용량+카테고리)    : {stats['h1_groups']:,} / {stats['h1_extra']:,}")
    print(f"    C(이름+영양+카테고리)    : {stats['h2_groups']:,} / {stats['h2_extra']:,}")


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

            with sqlite3.connect(DB_FILE) as conn:
                result = run_dedupe(conn)
                after = get_duplicate_stats(conn)

            print("\n  ✅ [삭제 결과]")
            print(f"    - 규칙 A 삭제: {result['removed_a']:,}건")
            print(f"    - 규칙 B 삭제: {result['removed_b']:,}건")
            print(f"    - 규칙 C 삭제: {result['removed_c']:,}건")
            print(f"    - 총 삭제   : {result['removed_total']:,}건")
            print(f"    - 삭제 목록 CSV : {result['csv_path']}")

            print("\n  📌 [실행 후 통계]")
            _print_duplicate_stats(after)
        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def main() -> None:
    while True:
        print_header()
        print(_bar())
        print("  🎛️ [ 메인 메뉴 ]")
        print("    [1] 👀 데이터 조회/탐색 (viewer)")
        print("    [2] 🧪 원재료명 추출 실행")
        print("    [3] 🌐 공공 API 데이터 수집")
        print("    [4] 🧹 중복 데이터 점검/삭제")
        print("    [q] 🚪 종료")
        print(_bar())
        choice = input("  👉 선택 : ").strip().lower()

        if choice == "1":
            run_data_viewer()
        elif choice == "2":
            run_ingredient_menu()
        elif choice == "3":
            run_public_api_collection()
        elif choice == "4":
            run_duplicate_menu()
        elif choice == "q":
            print("\n  👋 실행기를 종료합니다.\n")
            break
        else:
            print("\n  ⚠️ 올바른 메뉴 번호를 입력해주세요.\n")


if __name__ == "__main__":
    main()
