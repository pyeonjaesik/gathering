"""
공공데이터 식품가공정보 API → 로컬 SQLite DB 저장
"""

import sqlite3
import sys
import time

from api import fetch_page, fetch_total_count
from config import COLUMNS, DB_FILE, ROWS_PER_PAGE
from database import init_db, insert_rows

# 출력 너비
W = 60


def _bar(char: str = "─") -> str:
    return "  " + char * (W - 4)


def print_header() -> None:
    title = "공공데이터 식품가공정보 수집기"
    inner = W - 2
    pad_left = (inner - len(title)) // 2
    pad_right = inner - pad_left - len(title)
    print()
    print("╔" + "═" * inner + "╗")
    print("║" + " " * pad_left + title + " " * pad_right + "║")
    print("╚" + "═" * inner + "╝")
    print()


def print_section(title: str) -> None:
    print()
    print(_bar("─"))
    print(f"  {title}")
    print(_bar("─"))


def print_progress_bar(current: int, total: int, bar_width: int = 36) -> None:
    ratio = current / total if total > 0 else 1.0
    filled = int(bar_width * ratio)
    bar = "█" * filled + "░" * (bar_width - filled)
    percent = ratio * 100
    print(
        f"\n  진행률  [{bar}] {current:,}/{total:,}건 ({percent:.1f}%)",
        flush=True,
    )


def print_data_preview(rows: list[dict], preview_count: int = 3) -> None:
    """최근 저장된 데이터 샘플을 사람이 읽기 좋게 출력"""
    if not rows:
        return
    samples = rows[-preview_count:]
    count = len(samples)
    print(f"\n  ┌─ 저장 샘플 (최근 {count}건) {'─' * (W - 22)}┐")
    for i, row in enumerate(samples, 1):
        food_nm  = (row.get("foodNm")  or "—")[:22]
        food_cd  = (row.get("foodCd")  or "—")[:14]
        category = (row.get("foodLv4Nm") or row.get("foodLv3Nm") or "—")[:16]
        enerc    = row.get("enerc") or "—"
        print(f"  │  [{i}] 식품명 : {food_nm}")
        print(f"  │       코드   : {food_cd}")
        print(f"  │       분류   : {category}")
        print(f"  │       에너지 : {enerc} kcal")
        if i < count:
            print("  │")
    print("  └" + "─" * (W - 4) + "┘")


def format_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}초"
    m, s = divmod(int(seconds), 60)
    return f"{m}분 {s}초"


def main() -> None:
    # ── 사용자 입력 ─────────────────────────────────────────────
    raw = ""
    if len(sys.argv) >= 2:
        raw = sys.argv[1].strip()
    else:
        raw = input("저장할 데이터 개수를 입력하세요 (0 또는 '전체' = 전체 수집): ").strip()

    fetch_all = raw in ("0", "전체", "all")

    if fetch_all:
        # 전체 수집 모드: API에서 totalCount를 먼저 조회
        print_header()
        print("  전체 수집 모드 — API에서 전체 건수를 조회합니다...")
        target_count = fetch_total_count()
        if target_count <= 0:
            print("  오류: 전체 건수를 가져오지 못했습니다. API 키와 네트워크를 확인해주세요.")
            sys.exit(1)
        print(f"  API 전체 데이터 : {target_count:,}건")
        answer = input("  전체를 수집하시겠습니까? [y/N] : ").strip().lower()
        if answer != "y":
            print("  수집을 취소했습니다.")
            sys.exit(0)
    else:
        try:
            target_count = int(raw)
        except ValueError:
            print("오류: 숫자를 입력해주세요. 예) python main.py 500  /  python main.py 0")
            sys.exit(1)

        if target_count <= 0:
            print("오류: 1 이상의 숫자를 입력하거나, 전체 수집은 0을 입력하세요.")
            sys.exit(1)

    # ── 헤더 출력 ───────────────────────────────────────────────
    if not fetch_all:
        print_header()
    print(f"  목표   : {target_count:,}건")
    print(f"  저장소 : {DB_FILE}  (테이블: food_info)")

    # ── DB 초기화 ───────────────────────────────────────────────
    print_section("[ 1단계 ] DB 초기화")
    conn = sqlite3.connect(DB_FILE)
    print(f"  ✔ {DB_FILE} 연결 완료")
    init_db(conn)
    print("  ✔ food_info 테이블 준비 완료")

    # ── 데이터 수집 및 저장 ─────────────────────────────────────
    print_section("[ 2단계 ] 데이터 수집 및 저장")

    saved = 0
    page_no = 1
    start_time = time.time()

    while saved < target_count:
        remaining = target_count - saved
        num_of_rows = min(remaining, ROWS_PER_PAGE)
        range_start = saved + 1
        range_end   = saved + num_of_rows

        print(
            f"\n  📡 [Page {page_no}] API 요청 중"
            f"  ({range_start:,} ~ {range_end:,}번째 데이터)...",
            flush=True,
        )

        rows = fetch_page(page_no, num_of_rows)

        if not rows:
            print("\n  ⚠  데이터가 없거나 오류가 발생했습니다. 수집을 종료합니다.")
            break

        # 목표 초과 방지
        rows = rows[: target_count - saved]

        received = len(rows)
        print(f"  ✔ {received:,}건 수신 완료", flush=True)

        print(f"  💾 DB에 {received:,}건 저장 중...", flush=True)
        insert_rows(conn, rows)
        saved += received
        elapsed_now = time.time() - start_time
        print(f"  ✔ {received:,}건 저장 완료  (누적: {saved:,}건 / 경과: {format_elapsed(elapsed_now)})")

        print_data_preview(rows)
        print_progress_bar(saved, target_count)

        if received < num_of_rows:
            # 마지막 페이지
            break

        page_no += 1

    conn.close()
    elapsed_total = time.time() - start_time

    # ── 완료 요약 ───────────────────────────────────────────────
    print_section("[ 완료 ] 저장 요약")
    print(f"  ✔ 총 {saved:,}건 저장 완료")
    print(f"  📁 파일    : {DB_FILE}")
    print(f"  📋 테이블  : food_info")
    print(f"  📊 컬럼 수 : {saved:,}건 × {len(COLUMNS)}개 필드")
    print(f"  ⏱  소요    : {format_elapsed(elapsed_total)}")
    print()


if __name__ == "__main__":
    main()
