"""
공공데이터 식품가공정보 API → 로컬 SQLite DB 저장
"""

import math
import sqlite3
import sys
import time

from app.api import fetch_pages_parallel, fetch_total_count
from app.config import COLUMNS, DB_FILE, MAX_WORKERS, ROWS_PER_PAGE
from app.database import (
    get_completed_pages,
    init_db,
    init_progress_table,
    insert_rows,
    mark_page_done,
)

# 출력 너비
W = 60
CHUNK_RETRY_ROUNDS = 2


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


def expected_rows_for_page(page_no: int, target_count: int, rows_per_page: int) -> int:
    """목표 건수 기준으로 페이지에 속하는 최대 행 수 계산."""
    start = (page_no - 1) * rows_per_page
    if start >= target_count:
        return 0
    return min(rows_per_page, target_count - start)


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
    init_progress_table(conn)
    print("  ✔ food_info 테이블 준비 완료")

    # ── 데이터 수집 및 저장 ─────────────────────────────────────
    print_section("[ 2단계 ] 데이터 수집 및 저장")

    total_pages = math.ceil(target_count / ROWS_PER_PAGE)
    # 한 번에 처리할 페이지 묶음 크기 (MAX_WORKERS 배수로 설정)
    chunk_size = MAX_WORKERS

    completed_pages = get_completed_pages(conn, ROWS_PER_PAGE)
    completed_pages = {p for p in completed_pages if 1 <= p <= total_pages}
    remaining_pages = [p for p in range(1, total_pages + 1) if p not in completed_pages]

    progressed = sum(
        expected_rows_for_page(p, target_count, ROWS_PER_PAGE) for p in completed_pages
    )
    saved = 0
    failed_pages_run = 0
    last_rows: list[dict] = []
    start_time = time.time()

    print(
        f"  총 {total_pages:,}페이지 × 최대 {ROWS_PER_PAGE:,}건/페이지"
        f"  (병렬 {MAX_WORKERS}개 동시 요청)",
        flush=True,
    )
    if completed_pages:
        print(
            f"  ↺ 재개 모드: 완료 {len(completed_pages):,}페이지 건너뜀, "
            f"남은 {len(remaining_pages):,}페이지 처리",
            flush=True,
        )

    if not remaining_pages:
        print("  ✔ 이미 모든 페이지가 완료 상태입니다.")
        print_progress_bar(target_count, target_count)

    for chunk_start in range(0, len(remaining_pages), chunk_size):
        chunk_page_nos = remaining_pages[chunk_start : chunk_start + chunk_size]
        chunk_begin = chunk_page_nos[0]
        chunk_end = chunk_page_nos[-1]

        print(
            f"\n  📡 [{chunk_begin}~{chunk_end}페이지]"
            f" {len(chunk_page_nos)}개 동시 요청 중...",
            flush=True,
        )

        chunk_results = fetch_pages_parallel(chunk_page_nos, ROWS_PER_PAGE, MAX_WORKERS)
        retry_workers = max(1, MAX_WORKERS // 2)
        for retry_round in range(1, CHUNK_RETRY_ROUNDS + 1):
            failed_in_round = [
                p for p in chunk_page_nos
                if p not in chunk_results or not chunk_results[p][1]
            ]
            if not failed_in_round:
                break
            print(
                f"  ↺ 실패 페이지 재시도 {retry_round}/{CHUNK_RETRY_ROUNDS}: "
                f"{len(failed_in_round)}페이지",
                flush=True,
            )
            retry_results = fetch_pages_parallel(
                failed_in_round,
                ROWS_PER_PAGE,
                retry_workers,
            )
            chunk_results.update(retry_results)

        # 페이지 순서대로 DB에 삽입
        chunk_received = 0
        chunk_completed = 0
        chunk_failed_pages: list[int] = []
        for page_no in chunk_page_nos:
            page_result = chunk_results.get(page_no)
            if page_result is None:
                chunk_failed_pages.append(page_no)
                continue
            rows, ok = page_result
            if not ok:
                chunk_failed_pages.append(page_no)
                continue

            max_rows = expected_rows_for_page(page_no, target_count, ROWS_PER_PAGE)
            rows = rows[:max_rows]
            if rows:
                insert_rows(conn, rows)
                saved += len(rows)
                chunk_received += len(rows)
                last_rows = rows

            mark_page_done(conn, page_no, ROWS_PER_PAGE, len(rows))
            progressed += max_rows
            chunk_completed += 1

        failed_pages_run += len(chunk_failed_pages)
        if chunk_failed_pages:
            failed_preview = ", ".join(str(p) for p in chunk_failed_pages[:8])
            suffix = " ..." if len(chunk_failed_pages) > 8 else ""
            print(
                f"  ⚠ 이번 청크 실패 페이지 {len(chunk_failed_pages)}개: "
                f"{failed_preview}{suffix}",
                flush=True,
            )

        elapsed = time.time() - start_time
        print(
            f"  ✔ {chunk_received:,}건 저장 완료, {chunk_completed:,}페이지 완료 처리"
            f"  (이번 실행 누적 저장: {saved:,}건 / 경과: {format_elapsed(elapsed)})",
            flush=True,
        )
        print_progress_bar(min(progressed, target_count), target_count)

    final_completed = get_completed_pages(conn, ROWS_PER_PAGE)
    final_completed = {p for p in final_completed if 1 <= p <= total_pages}
    print_data_preview(last_rows)
    elapsed_total = time.time() - start_time

    # ── 완료 요약 ───────────────────────────────────────────────
    print_section("[ 완료 ] 저장 요약")
    print(f"  ✔ 이번 실행 저장: {saved:,}건")
    print(f"  ✔ 완료 페이지   : {len(final_completed):,}/{total_pages:,}페이지")
    print(f"  ⚠ 미완료 페이지 : {failed_pages_run:,}개 (다음 실행 시 자동 재시도)")
    print(f"  📁 파일    : {DB_FILE}")
    print(f"  📋 테이블  : food_info")
    print(f"  📊 컬럼 수 : {len(COLUMNS)}개 필드")
    print(f"  ⏱  소요    : {format_elapsed(elapsed_total)}")
    print()
    conn.close()


if __name__ == "__main__":
    main()
