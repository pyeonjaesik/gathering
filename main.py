"""
공공데이터 식품가공정보 API → 로컬 SQLite DB 저장
"""

import sqlite3
import sys

from api import fetch_page
from config import DB_FILE, ROWS_PER_PAGE
from database import init_db, insert_rows


def main() -> None:
    # 사용자 입력: 저장할 데이터 개수
    if len(sys.argv) >= 2:
        try:
            target_count = int(sys.argv[1])
        except ValueError:
            print("오류: 숫자를 입력해주세요. 예) python main.py 500")
            sys.exit(1)
    else:
        try:
            target_count = int(input("저장할 데이터 개수를 입력하세요: ").strip())
        except ValueError:
            print("오류: 올바른 숫자를 입력해주세요.")
            sys.exit(1)

    if target_count <= 0:
        print("오류: 1 이상의 숫자를 입력해주세요.")
        sys.exit(1)

    print(f"\n목표: {target_count}건을 {DB_FILE} 에 저장합니다.\n")

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)

    saved = 0
    page_no = 1

    while saved < target_count:
        remaining = target_count - saved
        num_of_rows = min(remaining, ROWS_PER_PAGE)

        rows = fetch_page(page_no, num_of_rows)
        if not rows:
            print("더 이상 데이터가 없거나 오류가 발생했습니다. 종료합니다.")
            break

        # 목표 초과 방지 (API가 numOfRows보다 더 줄 경우 대비)
        rows = rows[: target_count - saved]

        insert_rows(conn, rows)
        saved += len(rows)
        print(f"  누적 저장: {saved}/{target_count}건", flush=True)

        if len(rows) < num_of_rows:
            # 마지막 페이지
            break

        page_no += 1

    conn.close()
    print(f"\n완료: 총 {saved}건이 '{DB_FILE}' (테이블: food_info) 에 저장되었습니다.")


if __name__ == "__main__":
    main()
