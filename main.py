"""
공공데이터 식품가공정보 API → 로컬 SQLite DB 저장
"""

import requests
import xml.etree.ElementTree as ET
import sqlite3
import sys

# ===== 설정 =====
SERVICE_KEY = "67ff6f3fae63eb631f0f9b320fc98bb7f53d28d9fcad137f98d351a7db3e9e4d"
BASE_URL = "http://api.data.go.kr/openapi/tn_pubr_public_nutri_process_info_api"
DB_FILE = "food_data.db"
ROWS_PER_PAGE = 1000  # 페이지당 요청 수 (최대 10000)

# 출력 컬럼 목록 (API 응답 필드)
COLUMNS = [
    "foodCd", "foodNm", "dataCd", "typeNm",
    "foodOriginCd", "foodOriginNm",
    "foodLv3Cd", "foodLv3Nm",
    "foodLv4Cd", "foodLv4Nm",
    "foodLv5Cd", "foodLv5Nm",
    "foodLv6Cd", "foodLv6Nm",
    "foodLv7Cd", "foodLv7Nm",
    "nutConSrtrQua",
    "enerc", "water", "prot", "fatce", "ash",
    "chocdf", "sugar", "fibtg",
    "ca", "fe", "p", "k", "nat",
    "vitaRae", "retol", "cartb",
    "thia", "ribf", "nia", "vitc", "vitd",
    "chole", "fasat", "fatrn",
    "srcCd", "srcNm",
    "servSize", "foodSize",
    "itemMnftrRptNo", "mfrNm", "imptNm", "distNm",
    "imptYn", "cooCd", "cooNm",
    "dataProdCd", "dataProdNm",
    "crtYmd", "crtrYmd",
    "instt_code", "instt_nm",
]
# =================


def init_db(conn: sqlite3.Connection) -> None:
    """테이블이 없으면 생성"""
    cols_def = ", ".join(f'"{col}" TEXT' for col in COLUMNS)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS food_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {cols_def}
        )
    """)
    conn.commit()


def fetch_page(page_no: int, num_of_rows: int) -> list[dict]:
    """API 한 페이지 호출 후 item 목록 반환"""
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": page_no,
        "numOfRows": num_of_rows,
        "type": "xml",
    }

    print(f"  [Page {page_no}] 요청 중 (numOfRows={num_of_rows})...", flush=True)
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
    except requests.RequestException as e:
        print(f"  HTTP 요청 실패: {e}", flush=True)
        return []

    if response.status_code != 200:
        print(f"  HTTP 오류: {response.status_code}", flush=True)
        return []

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as e:
        print(f"  XML 파싱 오류: {e}", flush=True)
        return []

    result_code = root.find(".//resultCode")
    if result_code is not None and result_code.text != "00":
        result_msg = root.find(".//resultMsg")
        msg = result_msg.text if result_msg is not None else "알 수 없음"
        print(f"  API 오류 [{result_code.text}]: {msg}", flush=True)
        return []

    items = root.findall(".//item")
    print(f"  → {len(items)}건 수신", flush=True)

    rows = []
    for item in items:
        row = {child.tag: child.text for child in item}
        rows.append(row)
    return rows


def insert_rows(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """rows를 DB에 삽입"""
    placeholders = ", ".join("?" for _ in COLUMNS)
    col_names = ", ".join(f'"{col}"' for col in COLUMNS)
    sql = f'INSERT INTO food_info ({col_names}) VALUES ({placeholders})'

    values = [tuple(row.get(col) for col in COLUMNS) for row in rows]
    conn.executemany(sql, values)
    conn.commit()


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
