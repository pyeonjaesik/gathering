"""
SQLite DB 초기화 및 데이터 삽입 기능
"""

import sqlite3

from config import COLUMNS


def init_db(conn: sqlite3.Connection) -> None:
    """테이블이 없으면 생성 후 유니크 인덱스 보장"""
    cols_def = ", ".join(f'"{col}" TEXT' for col in COLUMNS)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS food_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {cols_def}
        )
    """)
    conn.commit()
    _ensure_unique_index(conn)


def _ensure_unique_index(conn: sqlite3.Connection) -> None:
    """itemMnftrRptNo 컬럼에 유니크 인덱스가 없으면 생성.
    기존 테이블에 중복 데이터가 있을 경우 id가 가장 작은 행만 남기고 제거한다.
    NULL이거나 빈 문자열인 행은 중복 대상에서 제외한다.
    """
    cursor = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND tbl_name='food_info' AND name='uq_itemMnftrRptNo'"
    )
    if cursor.fetchone():
        return  # 이미 존재함

    # 중복 데이터 제거: 같은 itemMnftrRptNo 중 id가 가장 작은 행만 유지
    conn.execute("""
        DELETE FROM food_info
        WHERE itemMnftrRptNo IS NOT NULL
          AND itemMnftrRptNo != ''
          AND id NOT IN (
              SELECT MIN(id)
              FROM food_info
              WHERE itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != ''
              GROUP BY itemMnftrRptNo
          )
    """)

    # NULL·빈 문자열을 제외한 부분 유니크 인덱스 생성
    conn.execute("""
        CREATE UNIQUE INDEX uq_itemMnftrRptNo
        ON food_info("itemMnftrRptNo")
        WHERE "itemMnftrRptNo" IS NOT NULL AND "itemMnftrRptNo" != ''
    """)
    conn.commit()


def insert_rows(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """rows를 DB에 삽입. itemMnftrRptNo가 이미 존재하는 행은 무시(중복 방지)."""
    placeholders = ", ".join("?" for _ in COLUMNS)
    col_names = ", ".join(f'"{col}"' for col in COLUMNS)
    sql = f'INSERT OR IGNORE INTO food_info ({col_names}) VALUES ({placeholders})'

    values = [tuple(row.get(col) for col in COLUMNS) for row in rows]
    conn.executemany(sql, values)
    conn.commit()
