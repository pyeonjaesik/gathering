"""
SQLite DB 초기화 및 데이터 삽입 기능
"""

import sqlite3

from config import COLUMNS


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


def insert_rows(conn: sqlite3.Connection, rows: list[dict]) -> None:
    """rows를 DB에 삽입"""
    placeholders = ", ".join("?" for _ in COLUMNS)
    col_names = ", ".join(f'"{col}"' for col in COLUMNS)
    sql = f'INSERT INTO food_info ({col_names}) VALUES ({placeholders})'

    values = [tuple(row.get(col) for col in COLUMNS) for row in rows]
    conn.executemany(sql, values)
    conn.commit()
