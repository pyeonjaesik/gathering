"""
중복 데이터 점검/삭제 유틸리티.
"""

from __future__ import annotations

import csv
import os
import sqlite3
from datetime import datetime


NAME_NORM_EXPR = """
lower(
  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
    coalesce(foodNm,''),
    ' ',''), '-', ''), '_',''), '/', ''), '(', ''), ')', ''), '[', ''), ']', ''), '.', ''), ',', ''), '·', '')
)
"""


def duplicate_conditions() -> list[str]:
    return [
        "규칙 A: foodCd가 같은 행은 같은 제품으로 보고 1건만 유지",
        "규칙 B: foodNm 정규화 + foodSize + servSize + foodLv3Nm + foodLv4Nm이 같고, foodCd가 서로 다르면 중복 후보",
        "규칙 C: foodNm 정규화 + enerc/prot/fatce/chocdf + foodLv3Nm + foodLv4Nm이 같고, foodCd가 서로 다르면 중복 후보",
        "규칙 D: foodNm 정규화 + foodLv3Nm + foodLv4Nm이 같고, foodCd가 서로 다르면 중복 후보",
        "유지 우선순위: itemMnftrRptNo 존재 > 유효 mfrNm > 최신 crtrYmd > 최신 id",
    ]


def _group_stats(conn: sqlite3.Connection, grouping_sql: str) -> tuple[int, int]:
    query = f"""
    WITH g AS (
      {grouping_sql}
    )
    SELECT COUNT(*), COALESCE(SUM(cnt - 1), 0) FROM g
    """
    return conn.execute(query).fetchone()


def get_duplicate_stats(conn: sqlite3.Connection) -> dict[str, int]:
    foodcd_groups, foodcd_extra = _group_stats(
        conn,
        """
        SELECT foodCd, COUNT(*) AS cnt
        FROM processed_food_info
        WHERE foodCd IS NOT NULL AND foodCd != ''
        GROUP BY foodCd
        HAVING cnt > 1
        """,
    )

    h1_groups, h1_extra = _group_stats(
        conn,
        f"""
        WITH base AS (
          SELECT foodCd,
                 {NAME_NORM_EXPR} AS nm_norm,
                 coalesce(nullif(trim(foodSize),''),'∅') AS foodSize_n,
                 coalesce(nullif(trim(servSize),''),'∅') AS servSize_n,
                 coalesce(nullif(trim(foodLv3Nm),''),'∅') AS lv3_n,
                 coalesce(nullif(trim(foodLv4Nm),''),'∅') AS lv4_n
          FROM processed_food_info
        )
        SELECT nm_norm, foodSize_n, servSize_n, lv3_n, lv4_n,
               COUNT(*) AS cnt, COUNT(DISTINCT foodCd) AS ccd
        FROM base
        GROUP BY nm_norm, foodSize_n, servSize_n, lv3_n, lv4_n
        HAVING cnt > 1 AND ccd > 1
        """,
    )

    h2_groups, h2_extra = _group_stats(
        conn,
        f"""
        WITH base AS (
          SELECT foodCd,
                 {NAME_NORM_EXPR} AS nm_norm,
                 coalesce(nullif(trim(enerc),''),'∅') AS enerc_n,
                 coalesce(nullif(trim(prot),''),'∅') AS prot_n,
                 coalesce(nullif(trim(fatce),''),'∅') AS fatce_n,
                 coalesce(nullif(trim(chocdf),''),'∅') AS chocdf_n,
                 coalesce(nullif(trim(foodLv3Nm),''),'∅') AS lv3_n,
                 coalesce(nullif(trim(foodLv4Nm),''),'∅') AS lv4_n
          FROM processed_food_info
        )
        SELECT nm_norm, enerc_n, prot_n, fatce_n, chocdf_n, lv3_n, lv4_n,
               COUNT(*) AS cnt, COUNT(DISTINCT foodCd) AS ccd
        FROM base
        GROUP BY nm_norm, enerc_n, prot_n, fatce_n, chocdf_n, lv3_n, lv4_n
        HAVING cnt > 1 AND ccd > 1
        """,
    )

    h3_groups, h3_extra = _group_stats(
        conn,
        f"""
        WITH base AS (
          SELECT foodCd,
                 {NAME_NORM_EXPR} AS nm_norm,
                 coalesce(nullif(trim(foodLv3Nm),''),'∅') AS lv3_n,
                 coalesce(nullif(trim(foodLv4Nm),''),'∅') AS lv4_n
          FROM processed_food_info
        )
        SELECT nm_norm, lv3_n, lv4_n,
               COUNT(*) AS cnt, COUNT(DISTINCT foodCd) AS ccd
        FROM base
        GROUP BY nm_norm, lv3_n, lv4_n
        HAVING cnt > 1 AND ccd > 1
        """,
    )

    total = conn.execute("SELECT COUNT(*) FROM processed_food_info").fetchone()[0]
    return {
        "total_rows": total,
        "foodCd_groups": foodcd_groups,
        "foodCd_extra": foodcd_extra,
        "h1_groups": h1_groups,
        "h1_extra": h1_extra,
        "h2_groups": h2_groups,
        "h2_extra": h2_extra,
        "h3_groups": h3_groups,
        "h3_extra": h3_extra,
    }


def get_duplicate_samples(conn: sqlite3.Connection, limit: int = 10) -> list[tuple]:
    return conn.execute(
        """
        SELECT foodNm,
               COALESCE(NULLIF(TRIM(foodSize), ''), '∅') AS foodSize_n,
               COALESCE(NULLIF(TRIM(servSize), ''), '∅') AS servSize_n,
               COALESCE(NULLIF(TRIM(foodLv3Nm), ''), '∅') AS lv3_n,
               COALESCE(NULLIF(TRIM(foodLv4Nm), ''), '∅') AS lv4_n,
               COUNT(*) AS cnt,
               COUNT(DISTINCT foodCd) AS foodCd_cnt
        FROM processed_food_info
        GROUP BY foodNm, foodSize_n, servSize_n, lv3_n, lv4_n
        HAVING cnt > 1 AND foodCd_cnt > 1
        ORDER BY cnt DESC, foodCd_cnt DESC, foodNm
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def _ensure_removed_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dedupe_removed_log (
            removed_id INTEGER PRIMARY KEY,
            kept_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            removed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            grp_key TEXT,
            removed_foodCd TEXT,
            removed_foodNm TEXT,
            removed_itemMnftrRptNo TEXT,
            removed_mfrNm TEXT,
            removed_foodSize TEXT,
            removed_servSize TEXT,
            removed_enerc TEXT,
            removed_prot TEXT,
            removed_fatce TEXT,
            removed_chocdf TEXT,
            removed_foodLv3Nm TEXT,
            removed_foodLv4Nm TEXT,
            kept_foodCd TEXT,
            kept_foodNm TEXT,
            kept_itemMnftrRptNo TEXT,
            kept_mfrNm TEXT
        )
        """
    )
    conn.commit()


def run_dedupe(conn: sqlite3.Connection) -> dict[str, object]:
    _ensure_removed_log_table(conn)
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("BEGIN")
    conn.execute("DROP TABLE IF EXISTS _run_removed_ids")
    conn.execute("CREATE TEMP TABLE _run_removed_ids (removed_id INTEGER PRIMARY KEY)")

    removed_a = _run_rule_a_foodcd(conn)
    removed_b = _run_rule_b_h1(conn)
    removed_c = _run_rule_c_h2(conn)
    removed_d = _run_rule_d_name_category(conn)

    conn.commit()
    csv_path = _export_run_removed_csv(conn)
    return {
        "removed_a": removed_a,
        "removed_b": removed_b,
        "removed_c": removed_c,
        "removed_d": removed_d,
        "removed_total": removed_a + removed_b + removed_c + removed_d,
        "csv_path": csv_path,
    }


def _run_rule_a_foodcd(conn: sqlite3.Connection) -> int:
    conn.execute("DROP TABLE IF EXISTS _dup_a")
    conn.execute(
        """
        CREATE TEMP TABLE _dup_a AS
        WITH ranked AS (
            SELECT
                id, foodCd, foodNm, itemMnftrRptNo, mfrNm, foodSize, servSize,
                enerc, prot, fatce, chocdf, foodLv3Nm, foodLv4Nm, crtrYmd,
                ROW_NUMBER() OVER (
                    PARTITION BY foodCd
                    ORDER BY
                        CASE WHEN itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != '' THEN 0 ELSE 1 END,
                        CASE WHEN mfrNm IS NOT NULL AND mfrNm != '' AND mfrNm != '해당없음' THEN 0 ELSE 1 END,
                        CASE WHEN crtrYmd IS NOT NULL AND crtrYmd != '' THEN 0 ELSE 1 END,
                        crtrYmd DESC,
                        id DESC
                ) AS rn,
                FIRST_VALUE(id) OVER (
                    PARTITION BY foodCd
                    ORDER BY
                        CASE WHEN itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != '' THEN 0 ELSE 1 END,
                        CASE WHEN mfrNm IS NOT NULL AND mfrNm != '' AND mfrNm != '해당없음' THEN 0 ELSE 1 END,
                        CASE WHEN crtrYmd IS NOT NULL AND crtrYmd != '' THEN 0 ELSE 1 END,
                        crtrYmd DESC,
                        id DESC
                ) AS kept_id
            FROM processed_food_info
            WHERE foodCd IS NOT NULL AND foodCd != ''
        )
        SELECT *
        FROM ranked
        WHERE rn > 1
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO dedupe_removed_log (
            removed_id, kept_id, reason, grp_key,
            removed_foodCd, removed_foodNm, removed_itemMnftrRptNo, removed_mfrNm,
            removed_foodSize, removed_servSize, removed_enerc, removed_prot, removed_fatce, removed_chocdf,
            removed_foodLv3Nm, removed_foodLv4Nm,
            kept_foodCd, kept_foodNm, kept_itemMnftrRptNo, kept_mfrNm
        )
        SELECT
            d.id, d.kept_id, 'a:foodCd', d.foodCd,
            d.foodCd, d.foodNm, d.itemMnftrRptNo, d.mfrNm,
            d.foodSize, d.servSize, d.enerc, d.prot, d.fatce, d.chocdf,
            d.foodLv3Nm, d.foodLv4Nm,
            k.foodCd, k.foodNm, k.itemMnftrRptNo, k.mfrNm
        FROM _dup_a d
        JOIN processed_food_info k ON k.id = d.kept_id
        """
    )
    conn.execute("INSERT OR IGNORE INTO _run_removed_ids SELECT id FROM _dup_a")
    removed = conn.execute("DELETE FROM processed_food_info WHERE id IN (SELECT id FROM _dup_a)").rowcount
    return removed


def _run_rule_b_h1(conn: sqlite3.Connection) -> int:
    conn.execute("DROP TABLE IF EXISTS _dup_b")
    conn.execute(
        f"""
        CREATE TEMP TABLE _dup_b AS
        WITH base AS (
          SELECT id, foodCd, foodNm, itemMnftrRptNo, mfrNm, foodSize, servSize,
                 enerc, prot, fatce, chocdf, foodLv3Nm, foodLv4Nm,
                 {NAME_NORM_EXPR} AS nm_norm,
                 coalesce(nullif(trim(foodSize),''),'∅') AS foodSize_n,
                 coalesce(nullif(trim(servSize),''),'∅') AS servSize_n,
                 coalesce(nullif(trim(foodLv3Nm),''),'∅') AS lv3_n,
                 coalesce(nullif(trim(foodLv4Nm),''),'∅') AS lv4_n,
                 coalesce(nullif(trim(itemMnftrRptNo),''),'∅') AS rpt_n,
                 coalesce(nullif(trim(mfrNm),''),'∅') AS mfr_n,
                 coalesce(nullif(trim(crtrYmd),''),'∅') AS crtr_n
          FROM processed_food_info
        ), ranked AS (
          SELECT *,
                 nm_norm || '|' || foodSize_n || '|' || servSize_n || '|' || lv3_n || '|' || lv4_n AS grp_key,
                 ROW_NUMBER() OVER (
                   PARTITION BY nm_norm, foodSize_n, servSize_n, lv3_n, lv4_n
                   ORDER BY
                     CASE WHEN rpt_n != '∅' THEN 0 ELSE 1 END,
                     CASE WHEN mfr_n != '∅' AND mfr_n != '해당없음' THEN 0 ELSE 1 END,
                     CASE WHEN crtr_n != '∅' THEN 0 ELSE 1 END,
                     crtr_n DESC,
                     id DESC
                 ) AS rn,
                 FIRST_VALUE(id) OVER (
                   PARTITION BY nm_norm, foodSize_n, servSize_n, lv3_n, lv4_n
                   ORDER BY
                     CASE WHEN rpt_n != '∅' THEN 0 ELSE 1 END,
                     CASE WHEN mfr_n != '∅' AND mfr_n != '해당없음' THEN 0 ELSE 1 END,
                     CASE WHEN crtr_n != '∅' THEN 0 ELSE 1 END,
                     crtr_n DESC,
                     id DESC
                 ) AS kept_id
          FROM base
        )
        SELECT *
        FROM ranked r
        WHERE rn > 1
          AND EXISTS (
              SELECT 1 FROM ranked x
              WHERE x.grp_key = r.grp_key
              GROUP BY x.grp_key
              HAVING COUNT(DISTINCT x.foodCd) > 1
          )
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO dedupe_removed_log (
            removed_id, kept_id, reason, grp_key,
            removed_foodCd, removed_foodNm, removed_itemMnftrRptNo, removed_mfrNm,
            removed_foodSize, removed_servSize, removed_enerc, removed_prot, removed_fatce, removed_chocdf,
            removed_foodLv3Nm, removed_foodLv4Nm,
            kept_foodCd, kept_foodNm, kept_itemMnftrRptNo, kept_mfrNm
        )
        SELECT
            d.id, d.kept_id, 'b:name+size+serv+category', d.grp_key,
            d.foodCd, d.foodNm, d.itemMnftrRptNo, d.mfrNm,
            d.foodSize, d.servSize, d.enerc, d.prot, d.fatce, d.chocdf,
            d.foodLv3Nm, d.foodLv4Nm,
            k.foodCd, k.foodNm, k.itemMnftrRptNo, k.mfrNm
        FROM _dup_b d
        JOIN processed_food_info k ON k.id = d.kept_id
        """
    )
    conn.execute("INSERT OR IGNORE INTO _run_removed_ids SELECT id FROM _dup_b")
    removed = conn.execute("DELETE FROM processed_food_info WHERE id IN (SELECT id FROM _dup_b)").rowcount
    return removed


def _run_rule_c_h2(conn: sqlite3.Connection) -> int:
    conn.execute("DROP TABLE IF EXISTS _dup_c")
    conn.execute(
        f"""
        CREATE TEMP TABLE _dup_c AS
        WITH base AS (
          SELECT id, foodCd, foodNm, itemMnftrRptNo, mfrNm, foodSize, servSize,
                 enerc, prot, fatce, chocdf, foodLv3Nm, foodLv4Nm,
                 {NAME_NORM_EXPR} AS nm_norm,
                 coalesce(nullif(trim(enerc),''),'∅') AS enerc_n,
                 coalesce(nullif(trim(prot),''),'∅') AS prot_n,
                 coalesce(nullif(trim(fatce),''),'∅') AS fatce_n,
                 coalesce(nullif(trim(chocdf),''),'∅') AS chocdf_n,
                 coalesce(nullif(trim(foodLv3Nm),''),'∅') AS lv3_n,
                 coalesce(nullif(trim(foodLv4Nm),''),'∅') AS lv4_n,
                 coalesce(nullif(trim(itemMnftrRptNo),''),'∅') AS rpt_n,
                 coalesce(nullif(trim(mfrNm),''),'∅') AS mfr_n,
                 coalesce(nullif(trim(crtrYmd),''),'∅') AS crtr_n
          FROM processed_food_info
        ), ranked AS (
          SELECT *,
                 nm_norm || '|' || enerc_n || '|' || prot_n || '|' || fatce_n || '|' || chocdf_n || '|' || lv3_n || '|' || lv4_n AS grp_key,
                 ROW_NUMBER() OVER (
                   PARTITION BY nm_norm, enerc_n, prot_n, fatce_n, chocdf_n, lv3_n, lv4_n
                   ORDER BY
                     CASE WHEN rpt_n != '∅' THEN 0 ELSE 1 END,
                     CASE WHEN mfr_n != '∅' AND mfr_n != '해당없음' THEN 0 ELSE 1 END,
                     CASE WHEN crtr_n != '∅' THEN 0 ELSE 1 END,
                     crtr_n DESC,
                     id DESC
                 ) AS rn,
                 FIRST_VALUE(id) OVER (
                   PARTITION BY nm_norm, enerc_n, prot_n, fatce_n, chocdf_n, lv3_n, lv4_n
                   ORDER BY
                     CASE WHEN rpt_n != '∅' THEN 0 ELSE 1 END,
                     CASE WHEN mfr_n != '∅' AND mfr_n != '해당없음' THEN 0 ELSE 1 END,
                     CASE WHEN crtr_n != '∅' THEN 0 ELSE 1 END,
                     crtr_n DESC,
                     id DESC
                 ) AS kept_id
          FROM base
        )
        SELECT *
        FROM ranked r
        WHERE rn > 1
          AND EXISTS (
              SELECT 1 FROM ranked x
              WHERE x.grp_key = r.grp_key
              GROUP BY x.grp_key
              HAVING COUNT(DISTINCT x.foodCd) > 1
          )
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO dedupe_removed_log (
            removed_id, kept_id, reason, grp_key,
            removed_foodCd, removed_foodNm, removed_itemMnftrRptNo, removed_mfrNm,
            removed_foodSize, removed_servSize, removed_enerc, removed_prot, removed_fatce, removed_chocdf,
            removed_foodLv3Nm, removed_foodLv4Nm,
            kept_foodCd, kept_foodNm, kept_itemMnftrRptNo, kept_mfrNm
        )
        SELECT
            d.id, d.kept_id, 'c:name+nutrition+category', d.grp_key,
            d.foodCd, d.foodNm, d.itemMnftrRptNo, d.mfrNm,
            d.foodSize, d.servSize, d.enerc, d.prot, d.fatce, d.chocdf,
            d.foodLv3Nm, d.foodLv4Nm,
            k.foodCd, k.foodNm, k.itemMnftrRptNo, k.mfrNm
        FROM _dup_c d
        JOIN processed_food_info k ON k.id = d.kept_id
        """
    )
    conn.execute("INSERT OR IGNORE INTO _run_removed_ids SELECT id FROM _dup_c")
    removed = conn.execute("DELETE FROM processed_food_info WHERE id IN (SELECT id FROM _dup_c)").rowcount
    return removed


def _run_rule_d_name_category(conn: sqlite3.Connection) -> int:
    conn.execute("DROP TABLE IF EXISTS _dup_d")
    conn.execute(
        f"""
        CREATE TEMP TABLE _dup_d AS
        WITH base AS (
          SELECT id, foodCd, foodNm, itemMnftrRptNo, mfrNm, foodSize, servSize,
                 enerc, prot, fatce, chocdf, foodLv3Nm, foodLv4Nm,
                 {NAME_NORM_EXPR} AS nm_norm,
                 coalesce(nullif(trim(foodLv3Nm),''),'∅') AS lv3_n,
                 coalesce(nullif(trim(foodLv4Nm),''),'∅') AS lv4_n,
                 coalesce(nullif(trim(itemMnftrRptNo),''),'∅') AS rpt_n,
                 coalesce(nullif(trim(mfrNm),''),'∅') AS mfr_n,
                 coalesce(nullif(trim(crtrYmd),''),'∅') AS crtr_n
          FROM processed_food_info
        ), ranked AS (
          SELECT *,
                 nm_norm || '|' || lv3_n || '|' || lv4_n AS grp_key,
                 ROW_NUMBER() OVER (
                   PARTITION BY nm_norm, lv3_n, lv4_n
                   ORDER BY
                     CASE WHEN rpt_n != '∅' THEN 0 ELSE 1 END,
                     CASE WHEN mfr_n != '∅' AND mfr_n != '해당없음' THEN 0 ELSE 1 END,
                     CASE WHEN crtr_n != '∅' THEN 0 ELSE 1 END,
                     crtr_n DESC,
                     id DESC
                 ) AS rn,
                 FIRST_VALUE(id) OVER (
                   PARTITION BY nm_norm, lv3_n, lv4_n
                   ORDER BY
                     CASE WHEN rpt_n != '∅' THEN 0 ELSE 1 END,
                     CASE WHEN mfr_n != '∅' AND mfr_n != '해당없음' THEN 0 ELSE 1 END,
                     CASE WHEN crtr_n != '∅' THEN 0 ELSE 1 END,
                     crtr_n DESC,
                     id DESC
                 ) AS kept_id
          FROM base
        )
        SELECT *
        FROM ranked r
        WHERE rn > 1
          AND EXISTS (
              SELECT 1 FROM ranked x
              WHERE x.grp_key = r.grp_key
              GROUP BY x.grp_key
              HAVING COUNT(DISTINCT x.foodCd) > 1
          )
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO dedupe_removed_log (
            removed_id, kept_id, reason, grp_key,
            removed_foodCd, removed_foodNm, removed_itemMnftrRptNo, removed_mfrNm,
            removed_foodSize, removed_servSize, removed_enerc, removed_prot, removed_fatce, removed_chocdf,
            removed_foodLv3Nm, removed_foodLv4Nm,
            kept_foodCd, kept_foodNm, kept_itemMnftrRptNo, kept_mfrNm
        )
        SELECT
            d.id, d.kept_id, 'd:name+category', d.grp_key,
            d.foodCd, d.foodNm, d.itemMnftrRptNo, d.mfrNm,
            d.foodSize, d.servSize, d.enerc, d.prot, d.fatce, d.chocdf,
            d.foodLv3Nm, d.foodLv4Nm,
            k.foodCd, k.foodNm, k.itemMnftrRptNo, k.mfrNm
        FROM _dup_d d
        JOIN processed_food_info k ON k.id = d.kept_id
        """
    )
    conn.execute("INSERT OR IGNORE INTO _run_removed_ids SELECT id FROM _dup_d")
    removed = conn.execute("DELETE FROM processed_food_info WHERE id IN (SELECT id FROM _dup_d)").rowcount
    return removed


def _export_run_removed_csv(conn: sqlite3.Connection) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dedupe_removed_run_{timestamp}.csv"
    rows = conn.execute(
        """
        SELECT d.removed_id, d.kept_id, d.reason, d.removed_at, d.grp_key,
               d.removed_foodCd, d.removed_foodNm, d.removed_itemMnftrRptNo, d.removed_mfrNm,
               d.removed_foodSize, d.removed_servSize, d.removed_enerc, d.removed_prot, d.removed_fatce, d.removed_chocdf,
               d.removed_foodLv3Nm, d.removed_foodLv4Nm,
               d.kept_foodCd, d.kept_foodNm, d.kept_itemMnftrRptNo, d.kept_mfrNm
        FROM dedupe_removed_log d
        JOIN _run_removed_ids r ON r.removed_id = d.removed_id
        ORDER BY d.removed_id
        """
    ).fetchall()

    headers = [
        "removed_id",
        "kept_id",
        "reason",
        "removed_at",
        "grp_key",
        "removed_foodCd",
        "removed_foodNm",
        "removed_itemMnftrRptNo",
        "removed_mfrNm",
        "removed_foodSize",
        "removed_servSize",
        "removed_enerc",
        "removed_prot",
        "removed_fatce",
        "removed_chocdf",
        "removed_foodLv3Nm",
        "removed_foodLv4Nm",
        "kept_foodCd",
        "kept_foodNm",
        "kept_itemMnftrRptNo",
        "kept_mfrNm",
    ]
    with open(filename, "w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(headers)
        writer.writerows(rows)
    return os.path.abspath(filename)
