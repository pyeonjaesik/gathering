"""
신규 DB 뷰어
- 공공 API 원본(processed_food_info)
- 검색어 파이프라인(query_pool/query_runs/serp_cache/query_image_analysis_cache)
- 최종 산출물(food_final)
중심으로 운영 현황을 조회한다.
"""

from __future__ import annotations

import sqlite3
import sys
from typing import Any

from app.config import DB_FILE
from app.database import ensure_processed_food_table
from app.query_pipeline import init_query_pipeline_tables

W = 88


def _bar(ch: str = "─") -> str:
    return "  " + ch * (W - 4)


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row and row[0] > 0)


def _count(conn: sqlite3.Connection, table: str, where: str = "", params: tuple[Any, ...] = ()) -> int:
    sql = f"SELECT COUNT(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def print_header() -> None:
    title = "📚 통합 DB Viewer (Pipeline Edition)"
    inner = W - 2
    pad_l = max(0, (inner - len(title)) // 2)
    pad_r = max(0, inner - len(title) - pad_l)
    print()
    print("╔" + "═" * inner + "╗")
    print("║" + " " * pad_l + title + " " * pad_r + "║")
    print("╚" + "═" * inner + "╝")


def print_summary(conn: sqlite3.Connection) -> None:
    print("\n  🧾 [전체 요약]")
    total_food = _count(conn, "processed_food_info")
    unique_no = _count(conn, "processed_food_info", "itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != ''")
    query_pool = _count(conn, "query_pool")
    query_runs = _count(conn, "query_runs")
    serp_cache = _count(conn, "serp_cache")
    image_cache = _count(conn, "query_image_analysis_cache")
    final_rows = _count(conn, "food_final")
    print(f"    - processed_food_info(공공API 원본)      : {total_food:,}")
    print(f"    - 품목보고번호 보유 원본 건수   : {unique_no:,}")
    print(f"    - query_pool(검색어 풀)         : {query_pool:,}")
    print(f"    - query_runs(실행 로그)         : {query_runs:,}")
    print(f"    - serp_cache(URL 캐시)          : {serp_cache:,}")
    print(f"    - image_analysis_cache(패스결과): {image_cache:,}")
    print(f"    - food_final(최종 산출물)       : {final_rows:,}")


def show_food_search(conn: sqlite3.Connection) -> None:
    print("\n  🔎 [가공식품 공공API 원본 검색]")
    mode = input("  검색 기준 [1:품목보고번호, 2:식품명] : ").strip()
    if mode not in {"1", "2"}:
        print("  ⚠️ 올바른 번호를 입력해주세요.")
        return
    q = input("  검색어 : ").strip()
    if not q:
        print("  ⚠️ 검색어가 비어 있습니다.")
        return
    if mode == "1":
        sql = """
            SELECT foodNm, itemMnftrRptNo, mfrNm, enerc, prot, fatce, chocdf
            FROM processed_food_info
            WHERE itemMnftrRptNo LIKE ?
            LIMIT 30
        """
    else:
        sql = """
            SELECT foodNm, itemMnftrRptNo, mfrNm, enerc, prot, fatce, chocdf
            FROM processed_food_info
            WHERE foodNm LIKE ?
            LIMIT 30
        """
    rows = conn.execute(sql, (f"%{q}%",)).fetchall()
    if not rows:
        print("  (결과 없음)")
        return
    print(f"\n  결과 {len(rows):,}건 (최대 30건)")
    for i, row in enumerate(rows, 1):
        nm, no, mfr, en, pr, fa, ch = row
        print(
            f"  [{i:02}] {nm} | 번호={no or '-'} | 제조사={mfr or '-'} | "
            f"E/P/F/C={en or '-'} / {pr or '-'} / {fa or '-'} / {ch or '-'}"
        )


def show_query_pool(conn: sqlite3.Connection) -> None:
    print("\n  🧩 [검색어 풀 상위]")
    raw = input("  조회 개수 [기본 50] : ").strip()
    limit = 50
    if raw:
        try:
            limit = max(1, int(raw))
        except ValueError:
            pass
    rows = conn.execute(
        """
        SELECT id, query_text, source, status, priority_score, target_segment_score, run_count, last_run_at
        FROM query_pool
        ORDER BY priority_score DESC, target_segment_score DESC, id ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    if not rows:
        print("  (검색어 없음)")
        return
    for row in rows:
        qid, text, src, st, ps, ts, rc, lra = row
        print(
            f"  - id={qid} | pri={ps:.1f} seg={ts:.1f} | {st} | run={rc} | {src}"
        )
        print(f"    q={text}")
        print(f"    last={lra or '-'}")


def show_query_runs(conn: sqlite3.Connection) -> None:
    print("\n  🏃 [실행 이력]")
    raw = input("  조회 개수 [기본 30] : ").strip()
    limit = 30
    if raw:
        try:
            limit = max(1, int(raw))
        except ValueError:
            pass
    rows = conn.execute(
        """
        SELECT r.id, r.status, r.query_id, q.query_text, r.total_images, r.analyzed_images,
               r.pass2b_pass_count, r.pass4_pass_count, r.final_saved_count, r.overall_score,
               r.started_at, r.ended_at
        FROM query_runs r
        JOIN query_pool q ON q.id = r.query_id
        ORDER BY r.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    if not rows:
        print("  (실행 로그 없음)")
        return
    for row in rows:
        rid, st, qid, qt, total, analyzed, p2b, p4, saved, score, st_at, ed_at = row
        print(
            f"  - run={rid} | {st} | query_id={qid} | img={analyzed}/{total} | "
            f"p2b={p2b} p4={p4} saved={saved} | score={score:.1f}"
        )
        print(f"    q={qt}")
        print(f"    {st_at} -> {ed_at or '-'}")


def show_final_outputs(conn: sqlite3.Connection) -> None:
    print("\n  ✅ [최종 산출물 조회]")
    mode = input("  조회 기준 [1:품목보고번호, 2:제품명, 3:최근순] : ").strip()
    params: tuple[Any, ...]
    if mode == "1":
        q = input("  품목보고번호 검색어 : ").strip()
        sql = """
            SELECT id, product_name, item_mnftr_rpt_no, nutrition_source, source_image_url, created_at
            FROM food_final
            WHERE item_mnftr_rpt_no LIKE ?
            ORDER BY id DESC
            LIMIT 50
        """
        params = (f"%{q}%",)
    elif mode == "2":
        q = input("  제품명 검색어 : ").strip()
        sql = """
            SELECT id, product_name, item_mnftr_rpt_no, nutrition_source, source_image_url, created_at
            FROM food_final
            WHERE product_name LIKE ?
            ORDER BY id DESC
            LIMIT 50
        """
        params = (f"%{q}%",)
    elif mode == "3":
        sql = """
            SELECT id, product_name, item_mnftr_rpt_no, nutrition_source, source_image_url, created_at
            FROM food_final
            ORDER BY id DESC
            LIMIT 50
        """
        params = ()
    else:
        print("  ⚠️ 올바른 번호를 입력해주세요.")
        return
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        print("  (결과 없음)")
        return
    for row in rows:
        rid, name, no, ns, url, ct = row
        print(f"  - id={rid} | {name or '-'} | 번호={no or '-'} | nutrition={ns} | {ct}")
        print(f"    url={url or '-'}")


def show_mapping_coverage(conn: sqlite3.Connection) -> None:
    print("\n  🔗 [영양성분 매핑 커버리지]")
    total = _count(conn, "food_final")
    with_no = _count(conn, "food_final", "item_mnftr_rpt_no IS NOT NULL AND item_mnftr_rpt_no != ''")
    mapped = conn.execute(
        """
        SELECT COUNT(*)
        FROM food_final ff
        JOIN processed_food_info fi ON fi.itemMnftrRptNo = ff.item_mnftr_rpt_no
        WHERE ff.item_mnftr_rpt_no IS NOT NULL
          AND ff.item_mnftr_rpt_no != ''
          AND COALESCE(fi.enerc, '') != ''
        """
    ).fetchone()[0]
    missing = with_no - mapped
    ratio = (mapped / with_no * 100.0) if with_no else 0.0
    print(f"    - food_final 총 건수                  : {total:,}")
    print(f"    - 품목보고번호 보유 최종건수          : {with_no:,}")
    print(f"    - 공공API 영양정보 매핑 성공(번호기준): {mapped:,}")
    print(f"    - 매핑 미성공                         : {missing:,}")
    print(f"    - 매핑률                              : {ratio:.1f}%")


def show_pass_fail_summary(conn: sqlite3.Connection) -> None:
    print("\n  🧪 [패스 실패 요약]")
    rows = conn.execute(
        """
        SELECT COALESCE(fail_stage, 'none') AS stage, COUNT(*) AS cnt
        FROM query_image_analysis_cache
        GROUP BY stage
        ORDER BY cnt DESC
        """
    ).fetchall()
    if not rows:
        print("  (분석 캐시 없음)")
        return
    print("  단계별:")
    for stage, cnt in rows:
        print(f"    - {stage}: {cnt:,}")
    print("\n  실패 사유 상위 20:")
    reasons = conn.execute(
        """
        SELECT COALESCE(fail_reason, 'none') AS reason, COUNT(*) AS cnt
        FROM query_image_analysis_cache
        GROUP BY reason
        ORDER BY cnt DESC
        LIMIT 20
        """
    ).fetchall()
    for reason, cnt in reasons:
        print(f"    - {reason}: {cnt:,}")


def main() -> None:
    print_header()
    try:
        conn = sqlite3.connect(DB_FILE)
    except sqlite3.Error as exc:
        print(f"\n  ❌ DB 연결 실패: {exc}")
        sys.exit(1)

    ensure_processed_food_table(conn)

    if not _table_exists(conn, "processed_food_info"):
        print(f"\n  ❌ {DB_FILE}에 processed_food_info 테이블이 없습니다.")
        sys.exit(1)

    # 파이프라인 테이블이 아직 없으면 생성
    init_query_pipeline_tables(conn)

    while True:
        print_summary(conn)
        print("\n" + _bar())
        print("  [ 메뉴 ]")
        print("    [1] 가공식품 공공API 원본 검색 (processed_food_info)")
        print("    [2] 검색어 풀 조회 (query_pool)")
        print("    [3] 실행 이력 조회 (query_runs)")
        print("    [4] 최종 산출물 조회 (food_final)")
        print("    [5] 영양성분 매핑 커버리지")
        print("    [6] Pass 실패 사유 요약")
        print("    [q] 종료")
        print(_bar())
        choice = input("  👉 선택 : ").strip().lower()

        if choice == "1":
            show_food_search(conn)
        elif choice == "2":
            show_query_pool(conn)
        elif choice == "3":
            show_query_runs(conn)
        elif choice == "4":
            show_final_outputs(conn)
        elif choice == "5":
            show_mapping_coverage(conn)
        elif choice == "6":
            show_pass_fail_summary(conn)
        elif choice == "q":
            print("\n  👋 viewer 종료\n")
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")

    conn.close()
