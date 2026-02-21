"""
신규 DB 뷰어
- 공공 API 원본(processed_food_info)
- 검색어 파이프라인(query_pool/query_runs/serp_cache/query_image_analysis_cache)
- 최종 산출물(food_final)
중심으로 운영 현황을 조회한다.
"""

from __future__ import annotations

import html
import sqlite3
import sys
import webbrowser
from datetime import datetime
from pathlib import Path
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
    print("\n  🧩 [검색어 풀 조회: 브라우저 리포트]")
    open_query_pool_browser_report(conn)


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
    .glossary {{ margin-top: 10px; font-size: 13px; line-height: 1.6; }}
    .glossary b {{ display: inline-block; min-width: 180px; }}
    .muted {{ color: var(--muted); }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>검색어 풀 조회</h1>
      <div class="sub">query_pool 전체를 브라우저에서 조회합니다.</div>
      <div><strong>총 검색어:</strong> {len(rows):,}</div>
      <div style="margin-top:8px;"><strong>상태 분포</strong><br>{status_badges or "-"}</div>
      <div style="margin-top:8px;"><strong>소스 분포</strong><br>{source_badges or "-"}</div>
      <div class="glossary">
        <div><strong>용어 설명</strong></div>
        <div><b>pending</b>다음 실행 대상(대기열 포함)</div>
        <div><b>paused</b>저장만 해둔 상태(실행 제외)</div>
        <div><b>running</b>현재 실행 중</div>
        <div><b>done</b>최근 실행 완료</div>
        <div><b>failed</b>최근 실행 실패</div>
        <div><b>consumer_taxonomy_seed</b>공공 API 카테고리를 소비자 검색어로 변환해 자동 적재한 소스</div>
        <div class="muted">※ run, 마지막 실행 시각과 함께 보면 “실행 이력” 해석이 더 정확합니다.</div>
      </div>
    </div>

    <div class="card">
      <div class="controls">
        <input id="q" type="text" placeholder="검색어/소스/상태 텍스트 검색" />
        <select id="statusFilter">
          <option value="">전체 상태</option>
          <option value="pending">pending</option>
          <option value="paused">paused</option>
          <option value="running">running</option>
          <option value="done">done</option>
          <option value="failed">failed</option>
        </select>
      </div>
    </div>

    <div class="card" style="padding:0; overflow:auto; max-height:70vh;">
      <table id="tbl">
        <thead>
          <tr>
            <th>ID</th>
            <th>상태</th>
            <th>소스</th>
            <th>점수</th>
            <th>run</th>
            <th>마지막 실행</th>
            <th>검색어</th>
          </tr>
        </thead>
        <tbody>{tbody}</tbody>
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


def open_query_pool_browser_report(conn: sqlite3.Connection | None = None) -> Path:
    owns_conn = False
    if conn is None:
        conn = sqlite3.connect(DB_FILE)
        owns_conn = True
    try:
        init_query_pipeline_tables(conn)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, query_text, source, status, priority_score, run_count, last_run_at
            FROM query_pool
            ORDER BY priority_score DESC, id ASC
            """
        ).fetchall()

        reports_dir = Path(__file__).resolve().parent.parent / "reports" / "query_pool"
        reports_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = reports_dir / f"query_pool_{ts}.html"
        out_path.write_text(_build_query_pool_html(rows), encoding="utf-8")
        webbrowser.open_new_tab(out_path.resolve().as_uri())
        return out_path
    finally:
        if owns_conn:
            conn.close()


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
