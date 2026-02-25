"""
신규 DB 뷰어
- 공공 API 원본(processed_food_info)
- 검색어 파이프라인(query_pool/query_runs/serp_cache/query_image_analysis_cache)
- 최종 산출물(food_final)
중심으로 운영 현황을 조회한다.
"""

from __future__ import annotations

import html
import json
import re
import sqlite3
import sys
import webbrowser
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse
from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import DB_FILE
from app.database import ensure_processed_food_table
from app.query_pipeline import init_query_pipeline_tables

W = 88
QUERY_POOL_WEB_PORT = 8765
_QUERY_POOL_SERVER_STARTED = False
_QUERY_POOL_SERVER_LOCK = threading.Lock()
FINAL_OUTPUTS_WEB_PORT = 8766
_FINAL_OUTPUTS_SERVER_STARTED = False
_FINAL_OUTPUTS_SERVER_LOCK = threading.Lock()


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
            f"<td><input type='checkbox' class='delChk' value='{int(r['id'])}' /> {int(r['id'])}</td>"
            f"<td>{html.escape(str(r['status'] or ''))}</td>"
            f"<td>{html.escape(str(r['source'] or ''))}</td>"
            f"<td class='num'>{float(r['priority_score'] or 0.0):.1f}</td>"
            f"<td class='num'>{int(r['run_count'] or 0)}</td>"
            f"<td>{html.escape(str(r['last_run_at'] or '-'))}</td>"
            f"<td>{html.escape(str(r['provider_pages'] or '-'))}</td>"
            f"<td class='num'>{int(r['final_saved_count'] or 0):,}</td>"
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
      grid-template-columns: 1fr 220px 220px;
      gap: 10px;
      align-items: center;
    }}
    .actions {{
      display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-top:10px;
    }}
    .btn {{
      font-size:13px; border:1px solid var(--line); border-radius:10px; padding:8px 12px; background:#fff; cursor:pointer;
    }}
    .btn.danger {{ border-color:#ef9a9a; color:#b91c1c; background:#fff5f5; }}
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
        <select id="sourceFilter">
          <option value="">전체 소스</option>
        </select>
      </div>
      <div class="actions">
        <button id="selectVisibleBtn" class="btn" type="button">보이는 항목 전체 선택</button>
        <button id="clearSelBtn" class="btn" type="button">선택 해제</button>
        <button id="rerunSearchBtn" class="btn" type="button">선택 검색어 검색만 다시</button>
        <button id="rerunAnalyzeBtn" class="btn" type="button">선택 검색어 URL까지 다시 분석</button>
        <button id="deleteBtn" class="btn danger" type="button">선택 검색어 삭제</button>
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
            <th>API별 최대 페이지</th>
            <th>최종 저장수</th>
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
    const srcf = document.getElementById('sourceFilter');
    const selectVisibleBtn = document.getElementById('selectVisibleBtn');
    const clearSelBtn = document.getElementById('clearSelBtn');
    const rerunSearchBtn = document.getElementById('rerunSearchBtn');
    const rerunAnalyzeBtn = document.getElementById('rerunAnalyzeBtn');
    const deleteBtn = document.getElementById('deleteBtn');
    const rows = Array.from(document.querySelectorAll('#tbl tbody tr'));

    // source filter options 동적 생성
    const sources = Array.from(new Set(rows.map((tr) => (tr.children[2]?.textContent || '').trim()).filter(Boolean))).sort();
    sources.forEach((s) => {{
      const opt = document.createElement('option');
      opt.value = s;
      opt.textContent = s;
      srcf.appendChild(opt);
    }});

    function applyFilter() {{
      const text = (q.value || '').toLowerCase();
      const st = (sf.value || '').toLowerCase();
      const src = (srcf.value || '').toLowerCase();
      rows.forEach((tr) => {{
        const t = tr.textContent.toLowerCase();
        const statusCell = (tr.children[1]?.textContent || '').toLowerCase().trim();
        const sourceCell = (tr.children[2]?.textContent || '').toLowerCase().trim();
        const matchText = !text || t.includes(text);
        const matchStatus = !st || statusCell === st;
        const matchSource = !src || sourceCell === src;
        tr.style.display = (matchText && matchStatus && matchSource) ? '' : 'none';
      }});
    }}
    q.addEventListener('input', applyFilter);
    sf.addEventListener('change', applyFilter);
    srcf.addEventListener('change', applyFilter);

    selectVisibleBtn.addEventListener('click', () => {{
      rows.forEach((tr) => {{
        if (tr.style.display === 'none') return;
        const ck = tr.querySelector('.delChk');
        if (ck) ck.checked = true;
      }});
    }});
    clearSelBtn.addEventListener('click', () => {{
      document.querySelectorAll('.delChk').forEach((ck) => ck.checked = false);
    }});
    rerunSearchBtn.addEventListener('click', () => {{
      const ids = Array.from(document.querySelectorAll('.delChk:checked')).map((ck) => ck.value);
      if (!ids.length) {{
        alert('재실행할 검색어를 선택하세요.');
        return;
      }}
      if (!confirm(`선택한 검색어 ${{ids.length}}개를 검색만 다시 실행 가능 상태로 리셋할까요?\\n(진행도만 초기화)`)) return;
      const qs = encodeURIComponent(ids.join(','));
      window.location.href = `/rerun?mode=search_only&ids=${{qs}}`;
    }});
    rerunAnalyzeBtn.addEventListener('click', () => {{
      const ids = Array.from(document.querySelectorAll('.delChk:checked')).map((ck) => ck.value);
      if (!ids.length) {{
        alert('재분석할 검색어를 선택하세요.');
        return;
      }}
      if (!confirm(`선택한 검색어 ${{ids.length}}개의 URL 분석 캐시를 함께 지울까요?\\n(검색+URL 재분석 가능)`)) return;
      const qs = encodeURIComponent(ids.join(','));
      window.location.href = `/rerun?mode=reanalyze_urls&ids=${{qs}}`;
    }});
    deleteBtn.addEventListener('click', () => {{
      const ids = Array.from(document.querySelectorAll('.delChk:checked')).map((ck) => ck.value);
      if (!ids.length) {{
        alert('삭제할 검색어를 선택하세요.');
        return;
      }}
      if (!confirm(`선택한 검색어 ${{ids.length}}개를 삭제할까요?\\n(관련 실행로그/캐시도 함께 삭제)`)) return;
      const qs = encodeURIComponent(ids.join(','));
      window.location.href = `/delete?ids=${{qs}}`;
    }});
  </script>
</body>
</html>
"""


def _fetch_query_pool_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT
          q.id,
          q.query_text,
          q.source,
          q.status,
          q.priority_score,
          q.run_count,
          q.last_run_at,
          COALESCE((
            SELECT COUNT(*)
            FROM food_final f
            WHERE f.source_query_id = q.id
          ), 0) AS final_saved_count,
          COALESCE((
            SELECT GROUP_CONCAT(p.provider || ':' || p.max_page_done, ' | ')
            FROM query_provider_progress p
            WHERE p.query_norm = q.query_norm
          ), '-') AS provider_pages
        FROM query_pool q
        ORDER BY q.priority_score DESC, q.id ASC
        """
    ).fetchall()


def _delete_query_pool_ids(ids: list[int]) -> int:
    if not ids:
        return 0
    conn = sqlite3.connect(DB_FILE)
    try:
        init_query_pipeline_tables(conn)
        conn.row_factory = sqlite3.Row
        qmarks = ",".join(["?"] * len(ids))
        rows = conn.execute(
            f"SELECT id, query_norm FROM query_pool WHERE id IN ({qmarks})",
            tuple(ids),
        ).fetchall()
        if not rows:
            return 0
        valid_ids = [int(r["id"]) for r in rows]
        norms = [str(r["query_norm"] or "") for r in rows]
        qid_marks = ",".join(["?"] * len(valid_ids))
        norm_marks = ",".join(["?"] * len(norms))
        # 실행/캐시 정리
        conn.execute(f"DELETE FROM serp_cache WHERE query_id IN ({qid_marks})", tuple(valid_ids))
        conn.execute(f"DELETE FROM query_runs WHERE query_id IN ({qid_marks})", tuple(valid_ids))
        conn.execute(f"DELETE FROM food_final WHERE source_query_id IN ({qid_marks})", tuple(valid_ids))
        if norms:
            conn.execute(
                f"DELETE FROM query_provider_progress WHERE query_norm IN ({norm_marks})",
                tuple(norms),
            )
        conn.execute(f"DELETE FROM query_pool WHERE id IN ({qid_marks})", tuple(valid_ids))
        conn.commit()
        return len(valid_ids)
    finally:
        conn.close()


def _reset_query_pool_for_rerun(ids: list[int], mode: str) -> dict[str, int]:
    if not ids:
        return {"queries": 0, "provider_progress_reset": 0, "url_cache_deleted": 0}
    conn = sqlite3.connect(DB_FILE)
    try:
        init_query_pipeline_tables(conn)
        conn.row_factory = sqlite3.Row
        qmarks = ",".join(["?"] * len(ids))
        rows = conn.execute(
            f"SELECT id, query_norm FROM query_pool WHERE id IN ({qmarks})",
            tuple(ids),
        ).fetchall()
        if not rows:
            return {"queries": 0, "provider_progress_reset": 0, "url_cache_deleted": 0}
        qids = [int(r["id"]) for r in rows]
        norms = [str(r["query_norm"] or "") for r in rows if str(r["query_norm"] or "").strip()]
        qid_marks = ",".join(["?"] * len(qids))

        provider_progress_reset = 0
        if norms:
            norm_marks = ",".join(["?"] * len(norms))
            cur = conn.execute(
                f"DELETE FROM query_provider_progress WHERE query_norm IN ({norm_marks})",
                tuple(norms),
            )
            provider_progress_reset = int(cur.rowcount or 0)

        # 재실행 가능 상태로 복귀
        conn.execute(
            f"UPDATE query_pool SET status='pending', updated_at=CURRENT_TIMESTAMP WHERE id IN ({qid_marks})",
            tuple(qids),
        )

        url_cache_deleted = 0
        if mode == "reanalyze_urls":
            urows = conn.execute(
                f"SELECT DISTINCT image_url FROM serp_cache WHERE query_id IN ({qid_marks})",
                tuple(qids),
            ).fetchall()
            urls = [str(r[0] or "").strip() for r in urows if str(r[0] or "").strip()]
            if urls:
                url_marks = ",".join(["?"] * len(urls))
                cur2 = conn.execute(
                    f"DELETE FROM query_image_analysis_cache WHERE image_url IN ({url_marks})",
                    tuple(urls),
                )
                url_cache_deleted = int(cur2.rowcount or 0)

        conn.commit()
        return {
            "queries": len(qids),
            "provider_progress_reset": provider_progress_reset,
            "url_cache_deleted": url_cache_deleted,
        }
    finally:
        conn.close()


def _ensure_query_pool_server() -> None:
    global _QUERY_POOL_SERVER_STARTED
    with _QUERY_POOL_SERVER_LOCK:
        if _QUERY_POOL_SERVER_STARTED:
            return

        class _QueryPoolHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
                path = parsed.path
                if path == "/delete":
                    qs = parse_qs(parsed.query)
                    raw_ids = (qs.get("ids") or [""])[0]
                    ids: list[int] = []
                    for t in raw_ids.split(","):
                        t = t.strip()
                        if t.isdigit():
                            ids.append(int(t))
                    deleted = _delete_query_pool_ids(ids)
                    body = (
                        "<html><head><meta charset='utf-8'></head><body>"
                        f"<script>alert('삭제 완료: {deleted}건'); location.href='/'</script>"
                        "</body></html>"
                    ).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if path == "/rerun":
                    qs = parse_qs(parsed.query)
                    mode = str((qs.get("mode") or ["search_only"])[0] or "search_only").strip().lower()
                    if mode not in ("search_only", "reanalyze_urls"):
                        mode = "search_only"
                    raw_ids = (qs.get("ids") or [""])[0]
                    ids: list[int] = []
                    for t in raw_ids.split(","):
                        t = t.strip()
                        if t.isdigit():
                            ids.append(int(t))
                    out = _reset_query_pool_for_rerun(ids, mode=mode)
                    body = (
                        "<html><head><meta charset='utf-8'></head><body>"
                        "<script>"
                        f"alert('리셋 완료\\n검색어: {out.get('queries',0)}건\\n진행도 리셋: {out.get('provider_progress_reset',0)}건\\nURL 캐시 삭제: {out.get('url_cache_deleted',0)}건');"
                        "location.href='/'"
                        "</script>"
                        "</body></html>"
                    ).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                conn = sqlite3.connect(DB_FILE)
                try:
                    init_query_pipeline_tables(conn)
                    rows = _fetch_query_pool_rows(conn)
                    html_text = _build_query_pool_html(rows)
                finally:
                    conn.close()
                body = html_text.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

        server = HTTPServer(("127.0.0.1", QUERY_POOL_WEB_PORT), _QueryPoolHandler)
        th = threading.Thread(target=server.serve_forever, daemon=True)
        th.start()
        _QUERY_POOL_SERVER_STARTED = True


def open_query_pool_browser_report(conn: sqlite3.Connection | None = None) -> Path:
    # conn 인자는 호환을 위해 유지. 실제 렌더/삭제는 로컬 HTTP 뷰가 DB를 직접 조회.
    _ensure_query_pool_server()
    url = f"http://127.0.0.1:{QUERY_POOL_WEB_PORT}/"
    webbrowser.open_new_tab(url)
    return Path(url)


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


def _fetch_final_output_rows(conn: sqlite3.Connection, limit: int = 100) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT
          f.id, f.product_name, f.item_mnftr_rpt_no, f.ingredients_text, f.nutrition_text,
          f.nutrition_source, f.source_image_url, f.created_at,
          c.data_source_path,
          c.raw_pass2a, c.raw_pass2b, c.raw_pass3, c.raw_pass4
        FROM food_final f
        LEFT JOIN query_image_analysis_cache c ON c.image_url = f.source_image_url
        ORDER BY f.id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()


def _reset_image_for_recheck(image_url: str, mode: str = "cache") -> dict[str, int]:
    url = str(image_url or "").strip()
    if not url:
        return {"cache_deleted": 0, "final_deleted": 0}
    conn = sqlite3.connect(DB_FILE)
    try:
        init_query_pipeline_tables(conn)
        cur1 = conn.execute("DELETE FROM query_image_analysis_cache WHERE image_url = ?", (url,))
        cache_deleted = int(cur1.rowcount or 0)
        final_deleted = 0
        if mode == "full":
            cur2 = conn.execute("DELETE FROM food_final WHERE source_image_url = ?", (url,))
            final_deleted = int(cur2.rowcount or 0)
        conn.commit()
        return {"cache_deleted": cache_deleted, "final_deleted": final_deleted}
    finally:
        conn.close()


def _build_final_outputs_html(rows: list[sqlite3.Row]) -> str:
    def _parse_json_like(text: str | None) -> dict[str, Any] | None:
        raw = str(text or "").strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            pass
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                return None
        return None

    def _fmt_amount(v: Any) -> str:
        t = str(v or "").strip()
        return f" {t}" if t else ""

    def _fmt_origin(origin: Any, detail: Any) -> str:
        o = str(origin or "").strip()
        d = str(detail or "").strip()
        if o and d:
            return f" ({o}, {d})"
        if o:
            return f" ({o})"
        if d:
            return f" ({d})"
        return ""

    def _render_sub_items(items: list[dict[str, Any]], depth: int = 0) -> str:
        if not items:
            return ""
        buf: list[str] = []
        for node in items:
            if not isinstance(node, dict):
                continue
            name = str(node.get("name") or node.get("ingredient_name") or "").strip() or "미확인"
            origin = _fmt_origin(node.get("origin"), node.get("origin_detail"))
            amount = _fmt_amount(node.get("amount"))
            buf.append(f"<li><span class='nm'>{html.escape(name)}</span>{html.escape(origin)}{html.escape(amount)}")
            children = node.get("sub_ingredients") or []
            if isinstance(children, list) and children:
                buf.append(f"<ul class='sub depth-{depth+1}'>")
                buf.append(_render_sub_items(children, depth + 1))
                buf.append("</ul>")
            buf.append("</li>")
        return "".join(buf)

    def _format_ingredients_block(raw_text: str | None) -> str:
        parsed = _parse_json_like(raw_text)
        items = []
        if parsed and isinstance(parsed, dict):
            maybe_items = parsed.get("ingredients_items")
            if isinstance(maybe_items, list):
                items = maybe_items
        if not items:
            return "<span class='muted'>구조화 데이터 없음</span>"
        out: list[str] = ["<ul class='ing-root'>"]
        for it in items:
            if not isinstance(it, dict):
                continue
            name = str(it.get("ingredient_name") or "").strip() or "미확인"
            origin = _fmt_origin(it.get("origin"), it.get("origin_detail"))
            amount = _fmt_amount(it.get("amount"))
            out.append(f"<li><span class='nm'>{html.escape(name)}</span>{html.escape(origin)}{html.escape(amount)}")
            subs = it.get("sub_ingredients") or []
            if isinstance(subs, list) and subs:
                out.append("<ul class='sub depth-1'>")
                out.append(_render_sub_items(subs, 1))
                out.append("</ul>")
            out.append("</li>")
        out.append("</ul>")
        return "".join(out)

    def _product_source_label(v: str | None) -> str:
        value = str(v or "").strip()
        if value == "public_food_enriched":
            return "공공DB"
        if value == "image_full_extraction":
            return "이미지"
        return "-"

    def _nutrition_source_label(v: str | None) -> str:
        value = str(v or "").strip()
        if value == "public_food_db":
            return "공공DB"
        if value == "image_pass4":
            return "이미지(Pass4)"
        return value or "-"

    def _extract_tagged_block(raw: str | None, tag: str) -> str | None:
        txt = str(raw or "").strip()
        if not txt:
            return None
        m = re.search(
            rf"\[{re.escape(tag)}\]\s*\n?(.*?)(?=\n\s*\[[A-Z0-9\-]+\]\s*\n|\Z)",
            txt,
            flags=re.DOTALL,
        )
        if m:
            v = str(m.group(1) or "").strip()
            return v or None
        return None

    trs: list[str] = []
    for r in rows:
        rid, name, rpt, ing_raw, nut_raw, nut_src, img_url, created_at, data_src, raw2a, raw2b, raw3, raw4 = r
        ing_html = _format_ingredients_block(ing_raw)
        nut_preview = html.escape(str(nut_raw or "")[:500]) if nut_raw else "-"
        raw2a_txt = str(raw2a or "").strip()
        raw2b_txt = str(raw2b or "").strip()
        raw3_ing_txt = _extract_tagged_block(raw3, "PASS3-INGREDIENTS") or ""
        raw3_nut_txt = _extract_tagged_block(raw3, "PASS3-NUTRITION") or ""
        raw4_ing_txt = _extract_tagged_block(raw4, "PASS4-INGREDIENTS") or ""
        raw4_nut_txt = _extract_tagged_block(raw4, "PASS4-NUTRITION") or ""
        has2a = "1" if raw2a_txt else "0"
        has2b = "1" if raw2b_txt else "0"
        has3i = "1" if raw3_ing_txt else "0"
        has3n = "1" if raw3_nut_txt else "0"
        has4i = "1" if raw4_ing_txt else "0"
        has4n = "1" if raw4_nut_txt else "0"
        safe_url = html.escape(str(img_url or ""))
        trs.append(
            f"<tr data-has-pass2a='{has2a}' data-has-pass2b='{has2b}' data-has-pass3ing='{has3i}' data-has-pass3nut='{has3n}' data-has-pass4ing='{has4i}' data-has-pass4nut='{has4n}'>"
            f"<td class='id'>{int(rid)}</td>"
            f"<td class='img'><img src='{safe_url}' alt='img' loading='lazy' /><button type='button' class='u url-copy' data-url='{safe_url}'>{html.escape(str(img_url or '-'))}</button></td>"
            f"<td>{html.escape(str(name or '-'))}</td>"
            f"<td>{html.escape(str(rpt or '-'))}</td>"
            f"<td class='ing'>{ing_html}</td>"
            f"<td><pre>{nut_preview}</pre></td>"
            "<td class='raws'>"
            f"<details class='raw-block raw-pass2a' {'open' if raw2a_txt else ''}><summary>Pass2A RAW</summary><pre class='raw-pre'>{html.escape(raw2a_txt or '-')}</pre></details>"
            f"<details class='raw-block raw-pass2b' {'open' if raw2b_txt else ''}><summary>Pass2B RAW</summary><pre class='raw-pre'>{html.escape(raw2b_txt or '-')}</pre></details>"
            f"<details class='raw-block raw-pass3ing' {'open' if raw3_ing_txt else ''}><summary>Pass3-ING RAW</summary><pre class='raw-pre'>{html.escape(raw3_ing_txt or '-')}</pre></details>"
            f"<details class='raw-block raw-pass3nut' {'open' if raw3_nut_txt else ''}><summary>Pass3-NUT RAW</summary><pre class='raw-pre'>{html.escape(raw3_nut_txt or '-')}</pre></details>"
            f"<details class='raw-block raw-pass4ing' {'open' if raw4_ing_txt else ''}><summary>Pass4-ING RAW</summary><pre class='raw-pre'>{html.escape(raw4_ing_txt or '-')}</pre></details>"
            f"<details class='raw-block raw-pass4nut' {'open' if raw4_nut_txt else ''}><summary>Pass4-NUT RAW</summary><pre class='raw-pre'>{html.escape(raw4_nut_txt or '-')}</pre></details>"
            "</td>"
            f"<td><div>제품명 출처: <b>{html.escape(_product_source_label(data_src))}</b></div><div>영양성분 출처: <b>{html.escape(_nutrition_source_label(nut_src))}</b></div></td>"
            f"<td><div class='resetCol'><button type='button' class='mini reset-btn' data-mode='cache' data-url='{safe_url}'>캐시만 삭제</button>"
            f"<button type='button' class='mini danger reset-btn' data-mode='full' data-url='{safe_url}'>캐시+최종 산출물 삭제</button></div></td>"
            f"<td>{html.escape(str(created_at or '-'))}</td>"
            "</tr>"
        )

    body = "\n".join(trs)
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>최종 산출물 조회</title>
  <style>
    body {{ margin:0; padding:18px; font-family:'Apple SD Gothic Neo','Noto Sans KR','Malgun Gothic',sans-serif; background:#f6f8fc; color:#1f2937; }}
    .card {{ background:#fff; border:1px solid #dbe2ee; border-radius:12px; padding:12px; box-shadow:0 2px 10px rgba(0,0,0,0.04); margin-bottom:12px; }}
    h1 {{ margin:0 0 8px; font-size:22px; }}
    .sub {{ color:#6b7280; font-size:13px; }}
    .toolbar {{ display:flex; gap:10px; align-items:center; margin-top:8px; flex-wrap:wrap; }}
    .toolbar select {{ padding:4px 8px; font-size:12px; }}
    table {{ width:100%; border-collapse:collapse; font-size:12px; background:#fff; }}
    th, td {{ border:1px solid #e5ebf5; padding:6px; vertical-align:top; }}
    th {{ background:#eef3ff; position:sticky; top:0; z-index:1; }}
    .id {{ white-space:nowrap; text-align:right; }}
    .img img {{ width:92px; height:92px; object-fit:cover; border-radius:8px; border:1px solid #dbe2ee; display:block; margin-bottom:4px; }}
    .u {{ max-width:220px; word-break:break-all; color:#6b7280; font-size:11px; border:0; background:transparent; padding:0; text-align:left; cursor:pointer; text-decoration:underline; text-underline-offset:2px; }}
    .u.copied {{ color:#0f766e; font-weight:700; text-decoration:none; }}
    .ing {{ min-width:320px; }}
    .ing-root,.sub {{ margin:0; padding-left:16px; }}
    .sub {{ margin-top:3px; }}
    .nm {{ font-weight:600; }}
    .muted {{ color:#6b7280; }}
    pre {{ margin:0; white-space:pre-wrap; word-break:break-word; max-width:360px; }}
    .raws {{ min-width:440px; }}
    .raw-block {{ margin-bottom:6px; }}
    .raw-block summary {{ cursor:pointer; font-weight:600; color:#334155; }}
    .raw-pre {{ max-width:420px; max-height:180px; overflow:auto; background:#f8fafc; border:1px solid #e2e8f0; padding:6px; border-radius:6px; }}
    .resetCol {{ display:flex; gap:4px; flex-wrap:wrap; min-width:160px; }}
    .mini {{ font-size:11px; border:1px solid #cbd5e1; border-radius:6px; padding:2px 6px; background:#fff; cursor:pointer; }}
    .mini.danger {{ border-color:#fca5a5; color:#b91c1c; background:#fff5f5; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>최종 산출물 조회</h1>
    <div class="sub">이미지별 재검수 리셋 지원(캐시 삭제 / 캐시+최종산출물 삭제)</div>
    <div>총 {len(rows):,}건</div>
    <div class="toolbar">
      <label for="pass-filter">Raw 필터:</label>
      <select id="pass-filter">
        <option value="all">전체</option><option value="pass2a">Pass2A RAW 있는 항목</option><option value="pass2b">Pass2B RAW 있는 항목</option>
        <option value="pass3ing">Pass3-ING RAW 있는 항목</option><option value="pass3nut">Pass3-NUT RAW 있는 항목</option>
        <option value="pass4ing">Pass4-ING RAW 있는 항목</option><option value="pass4nut">Pass4-NUT RAW 있는 항목</option>
      </select>
      <label for="raw-view">표시 RAW:</label>
      <select id="raw-view">
        <option value="all">모두</option><option value="pass2a">Pass2A</option><option value="pass2b">Pass2B</option>
        <option value="pass3ing">Pass3-ING</option><option value="pass3nut">Pass3-NUT</option>
        <option value="pass4ing">Pass4-ING</option><option value="pass4nut">Pass4-NUT</option>
      </select>
    </div>
  </div>
  <div class="card" style="padding:0; overflow:auto; max-height:78vh;">
    <table>
      <thead>
        <tr><th>ID</th><th>이미지</th><th>제품명</th><th>품목보고번호</th><th>원재료(구조화)</th><th>영양성분 RAW</th><th>Pass RAW</th><th>소스</th><th>재검수</th><th>수집시각</th></tr>
      </thead>
      <tbody>{body}</tbody>
    </table>
  </div>
  <script>
    (function () {{
      const passFilterEl = document.getElementById('pass-filter');
      const rawViewEl = document.getElementById('raw-view');
      const rows = Array.from(document.querySelectorAll('tbody tr'));
      const rawClasses = ['pass2a','pass2b','pass3ing','pass3nut','pass4ing','pass4nut'];
      function applyFilters() {{
        const pf = (passFilterEl && passFilterEl.value) || 'all';
        const rv = (rawViewEl && rawViewEl.value) || 'all';
        rows.forEach((tr) => {{
          const showRow = (pf === 'all') ? true : (tr.getAttribute('data-has-' + pf) === '1');
          tr.style.display = showRow ? '' : 'none';
          rawClasses.forEach((k) => {{
            const el = tr.querySelector('.raw-' + k);
            if (!el) return;
            el.style.display = (rv === 'all' || rv === k) ? '' : 'none';
          }});
        }});
      }}
      if (passFilterEl) passFilterEl.addEventListener('change', applyFilters);
      if (rawViewEl) rawViewEl.addEventListener('change', applyFilters);
      applyFilters();
      document.querySelectorAll('.url-copy').forEach((el) => {{
        el.addEventListener('click', async () => {{
          const v = el.getAttribute('data-url') || '';
          if (!v) return;
          try {{ await navigator.clipboard.writeText(v); }}
          catch (_err) {{
            const ta = document.createElement('textarea'); ta.value = v; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta);
          }}
          const old = el.textContent; el.classList.add('copied'); el.textContent = '복사됨';
          setTimeout(() => {{ el.classList.remove('copied'); el.textContent = old; }}, 900);
        }});
      }});
      document.querySelectorAll('.reset-btn').forEach((btn) => {{
        btn.addEventListener('click', () => {{
          const mode = btn.getAttribute('data-mode') || 'cache';
          const url = btn.getAttribute('data-url') || '';
          if (!url) return;
          const msg = mode === 'full'
            ? '해당 URL의 분석 캐시 + 최종산출물을 삭제합니다. 계속할까요?'
            : '해당 URL의 분석 캐시만 삭제합니다. 재검수 가능 상태로 만듭니다. 계속할까요?';
          if (!confirm(msg)) return;
          window.location.href = `/reset?mode=${{encodeURIComponent(mode)}}&url=${{encodeURIComponent(url)}}`;
        }});
      }});
    }})();
  </script>
</body>
</html>
"""


def _ensure_final_outputs_server() -> None:
    global _FINAL_OUTPUTS_SERVER_STARTED
    with _FINAL_OUTPUTS_SERVER_LOCK:
        if _FINAL_OUTPUTS_SERVER_STARTED:
            return

        class _FinalOutputsHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/reset":
                    qs = parse_qs(parsed.query)
                    mode = str((qs.get("mode") or ["cache"])[0] or "cache").strip().lower()
                    url = str((qs.get("url") or [""])[0] or "").strip()
                    out = _reset_image_for_recheck(url, mode=mode)
                    body = (
                        "<html><head><meta charset='utf-8'></head><body>"
                        f"<script>alert('리셋 완료\\ncache 삭제: {out.get('cache_deleted',0)}건\\nfinal 삭제: {out.get('final_deleted',0)}건'); location.href='/'</script>"
                        "</body></html>"
                    ).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                conn = sqlite3.connect(DB_FILE)
                try:
                    init_query_pipeline_tables(conn)
                    rows = _fetch_final_output_rows(conn, limit=300)
                    html_text = _build_final_outputs_html(rows)
                finally:
                    conn.close()
                body = html_text.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

        server = HTTPServer(("127.0.0.1", FINAL_OUTPUTS_WEB_PORT), _FinalOutputsHandler)
        th = threading.Thread(target=server.serve_forever, daemon=True)
        th.start()
        _FINAL_OUTPUTS_SERVER_STARTED = True


def open_final_outputs_browser_report() -> str:
    _ensure_final_outputs_server()
    url = f"http://127.0.0.1:{FINAL_OUTPUTS_WEB_PORT}/"
    webbrowser.open_new_tab(url)
    return url


def show_final_outputs(conn: sqlite3.Connection) -> None:  # noqa: ARG001
    print("\n  ✅ [최종 산출물 조회: 브라우저 리포트]")
    url = open_final_outputs_browser_report()
    print(f"  🌐 브라우저 리포트: {url}")


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
