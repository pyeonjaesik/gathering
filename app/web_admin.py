"""
파이프라인 실행 어드민 대시보드
실행:
    uv run streamlit run app/web_admin.py --server.port 8503
"""

from __future__ import annotations

import queue
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import DB_FILE
from app.main import execute_query_pipeline_run
from app.query_pipeline import init_query_pipeline_tables, normalize_query, upsert_query


@dataclass
class AdminRunJob:
    mode: str
    provider: str
    query_limit: int
    max_pages: int
    max_images: int
    pass_workers: int
    direct_query: str | None


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_state() -> None:
    if "admin_thread" not in st.session_state:
        st.session_state.admin_thread = None
    if "admin_log_queue" not in st.session_state:
        st.session_state.admin_log_queue = queue.Queue()
    if "admin_logs" not in st.session_state:
        st.session_state.admin_logs = []
    if "admin_last_report" not in st.session_state:
        st.session_state.admin_last_report = None
    if "admin_last_error" not in st.session_state:
        st.session_state.admin_last_error = None


def _running() -> bool:
    th = st.session_state.admin_thread
    return bool(th and th.is_alive())


def _runner(job: AdminRunJob) -> None:
    q = st.session_state.admin_log_queue

    def _logger(line: str) -> None:
        q.put(f"LOG::{line}")

    try:
        report_path = execute_query_pipeline_run(
            mode=job.mode,
            provider=job.provider,
            query_limit=job.query_limit,
            max_pages=job.max_pages,
            max_images=job.max_images,
            pass_workers=job.pass_workers,
            direct_query=job.direct_query,
            open_browser_report=False,
            logger=_logger,
        )
        q.put(f"REPORT::{report_path or ''}")
    except Exception as exc:  # pylint: disable=broad-except
        q.put(f"ERROR::{exc}")
    finally:
        q.put("DONE::")


def _start(job: AdminRunJob) -> None:
    if _running():
        return
    st.session_state.admin_logs = []
    st.session_state.admin_last_report = None
    st.session_state.admin_last_error = None
    th = threading.Thread(target=_runner, args=(job,), daemon=True)
    st.session_state.admin_thread = th
    th.start()


def _drain_events() -> None:
    q = st.session_state.admin_log_queue
    while True:
        try:
            msg = q.get_nowait()
        except queue.Empty:
            break
        if msg.startswith("LOG::"):
            st.session_state.admin_logs.append(msg[5:])
        elif msg.startswith("REPORT::"):
            st.session_state.admin_last_report = msg[8:] or None
        elif msg.startswith("ERROR::"):
            st.session_state.admin_last_error = msg[7:]
            st.session_state.admin_logs.append(f"❌ {msg[7:]}")
    if len(st.session_state.admin_logs) > 2000:
        st.session_state.admin_logs = st.session_state.admin_logs[-2000:]


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]


def _load_kpis(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "query_total": conn.execute("SELECT COUNT(*) FROM query_pool").fetchone()[0],
        "query_pending": conn.execute("SELECT COUNT(*) FROM query_pool WHERE status='pending'").fetchone()[0],
        "query_paused": conn.execute("SELECT COUNT(*) FROM query_pool WHERE status='paused'").fetchone()[0],
        "run_total": conn.execute("SELECT COUNT(*) FROM query_runs").fetchone()[0],
        "run_running": conn.execute("SELECT COUNT(*) FROM query_runs WHERE status='running'").fetchone()[0],
        "run_done": conn.execute("SELECT COUNT(*) FROM query_runs WHERE status='done'").fetchone()[0],
        "run_failed": conn.execute("SELECT COUNT(*) FROM query_runs WHERE status='failed'").fetchone()[0],
        "img_cache_total": conn.execute("SELECT COUNT(*) FROM query_image_analysis_cache").fetchone()[0],
        "img_cache_p4": conn.execute("SELECT COUNT(*) FROM query_image_analysis_cache WHERE pass4_ok=1").fetchone()[0],
        "food_final": conn.execute("SELECT COUNT(*) FROM food_final").fetchone()[0],
    }


def _load_query_pool(conn: sqlite3.Connection, keyword: str = "", status: str = "all") -> list[dict[str, Any]]:
    where = []
    args: list[Any] = []
    if keyword.strip():
        where.append("(query_text LIKE ? OR query_norm LIKE ?)")
        like = f"%{keyword.strip()}%"
        args.extend([like, like])
    if status != "all":
        where.append("status=?")
        args.append(status)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(
        f"""
        SELECT id, query_text, query_norm, source, priority_score, status, run_count, last_run_at, updated_at, notes
        FROM query_pool
        {where_sql}
        ORDER BY priority_score DESC, id ASC
        LIMIT 1000
        """,
        args,
    ).fetchall()
    return _rows_to_dicts(rows)


def _load_runs(conn: sqlite3.Connection, limit: int = 200) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT r.id AS run_id, r.query_id, q.query_text, r.status, r.started_at, r.ended_at,
               r.total_images, r.analyzed_images, r.pass2b_pass_count, r.pass4_pass_count,
               r.final_saved_count, r.api_calls, r.error_message
        FROM query_runs r
        JOIN query_pool q ON q.id = r.query_id
        ORDER BY r.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return _rows_to_dicts(rows)


def _load_run_image_attempts(conn: sqlite3.Connection, run_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          s.page,
          s.rank_in_page,
          s.image_url,
          s.source,
          s.title,
          c.last_run_id,
          c.pass1_ok,
          c.pass2a_ok,
          c.pass2b_ok,
          c.pass3_ok,
          c.pass4_ok,
          c.pass1_attempted,
          c.pass2a_attempted,
          c.pass2b_attempted,
          c.pass3_ing_attempted,
          c.pass3_nut_attempted,
          c.pass4_ing_attempted,
          c.pass4_nut_attempted,
          c.fail_stage,
          c.fail_reason,
          c.data_source_path,
          c.nutrition_data_source,
          c.public_food_matched,
          c.analyzed_at,
          f.id AS food_final_id,
          f.item_mnftr_rpt_no,
          f.product_name
        FROM serp_cache s
        LEFT JOIN query_image_analysis_cache c ON c.image_url = s.image_url
        LEFT JOIN food_final f ON f.source_run_id = s.run_id AND f.source_image_url = s.image_url
        WHERE s.run_id=?
        ORDER BY s.page ASC, s.rank_in_page ASC, s.id ASC
        """,
        (run_id,),
    ).fetchall()
    return _rows_to_dicts(rows)


def _load_food_final(conn: sqlite3.Connection, keyword: str = "", limit: int = 500) -> list[dict[str, Any]]:
    where = []
    args: list[Any] = []
    if keyword.strip():
        where.append(
            "(" 
            "COALESCE(item_mnftr_rpt_no,'') LIKE ? OR "
            "COALESCE(product_name,'') LIKE ? OR "
            "COALESCE(ingredients_text,'') LIKE ?"
            ")"
        )
        like = f"%{keyword.strip()}%"
        args.extend([like, like, like])
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    args.append(int(limit))
    rows = conn.execute(
        f"""
        SELECT id, created_at, source_query_id, source_run_id, source_image_url,
               item_mnftr_rpt_no, product_name, nutrition_source,
               ingredients_text, nutrition_text
        FROM food_final
        {where_sql}
        ORDER BY id DESC
        LIMIT ?
        """,
        args,
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["ingredients_preview"] = (d.get("ingredients_text") or "")[:160]
        d["nutrition_preview"] = (d.get("nutrition_text") or "")[:160]
        out.append(d)
    return out


def _render_failure_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bucket: dict[tuple[str, str], int] = {}
    for r in rows:
        p4 = r.get("pass4_ok")
        if p4 == 1:
            continue
        key = (str(r.get("fail_stage") or "-"), str(r.get("fail_reason") or "-"))
        bucket[key] = bucket.get(key, 0) + 1
    summary = []
    for (stage, reason), count in sorted(bucket.items(), key=lambda x: x[1], reverse=True):
        summary.append({"fail_stage": stage, "fail_reason": reason, "count": count})
    return summary


def main() -> None:
    st.set_page_config(page_title="파이프라인 실행 어드민", page_icon="🧭", layout="wide")
    _ensure_state()
    _drain_events()

    with _conn() as conn:
        init_query_pipeline_tables(conn)
        k = _load_kpis(conn)

    st.title("🧭 파이프라인 실행 어드민")
    st.caption("검색어 관리, 실행, 실행 상세 추적, 최종 저장 DB 조회를 한 화면에서 관리합니다.")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("검색어", f"{k['query_total']:,}")
    c2.metric("pending", f"{k['query_pending']:,}")
    c3.metric("실행중 run", f"{k['run_running']:,}")
    c4.metric("Pass4 성공 캐시", f"{k['img_cache_p4']:,}")
    c5.metric("최종 저장", f"{k['food_final']:,}")

    t_query, t_run, t_final = st.tabs(["1) 검색어 관리/실행", "2) 실행 상세 추적", "3) 최종DB 조회"])

    with t_query:
        st.subheader("검색어 추가")
        with st.form("add_query_form", clear_on_submit=True):
            q_text = st.text_input("검색어")
            q_priority = st.number_input("우선순위 점수", min_value=0.0, value=100.0, step=10.0)
            q_source = st.text_input("source", value="manual_admin")
            q_notes = st.text_input("notes", value="")
            add_btn = st.form_submit_button("검색어 저장")
        if add_btn:
            if not q_text.strip():
                st.warning("검색어를 입력하세요.")
            else:
                with _conn() as conn:
                    init_query_pipeline_tables(conn)
                    qid = upsert_query(
                        conn,
                        q_text.strip(),
                        source=q_source.strip() or "manual_admin",
                        priority_score=float(q_priority),
                        target_segment_score=0.0,
                        status="pending",
                        notes=q_notes.strip() or None,
                    )
                st.success(f"저장 완료: query_id={qid}, norm={normalize_query(q_text)}")

        st.divider()
        st.subheader("검색어 풀")
        f1, f2 = st.columns([3, 1])
        with f1:
            keyword = st.text_input("검색어 필터", value="")
        with f2:
            status = st.selectbox("status", ["all", "pending", "paused", "running", "done", "failed"], index=0)

        with _conn() as conn:
            qrows = _load_query_pool(conn, keyword=keyword, status=status)
        st.dataframe(qrows, use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("실행")
        run_mode_label = st.radio("실행 대상", ["검색어 풀 상위 N개", "특정 검색어 1개"], horizontal=True)
        run_mode = "1" if run_mode_label == "검색어 풀 상위 N개" else "2"

        provider = st.selectbox("provider", ["google", "naver_official", "naver_blog", "naver_shop"], index=0)
        r1, r2, r3, r4 = st.columns(4)
        with r1:
            query_limit = st.number_input("검색어 개수(N)", min_value=1, max_value=100, value=3, step=1)
        with r2:
            max_pages = st.number_input("최대 페이지", min_value=1, max_value=20, value=1, step=1)
        with r3:
            max_images = st.number_input("검색어당 최대 이미지(0=전체)", min_value=0, max_value=10000, value=0, step=10)
        with r4:
            pass_workers = st.number_input("Pass 동시호출", min_value=1, max_value=50, value=5, step=1)

        selected_direct_query: str | None = None
        if run_mode == "2":
            if not qrows:
                st.warning("검색어 풀이 비어 있습니다. 먼저 추가하세요.")
            else:
                options = [
                    f"[{int(r['id'])}] {str(r['query_text'])} | pri={float(r['priority_score']):.1f} | {str(r['status'])}"
                    for r in qrows[:300]
                ]
                picked = st.selectbox("실행할 검색어 선택", options=options)
                picked_id = int(picked.split("]", 1)[0][1:])
                hit = [r for r in qrows if int(r["id"]) == picked_id]
                if hit:
                    selected_direct_query = str(hit[0]["query_text"])

        start_disabled = _running() or (run_mode == "2" and not selected_direct_query)
        if st.button("🚀 실행 시작", disabled=start_disabled, use_container_width=True):
            _start(
                AdminRunJob(
                    mode=run_mode,
                    provider=provider,
                    query_limit=int(query_limit),
                    max_pages=int(max_pages),
                    max_images=int(max_images),
                    pass_workers=int(pass_workers),
                    direct_query=selected_direct_query,
                )
            )
            st.success("실행 시작. 아래 탭에서 실시간 추적하세요.")

        st.subheader("실시간 로그")
        if st.session_state.admin_logs:
            st.code("\n".join(st.session_state.admin_logs[-500:]), language="text")
        else:
            st.info("로그 없음")
        if st.session_state.admin_last_error:
            st.error(f"실행 오류: {st.session_state.admin_last_error}")
        if st.session_state.admin_last_report:
            p = Path(str(st.session_state.admin_last_report))
            if p.exists():
                st.link_button("📄 실행 결과 HTML 열기", p.resolve().as_uri(), use_container_width=True)

    with t_run:
        st.subheader("최근 실행(run) 목록")
        with _conn() as conn:
            runs = _load_runs(conn, limit=300)
        st.dataframe(runs, use_container_width=True, hide_index=True)

        if not runs:
            st.info("run 데이터가 없습니다.")
        else:
            run_ids = [int(r["run_id"]) for r in runs]
            run_id = st.selectbox("상세 추적할 run_id", options=run_ids)
            with _conn() as conn:
                attempts = _load_run_image_attempts(conn, run_id=int(run_id))

            st.markdown("#### 이미지별 시도 상세 (URL 단위)")
            if not attempts:
                st.info("선택 run의 이미지 시도 데이터가 없습니다.")
            else:
                st.dataframe(attempts, use_container_width=True, hide_index=True)
                st.markdown("#### 실패 사유 요약")
                fail_summary = _render_failure_summary(attempts)
                if fail_summary:
                    st.dataframe(fail_summary, use_container_width=True, hide_index=True)
                else:
                    st.success("실패 항목 없음")

    with t_final:
        st.subheader("최종 저장 DB (food_final)")
        fkw = st.text_input("필터(품목보고번호/제품명/원재료)", value="")
        flim = st.number_input("조회 개수", min_value=20, max_value=5000, value=300, step=20)
        with _conn() as conn:
            finals = _load_food_final(conn, keyword=fkw, limit=int(flim))

        if not finals:
            st.info("조건에 맞는 최종 저장 데이터가 없습니다.")
        else:
            rows = []
            for r in finals:
                rows.append(
                    {
                        "id": r["id"],
                        "created_at": r["created_at"],
                        "source_query_id": r["source_query_id"],
                        "source_run_id": r["source_run_id"],
                        "source_image_url": r["source_image_url"],
                        "item_mnftr_rpt_no": r["item_mnftr_rpt_no"],
                        "product_name": r["product_name"],
                        "nutrition_source": r["nutrition_source"],
                        "ingredients_preview": r["ingredients_preview"],
                        "nutrition_preview": r["nutrition_preview"],
                    }
                )
            st.dataframe(rows, use_container_width=True, hide_index=True)

            picked_id = st.selectbox("원문 확인할 food_final.id", options=[int(r["id"]) for r in finals])
            row = [r for r in finals if int(r["id"]) == int(picked_id)][0]
            st.markdown("#### 상세 원문")
            st.write(f"- 품목보고번호: `{row['item_mnftr_rpt_no'] or '-'}`")
            st.write(f"- 제품명: `{row['product_name'] or '-'}`")
            st.write(f"- 이미지 URL: {row['source_image_url']}")
            st.image(str(row["source_image_url"]), use_container_width=True)
            st.markdown("**원재료 원문**")
            st.code(str(row.get("ingredients_text") or ""), language="text")
            st.markdown("**영양성분 원문**")
            st.code(str(row.get("nutrition_text") or ""), language="text")

    auto = st.checkbox("2초 자동 새로고침", value=True)
    if auto and _running():
        time.sleep(2)
        st.rerun()


if __name__ == "__main__":
    main()
