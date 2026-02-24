"""
파이프라인 실행 브라우저 UI
실행:
    uv run streamlit run app/web_query_pipeline_ui.py --server.port 8502
"""

from __future__ import annotations

import queue
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import DB_FILE
from app.main import execute_query_pipeline_run
from app.query_pipeline import init_query_pipeline_tables


@dataclass
class QueryRunJob:
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
    if "job_thread" not in st.session_state:
        st.session_state.job_thread = None
    if "log_queue" not in st.session_state:
        st.session_state.log_queue = queue.Queue()
    if "log_lines" not in st.session_state:
        st.session_state.log_lines = []
    if "last_report" not in st.session_state:
        st.session_state.last_report = None
    if "run_error" not in st.session_state:
        st.session_state.run_error = None


def _running() -> bool:
    th = st.session_state.job_thread
    return bool(th and th.is_alive())


def _runner(job: QueryRunJob) -> None:
    st.session_state.run_error = None

    def _logger(line: str) -> None:
        st.session_state.log_queue.put(str(line))

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
        st.session_state.last_report = report_path
    except Exception as exc:  # pylint: disable=broad-except
        st.session_state.run_error = str(exc)
        st.session_state.log_queue.put(f"❌ 실행 예외: {exc}")


def _start(job: QueryRunJob) -> None:
    if _running():
        return
    st.session_state.log_lines = []
    st.session_state.last_report = None
    th = threading.Thread(target=_runner, args=(job,), daemon=True)
    st.session_state.job_thread = th
    th.start()


def _drain_logs() -> None:
    q = st.session_state.log_queue
    while True:
        try:
            line = q.get_nowait()
        except queue.Empty:
            break
        st.session_state.log_lines.append(line)
    if len(st.session_state.log_lines) > 1200:
        st.session_state.log_lines = st.session_state.log_lines[-1200:]


def _load_dashboard() -> dict:
    with _conn() as conn:
        init_query_pipeline_tables(conn)
        all_runs = conn.execute("SELECT COUNT(*) FROM query_runs").fetchone()[0]
        running_runs = conn.execute("SELECT COUNT(*) FROM query_runs WHERE status='running'").fetchone()[0]
        done_runs = conn.execute("SELECT COUNT(*) FROM query_runs WHERE status='done'").fetchone()[0]
        failed_runs = conn.execute("SELECT COUNT(*) FROM query_runs WHERE status='failed'").fetchone()[0]

        total_queries = conn.execute("SELECT COUNT(*) FROM query_pool").fetchone()[0]
        pending_queries = conn.execute("SELECT COUNT(*) FROM query_pool WHERE status='pending'").fetchone()[0]

        cache_total = conn.execute("SELECT COUNT(*) FROM query_image_analysis_cache").fetchone()[0]
        cache_pass4 = conn.execute("SELECT COUNT(*) FROM query_image_analysis_cache WHERE pass4_ok=1").fetchone()[0]
        cache_fail = conn.execute("SELECT COUNT(*) FROM query_image_analysis_cache WHERE pass4_ok!=1").fetchone()[0]

        food_final = conn.execute("SELECT COUNT(*) FROM food_final").fetchone()[0]

        recent_runs = conn.execute(
            """
            SELECT r.id, q.query_text, r.status, r.started_at, r.ended_at,
                   r.total_images, r.analyzed_images, r.final_saved_count, r.api_calls
            FROM query_runs r
            JOIN query_pool q ON q.id=r.query_id
            ORDER BY r.id DESC
            LIMIT 30
            """
        ).fetchall()

        recent_images = conn.execute(
            """
            SELECT image_url, pass1_ok, pass2a_ok, pass2b_ok, pass3_ok, pass4_ok,
                   fail_stage, fail_reason, analyzed_at
            FROM query_image_analysis_cache
            ORDER BY id DESC
            LIMIT 60
            """
        ).fetchall()

    return {
        "all_runs": all_runs,
        "running_runs": running_runs,
        "done_runs": done_runs,
        "failed_runs": failed_runs,
        "total_queries": total_queries,
        "pending_queries": pending_queries,
        "cache_total": cache_total,
        "cache_pass4": cache_pass4,
        "cache_fail": cache_fail,
        "food_final": food_final,
        "recent_runs": recent_runs,
        "recent_images": recent_images,
    }


def main() -> None:
    st.set_page_config(page_title="파이프라인 실행 실시간 대시보드", page_icon="🚀", layout="wide")
    _ensure_state()

    st.title("🚀 파이프라인 실행 실시간 대시보드")
    st.caption("검색어 선택/직접입력으로 실행하고, Pass 진행과 DB 누적 상황을 실시간으로 확인합니다.")

    with st.expander("⚙️ 실행 설정", expanded=True):
        mode_label = st.radio("검색어 입력 방식", ["검색어 풀", "직접 입력"], horizontal=True)
        mode = "1" if mode_label == "검색어 풀" else "2"

        if mode == "2":
            direct_query = st.text_input("직접 검색어", value="")
            query_limit = 1
        else:
            direct_query = None
            query_limit = st.number_input("실행할 검색어 개수", min_value=1, max_value=100, value=3, step=1)

        provider = st.selectbox(
            "이미지 수집 엔진",
            options=["google", "naver_official", "naver_blog", "naver_shop"],
            index=0,
        )
        c1, c2, c3 = st.columns(3)
        with c1:
            max_pages = st.number_input("최대 페이지", min_value=1, max_value=20, value=1, step=1)
        with c2:
            max_images = st.number_input("검색어당 최대 이미지(0=전체)", min_value=0, max_value=10000, value=0, step=10)
        with c3:
            pass_workers = st.number_input("Pass 동시호출", min_value=1, max_value=50, value=5, step=1)

        run_disabled = _running() or (mode == "2" and not str(direct_query or "").strip())
        if st.button("🚀 실행 시작", disabled=run_disabled, use_container_width=True):
            _start(
                QueryRunJob(
                    mode=mode,
                    provider=provider,
                    query_limit=int(query_limit),
                    max_pages=int(max_pages),
                    max_images=int(max_images),
                    pass_workers=int(pass_workers),
                    direct_query=direct_query,
                )
            )
            st.success("실행을 시작했습니다. 아래 로그/DB 현황이 자동 갱신됩니다.")

    _drain_logs()
    dash = _load_dashboard()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("실행중 run", f"{dash['running_runs']:,}")
    m2.metric("완료 run", f"{dash['done_runs']:,}")
    m3.metric("실패 run", f"{dash['failed_runs']:,}")
    m4.metric("총 run", f"{dash['all_runs']:,}")

    n1, n2, n3, n4 = st.columns(4)
    n1.metric("검색어 풀", f"{dash['total_queries']:,}")
    n2.metric("pending 검색어", f"{dash['pending_queries']:,}")
    n3.metric("이미지 분석 캐시", f"{dash['cache_total']:,}")
    n4.metric("최종 저장(food_final)", f"{dash['food_final']:,}")

    p1, p2 = st.columns(2)
    p1.metric("Pass4 성공 캐시", f"{dash['cache_pass4']:,}")
    p2.metric("Pass4 미통과 캐시", f"{dash['cache_fail']:,}")

    st.subheader("🧾 실시간 실행 로그")
    if st.session_state.log_lines:
        st.code("\n".join(st.session_state.log_lines[-400:]), language="text")
    else:
        st.info("아직 로그가 없습니다.")

    if st.session_state.run_error:
        st.error(f"실행 오류: {st.session_state.run_error}")

    if st.session_state.last_report:
        report_path = Path(str(st.session_state.last_report))
        st.success(f"리포트 생성 완료: {report_path}")
        if report_path.exists():
            st.link_button("📄 결과 HTML 열기", report_path.resolve().as_uri(), use_container_width=True)

    st.subheader("📌 최근 실행(run) 현황")
    st.dataframe(dash["recent_runs"], use_container_width=True, hide_index=True)

    st.subheader("🖼️ 최근 이미지별 Pass 상태")
    st.dataframe(dash["recent_images"], use_container_width=True, hide_index=True)

    auto = st.checkbox("2초 자동 새로고침", value=True)
    if auto and _running():
        time.sleep(2)
        st.rerun()


if __name__ == "__main__":
    main()
