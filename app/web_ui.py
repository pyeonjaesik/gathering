"""
브라우저 기반 원재료 수집 모니터.

실행:
    uv run streamlit run app/web_ui.py
"""

from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import streamlit as st

# Streamlit 실행 시 모듈 경로가 app/로 잡히는 경우를 대비해 프로젝트 루트를 경로에 추가.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import DB_FILE
from app.ingredient_enricher import diagnose_analysis, get_priority_subcategories, run_enricher


@dataclass
class RunJob:
    limit: int
    quiet: bool
    lv3: str | None
    lv4: str | None


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def _fmt_dt(value: str | None) -> str:
    if not value:
        return "-"
    return value


def _safe_json(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def _ensure_state() -> None:
    if "run_thread" not in st.session_state:
        st.session_state.run_thread = None
    if "run_status" not in st.session_state:
        st.session_state.run_status = {
            "running": False,
            "started_at": None,
            "finished_at": None,
            "error": None,
            "last_job": None,
        }


def _is_running() -> bool:
    th = st.session_state.run_thread
    return bool(th and th.is_alive())


def _runner(job: RunJob) -> None:
    st.session_state.run_status["running"] = True
    st.session_state.run_status["error"] = None
    st.session_state.run_status["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.run_status["finished_at"] = None
    st.session_state.run_status["last_job"] = job
    try:
        run_enricher(
            limit=job.limit,
            quiet=job.quiet,
            lv3=job.lv3,
            lv4=job.lv4,
        )
    except Exception as exc:  # pylint: disable=broad-except
        st.session_state.run_status["error"] = str(exc)
    finally:
        st.session_state.run_status["running"] = False
        st.session_state.run_status["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _start_job(job: RunJob) -> None:
    if _is_running():
        return
    thread = threading.Thread(target=_runner, args=(job,), daemon=True)
    st.session_state.run_thread = thread
    thread.start()


def _load_overview(conn: sqlite3.Connection) -> dict[str, int]:
    total_target = conn.execute(
        """
        SELECT COUNT(DISTINCT itemMnftrRptNo)
        FROM processed_food_info
        WHERE itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != ''
        """
    ).fetchone()[0]
    info_cnt = conn.execute("SELECT COUNT(*) FROM ingredient_info").fetchone()[0]
    attempt_total = conn.execute("SELECT COUNT(*) FROM ingredient_attempts").fetchone()[0]
    matched = conn.execute(
        "SELECT COUNT(*) FROM ingredient_attempts WHERE status='matched'"
    ).fetchone()[0]
    unmatched = conn.execute(
        "SELECT COUNT(*) FROM ingredient_attempts WHERE status='unmatched'"
    ).fetchone()[0]
    failed = conn.execute(
        "SELECT COUNT(*) FROM ingredient_attempts WHERE status='failed'"
    ).fetchone()[0]
    in_progress = conn.execute(
        "SELECT COUNT(*) FROM ingredient_attempts WHERE status='in_progress'"
    ).fetchone()[0]
    return {
        "total_target": total_target,
        "info_cnt": info_cnt,
        "attempt_total": attempt_total,
        "matched": matched,
        "unmatched": unmatched,
        "failed": failed,
        "in_progress": in_progress,
    }


def _load_in_progress(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          ia.query_itemMnftrRptNo,
          ia.query_food_name,
          ia.query_mfr_name,
          ia.searched_query,
          ia.images_requested,
          ia.images_analyzed,
          ia.started_at,
          (
            SELECT COUNT(*)
            FROM ingredient_extractions ie
            WHERE ie.query_itemMnftrRptNo = ia.query_itemMnftrRptNo
          ) AS analyzed_rows
        FROM ingredient_attempts ia
        WHERE ia.status = 'in_progress'
        ORDER BY ia.started_at DESC
        """
    ).fetchall()


def _load_recent_logs(conn: sqlite3.Connection, limit: int = 100) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          ie.query_itemMnftrRptNo,
          ie.query_food_name,
          ie.image_rank,
          ie.image_url,
          ie.extracted_itemMnftrRptNo,
          ie.ingredients_text,
          ie.matched_target,
          ie.raw_payload,
          ie.created_at
        FROM ingredient_extractions ie
        ORDER BY ie.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        payload = _safe_json(row["raw_payload"])
        status_label, reason = diagnose_analysis(payload, row["query_itemMnftrRptNo"])
        ingredients = row["ingredients_text"] or ""
        items.append(
            {
                "시각": row["created_at"],
                "질의번호": row["query_itemMnftrRptNo"],
                "상품명": row["query_food_name"],
                "rank": row["image_rank"],
                "이미지URL": row["image_url"],
                "추출번호": row["extracted_itemMnftrRptNo"] or "-",
                "매칭": "YES" if row["matched_target"] == 1 else "NO",
                "상태": status_label,
                "사유": reason,
                "원재료추출": (ingredients[:120] + "...") if len(ingredients) > 120 else (ingredients or "-"),
                "원재료전체": ingredients or "-",
            }
        )
    return items


def _load_live_processing_rows(conn: sqlite3.Connection, limit: int = 200) -> list[dict[str, Any]]:
    """현재 in_progress 대상에서 방금 분석된 이미지 로그를 실시간 조회."""
    rows = conn.execute(
        """
        SELECT
          ie.query_itemMnftrRptNo,
          ie.query_food_name,
          ie.image_rank,
          ie.image_url,
          ie.extracted_itemMnftrRptNo,
          ie.ingredients_text,
          ie.matched_target,
          ie.raw_payload,
          ie.created_at
        FROM ingredient_extractions ie
        JOIN ingredient_attempts ia
          ON ia.query_itemMnftrRptNo = ie.query_itemMnftrRptNo
        WHERE ia.status = 'in_progress'
        ORDER BY ie.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        payload = _safe_json(row["raw_payload"])
        status_label, reason = diagnose_analysis(payload, row["query_itemMnftrRptNo"])
        ingredients = row["ingredients_text"] or ""
        result.append(
            {
                "시각": row["created_at"],
                "질의번호": row["query_itemMnftrRptNo"],
                "상품명": row["query_food_name"],
                "이미지순번": row["image_rank"],
                "이미지URL": row["image_url"],
                "추출번호": row["extracted_itemMnftrRptNo"] or "-",
                "매칭": "YES" if row["matched_target"] == 1 else "NO",
                "상태": status_label,
                "사유": reason,
                "원재료": (ingredients[:100] + "...") if len(ingredients) > 100 else (ingredients or "-"),
            }
        )
    return result


def _load_priority_df(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    categories = get_priority_subcategories(conn)
    result: list[dict[str, Any]] = []
    for idx, row in enumerate(categories, start=1):
        result.append(
            {
                "No": idx,
                "우선순위": row["priority"],
                "대분류": row["lv3"],
                "중분류": row["lv4"],
                "총상품": row["total_count"],
                "시도완료": row["attempted_count"],
                "성공수집": row["success_count"],
                "수집률(%)": round(row["success_rate"], 1),
            }
        )
    return result


def main() -> None:
    st.set_page_config(
        page_title="원재료 분석 실시간 모니터",
        page_icon="🧪",
        layout="wide",
    )
    _ensure_state()

    st.title("🧪 원재료 분석 실시간 모니터")
    st.caption("카테고리 선택 후 실행하면 DB 기준으로 진행상황/실패사유/이미지 URL을 실시간 추적합니다.")

    conn = _conn()
    overview = _load_overview(conn)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("전체 대상(고유 품목번호)", f"{overview['total_target']:,}")
    c2.metric("원재료 확보", f"{overview['info_cnt']:,}")
    c3.metric("시도 이력", f"{overview['attempt_total']:,}")
    c4.metric("진행중", f"{overview['in_progress']:,}")

    c5, c6, c7 = st.columns(3)
    c5.metric("matched", f"{overview['matched']:,}")
    c6.metric("unmatched", f"{overview['unmatched']:,}")
    c7.metric("failed", f"{overview['failed']:,}")

    with st.expander("⚙️ 실행 설정", expanded=True):
        categories = _load_priority_df(conn)
        labels = [f"{r['No']:>3}. {r['우선순위']} | {r['대분류']} > {r['중분류']} (총 {r['총상품']:,})" for r in categories]
        pick = st.selectbox("중분류 선택", options=list(range(len(labels))), format_func=lambda i: labels[i])
        selected = categories[pick]

        all_mode = st.checkbox("전체 처리(남은 대상 전부)", value=False)
        limit = 0 if all_mode else st.number_input("처리 개수", min_value=1, value=20, step=1)
        quiet = st.checkbox("이미지별 상세 로그 축약(quiet)", value=False)

        running = _is_running()
        if running:
            st.warning("현재 수집 작업이 실행 중입니다. 중복 실행은 막아둡니다.")
        start = st.button("🚀 선택 카테고리 실행", disabled=running)
        if start:
            _start_job(
                RunJob(
                    limit=int(limit),
                    quiet=bool(quiet),
                    lv3=selected["대분류"],
                    lv4=selected["중분류"],
                )
            )
            st.success("작업을 시작했습니다. 아래 실시간 패널에서 진행상황을 확인하세요.")

    status = st.session_state.run_status
    st.subheader("📡 실행 상태")
    st.write(
        {
            "running": _is_running(),
            "started_at": _fmt_dt(status.get("started_at")),
            "finished_at": _fmt_dt(status.get("finished_at")),
            "error": status.get("error") or "-",
        }
    )

    st.subheader("🔄 현재 진행중 상품")
    in_progress = _load_in_progress(conn)
    if not in_progress:
        st.info("진행중인 상품이 없습니다.")
    else:
        table: list[dict[str, Any]] = []
        for row in in_progress:
            done = row["analyzed_rows"] or 0
            req = row["images_requested"] or 0
            table.append(
                {
                    "품목번호": row["query_itemMnftrRptNo"],
                    "상품명": row["query_food_name"],
                    "제조사": row["query_mfr_name"] or "-",
                    "검색어": row["searched_query"] or "-",
                    "이미지처리": f"{done}/{req if req else '-'}",
                    "시작시각": row["started_at"],
                }
            )
        st.dataframe(table, use_container_width=True, hide_index=True)

    st.subheader("⚡ 지금 분석중인 이미지 로그 (실시간)")
    live_rows = _load_live_processing_rows(conn, limit=300)
    if not live_rows:
        st.info("현재 in_progress 대상의 이미지 분석 로그가 아직 없습니다.")
    else:
        st.dataframe(
            live_rows,
            use_container_width=True,
            hide_index=True,
            column_config={
                "이미지URL": st.column_config.LinkColumn("이미지URL", display_text="열기"),
            },
        )

    st.subheader("🧾 최근 이미지 분석 로그")
    logs = _load_recent_logs(conn, limit=180)
    if not logs:
        st.info("아직 분석 로그가 없습니다.")
    else:
        st.markdown("#### 🖼️ 실시간 이미지 미리보기")
        st.caption("최근 분석된 이미지를 클릭 없이 바로 확인할 수 있습니다.")
        preview_count = st.slider("미리보기 개수", min_value=3, max_value=18, value=9, step=3)
        cols_per_row = 3
        preview_logs = logs[:preview_count]
        for i in range(0, len(preview_logs), cols_per_row):
            cols = st.columns(cols_per_row)
            chunk = preview_logs[i : i + cols_per_row]
            for col, item in zip(cols, chunk):
                with col:
                    st.image(item["이미지URL"], use_container_width=True)
                    st.caption(
                        f"{item['시각']} | #{item['rank']} | {item['상태']}"
                    )
                    st.write(f"**질의번호**: `{item['질의번호']}`")
                    st.write(f"**추출번호**: `{item['추출번호']}` | **매칭**: {item['매칭']}")
                    st.write(f"**사유**: {item['사유']}")
                    st.write(f"**원재료**: {item['원재료추출']}")
                    with st.expander("원재료 전체 텍스트 / URL"):
                        st.code(item["원재료전체"])
                        st.code(item["이미지URL"])

        st.markdown("#### 📋 최근 로그 테이블")
        st.dataframe(
            logs,
            use_container_width=True,
            hide_index=True,
            column_config={
                "이미지URL": st.column_config.LinkColumn("이미지URL", display_text="열기"),
            },
        )

    conn.close()

    auto_refresh = st.checkbox("2초 자동 새로고침", value=True)
    if auto_refresh and _is_running():
        time.sleep(2)
        st.rerun()


if __name__ == "__main__":
    main()
