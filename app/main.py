"""
통합 실행 허브
- 데이터 조회 뷰어
- 원재료명 추출 파이프라인
- 공공 API 수집
"""

import os
import json
import html
import io
import socket
import subprocess
import sys
import time
import re
import shutil
import webbrowser
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait
from typing import Callable, Any
from collections import Counter
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, quote, urlencode, urlparse

import sqlite3
import random
import requests
from app import collector, viewer
from app.backup_tools import create_backup, list_backups, read_backup_metadata, restore_backup, verify_backup
from app.config import DB_FILE
from app.dedupe_tools import (
    duplicate_conditions,
    get_duplicate_samples,
    get_duplicate_stats,
    run_dedupe,
)
from app.database import ensure_processed_food_table
from app.database import (
    clear_haccp_data,
    clear_haccp_parse_block,
    clear_haccp_parsed_cache,
    clear_haccp_progress,
    get_haccp_completed_pages,
    init_haccp_tables,
    mark_haccp_page_done,
    upsert_haccp_parse_block,
    upsert_haccp_parsed_row,
    upsert_haccp_rows,
)
from app.ingredient_enricher import (
    diagnose_analysis,
    get_priority_subcategories,
    run_enricher,
    run_enricher_for_report_no,
)
from app.analyzer import URLIngredientAnalyzer
from app.analyzer.core import _extract_first_json_object, _extract_openai_text
from app.analyzer.pass1_precheck import run_pass1_precheck
from app.query_image_benchmark import run_query_image_benchmark_interactive
from app.serp_simple_debug import run_serp_simple_debug_interactive
from app.query_pipeline import (
    cache_serp_images,
    finish_query_run,
    get_image_analysis_cache,
    get_provider_max_page_done,
    init_query_pipeline_tables,
    list_next_queries,
    list_recent_runs,
    start_query_run,
    upsert_provider_max_page_done,
    upsert_food_final,
    upsert_image_analysis_cache,
    upsert_query,
)
from app.export_to_backend import (
    fetch_food_final_rows,
    fetch_haccp_parsed_rows,
    send_batches,
    transform_food_final_row,
    transform_haccp_parsed_row,
)
from app.haccp_api import (
    fetch_haccp_page,
    fetch_haccp_products_by_report_no,
    fetch_haccp_total_count,
)
from app.haccp_code_parser import (
    dumps_json,
)
from app.haccp_audit import (
    init_haccp_audit_tables,
    clear_haccp_audit_tables,
    run_haccp_parsed_audit,
    fetch_haccp_audit_summary,
    fetch_haccp_audit_top,
)

W = 68
WEB_UI_PORT = 8501
WEB_UI_URL = f"http://localhost:{WEB_UI_PORT}"
QUERY_WEB_UI_PORT = 8502
QUERY_WEB_UI_URL = f"http://localhost:{QUERY_WEB_UI_PORT}"
ADMIN_WEB_UI_PORT = 8503
ADMIN_WEB_UI_URL = f"http://localhost:{ADMIN_WEB_UI_PORT}"
HACCP_PARSED_WEB_PORT = 8504

_HACCP_PARSED_WEB_SERVER: HTTPServer | None = None
_HACCP_PARSED_WEB_THREAD: threading.Thread | None = None


def _normalize_report_no(value: str | None) -> str | None:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    if len(digits) < 10 or len(digits) > 16:
        return None
    return digits


def _extract_report_no_candidates(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    # 복수 표기를 먼저 큰 구분자로 분리하고, 각 토큰에서 10~16자리 숫자 시퀀스를 추출
    parts = [p for p in re.split(r"[\/,;|\n]+", text) if p and str(p).strip()]
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        part_text = str(part)
        # 하이픈/괄호/F1 라벨 등이 섞여 있어도 숫자 블록 단위로 찾는다.
        for m in re.finditer(r"\d[\d\-\s]{8,24}\d|\d{10,16}", part_text):
            digits = re.sub(r"[^0-9]", "", m.group(0))
            if len(digits) < 10 or len(digits) > 16:
                continue
            if digits in seen:
                continue
            seen.add(digits)
            out.append(digits)
    # 위 정규식으로 못 잡는 경우 대비: 전체 텍스트에서도 한 번 더 탐색
    if not out:
        for m in re.finditer(r"\d{10,16}", text):
            digits = m.group(0)
            if digits in seen:
                continue
            seen.add(digits)
            out.append(digits)
    return out


def _select_best_report_candidate(
    report_candidates: list[str],
    public_food_index: dict[str, dict],
) -> tuple[str | None, dict | None, str]:
    if not report_candidates:
        return (None, None, "no_candidates")
    # 우선순위: has_name+has_nutrition > has_nutrition > any-hit > first-candidate
    best_no = None
    best_row = None
    best_score = -1
    for no in report_candidates:
        row = public_food_index.get(no)
        if not row:
            continue
        score = 0
        if bool(row.get("has_name")) and bool(row.get("has_nutrition")):
            score = 300
        elif bool(row.get("has_nutrition")):
            score = 200
        else:
            score = 100
        if score > best_score:
            best_score = score
            best_no = no
            best_row = row
    if best_no is not None:
        if best_score >= 300:
            return (best_no, best_row, "public_best_name_nutrition")
        if best_score >= 200:
            return (best_no, best_row, "public_best_nutrition")
        return (best_no, best_row, "public_any_hit")
    return (report_candidates[0], None, "first_candidate_no_public_match")


_INGREDIENT_CONFUSION_PAIRS: tuple[tuple[str, str], ...] = (
    ("알룰로스", "과당"),
    ("유청단백", "대두단백"),
    ("에리스리톨", "자일리톨"),
)


def _norm_for_match(text: str | None) -> str:
    v = str(text or "").strip().lower()
    v = re.sub(r"\s+", "", v)
    v = re.sub(r"[^\w가-힣]", "", v)
    return v


def _is_edit_distance_le1(a: str, b: str) -> bool:
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    # same length: at most one substitution
    if la == lb:
        mismatches = 0
        for i in range(la):
            if a[i] != b[i]:
                mismatches += 1
                if mismatches > 1:
                    return False
        return True
    # one insertion/deletion
    if la > lb:
        a, b = b, a
        la, lb = lb, la
    i = j = 0
    used = False
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
            continue
        if used:
            return False
        used = True
        j += 1
    return True


def _validate_pass3_ingredients_verbatim(
    ingredients_text: str | None,
    evidence_text: str | None,
) -> dict[str, Any]:
    pred_raw = str(ingredients_text or "").strip()
    src_raw = str(evidence_text or "").strip()
    pred_norm = _norm_for_match(pred_raw)
    src_norm = _norm_for_match(src_raw)

    if not pred_raw:
        return {
            "status": "fail",
            "code": "pass3_ingredients_missing",
            "message": "원재료명이 비어 있습니다.",
            "pred_raw": pred_raw,
            "source_raw": src_raw,
            "pred_norm": pred_norm,
            "source_norm": src_norm,
        }

    if not src_raw:
        return {
            "status": "warn",
            "code": "pass3_evidence_missing",
            "message": "원문 근거 텍스트가 없어 보수 검증을 축소 적용했습니다.",
            "pred_raw": pred_raw,
            "source_raw": src_raw,
            "pred_norm": pred_norm,
            "source_norm": src_norm,
        }

    # 혼동쌍은 완전일치 원칙(의미 변경 금지)
    for a, b in _INGREDIENT_CONFUSION_PAIRS:
        an = _norm_for_match(a)
        bn = _norm_for_match(b)
        src_has_a = an in src_norm
        src_has_b = bn in src_norm
        pred_has_a = an in pred_norm
        pred_has_b = bn in pred_norm
        if src_has_a and pred_has_b and not src_has_b:
            return {
                "status": "fail",
                "code": "ingredient_substitution_mismatch",
                "message": f"원문 '{a}'를 예측에서 '{b}'로 치환한 것으로 판단되어 폐기합니다.",
                "source_term": a,
                "pred_term": b,
                "pred_raw": pred_raw,
                "source_raw": src_raw,
                "pred_norm": pred_norm,
                "source_norm": src_norm,
            }
        if src_has_b and pred_has_a and not src_has_a:
            return {
                "status": "fail",
                "code": "ingredient_substitution_mismatch",
                "message": f"원문 '{b}'를 예측에서 '{a}'로 치환한 것으로 판단되어 폐기합니다.",
                "source_term": b,
                "pred_term": a,
                "pred_raw": pred_raw,
                "source_raw": src_raw,
                "pred_norm": pred_norm,
                "source_norm": src_norm,
            }

    # 띄어쓰기/줄바꿈 차이는 허용. 완전 포함이면 통과.
    if pred_norm and pred_norm in src_norm:
        return {
            "status": "pass",
            "code": "ok_verbatim_match",
            "message": "원문 근거와 일치합니다(공백/줄바꿈 차이 허용).",
            "pred_raw": pred_raw,
            "source_raw": src_raw,
            "pred_norm": pred_norm,
            "source_norm": src_norm,
        }

    # 근거 미포함이지만 1글자 오탈자 수준이면 경고 통과
    if pred_norm and src_norm and _is_edit_distance_le1(pred_norm, src_norm):
        return {
            "status": "warn",
            "code": "ok_minor_ocr_tolerated",
            "message": "원문과 1글자 내 OCR 오차로 판단되어 통과 처리했습니다.",
            "pred_raw": pred_raw,
            "source_raw": src_raw,
            "pred_norm": pred_norm,
            "source_norm": src_norm,
        }

    return {
        "status": "fail",
        "code": "ingredient_evidence_mismatch",
        "message": "원문 근거와 예측 원재료명이 일치하지 않아 보수적으로 폐기합니다.",
        "pred_raw": pred_raw,
        "source_raw": src_raw,
        "pred_norm": pred_norm,
        "source_norm": src_norm,
    }


def _build_public_food_index(conn: sqlite3.Connection) -> dict[str, dict]:
    cur = conn.execute(
        """
        SELECT itemMnftrRptNo, foodNm, enerc, prot, fatce, chocdf, sugar, nat
        FROM processed_food_info
        WHERE COALESCE(itemMnftrRptNo, '') != ''
        """
    )
    out: dict[str, dict] = {}
    for row in cur.fetchall():
        item_no = row[0]
        norm = _normalize_report_no(item_no)
        if not norm:
            continue
        product_name = (row[1] or "").strip()
        fields = {
            "열량(kcal)": row[2],
            "단백질(g)": row[3],
            "지방(g)": row[4],
            "탄수화물(g)": row[5],
            "당류(g)": row[6],
            "나트륨(mg)": row[7],
        }
        nutrition_payload: dict[str, str] = {}
        filled_cnt = 0
        for k, v in fields.items():
            txt = (str(v).strip() if v is not None else "")
            if txt:
                nutrition_payload[k] = txt
                filled_cnt += 1
        nutrition_text = json.dumps(
            {"source": "public_food_db", "items": nutrition_payload},
            ensure_ascii=False,
        ) if nutrition_payload else None
        candidate = {
            "item_mnftr_rpt_no": str(item_no),
            "product_name": product_name or None,
            "nutrition_text": nutrition_text,
            "has_name": bool(product_name),
            "has_nutrition": bool(nutrition_text),
            "nutrition_fields_count": filled_cnt,
        }
        prev = out.get(norm)
        if prev is None:
            out[norm] = candidate
            continue
        prev_score = int(prev.get("has_name", False)) * 100 + int(prev.get("nutrition_fields_count", 0))
        new_score = int(candidate.get("has_name", False)) * 100 + int(candidate.get("nutrition_fields_count", 0))
        if new_score > prev_score:
            out[norm] = candidate
    return out


def _write_query_execution_html_report(reports: list[dict]) -> Path:
    reports_dir = Path(__file__).resolve().parent.parent / "reports" / "query_runs"
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = reports_dir / f"query_run_{ts}.html"

    total_queries = len(reports)
    total_images = sum(int(r.get("total_images") or 0) for r in reports)
    total_analyzed = sum(int(r.get("analyzed_images") or 0) for r in reports)
    total_saved = sum(int(r.get("final_saved_count") or 0) for r in reports)
    total_api_calls = sum(int(r.get("api_calls") or 0) for r in reports)

    def _yn(v: bool | None) -> str:
        if v is True:
            return "✅"
        if v is False:
            return "❌"
        return "-"

    def _pass_level(row: dict) -> int:
        if bool(row.get("pass4_ok")):
            return 4
        if bool(row.get("pass3_ok")):
            return 3
        if bool(row.get("p2a_ok")) and bool(row.get("p2b_ok")):
            return 2
        if bool(row.get("pass1_ok")):
            return 1
        return 0

    def _extract_raw_section(raw_text: str, start_marker: str, end_markers: list[str]) -> str:
        text = str(raw_text or "")
        if not text:
            return ""
        start_idx = text.find(start_marker)
        if start_idx < 0:
            return ""
        body_start = start_idx + len(start_marker)
        body_end = len(text)
        for marker in end_markers:
            idx = text.find(marker, body_start)
            if idx >= 0 and idx < body_end:
                body_end = idx
        return text[body_start:body_end].strip()

    def _try_parse_ingredients_items(raw_text: str) -> list[dict]:
        text = str(raw_text or "").strip()
        if not text:
            return []
        parsed: Any | None = None
        try:
            parsed = json.loads(text)
        except Exception:
            # 모델 응답에 코드펜스가 섞여 들어온 경우를 완화한다.
            fenced = text
            if fenced.startswith("```"):
                fenced = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", fenced).strip()
                fenced = re.sub(r"\n?```$", "", fenced).strip()
                try:
                    parsed = json.loads(fenced)
                except Exception:
                    parsed = None
        if isinstance(parsed, dict):
            items = parsed.get("ingredients_items")
            if isinstance(items, list):
                return [x for x in items if isinstance(x, dict)]
            return []
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict)]
        return []

    def _render_ingredient_tree(items: list[dict]) -> str:
        def _node_html(node: dict, depth: int) -> str:
            name = str(node.get("ingredient_name") or node.get("name") or "").strip() or "(이름없음)"
            amount = str(node.get("amount_ratio") or node.get("amount") or "").strip()
            origin = str(node.get("origin") or "").strip()
            origin_detail = str(node.get("origin_detail") or "").strip()
            metas: list[str] = []
            metas.append(f"<span class='ingMeta level'>Lv{depth + 1}</span>")
            if amount:
                metas.append(f"<span class='ingMeta'>함량 {html.escape(amount)}</span>")
            if origin:
                metas.append(f"<span class='ingMeta'>원산지 {html.escape(origin)}</span>")
            if origin_detail:
                metas.append(f"<span class='ingMeta'>상세 {html.escape(origin_detail)}</span>")
            children = node.get("sub_ingredients")
            children_html = "<div class='ingChildren none'>하위 원재료 없음</div>"
            if isinstance(children, list) and children:
                child_nodes = "".join(_node_html(ch, depth + 1) for ch in children if isinstance(ch, dict))
                if child_nodes:
                    children_html = f"<div class='ingChildren'>{child_nodes}</div>"
                    return (
                        f"<details class='ingNode depth-{depth}' open>"
                        f"<summary class='ingLine'><span class='ingName'>{html.escape(name)}</span>{''.join(metas)}</summary>"
                        f"{children_html}"
                        f"</details>"
                    )
            return f"<div class='ingLeaf depth-{depth}'><div class='ingLine'><span class='ingName'>{html.escape(name)}</span>{''.join(metas)}</div></div>"

        if not items:
            return "<span class='small'>파싱된 원재료 구조가 없습니다.</span>"
        cards = []
        for i, it in enumerate((x for x in items if isinstance(x, dict)), 1):
            cards.append(
                "<div class='ingRootCard'>"
                f"<div class='ingRootIdx'>#{i}</div>"
                f"{_node_html(it, 0)}"
                "</div>"
            )
        return f"<div class='ingSummary'>구조화 항목 {len(cards)}개</div><div class='ingTree'>{''.join(cards)}</div>"

    def _format_ingredients_rich_html(raw_text: str | None) -> str:
        text = str(raw_text or "").strip()
        if not text:
            return "<span class='small'>원재료명 없음</span>"
        items = _try_parse_ingredients_items(text)
        if items:
            return _render_ingredient_tree(items)
        parts = [p.strip() for p in re.split(r"[,;\n]+", text) if p and p.strip()]
        if len(parts) >= 2:
            cards = []
            for i, p in enumerate(parts[:80], 1):
                cards.append(
                    "<div class='ingRootCard'>"
                    f"<div class='ingRootIdx'>#{i}</div>"
                    f"<div class='ingLeaf depth-0'><div class='ingLine'><span class='ingName'>{html.escape(p)}</span>"
                    "<span class='ingMeta warn'>텍스트 분해</span></div></div>"
                    "</div>"
                )
            more = f"<div class='small'>+{len(parts)-80}개 더 있음</div>" if len(parts) > 80 else ""
            return f"<div class='ingSummary'>텍스트 분해 {len(parts)}개</div><div class='ingTree'>{''.join(cards)}</div>{more}"
        return f"<code>{html.escape(text[:1200])}</code>"

    all_rows: list[dict] = []
    for q in reports:
        for r in list(q.get("images") or []):
            rr = dict(r)
            rr["query_text"] = q.get("query_text")
            rr["provider"] = q.get("provider")
            rr["run_id"] = q.get("run_id")
            rr["query_status"] = q.get("status")
            rr["pass_level"] = _pass_level(rr)
            all_rows.append(rr)

    by_level = {
        0: sum(1 for r in all_rows if int(r.get("pass_level") or 0) == 0),
        1: sum(1 for r in all_rows if int(r.get("pass_level") or 0) >= 1),
        2: sum(1 for r in all_rows if int(r.get("pass_level") or 0) >= 2),
        3: sum(1 for r in all_rows if int(r.get("pass_level") or 0) >= 3),
        4: sum(1 for r in all_rows if int(r.get("pass_level") or 0) >= 4),
    }

    html_parts: list[str] = []
    html_parts.append("<!doctype html><html lang='ko'><head><meta charset='utf-8'>")
    html_parts.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")
    html_parts.append("<title>파이프라인 실행 결과 리포트</title>")
    html_parts.append(
        "<style>"
        "body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f6f7fb;color:#111;margin:24px;}"
        ".wrap{max-width:1600px;margin:0 auto;}"
        ".top,.flt,.q{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:14px;margin-bottom:12px;}"
        ".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;}"
        ".kpi{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:10px;}"
        ".kpi .v{font-size:24px;font-weight:800;}"
        ".meta{font-size:13px;color:#555;margin:2px 0;}"
        ".ok{color:#0a7f2e;font-weight:700;}.bad{color:#c21f39;font-weight:700;}.skip{color:#6b7280;font-weight:700;}"
        ".badge{display:inline-block;padding:3px 8px;border-radius:999px;background:#eef2ff;border:1px solid #dbe4ff;font-size:11px;margin-right:6px;}"
        ".ctrl{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}"
        ".ctrl input,.ctrl select{height:34px;border:1px solid #d1d5db;border-radius:8px;padding:0 10px;font-size:13px;}"
        "table{width:100%;border-collapse:collapse;margin-top:10px;font-size:12px;background:#fff;}"
        "th,td{border:1px solid #e5e7eb;padding:8px;vertical-align:top;text-align:left;}"
        "th{background:#f3f4f6;position:sticky;top:0;z-index:1;}"
        ".img{width:88px;height:88px;object-fit:cover;border:1px solid #e5e7eb;border-radius:8px;background:#fafafa;padding:2px;}"
        "code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;word-break:break-word;font-size:11px;}"
        "details{margin-top:4px;}"
        ".raw{max-height:220px;overflow:auto;background:#fafafa;border:1px solid #eee;padding:6px;border-radius:6px;}"
        ".small{font-size:12px;color:#666;}"
        ".ingRaw{max-height:140px;overflow:auto;background:#fbfcfe;border:1px solid #e5e7eb;padding:6px;border-radius:6px;}"
        ".ingPane{min-width:340px;max-width:660px;}"
        ".ingSummary{font-size:11px;color:#475569;margin:4px 0 6px 0;font-weight:700;}"
        ".ingTree{display:grid;gap:8px;}"
        ".ingRootCard{background:linear-gradient(180deg,#fbfdff 0%,#f8fbff 100%);border:1px solid #dbe7f7;border-radius:10px;padding:8px;}"
        ".ingRootIdx{display:inline-block;font-size:11px;color:#0f4a8a;background:#e5efff;border:1px solid #cfe0ff;border-radius:999px;padding:1px 8px;margin-bottom:6px;font-weight:700;}"
        ".ingNode,.ingLeaf{border-left:2px solid #dbe4ef;padding-left:8px;margin-left:2px;}"
        ".ingNode + .ingNode,.ingLeaf + .ingNode,.ingNode + .ingLeaf,.ingLeaf + .ingLeaf{margin-top:6px;}"
        ".ingNode summary{cursor:pointer;list-style:none;}"
        ".ingNode summary::-webkit-details-marker{display:none;}"
        ".ingNode summary::before{content:'▾';display:inline-block;margin-right:6px;color:#2563eb;font-weight:700;}"
        ".ingNode:not([open]) summary::before{content:'▸';}"
        ".ingChildren{margin:6px 0 0 8px;display:grid;gap:6px;}"
        ".ingChildren.none{font-size:11px;color:#94a3b8;font-style:italic;}"
        ".ingLine{display:flex;gap:6px;flex-wrap:wrap;align-items:center;line-height:1.4;}"
        ".ingName{font-weight:800;color:#0f172a;font-size:12px;}"
        ".ingMeta{font-size:10px;background:#eef2ff;border:1px solid #dbe4ff;color:#1f3a8a;border-radius:999px;padding:1px 7px;font-weight:700;}"
        ".ingMeta.level{background:#e8fff4;border-color:#bef3d2;color:#0f766e;}"
        ".ingMeta.warn{background:#fff7ed;border-color:#fed7aa;color:#9a3412;}"
        ".urlCell{display:flex;align-items:center;gap:8px;white-space:nowrap;}"
        ".copyBtn{height:28px;padding:0 10px;border:1px solid #cfd6e3;border-radius:8px;background:#fff;cursor:pointer;font-size:12px;}"
        ".copyBtn:hover{background:#f3f6fb;}"
        "</style>"
    )
    html_parts.append("</head><body><div class='wrap'>")
    html_parts.append("<div class='top'>")
    html_parts.append("<h2 style='margin:0 0 8px 0;'>파이프라인 실행 결과 리포트</h2>")
    html_parts.append(f"<div class='meta'>생성시각: {html.escape(time.strftime('%Y-%m-%d %H:%M:%S'))}</div>")
    html_parts.append("</div>")
    html_parts.append("<div class='grid'>")
    html_parts.append(f"<div class='kpi'><div>검색어</div><div class='v'>{total_queries:,}</div></div>")
    html_parts.append(f"<div class='kpi'><div>수집 이미지</div><div class='v'>{total_images:,}</div></div>")
    html_parts.append(f"<div class='kpi'><div>분석 시도</div><div class='v'>{total_analyzed:,}</div></div>")
    html_parts.append(f"<div class='kpi'><div>최종 저장</div><div class='v'>{total_saved:,}</div></div>")
    html_parts.append(f"<div class='kpi'><div>API 호출</div><div class='v'>{total_api_calls:,}</div></div>")
    html_parts.append("</div>")

    html_parts.append("<div class='flt'>")
    html_parts.append("<div class='ctrl'>")
    html_parts.append("<label>검색: <input id='qFilter' type='text' placeholder='query/url/사유/번호' /></label>")
    html_parts.append(
        "<label>Pass 필터: <select id='passFilter'>"
        "<option value='all'>전체</option>"
        "<option value='processed'>이번 실행 검수만</option>"
        "<option value='2a'>Pass2A 통과</option>"
        "<option value='2b'>Pass2B 통과</option>"
        "<option value='3ing'>Pass3-ING 통과</option>"
        "<option value='3nut'>Pass3-NUT 통과</option>"
        "<option value='4ing'>Pass4-ING 통과</option>"
        "<option value='4nut'>Pass4-NUT 통과</option>"
        "<option value='fail'>실패만</option>"
        "<option value='saved'>저장만</option>"
        "</select></label>"
    )
    html_parts.append("<label>Raw 보기: <select id='rawFilter'>"
                      "<option value='all'>전체</option>"
                      "<option value='none'>숨김</option>"
                      "<option value='2a'>Pass2A</option>"
                      "<option value='2b'>Pass2B</option>"
                      "<option value='3ing'>Pass3-ING</option>"
                      "<option value='3nut'>Pass3-NUT</option>"
                      "<option value='4ing'>Pass4-ING</option>"
                      "<option value='4nut'>Pass4-NUT</option>"
                      "</select></label>")
    html_parts.append("</div>")
    html_parts.append(
        f"<div class='small' style='margin-top:8px;'>"
        f"Pass1+ {by_level[1]:,} | Pass2+ {by_level[2]:,} | Pass3+ {by_level[3]:,} | Pass4 {by_level[4]:,}"
        "</div>"
    )
    html_parts.append("</div>")

    for q in reports:
        qtext = str(q.get("query_text") or "")
        qstatus = str(q.get("status") or "")
        qreason = str(q.get("reason") or "")
        qclass = "ok" if qstatus == "done" else ("skip" if qstatus.startswith("skipped") else "bad")
        html_parts.append("<div class='q'>")
        html_parts.append(f"<h3 style='margin:0 0 6px 0;'>{html.escape(qtext)}</h3>")
        html_parts.append(
            f"<div class='meta'><span class='badge'>provider: {html.escape(str(q.get('provider') or ''))}</span>"
            f"<span class='badge'>run_id: {html.escape(str(q.get('run_id') or '-'))}</span>"
            f"<span class='badge'>status: <span class='{qclass}'>{html.escape(qstatus)}</span></span>"
            f"{' | reason: ' + html.escape(qreason) if qreason else ''}</div>"
        )
        html_parts.append(
            "<div class='meta'>"
            f"total={int(q.get('total_images') or 0)}, analyzed={int(q.get('analyzed_images') or 0)}, "
            f"saved={int(q.get('final_saved_count') or 0)}, api_calls={int(q.get('api_calls') or 0)}"
            "</div>"
        )

        rows = list(q.get("images") or [])
        if not rows:
            html_parts.append("<div class='small' style='margin-top:8px;'>이미지 항목 없음</div></div>")
            continue

        html_parts.append("<table><thead><tr>"
                          "<th>No</th><th>이미지</th><th>해상도/토큰</th><th>결과</th><th>Pass</th><th>사유</th>"
                          "<th>제품명</th><th>품목보고번호</th><th>원재료 RAW+파싱</th><th>출처</th><th>Raw</th>"
                          "</tr></thead><tbody>")
        for r in rows:
            data_source_path = str(r.get("data_source_path") or "-")
            nutrition_source = str(r.get("nutrition_data_source") or "-")
            product_source = "공공DB" if data_source_path == "public_food_enriched" else ("이미지" if data_source_path == "image_full_extraction" else "-")
            nutrition_source_label = "공공DB" if nutrition_source == "public_food_db" else ("이미지" if nutrition_source == "image_pass4" else nutrition_source)
            pass_ok = (
                f"P1:{_yn(r.get('pass1_ok'))} "
                f"P2A:{_yn(r.get('p2a_ok'))} "
                f"P2B:{_yn(r.get('p2b_ok'))} "
                f"P3:{_yn(r.get('pass3_ok'))} "
                f"P4:{_yn(r.get('pass4_ok'))}"
            )
            url = str(r.get("url") or "")
            product_name = str(r.get("product_name") or "")
            report_no = str(r.get("report_no") or "")
            fail_reason = str(r.get("fail_reason") or "")
            status = str(r.get("status") or "")
            level = _pass_level(r)
            raw2a = str(r.get("raw_pass2a") or "")
            raw2b = str(r.get("raw_pass2b") or "")
            raw3 = str(r.get("raw_pass3") or "")
            raw4 = str(r.get("raw_pass4") or "")
            ingredients_raw = str(r.get("ingredients_text_raw") or r.get("ingredients_text") or "")
            ingredients_corrected = str(r.get("ingredients_text_corrected") or r.get("ingredients_text") or "")
            boundary_corrected = bool(r.get("pass3_boundary_corrected"))
            boundary_confidence = r.get("pass3_boundary_confidence")
            boundary_issues = str(r.get("pass3_boundary_issue_codes_json") or "")
            ingredients_rich_html = _format_ingredients_rich_html(ingredients_corrected or ingredients_raw)
            raw3_ing = _extract_raw_section(raw3, "[PASS3-INGREDIENTS]\n", ["[PASS3-NUTRITION]\n"])
            raw3_nut = _extract_raw_section(raw3, "[PASS3-NUTRITION]\n", [])
            if (not raw3_ing) and raw3 and ("[PASS3-" not in raw3):
                raw3_ing = raw3
            raw4_ing = _extract_raw_section(raw4, "[PASS4-INGREDIENTS]\n", ["[PASS4-NUTRITION]\n"])
            raw4_nut = _extract_raw_section(raw4, "[PASS4-NUTRITION]\n", [])
            if (not raw4_ing) and raw4 and ("[PASS4-" not in raw4):
                raw4_ing = raw4
            p2a_ok = bool(r.get("p2a_ok"))
            p2b_ok = bool(r.get("p2b_ok"))
            p3_ing_ok = bool(r.get("pass3_ok"))
            p3_nut_ok = bool(r.get("pass3_nut_attempted")) and bool((r.get("nutrition_text") or "").strip())
            p4_ing_ok = bool(r.get("pass4_ok"))
            p4_nut_ok = bool(r.get("pass4_nut_attempted")) and bool((r.get("nutrition_text") or "").strip())
            ow = r.get("orig_w")
            oh = r.get("orig_h")
            otok = r.get("orig_est_tokens")
            p2w = r.get("pass2_w")
            p2h = r.get("pass2_h")
            p2tok = r.get("pass2_est_tokens")
            p2r = bool(r.get("pass2_resized"))
            p3w = r.get("pass3_w")
            p3h = r.get("pass3_h")
            p3tok = r.get("pass3_est_tokens")
            p3r = bool(r.get("pass3_resized"))
            resize_code = str(r.get("resize_policy_code") or "-")
            resize_reason = str(r.get("resize_policy_reason") or "-")
            otok_txt = f"{int(otok):,}" if otok is not None else "-"
            p2tok_txt = f"{int(p2tok):,}" if p2tok is not None else "-"
            p3tok_txt = f"{int(p3tok):,}" if p3tok is not None else "-"
            size_tok_txt = (
                f"원본: <b>{ow or '-'}x{oh or '-'}</b> / tok~<b>{otok_txt}</b><br>"
                f"P2: <b>{p2w or '-'}x{p2h or '-'}</b> / tok~<b>{p2tok_txt}</b> / resized={'Y' if p2r else 'N'}<br>"
                f"P3: <b>{p3w or '-'}x{p3h or '-'}</b> / tok~<b>{p3tok_txt}</b> / resized={'Y' if p3r else 'N'}<br>"
                f"<span class='small'>policy: {html.escape(resize_code)} | {html.escape(resize_reason[:120])}</span>"
            )
            html_parts.append(
                f"<tr class='rowItem' data-pass='{level}' data-status='{html.escape(status)}'"
                f" data-p2a='{1 if p2a_ok else 0}' data-p2b='{1 if p2b_ok else 0}'"
                f" data-p3ing='{1 if p3_ing_ok else 0}' data-p3nut='{1 if p3_nut_ok else 0}'"
                f" data-p4ing='{1 if p4_ing_ok else 0}' data-p4nut='{1 if p4_nut_ok else 0}'>"
                f"<td>{int(r.get('idx') or 0)}</td>"
                f"<td><img class='img' src='{html.escape(url)}' loading='lazy' referrerpolicy='no-referrer'>"
                f"<div class='urlCell'>"
                f"<button class='copyBtn' data-url='{html.escape(url, quote=True)}'>URL 복사</button>"
                f"<span class='small'>{html.escape((url.split('/')[2] if '://' in url else url)[:28])}</span>"
                f"</div></td>"
                f"<td>{size_tok_txt}</td>"
                f"<td>{html.escape(status)}</td>"
                f"<td>{html.escape(pass_ok)}</td>"
                f"<td><code>{html.escape(fail_reason[:300] if fail_reason else '-')}</code></td>"
                f"<td>{html.escape(product_name or '-')}</td>"
                f"<td>{html.escape(report_no or '-')}</td>"
                f"<td class='ingPane'>"
                f"<details><summary>원재료 RAW {'(있음)' if ingredients_raw else '(없음)'}</summary><div class='ingRaw'><code>{html.escape(ingredients_raw or '-')}</code></div></details>"
                f"<details><summary>원재료 Corrected {'(있음)' if ingredients_corrected else '(없음)'}</summary><div class='ingRaw'><code>{html.escape(ingredients_corrected or '-')}</code></div></details>"
                f"<div class='small'>Pass3 경계교정: {'Y' if boundary_corrected else 'N'} | conf: {html.escape(str(boundary_confidence if boundary_confidence is not None else '-'))}</div>"
                f"<div class='small'>issue: {html.escape(boundary_issues[:200] if boundary_issues else '-')}</div>"
                f"{ingredients_rich_html}"
                f"</td>"
                f"<td>제품명: <b>{html.escape(product_source)}</b><br>영양성분: <b>{html.escape(str(nutrition_source_label or '-'))}</b></td>"
                "<td>"
                f"<details class='rawBlock raw2a'><summary>Pass2A raw {'(있음)' if raw2a else '(없음)'}</summary><div class='raw'><code>{html.escape(raw2a or '-')}</code></div></details>"
                f"<details class='rawBlock raw2b'><summary>Pass2B raw {'(있음)' if raw2b else '(없음)'}</summary><div class='raw'><code>{html.escape(raw2b or '-')}</code></div></details>"
                f"<details class='rawBlock raw3ing'><summary>Pass3-ING raw {'(있음)' if raw3_ing else '(없음)'}</summary><div class='raw'><code>{html.escape(raw3_ing or '-')}</code></div></details>"
                f"<details class='rawBlock raw3nut'><summary>Pass3-NUT raw {'(있음)' if raw3_nut else '(없음)'}</summary><div class='raw'><code>{html.escape(raw3_nut or '-')}</code></div></details>"
                f"<details class='rawBlock raw4ing'><summary>Pass4-ING raw {'(있음)' if raw4_ing else '(없음)'}</summary><div class='raw'><code>{html.escape(raw4_ing or '-')}</code></div></details>"
                f"<details class='rawBlock raw4nut'><summary>Pass4-NUT raw {'(있음)' if raw4_nut else '(없음)'}</summary><div class='raw'><code>{html.escape(raw4_nut or '-')}</code></div></details>"
                "</td>"
                "</tr>"
            )
        html_parts.append("</tbody></table></div>")

    html_parts.append(
        """
<script>
(() => {
  const qInput = document.getElementById('qFilter');
  const passSel = document.getElementById('passFilter');
  const rawSel = document.getElementById('rawFilter');
  const rows = Array.from(document.querySelectorAll('tr.rowItem'));

  function passMatch(row, passVal) {
    const level = parseInt(row.dataset.pass || "0", 10);
    const status = (row.dataset.status || "").toLowerCase();
    const p2a = row.dataset.p2a === '1';
    const p2b = row.dataset.p2b === '1';
    const p3ing = row.dataset.p3ing === '1';
    const p3nut = row.dataset.p3nut === '1';
    const p4ing = row.dataset.p4ing === '1';
    const p4nut = row.dataset.p4nut === '1';
    if (passVal === 'all') return true;
    if (passVal === 'processed') return status !== 'skipped_by_existing_history';
    if (passVal === '2a') return p2a;
    if (passVal === '2b') return p2b;
    if (passVal === '3ing') return p3ing;
    if (passVal === '3nut') return p3nut;
    if (passVal === '4ing') return p4ing;
    if (passVal === '4nut') return p4nut;
    if (passVal === 'fail') return status !== 'saved';
    if (passVal === 'saved') return status === 'saved';
    const need = parseInt(passVal, 10);
    if (Number.isNaN(need)) return true;
    return level >= need;
  }

  function textMatch(row, q) {
    if (!q) return true;
    const txt = (row.textContent || "").toLowerCase();
    return txt.includes(q);
  }

  function rawToggle(mode) {
    const all = Array.from(document.querySelectorAll('.rawBlock'));
    if (mode === 'all') {
      all.forEach(el => { el.style.display = 'block'; });
      return;
    }
    all.forEach(el => { el.style.display = 'none'; });
    if (mode === 'none') return;
    const show = Array.from(document.querySelectorAll('.raw' + mode));
    show.forEach(el => { el.style.display = 'block'; });
  }

  function apply() {
    const q = (qInput.value || '').toLowerCase().trim();
    const passVal = passSel.value;
    rows.forEach(row => {
      const ok = passMatch(row, passVal) && textMatch(row, q);
      row.style.display = ok ? '' : 'none';
    });
    rawToggle(rawSel.value);
  }

  qInput.addEventListener('input', apply);
  passSel.addEventListener('change', apply);
  rawSel.addEventListener('change', apply);
  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('.copyBtn');
    if (!btn) return;
    const url = btn.getAttribute('data-url') || '';
    try {
      await navigator.clipboard.writeText(url);
      const prev = btn.textContent;
      btn.textContent = '복사됨';
      setTimeout(() => { btn.textContent = prev || 'URL 복사'; }, 900);
    } catch (_) {
      const ta = document.createElement('textarea');
      ta.value = url;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
  });
  apply();
})();
</script>
"""
    )

    html_parts.append("</div></body></html>")
    out_path.write_text("\n".join(html_parts), encoding="utf-8")
    return out_path


def _bar(char: str = "─") -> str:
    return "  " + char * (W - 4)


def _display_width(text: str) -> int:
    return sum(2 if ord(c) > 127 else 1 for c in text or "")


def _trunc_display(text: str, max_w: int) -> str:
    result = []
    width = 0
    for c in text or "":
        cw = 2 if ord(c) > 127 else 1
        if width + cw > max_w:
            break
        result.append(c)
        width += cw
    return "".join(result)


def _fixed_display(text: str, max_w: int) -> str:
    t = _trunc_display(text, max_w)
    return t + " " * (max_w - _display_width(t))


def print_header() -> None:
    title = "🍽️ 식품 데이터 통합 실행기"
    inner = W - 2
    pad_left = (inner - len(title)) // 2
    pad_right = inner - pad_left - len(title)
    print()
    print("╔" + "═" * inner + "╗")
    print("║" + " " * pad_left + title + " " * pad_right + "║")
    print("╚" + "═" * inner + "╝")
    print()


def run_data_viewer() -> None:
    print("\n  👀 [실행] 데이터 조회 뷰어를 시작합니다.\n")
    viewer.main()


def _is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _run_streamlit_app(*, app_path: str, port: int, url: str, log_name: str) -> None:
    if _is_port_open(port):
        print(f"\n  🌐 웹 UI가 이미 실행 중입니다. 브라우저를 엽니다: {url}")
        webbrowser.open_new_tab(url)
        return

    project_root = Path(__file__).resolve().parent.parent
    log_path = project_root / log_name
    env = os.environ.copy()
    env.setdefault("UV_CACHE_DIR", "/tmp/uv-cache")

    cmd = [
        "uv",
        "run",
        "streamlit",
        "run",
        app_path,
        "--server.port",
        str(port),
        "--server.headless",
        "true",
    ]

    print("\n  🚀 웹 UI 서버를 시작합니다...")
    print(f"  - URL: {url}")
    print(f"  - 로그: {log_path}")

    try:
        with open(log_path, "a", encoding="utf-8") as logf:
            subprocess.Popen(  # noqa: S603
                cmd,
                cwd=str(project_root),
                env=env,
                stdout=logf,
                stderr=logf,
                start_new_session=True,
            )
    except FileNotFoundError:
        print("  ❌ uv 명령을 찾지 못했습니다. `uv` 설치 상태를 확인해주세요.")
        return
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  ❌ 웹 UI 실행 실패: {exc}")
        return

    for _ in range(20):
        if _is_port_open(port):
            print("  ✅ 웹 UI 준비 완료. 브라우저를 엽니다.")
            webbrowser.open_new_tab(url)
            return
        time.sleep(0.5)

    print("  ⚠️ 서버 시작이 지연되고 있습니다. 수동으로 URL을 열어주세요.")
    print(f"  👉 {url}")
    print(f"  💡 문제 확인: {log_path}")


def run_web_monitor() -> None:
    _run_streamlit_app(
        app_path="app/web_ui.py",
        port=WEB_UI_PORT,
        url=WEB_UI_URL,
        log_name="streamlit_web_ui.log",
    )


def run_query_web_monitor() -> None:
    _run_streamlit_app(
        app_path="app/web_query_pipeline_ui.py",
        port=QUERY_WEB_UI_PORT,
        url=QUERY_WEB_UI_URL,
        log_name="streamlit_query_web_ui.log",
    )

def run_admin_web() -> None:
    _run_streamlit_app(
        app_path="app/web_admin.py",
        port=ADMIN_WEB_UI_PORT,
        url=ADMIN_WEB_UI_URL,
        log_name="streamlit_admin_web_ui.log",
    )


def run_image_analyzer_test() -> None:
    print("\n  🧪 [이미지 URL analyze 테스트]")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        print("  ❌ OPENAI_API_KEY 환경변수가 필요합니다.")
        return

    print("  🔹 입력 방법:")
    print("    - 일반 URL 직접 입력")
    print("    - data URL은 길어서 `paste` 모드 권장")
    print("    - 파일에서 읽기: @/path/to/data_url.txt")
    raw_input = input("  🔹 이미지 입력(URL / paste / @파일): ").strip()
    image_url = raw_input
    if raw_input.lower() == "paste":
        print("  📋 data URL을 붙여넣고 마지막 줄에 END 입력:")
        lines: list[str] = []
        while True:
            line = input()
            if line.strip() == "END":
                break
            lines.append(line.strip())
        image_url = "".join(lines).strip()
    elif raw_input.startswith("@"):
        p = Path(raw_input[1:]).expanduser()
        if not p.exists():
            print(f"  ❌ 파일을 찾지 못했습니다: {p}")
            return
        image_url = p.read_text(encoding="utf-8").strip()

    # data URL은 복붙 시 공백/줄바꿈이 섞일 수 있어 제거
    if image_url.startswith("data:image/"):
        image_url = re.sub(r"\s+", "", image_url)

    if not image_url:
        print("  ⚠️ URL을 입력해주세요.")
        return

    target_no = input("  🔹 타깃 품목보고번호(선택, Enter 생략): ").strip()
    target_no = target_no or None

    analyzer = URLIngredientAnalyzer(api_key=openai_api_key)
    print("\n  🔍 분석 중...")
    try:
        result = analyzer.analyze(image_url=image_url, target_item_rpt_no=target_no)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  ❌ 분석 실패: {exc}")
        return

    print("\n  ✅ [analyze 결과]")
    print(f"  - itemMnftrRptNo : {result.get('itemMnftrRptNo') or '없음'}")
    print(f"  - is_flat        : {result.get('is_flat')}")
    print(f"  - is_table_format: {result.get('is_table_format')}")
    print(f"  - has_ingredients: {result.get('has_ingredients')}")
    print(f"  - has_rect_box   : {result.get('has_rect_ingredient_box')}")
    print(f"  - has_report_lbl : {result.get('has_report_label')}")
    print(f"  - product_name   : {result.get('product_name_in_image') or '없음'}")
    print(f"  - brand          : {result.get('brand') or '없음'}")
    print(f"  - manufacturer   : {result.get('manufacturer') or '없음'}")
    print(f"  - note           : {result.get('note') or '없음'}")

    ingredients = (result.get("ingredients_text") or "").strip()
    if ingredients:
        preview = ingredients if len(ingredients) <= 240 else ingredients[:240] + "..."
        print(f"  - ingredients    : {preview}")
    else:
        print("  - ingredients    : 없음")

    if target_no:
        status, reason = diagnose_analysis(result, target_no)
        print("\n  📌 [타깃 기준 진단]")
        print(f"  - target         : {target_no}")
        print(f"  - status         : {status}")
        print(f"  - reason         : {reason}")

    print("\n  🧾 [원본 JSON]")
    print(json.dumps(result, ensure_ascii=False, indent=2))


def _latest_benchmark_summary_path() -> Path | None:
    root = Path(__file__).resolve().parent.parent / "validation_reports"
    if not root.exists():
        return None
    candidates = [p / "summary.json" for p in root.glob("benchmark_*") if (p / "summary.json").exists()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def run_benchmark_menu() -> None:
    while True:
        print("\n  📊 [벤치마크]")
        print("    [1] 검색어 기반 이미지 벤치마크 (SERP + Pass 분석)")
        print("    [2] SERP 테스트/디버그 통합 (브라우저 리포트)")
        print("    [b] 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            try:
                run_query_image_benchmark_interactive()
            except Exception as exc:  # pylint: disable=broad-except
                print(f"  ❌ 실행 실패: {exc}")
        elif sub == "2":
            try:
                run_serp_simple_debug_interactive()
            except Exception as exc:  # pylint: disable=broad-except
                print(f"  ❌ 실행 실패: {exc}")

        elif sub == "b":
            return
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_ingredient_menu() -> None:
    if not os.getenv("SERPAPI_KEY"):
        print("\n  ❌ 오류: SERPAPI_KEY 환경변수가 필요합니다.")
        print('  💡 예) export SERPAPI_KEY="YOUR_KEY"')
        return

    print("\n  🧪 [원재료명 추출 방식 선택]")
    print("    [1] 우선순위 중분류에서 선택")
    print("    [2] 품목보고번호 직접 입력 (1건)")
    print("    [b] 취소")
    mode = input("  👉 선택 : ").strip().lower()

    if mode == "b":
        print("  ↩️ 원재료 추출을 취소했습니다.")
        return

    if mode == "2":
        report_no = input("  🔹 품목보고번호 입력: ").strip()
        if not report_no:
            print("  ⚠️ 품목보고번호를 입력해주세요.")
            return
        raw_quiet = input("  🔹 이미지별 상세 로그 생략? [y/N]: ").strip().lower()
        quiet = raw_quiet == "y"
        print("\n  🚀 [실행] 지정한 품목보고번호 1건 분석을 시작합니다.\n")
        run_enricher_for_report_no(report_no=report_no, quiet=quiet)
        return

    if mode != "1":
        print("  ⚠️ 올바른 번호를 입력해주세요.")
        return

    print("\n  🧪 [원재료명 추출 대상 선택: 중분류]")
    with sqlite3.connect(DB_FILE) as conn:
        categories = get_priority_subcategories(conn)

    if not categories:
        print("  ⚠️ 대상 카테고리를 찾지 못했습니다.")
        return

    col_no = 4
    col_pr = 4
    col_cat = 38
    col_total = 8
    col_attempt = 8
    col_success = 8
    col_rate = 7

    header = (
        f"  {_fixed_display('No', col_no)}  "
        f"{_fixed_display('우선', col_pr)}  "
        f"{_fixed_display('대분류 > 중분류', col_cat)}  "
        f"{_fixed_display('총상품', col_total)}  "
        f"{_fixed_display('시도완료', col_attempt)}  "
        f"{_fixed_display('성공수집', col_success)}  "
        f"{_fixed_display('수집률', col_rate)}"
    )
    print(_bar())
    print(header)
    print(_bar())
    for idx, row in enumerate(categories, 1):
        label = f"{row['lv3']} > {row['lv4']}"
        label = _trunc_display(label, col_cat)
        total_txt = f"{row['total_count']:,}"
        attempted_txt = f"{row['attempted_count']:,}"
        success_txt = f"{row['success_count']:,}"
        rate_txt = f"{row['success_rate']:.1f}%"
        line = (
            f"  {_fixed_display(str(idx), col_no)}  "
            f"{_fixed_display(str(row['priority']), col_pr)}  "
            f"{_fixed_display(label, col_cat)}  "
            f"{_fixed_display(total_txt, col_total)}  "
            f"{_fixed_display(attempted_txt, col_attempt)}  "
            f"{_fixed_display(success_txt, col_success)}  "
            f"{_fixed_display(rate_txt, col_rate)}"
        )
        print(line)
    print(_bar())

    raw_pick = input("  👉 실행할 번호 선택 (b: 취소): ").strip().lower()
    if raw_pick == "b":
        print("  ↩️ 원재료 추출을 취소했습니다.")
        return
    if not raw_pick.isdigit():
        print("  ⚠️ 숫자로 입력해주세요.")
        return

    pick = int(raw_pick)
    if pick < 1 or pick > len(categories):
        print("  ⚠️ 범위를 벗어난 번호입니다.")
        return

    selected = categories[pick - 1]
    print("\n  ⚙️ [실행 옵션]")
    raw_limit = input("  🔹 처리 수 입력 (0 또는 '전체' = 전체, 숫자 = 일부) [기본 20]: ").strip()
    raw_quiet = input("  🔹 이미지별 상세 로그 생략? [y/N]: ").strip().lower()

    limit = 20
    if raw_limit:
        normalized = raw_limit.strip().lower()
        if normalized in ("전체", "all"):
            limit = 0
        else:
            try:
                limit = int(raw_limit)
                if limit < 0:
                    print("  ⚠️ 음수는 사용할 수 없습니다. 기본값 20으로 진행합니다.")
                    limit = 20
            except ValueError:
                print("  ⚠️ 잘못된 limit 입력입니다. 기본값 20으로 진행합니다.")

    quiet = raw_quiet == "y"

    print("\n  🚀 [실행] 선택한 중분류의 원재료 수집을 시작합니다.")
    print(f"  🎯 대상: {selected['lv3']} > {selected['lv4']}")
    print(
        f"  📦 현황: 총 {selected['total_count']:,} / "
        f"시도 {selected['attempted_count']:,} / 성공 {selected['success_count']:,} "
        f"({selected['success_rate']:.1f}%)"
    )
    if limit == 0:
        print("  🧭 실행 범위: 전체 대상 처리")
    else:
        print(f"  🧭 실행 범위: 최대 {limit:,}건 처리")
    print()
    run_enricher(
        limit=limit,
        quiet=quiet,
        lv3=selected["lv3"],
        lv4=selected["lv4"],
    )


def run_public_api_collection() -> None:
    print("\n  🌐 [공공 API 수집 설정]")
    raw = input("  🔹 저장할 데이터 개수 입력 (0 또는 '전체' = 전체 수집): ").strip()
    if not raw:
        print("  ⚠️ 입력이 비어 있어 수집을 취소합니다.")
        return

    # collector.main()은 sys.argv를 읽으므로 일시적으로 주입
    argv_backup = sys.argv[:]
    try:
        sys.argv = ["collector.py", raw]
        collector.main()
    finally:
        sys.argv = argv_backup


def run_public_api_menu() -> None:
    while True:
        print("\n  🌐 [공공 API 하위 메뉴]")
        print("    [1] 가공식품")
        print("    [2] HACCP")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            run_public_api_processed_food_menu()
        elif sub == "2":
            run_public_api_haccp_menu()
        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_public_api_processed_food_menu() -> None:
    while True:
        print("\n  🥫 [공공 API > 가공식품]")
        print("    [1] 가공식품 데이터 수집")
        print("    [2] 가공식품 중복 데이터 점검/삭제")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()
        if sub == "1":
            run_public_api_collection()
        elif sub == "2":
            run_duplicate_menu()
        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_public_api_haccp_menu() -> None:
    while True:
        print("\n  🧪 [공공 API > HACCP]")
        print("    [1] HACCP API 조회 (품목보고번호)")
        print("    [2] HACCP API 전체 수집")
        print("    [3] HACCP 진행 캐시 초기화")
        print("    [4] HACCP 데이터 테이블 비우기")
        print("    [5] HACCP Pass4 파싱 실행 (실시간/Batch)")
        print("    [6] HACCP 파싱 캐시 비우기")
        print("    [7] HACCP 파싱 결과 조회 (브라우저)")
        print("    [8] HACCP 파싱 감사 실행 (haccp_parsed_cache)")
        print("    [9] HACCP 감사 결과 요약/상위 조회")
        print("    [0] HACCP 감사 캐시 비우기")
        print("    [10] HACCP backend Import 전송")
        print("    [11] HACCP backend Import payload JSON 파일 생성")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()
        if sub == "1":
            run_haccp_lookup_menu()
        elif sub == "2":
            run_haccp_full_collection_menu()
        elif sub == "3":
            run_haccp_progress_reset_menu()
        elif sub == "4":
            run_haccp_data_reset_menu()
        elif sub == "5":
            run_haccp_pass4_parse_menu()
        elif sub == "6":
            run_haccp_parsed_reset_menu()
        elif sub == "7":
            run_haccp_parsed_view_menu()
        elif sub == "8":
            run_haccp_audit_run_menu()
        elif sub == "9":
            run_haccp_audit_summary_menu()
        elif sub == "0":
            run_haccp_audit_clear_menu()
        elif sub == "10":
            run_haccp_backend_import_menu()
        elif sub == "11":
            run_haccp_backend_import_payload_view_menu()
        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_haccp_audit_run_menu() -> None:
    print("\n  🧭 [HACCP 파싱 감사 실행]")
    raw_ver = input("  🔹 감사 버전 [기본 haccp_audit_v1]: ").strip()
    audit_version = raw_ver or "haccp_audit_v1"
    raw_limit = input("  🔹 최대 감사 건수 [기본 0=전체]: ").strip()
    limit = int(raw_limit) if raw_limit.isdigit() else 0
    limit = max(0, limit)
    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_tables(conn)
        init_haccp_audit_tables(conn)
        summary = run_haccp_parsed_audit(
            conn,
            audit_version=audit_version,
            limit=limit,
        )
    print("\n  ✅ HACCP 감사 완료")
    print(f"  - audit_version : {summary.get('audit_version')}")
    print(f"  - 대상 상품수    : {int(summary.get('target_products') or 0):,}")
    print(f"  - 토큰 수       : {int(summary.get('token_count') or 0):,}")
    print(f"  - 결과 건수      : {int(summary.get('result_count') or 0):,}")
    print(f"  - 이슈 건수      : {int(summary.get('issue_count') or 0):,}")
    print(f"  - JSON 파싱 실패 : {int(summary.get('parse_json_errors') or 0):,}")


def run_haccp_audit_summary_menu() -> None:
    print("\n  📊 [HACCP 감사 결과 요약]")
    raw_ver = input("  🔹 조회할 감사 버전 [기본 haccp_audit_v1]: ").strip()
    audit_version = raw_ver or "haccp_audit_v1"
    raw_top = input("  🔹 상위 의심 건수 [기본 20]: ").strip()
    top_n = int(raw_top) if raw_top.isdigit() else 20
    top_n = max(1, min(200, top_n))
    api_base = ""
    try:
        port = _ensure_haccp_parsed_server()
        api_base = f"http://127.0.0.1:{int(port)}"
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  ⚠️ 캐시 액션 서버 준비 실패: {exc}")
    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_audit_tables(conn)
        summary = fetch_haccp_audit_summary(conn, audit_version=audit_version)
        if int(summary.get("rows") or 0) <= 0:
            print("\n  ℹ️ 감사 결과가 없습니다. 먼저 [8] 감사 실행을 진행하세요.")
            return
        rows = conn.execute(
            """
            SELECT r.prdlstReportNo, r.suspicion_score, r.error_count, r.warn_count,
                   r.node_count, r.max_depth, r.low_freq_node_count, r.rule_hits_json, r.audited_at,
                   COALESCE(h.prdlstNm,'') AS prdlstNm,
                   p.ingredients_items_json, p.nutrition_items_json, p.source_rawmtrl, p.source_nutrient
            FROM haccp_audit_result r
            LEFT JOIN haccp_product_info h ON h.prdlstReportNo = r.prdlstReportNo
            LEFT JOIN haccp_parsed_cache p ON p.prdlstReportNo = r.prdlstReportNo
            WHERE r.audit_version=?
            ORDER BY r.suspicion_score DESC, r.error_count DESC, r.warn_count DESC, r.prdlstReportNo
            LIMIT ?
            """,
            (audit_version, top_n),
        ).fetchall()
        token_rows = conn.execute(
            """
            SELECT token_norm, product_count
            FROM haccp_audit_token_stats
            WHERE audit_version=?
            """,
            (audit_version,),
        ).fetchall()
        issue_rows = conn.execute(
            """
            SELECT prdlstReportNo, severity, rule_code, path, node_name, origin, amount, evidence
            FROM haccp_audit_issue
            WHERE audit_version=?
            ORDER BY id DESC
            """,
            (audit_version,),
        ).fetchall()
    token_product_count = {str(x[0] or ""): int(x[1] or 0) for x in token_rows}

    issues_by_report: dict[str, list[dict[str, str]]] = {}
    for ir in issue_rows:
        rpt = str(ir[0] or "").strip()
        if not rpt:
            continue
        bucket = issues_by_report.setdefault(rpt, [])
        if len(bucket) >= 20:
            continue
        bucket.append(
            {
                "severity": str(ir[1] or ""),
                "rule_code": str(ir[2] or ""),
                "path": str(ir[3] or ""),
                "node_name": str(ir[4] or ""),
                "origin": str(ir[5] or ""),
                "amount": str(ir[6] or ""),
                "evidence": str(ir[7] or ""),
            }
        )

    cards: list[str] = []
    for idx, r in enumerate(rows, start=1):
        rpt = str(r[0] or "")
        score = int(r[1] or 0)
        err = int(r[2] or 0)
        warn = int(r[3] or 0)
        node_count = int(r[4] or 0)
        max_depth = int(r[5] or 0)
        low_freq = int(r[6] or 0)
        rule_hits = str(r[7] or "")
        audited_at = str(r[8] or "")
        pname = str(r[9] or "").strip()
        ing_json = str(r[10] or "")
        nut_json = str(r[11] or "")
        src_rawmtrl = str(r[12] or "")
        src_nutrient = str(r[13] or "")
        sev_class = "sev-high" if score >= 60 else ("sev-mid" if score >= 30 else "sev-low")
        issues = issues_by_report.get(rpt, [])
        issue_html = ""
        if issues:
            rows_html: list[str] = []
            for it in issues[:12]:
                sev = str(it.get("severity") or "")
                sev_badge = "err" if sev == "error" else "warn"
                rows_html.append(
                    "<tr>"
                    f"<td><span class='badge {sev_badge}'>{html.escape(sev or '-')}</span></td>"
                    f"<td>{html.escape(str(it.get('rule_code') or '-'))}</td>"
                    f"<td>{html.escape(str(it.get('path') or '-'))}</td>"
                    f"<td>{html.escape(str(it.get('node_name') or '-'))}</td>"
                    f"<td>{html.escape(str(it.get('origin') or '-'))}</td>"
                    f"<td>{html.escape(str(it.get('amount') or '-'))}</td>"
                    f"<td>{html.escape(str(it.get('evidence') or '-'))}</td>"
                    "</tr>"
                )
            issue_html = (
                "<details class='issue-wrap'><summary>이슈 상세 보기</summary>"
                "<table class='issue-table'><thead><tr>"
                "<th>severity</th><th>rule</th><th>path</th><th>name</th><th>origin</th><th>amount</th><th>evidence</th>"
                "</tr></thead><tbody>"
                + "".join(rows_html)
                + "</tbody></table></details>"
            )
        cards.append(
            f"<article class='card {sev_class}' data-score='{score}' data-rpt='{html.escape(rpt)}'>"
            "<div class='card-head'>"
            "<div class='left'>"
            f"<span class='idx'>#{idx}</span>"
            f"<span class='rpt'>{html.escape(rpt)}</span>"
            f"<span class='pname'>{html.escape(pname or '(상품명 없음)')}</span>"
            "</div>"
            "<div class='right'>"
            f"<span class='pill score'>score {score:,}</span>"
            f"<span class='pill err'>error {err:,}</span>"
            f"<span class='pill warn'>warn {warn:,}</span>"
            f"<span class='pill'>nodes {node_count:,}</span>"
            f"<span class='pill'>depth {max_depth:,}</span>"
            f"<span class='pill'>low-freq {low_freq:,}</span>"
            "</div>"
            "</div>"
            "<div class='meta-row'>"
            f"<span class='meta'>audited_at: {html.escape(audited_at)}</span>"
            "<div class='actions'>"
            f"<button class='act btn-del' data-act='delete' data-rpt='{html.escape(rpt)}'>캐시 삭제</button>"
            f"<button class='act btn-block' data-act='delete_block' data-rpt='{html.escape(rpt)}'>삭제 + 재파싱 차단</button>"
            "</div>"
            "</div>"
            "<details><summary>rule_hits_json</summary>"
            f"<pre>{html.escape(_pretty_json_for_report(rule_hits))}</pre></details>"
            "<section class='compare-wrap'>"
            "<h4>원재료: 구조화 GUI + RAW</h4>"
            "<div class='compare-grid'>"
            f"<div class='pane pane-parsed'><h5>원재료 구조화 GUI</h5><div class='ingredient-tree'>{_build_haccp_ingredient_preview(ing_json, token_product_count=token_product_count)}</div></div>"
            f"<div class='pane pane-raw'><h5>원재료 RAW</h5><pre>{html.escape(src_rawmtrl or '-')}</pre></div>"
            "</div>"
            "</section>"
            "<section class='compare-wrap'>"
            "<h4>영양성분: 구조화 GUI + RAW</h4>"
            "<div class='compare-grid'>"
            f"<div class='pane pane-parsed'><h5>영양성분 구조화 GUI</h5>{_build_haccp_nutrition_preview(nut_json)}</div>"
            f"<div class='pane pane-raw'><h5>영양성분 RAW</h5><pre>{html.escape(src_nutrient or '-')}</pre></div>"
            "</div>"
            "</section>"
            f"{issue_html}"
            "</article>"
        )

    top_rules = summary.get("top_rules") or []
    top_rule_chips = "".join(
        f"<span class='chip'>{html.escape(str(k))}: <b>{int(v):,}</b></span>"
        for k, v in top_rules
    )
    html_doc = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>HACCP 감사 결과 브라우저</title>
  <style>
    :root {{
      --bg:#f5f7fb; --card:#fff; --ink:#13202b; --muted:#607080; --line:#d9e1ea;
      --high:#8a1538; --mid:#915200; --low:#0d6a58;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:linear-gradient(180deg,#eaf0f7 0%, var(--bg) 30%); color:var(--ink); font-family:'Pretendard','Noto Sans KR',sans-serif; }}
    .wrap {{ max-width:1320px; margin:0 auto; padding:22px; }}
    .hero {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:18px; box-shadow:0 8px 24px rgba(14,30,45,.06); }}
    h1 {{ margin:0 0 8px; font-size:28px; }}
    .meta {{ color:var(--muted); font-size:13px; }}
    .kpis {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:12px; }}
    .chip {{ background:#eef3f8; border:1px solid #d6e0ea; border-radius:999px; padding:6px 10px; font-size:12px; }}
    .controls {{ margin:14px 0; display:flex; gap:8px; flex-wrap:wrap; }}
    .controls input {{ padding:10px 12px; border:1px solid var(--line); border-radius:10px; min-width:280px; }}
    .compare-wrap {{ margin-top:10px; }}
    .compare-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
    .pane {{ border:1px solid var(--line); border-radius:12px; background:#f8fcfd; padding:10px; }}
    .pane h5 {{ margin:0 0 6px; font-size:12px; color:#5f7080; }}
    .ingredient-tree {{ border:1px solid var(--line); border-radius:12px; background:#f9fcfd; padding:10px; }}
    .ing-tree {{ list-style:none; margin:0; padding-left:16px; border-left:2px solid #dce8ee; }}
    .ing-tree.depth-0 {{ padding-left:8px; border-left:none; }}
    .ing-node {{ margin:8px 0; }}
    .ing-row {{ display:flex; flex-wrap:wrap; gap:8px; align-items:flex-start; }}
    .ing-row .name {{ font-weight:700; color:#0f2c3a; background:#edf6fa; border:1px solid #d8e8f1; border-radius:8px; padding:4px 8px; }}
    .ing-row .meta {{ display:flex; flex-wrap:wrap; gap:6px; }}
    .meta-chip {{ font-size:11px; background:#f2f5f7; border:1px solid #dde6eb; border-radius:999px; padding:3px 8px; color:#30424e; }}
    .meta-chip b {{ margin-right:4px; color:#4b5f6e; font-weight:700; }}
    .meta-chip.amount {{ background:#fff8e9; border-color:#efd9ad; }}
    .nut-table {{ width:100%; border-collapse:collapse; background:#fffdf8; border:1px solid #efdfbf; border-radius:10px; overflow:hidden; font-size:12px; }}
    .nut-table th, .nut-table td {{ border-bottom:1px solid #f1e5cb; padding:8px 10px; text-align:left; }}
    .nut-table th {{ background:#fff4dc; color:#5d4a22; font-weight:700; }}
    .nut-table td.num {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-weight:700; color:#7a4310; }}
    .nut-table tbody tr:last-child td {{ border-bottom:none; }}
    .grid {{ display:grid; gap:12px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-left:6px solid #d7dee8; border-radius:14px; padding:12px; }}
    .card.sev-high {{ border-left-color:#e34b79; }}
    .card.sev-mid {{ border-left-color:#efb14b; }}
    .card.sev-low {{ border-left-color:#49b89d; }}
    .card-head {{ display:flex; justify-content:space-between; gap:10px; flex-wrap:wrap; }}
    .left {{ display:flex; align-items:center; gap:8px; flex-wrap:wrap; }}
    .right {{ display:flex; align-items:center; gap:6px; flex-wrap:wrap; }}
    .idx {{ font-weight:800; color:#31576b; }}
    .rpt {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; background:#edf3f8; border:1px solid #dbe5ef; border-radius:8px; padding:3px 7px; font-size:13px; }}
    .pname {{ background:#f1f8f3; border:1px solid #d7e8dc; border-radius:8px; padding:3px 7px; font-size:12px; max-width:560px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .pill {{ border-radius:999px; padding:3px 8px; font-size:11px; border:1px solid #d6e0ea; background:#f0f4f8; }}
    .pill.score {{ background:#eef2ff; border-color:#cad6ff; font-weight:700; }}
    .pill.err {{ background:#ffe9ef; border-color:#f4b9ca; color:#8c153b; }}
    .pill.warn {{ background:#fff4e3; border-color:#efce9d; color:#88520f; }}
    .meta-row {{ margin-top:6px; }}
    .actions {{ margin-top:8px; display:flex; gap:8px; flex-wrap:wrap; }}
    .act {{ border:1px solid #d6e0ea; border-radius:8px; padding:6px 10px; background:#f5f8fb; cursor:pointer; font-size:12px; font-weight:700; }}
    .act:hover {{ background:#ebf1f7; }}
    .act.btn-del {{ color:#8d1238; border-color:#f2b8c8; background:#ffeaf0; }}
    .act.btn-block {{ color:#6b3e00; border-color:#efcf9f; background:#fff3e0; }}
    pre {{ margin:8px 0 0; background:#f8fbfd; border:1px solid var(--line); border-radius:10px; padding:10px; font-size:12px; line-height:1.45; white-space:pre-wrap; word-break:break-word; }}
    details summary {{ cursor:pointer; color:#1f4f8a; font-weight:600; margin-top:8px; }}
    .issue-table {{ width:100%; border-collapse:collapse; margin-top:8px; font-size:12px; }}
    .issue-table th, .issue-table td {{ border:1px solid #dce4ec; padding:6px 8px; text-align:left; vertical-align:top; }}
    .issue-table th {{ background:#f0f5fa; }}
    .badge {{ border-radius:999px; padding:2px 7px; font-size:11px; font-weight:700; text-transform:uppercase; }}
    .badge.err {{ background:#ffe6ee; color:#8d1238; }}
    .badge.warn {{ background:#fff2df; color:#8a5206; }}
    @media (max-width: 900px) {{ .wrap {{ padding:12px; }} .compare-grid {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>HACCP 감사 결과 브라우저</h1>
      <div class="meta">DB: food_data.db · table: haccp_audit_result / haccp_audit_issue · audit_version={html.escape(audit_version)}</div>
      <div class="meta">cache_api: {html.escape(api_base or 'N/A')}</div>
      <div class="kpis">
        <span class="chip">rows: <b>{int(summary.get('rows') or 0):,}</b></span>
        <span class="chip">errors: <b>{int(summary.get('errors') or 0):,}</b></span>
        <span class="chip">warns: <b>{int(summary.get('warns') or 0):,}</b></span>
        <span class="chip">avg_score: <b>{float(summary.get('avg_score') or 0.0):.2f}</b></span>
        <span class="chip">max_score: <b>{int(summary.get('max_score') or 0):,}</b></span>
      </div>
      <div class="kpis">{top_rule_chips or "<span class='chip'>top_rules 없음</span>"}</div>
    </div>
    <div class="controls">
      <input id="q" type="text" placeholder="품목보고번호/상품명/규칙 검색">
    </div>
    <div id="list" class="grid">
      {''.join(cards) if cards else "<article class='card'>데이터가 없습니다.</article>"}
    </div>
  </div>
  <script>
    const API_BASE = {json.dumps(api_base)};
    const q = document.getElementById('q');
    const cards = Array.from(document.querySelectorAll('.card'));
    function applyFilter() {{
      const kw = (q.value || '').toLowerCase();
      cards.forEach(card => {{
        const t = card.textContent.toLowerCase();
        card.style.display = (!kw || t.includes(kw)) ? '' : 'none';
      }});
    }}
    q.addEventListener('input', applyFilter);
    async function callCacheAction(act, rpt, btnEl) {{
      if (!API_BASE) {{
        alert('캐시 액션 서버가 준비되지 않았습니다. 다시 시도하세요.');
        return;
      }}
      const target = String(rpt || '').replace(/[^0-9]/g, '');
      if (!target) {{
        alert('품목보고번호가 비어있습니다.');
        return;
      }}
      const msg = act === 'delete_block'
        ? '해당 품목보고번호 캐시를 삭제하고 재파싱 차단할까요?'
        : '해당 품목보고번호 캐시를 삭제할까요?';
      if (!confirm(msg)) return;
      const prev = btnEl ? btnEl.textContent : '';
      if (btnEl) {{ btnEl.disabled = true; btnEl.textContent = '처리중...'; }}
      try {{
        const url = `${{API_BASE}}/cache_api?action=${{encodeURIComponent(act)}}&report_no=${{encodeURIComponent(target)}}`;
        const res = await fetch(url, {{ method: 'GET' }});
        const data = await res.json();
        if (!res.ok || !data.ok) {{
          throw new Error(data.error || `http_${{res.status}}`);
        }}
        const card = btnEl ? btnEl.closest('.card') : null;
        if (card) {{
          card.style.opacity = '0.45';
          card.style.filter = 'grayscale(0.2)';
        }}
        alert(`완료: report_no=${{target}} / deleted=${{data.deleted || 0}}` + (act === 'delete_block' ? ' / blocked=Y' : ''));
      }} catch (e) {{
        alert('실패: ' + (e && e.message ? e.message : e));
      }} finally {{
        if (btnEl) {{ btnEl.disabled = false; btnEl.textContent = prev || '실행'; }}
      }}
    }}
    document.querySelectorAll('.act[data-act]').forEach((btn) => {{
      btn.addEventListener('click', (ev) => {{
        const b = ev.currentTarget;
        callCacheAction(b.getAttribute('data-act'), b.getAttribute('data-rpt'), b);
      }});
    }});
    applyFilter();
  </script>
</body>
</html>"""
    out_dir = Path("reports") / "haccp_audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"haccp_audit_{audit_version}_{ts}.html"
    out_path.write_text(html_doc, encoding="utf-8")
    print(f"\n  ✅ 브라우저 리포트 생성: {out_path}")
    try:
        webbrowser.open(out_path.resolve().as_uri())
        print("  🖥️ 브라우저 자동 열기 완료")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  ⚠️ 브라우저 자동 열기 실패: {exc}")


def run_haccp_audit_clear_menu() -> None:
    print("\n  🧹 [HACCP 감사 캐시 비우기]")
    raw_ver = input("  🔹 삭제할 감사 버전 [비우면 전체 삭제]: ").strip()
    confirm = input("  ⚠️ 삭제를 진행할까요? [y/N]: ").strip().lower()
    if confirm != "y":
        print("  취소했습니다.")
        return
    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_audit_tables(conn)
        deleted = clear_haccp_audit_tables(conn, audit_version=(raw_ver or None))
    print("\n  ✅ 감사 캐시 삭제 완료")
    print(f"  - token_stats: {int(deleted.get('token_stats') or 0):,}")
    print(f"  - results    : {int(deleted.get('results') or 0):,}")
    print(f"  - issues     : {int(deleted.get('issues') or 0):,}")


def run_haccp_lookup_menu() -> None:
    from app import config as app_config
    app_config.reload_dotenv()

    print("\n  🧪 [HACCP API 조회 - 품목보고번호]")
    raw_report_no = input("  🔹 품목보고번호 입력: ").strip()
    report_no = re.sub(r"[^0-9]", "", raw_report_no)
    if not report_no:
        print("  ⚠️ 품목보고번호(숫자)를 입력해주세요.")
        return

    raw_rows = input("  🔹 조회 건수(numOfRows) [기본 20]: ").strip()
    num_of_rows = int(raw_rows) if raw_rows.isdigit() else 20
    num_of_rows = max(1, min(100, num_of_rows))

    try:
        result = fetch_haccp_products_by_report_no(
            report_no=report_no,
            page_no=1,
            num_of_rows=num_of_rows,
        )
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  ❌ HACCP API 조회 실패: {exc}")
        return

    header = result.get("header") or {}
    body = result.get("body") or {}
    items = result.get("items") or []
    print("\n  📌 [응답 헤더]")
    print(f"    resultCode   : {header.get('resultCode')}")
    print(f"    resultMessage: {header.get('resultMessage')}")
    print("  📌 [응답 본문]")
    print(f"    pageNo       : {body.get('pageNo')}")
    print(f"    numOfRows    : {body.get('numOfRows')}")
    print(f"    totalCount   : {body.get('totalCount')}")
    print(f"    items        : {len(items):,}건")

    if not items:
        print("  ℹ️ 조회 결과가 없습니다.")
        return

    print("\n  📦 [조회 결과]")
    for i, it in enumerate(items, start=1):
        print(f"    [{i}] prdlstReportNo: {it.get('prdlstReportNo')}")
        print(f"        prdlstNm      : {it.get('prdlstNm')}")
        print(f"        manufacture   : {it.get('manufacture')}")
        print(f"        seller        : {it.get('seller')}")
        print(f"        productGb     : {it.get('productGb')}")
        print(f"        prdkind       : {it.get('prdkind')}")
        print(f"        capacity      : {it.get('capacity')}")
        print(f"        barcode       : {it.get('barcode')}")
        print(f"        allergy       : {it.get('allergy')}")
        print(f"        nutrient      : {it.get('nutrient')}")
        print(f"        rawmtrl       : {it.get('rawmtrl')}")
        print(f"        imgurl1       : {it.get('imgurl1')}")
        print(f"        imgurl2       : {it.get('imgurl2')}")


def run_haccp_full_collection_menu() -> None:
    from app import config as app_config
    app_config.reload_dotenv()

    print("\n  🌐 [HACCP API 전체 수집]")
    raw_rows = input("  🔹 페이지당 조회 건수(numOfRows) [기본 100]: ").strip()
    num_of_rows = int(raw_rows) if raw_rows.isdigit() else 100
    num_of_rows = max(1, min(1000, num_of_rows))

    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_tables(conn)
        try:
            total_count = fetch_haccp_total_count()
        except Exception as exc:  # pylint: disable=broad-except
            print(f"  ❌ 전체 건수 조회 실패: {exc}")
            return
        if total_count <= 0:
            print("  ⚠️ totalCount가 0입니다. API 키/권한/요청 제한을 확인해주세요.")
            return

        total_pages = (total_count + num_of_rows - 1) // num_of_rows
        completed_pages = {
            p for p in get_haccp_completed_pages(conn, num_of_rows)
            if 1 <= p <= total_pages
        }
        remaining_pages = [p for p in range(1, total_pages + 1) if p not in completed_pages]

        print(f"  📌 totalCount: {total_count:,}건")
        print(f"  📄 totalPages: {total_pages:,}페이지 (numOfRows={num_of_rows})")
        if completed_pages:
            print(f"  ↺ 재개 모드: 완료 {len(completed_pages):,}페이지 / 남은 {len(remaining_pages):,}페이지")
        if not remaining_pages:
            print("  ✅ 이미 모든 페이지가 완료 상태입니다.")
            return

        saved_total = 0
        failed_pages: list[int] = []
        started_at = time.time()
        for idx, page_no in enumerate(remaining_pages, start=1):
            try:
                result = fetch_haccp_page(page_no=page_no, num_of_rows=num_of_rows)
                items = result.get("items") or []
                saved = upsert_haccp_rows(conn, items)
                mark_haccp_page_done(conn, page_no, num_of_rows, saved)
                saved_total += saved
            except Exception as exc:  # pylint: disable=broad-except
                failed_pages.append(page_no)
                print(f"  ⚠️ 페이지 {page_no} 실패: {exc}")
                continue

            if (idx % 10 == 0) or (idx == len(remaining_pages)):
                elapsed = time.time() - started_at
                print(
                    f"  진행: {idx:,}/{len(remaining_pages):,}페이지 | "
                    f"이번 실행 저장 {saved_total:,}건 | 경과 {elapsed:.1f}s"
                )

        now_total = conn.execute("SELECT COUNT(*) FROM haccp_product_info").fetchone()[0]
        print("\n  ✅ HACCP 전체 수집 완료")
        print(f"  - 이번 실행 저장(upsert): {saved_total:,}건")
        print(f"  - 테이블 총 건수       : {now_total:,}건")
        if failed_pages:
            preview = ", ".join(str(p) for p in failed_pages[:12])
            suffix = " ..." if len(failed_pages) > 12 else ""
            print(f"  - 실패 페이지          : {len(failed_pages):,}개 ({preview}{suffix})")
            print("    다음 실행 시 실패 페이지부터 재개 가능합니다.")


def run_haccp_progress_reset_menu() -> None:
    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_tables(conn)
        deleted = clear_haccp_progress(conn)
    print(f"\n  ✅ HACCP 진행 캐시 초기화 완료: {deleted:,}건 삭제")


def run_haccp_data_reset_menu() -> None:
    answer = input("  ⚠️ haccp_product_info 전체 삭제합니다. 계속할까요? [y/N]: ").strip().lower()
    if answer != "y":
        print("  취소했습니다.")
        return
    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_tables(conn)
        deleted = clear_haccp_data(conn)
    print(f"\n  ✅ HACCP 데이터 삭제 완료: {deleted:,}건 삭제")


def run_haccp_pass4_parse_menu() -> None:
    from app import config as app_config
    app_config.reload_dotenv()

    print("\n  🤖 [HACCP Pass4 파싱 실행 - 정책형]")
    openai_api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not openai_api_key:
        print("  ❌ OPENAI_API_KEY 환경변수가 필요합니다.")
        return

    raw_limit = input("  🔹 최대 처리 건수 [기본 20, 0=전체]: ").strip()
    limit = int(raw_limit) if raw_limit.isdigit() else 20
    limit = max(0, limit)
    raw_target_report_nos = input("  🔹 품목보고번호 지정(쉼표구분, 비우면 전체 대상 규칙): ").strip()
    target_report_keys: list[str] = []
    if raw_target_report_nos:
        seen: set[str] = set()
        for tok in re.split(r"[,\s]+", raw_target_report_nos):
            digits = re.sub(r"[^0-9]", "", str(tok or ""))
            if not digits:
                continue
            if digits in seen:
                continue
            seen.add(digits)
            target_report_keys.append(digits)
        if not target_report_keys:
            print("  ⚠️ 품목보고번호 지정값에서 유효한 숫자를 찾지 못했습니다. 전체 대상 규칙으로 진행합니다.")
    raw_stratified = input("  🔹 층화 샘플링(카테고리 균등) 사용할까요? [y/N]: ").strip().lower()
    use_stratified = raw_stratified in ("y", "yes")
    strat_per_bucket = 3
    strat_bucket_limit = 10
    if use_stratified:
        raw_per_bucket = input("  🔹 층화 샘플: 카테고리당 건수 [기본 3]: ").strip()
        strat_per_bucket = int(raw_per_bucket) if raw_per_bucket.isdigit() else 3
        strat_per_bucket = max(1, min(20, strat_per_bucket))
        raw_bucket_limit = input("  🔹 층화 샘플: 최대 카테고리 수 [기본 10]: ").strip()
        strat_bucket_limit = int(raw_bucket_limit) if raw_bucket_limit.isdigit() else 10
        strat_bucket_limit = max(1, min(100, strat_bucket_limit))
    if target_report_keys and use_stratified:
        print("  ℹ️ 품목보고번호 지정 모드가 우선되어 층화 샘플링은 무시됩니다.")
        use_stratified = False

    raw_skip_done = input("  🔹 이미 Pass4 성공 저장된 품목보고번호는 스킵할까요? [Y/n]: ").strip().lower()
    skip_done = not (raw_skip_done in ("n", "no"))
    verbose = False
    raw_mode = input("  🔹 호출 방식 선택 [r=실시간, b=Batch] (기본 r): ").strip().lower()
    use_batch = raw_mode in ("b", "batch")
    workers = 1
    batch_poll_sec = 8
    batch_max_wait_min = 90
    if use_batch:
        raw_poll = input("  🔹 Batch 상태 확인 주기(초) [기본 8]: ").strip()
        batch_poll_sec = int(raw_poll) if raw_poll.isdigit() else 8
        batch_poll_sec = max(3, min(60, batch_poll_sec))
        raw_wait = input("  🔹 Batch 최대 대기시간(분) [기본 90]: ").strip()
        batch_max_wait_min = int(raw_wait) if raw_wait.isdigit() else 90
        batch_max_wait_min = max(5, min(24 * 60, batch_max_wait_min))
    else:
        raw_workers = input("  🔹 병렬 처리 개수 [기본 4, 최소 1]: ").strip()
        workers = int(raw_workers) if raw_workers.isdigit() else 4
        workers = max(1, min(16, workers))
    def _normalize_report_no_key(value: str | None) -> str:
        return re.sub(r"[^0-9]", "", str(value or ""))

    def _extract_nutrition_basis(text: str | None) -> dict[str, Any] | None:
        src = str(text or "").strip()
        if not src:
            return None
        def _num(v: str) -> str:
            return re.sub(r"[,\s]", "", str(v or ""))

        count_units = ("개", "알", "봉", "봉지", "포", "캔", "정", "스틱")

        # 1) 1회 제공량 우선
        m = re.search(r"1\s*회[^\n,]{0,80}\(\s*([\d,]+(?:\.\d+)?)\s*(g|ml)\s*\)", src, re.IGNORECASE)
        if not m:
            m = re.search(r"1\s*회\s*제공량\s*[:：]?\s*([\d,]+(?:\.\d+)?)\s*(g|ml)", src, re.IGNORECASE)
        if m:
            return {
                "per_amount": _num(m.group(1)),
                "per_unit": m.group(2).lower(),
                "basis_text": m.group(0),
            }

        # 1-b) 1회 제공량에서 g/ml가 없고 개수 단위만 있는 경우
        unit_pat = "|".join(count_units)
        m = re.search(rf"1\s*회[^\n,]{{0,50}}?([\d,]+(?:\.\d+)?)\s*({unit_pat})\b", src)
        if m:
            return {
                "per_amount": _num(m.group(1)),
                "per_unit": m.group(2),
                "basis_text": m.group(0),
            }

        # 2) 총내용량 기준
        m = re.search(r"총\s*내용량\s*[:：]?\s*([\d,]+(?:\.\d+)?)\s*(g|ml)", src, re.IGNORECASE)
        if m:
            return {
                "per_amount": _num(m.group(1)),
                "per_unit": m.group(2).lower(),
                "basis_text": m.group(0),
            }

        # 3) 총 N회 제공량 (1회 제공량이 없을 때만 사용)
        m = re.search(r"총\s*\d+\s*회\s*제공량\s*\(\s*([\d,]+(?:\.\d+)?)\s*(g|ml)\s*\)", src, re.IGNORECASE)
        if m:
            return {
                "per_amount": _num(m.group(1)),
                "per_unit": m.group(2).lower(),
                "basis_text": m.group(0),
            }

        # 4) 일반 N g/ml 당
        m = re.search(r"([\d,]+(?:\.\d+)?)\s*(g|ml)\s*당", src, re.IGNORECASE)
        if m:
            return {
                "per_amount": _num(m.group(1)),
                "per_unit": m.group(2).lower(),
                "basis_text": m.group(0),
            }

        # 5) 일반 N개/알/봉/포/캔 당 (g/ml가 없을 때만 fallback)
        m = re.search(rf"([\d,]+(?:\.\d+)?)\s*({unit_pat})\s*당", src)
        if m:
            return {
                "per_amount": _num(m.group(1)),
                "per_unit": m.group(2),
                "basis_text": m.group(0),
            }

        # 6) 100g/ml 기준 표기
        m = re.search(r"100\s*(g|ml)", src, re.IGNORECASE)
        if m:
            return {
                "per_amount": "100",
                "per_unit": m.group(1).lower(),
                "basis_text": m.group(0),
            }

        return None

    def _ensure_kcal_nutrition_item(items: list[dict[str, Any]], raw_text: str | None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = [x for x in items if isinstance(x, dict)]
        has_energy = False
        for it in out:
            name = str(it.get("name") or "").strip().lower()
            unit = str(it.get("unit") or "").strip().lower()
            if ("열량" in name) or ("에너지" in name) or (name == "kcal") or (unit == "kcal"):
                has_energy = True
                # kcal가 단위로 빠져있으면 보정
                if not unit:
                    it["unit"] = "kcal"
                if name == "kcal":
                    it["name"] = "열량"
                break
        if has_energy:
            return out

        src = str(raw_text or "")
        # 예: "총 내용량 (86g) 525kcal", "열량 95kcal"
        m = re.search(r"([\d,]+(?:\.\d+)?)\s*kcal", src, re.IGNORECASE)
        if not m:
            return out
        kcal_val = re.sub(r"[,\s]", "", m.group(1))
        if not kcal_val:
            return out
        out.insert(
            0,
            {
                "name": "열량",
                "value": kcal_val,
                "unit": "kcal",
                "daily_value": None,
            },
        )
        return out

    def _sanitize_ingredients_origin_fields(
        payload: dict[str, Any],
        raw_ingredients_text: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, int]]:
        """
        origin 필드에는 원산지 정보만 남긴다.
        - 원산지가 아닌 토큰(예: 밀, 합성보존료)은 sub_ingredients로 이동
        - 교정 후에도 origin이 유효하지 않으면 invalid로 집계
        """
        stats = {
            "nodes": 0,
            "origin_cleaned": 0,
            "moved_to_sub": 0,
            "lifted_from_sub": 0,
            "dropped_qualifier_sub": 0,
            "invalid_remaining": 0,
        }
        origin_generic = {"국산", "국내산", "수입산", "외국산", "기타외국산"}
        allergen_words = {
            "우유", "대두", "밀", "난류", "땅콩", "메밀", "고등어", "게", "새우",
            "돼지고기", "복숭아", "토마토", "아황산류", "호두", "닭고기", "쇠고기",
            "오징어", "조개류", "잣",
        }
        country_words = {
            "한국", "대한민국", "북한",
            "미국", "캐나다", "멕시코", "과테말라", "온두라스", "니카라과", "코스타리카", "파나마",
            "쿠바", "도미니카공화국", "자메이카", "아이티", "엘살바도르",
            "브라질", "아르헨티나", "칠레", "페루", "콜롬비아", "볼리비아", "에콰도르", "파라과이", "우루과이", "베네수엘라",
            "영국", "아일랜드", "독일", "프랑스", "이탈리아", "스페인", "포르투갈", "네덜란드", "벨기에", "룩셈부르크",
            "스위스", "오스트리아", "폴란드", "체코", "슬로바키아", "헝가리", "루마니아", "불가리아",
            "그리스", "크로아티아", "슬로베니아", "세르비아", "보스니아헤르체고비나", "몬테네그로", "북마케도니아", "알바니아",
            "노르웨이", "스웨덴", "핀란드", "덴마크", "아이슬란드", "에스토니아", "라트비아", "리투아니아",
            "러시아", "우크라이나", "벨라루스", "몰도바", "조지아", "아르메니아", "아제르바이잔",
            "중국", "일본", "대만", "홍콩", "마카오", "몽골",
            "인도", "파키스탄", "방글라데시", "네팔", "스리랑카", "부탄", "몰디브",
            "베트남", "태국", "인도네시아", "필리핀", "말레이시아", "싱가포르", "캄보디아", "라오스", "미얀마", "브루나이", "동티모르",
            "호주", "뉴질랜드", "파푸아뉴기니", "피지",
            "터키", "이란", "이라크", "시리아", "레바논", "요르단", "이스라엘", "사우디아라비아",
            "아랍에미리트", "카타르", "쿠웨이트", "오만", "예멘", "바레인",
            "이집트", "모로코", "알제리", "튀니지", "리비아", "수단", "에티오피아", "케냐", "탄자니아", "우간다",
            "나이지리아", "가나", "코트디부아르", "세네갈", "카메룬", "콩고", "콩고민주공화국",
            "남아공", "잠비아", "짐바브웨", "보츠와나", "나미비아", "모잠비크", "마다가스카르",
        }
        non_origin_banned = {
            "합성보존료", "향미증진제", "산도조절제", "유화제", "증점제", "감미료",
            "밀", "소맥", "소맥분", "대두", "옥수수", "정제소금", "설탕", "포도당",
        }

        raw_compact = re.sub(r"\s+", "", str(raw_ingredients_text or ""))

        def _norm_token(token: str) -> str:
            t = re.sub(r"[\s\(\)\[\]\{\}]+", "", str(token or ""))
            # "호주산등", "미국산등" 같은 표기를 원산지 토큰으로 정규화
            t = re.sub(r"[·\.,;:]+$", "", t)
            t = re.sub(r"등$", "", t)
            return t

        def _looks_origin_token(token: str) -> bool:
            t = _norm_token(token)
            if not t:
                return False
            # 원산지+비율 결합 표기 허용 (예: 이탈리아산67%)
            t = re.sub(r"\d+(?:\.\d+)?%$", "", t)
            if t in non_origin_banned:
                return False
            if t in origin_generic:
                return True
            if t in country_words:
                # 외국산:미국,인도 같은 상세 표현 대응
                return True
            if t.endswith("산"):
                # 예: 미국산, 호주산, 중국산만 허용하고
                # 젖산/구연산 같은 성분명은 원산지로 보지 않는다.
                core = t[:-1]
                if core in country_words:
                    return True
                if core in {"국내", "외국", "수입", "기타외국"}:
                    return True
            return False

        def _split_origin_chunks(text: str) -> list[str]:
            raw = str(text or "").strip()
            if not raw:
                return []
            chunks: list[str] = []
            for part in re.split(r"[,/;|]+", raw):
                part = str(part or "").strip()
                if not part:
                    continue
                # 예: 외국산(네덜란드·캐나다·미국)
                m = re.match(r"^(국산|국내산|수입산|외국산|기타외국산)\s*[\(\[]\s*(.+?)\s*[\)\]]?$", part)
                if m:
                    head = m.group(1).strip()
                    tail = m.group(2).strip()
                    if head:
                        chunks.append(head)
                    if tail:
                        chunks.extend([x.strip() for x in re.split(r"[,/;|·ㆍ]+", tail) if str(x).strip()])
                    continue
                if ":" in part:
                    head, tail = part.split(":", 1)
                    head = head.strip()
                    tail = tail.strip()
                    if head:
                        chunks.append(head)
                    if tail:
                        chunks.extend([x.strip() for x in re.split(r"[,/;|·ㆍ]+", tail) if str(x).strip()])
                else:
                    chunks.extend([x.strip() for x in re.split(r"[·ㆍ]+", part) if str(x).strip()])
            return [c for c in chunks if c]

        def _push_sub(node: dict[str, Any], name: str) -> None:
            nm = str(name or "").strip()
            if not nm:
                return
            subs = node.get("sub_ingredients")
            if not isinstance(subs, list):
                subs = []
            for it in subs:
                if not isinstance(it, dict):
                    continue
                existing = str(it.get("name") or it.get("ingredient_name") or "").strip()
                if existing == nm:
                    node["sub_ingredients"] = subs
                    return
            subs.append(
                {
                    "name": nm,
                    "origin": None,
                    "amount": None,
                    "sub_ingredients": [],
                }
            )
            node["sub_ingredients"] = subs

        def _merge_origin_tokens(node: dict[str, Any], tokens: list[str]) -> None:
            def _normalize_origin_label(token: str) -> str:
                t = _norm_token(token)
                if not t:
                    return ""
                amt = ""
                m_amt = re.search(r"(\d+(?:\.\d+)?%)$", t)
                if m_amt:
                    amt = m_amt.group(1)
                    t = t[: -len(amt)]
                if t in {"국내", "외국", "수입", "기타외국"}:
                    out = f"{t}산"
                    return f"{out} {amt}".strip() if amt else out
                if t in origin_generic:
                    return f"{t} {amt}".strip() if amt else t
                if t in country_words:
                    out = t if t.endswith("산") else f"{t}산"
                    return f"{out} {amt}".strip() if amt else out
                if t.endswith("산"):
                    core = t[:-1]
                    if core in country_words:
                        out = f"{core}산"
                        return f"{out} {amt}".strip() if amt else out
                out = t
                return f"{out} {amt}".strip() if amt else out

            current = str(node.get("origin") or "").strip()
            merged_tokens = _split_origin_chunks(current) + [str(t).strip() for t in tokens if str(t).strip()]
            valid: list[str] = []
            for tok in merged_tokens:
                if _looks_origin_token(tok):
                    normed = _normalize_origin_label(tok)
                    if normed:
                        valid.append(normed)
            seen: set[str] = set()
            valid = [x for x in valid if (not (x in seen or seen.add(x)))]
            # 단일 국가에서 "국가산" + "국가산 100%"가 함께 있으면 100% 표기는 중복으로 본다.
            # 예: "이스라엘산, 이스라엘산 100%" -> "이스라엘산"
            base_only: dict[str, set[str]] = {}
            for x in valid:
                m = re.match(r"^(.*?산)(?:\s+(\d+(?:\.\d+)?%))?$", x)
                if not m:
                    continue
                b = str(m.group(1) or "").strip()
                a = str(m.group(2) or "").strip()
                if not b:
                    continue
                base_only.setdefault(b, set()).add(a or "")
            cleaned_valid: list[str] = []
            for x in valid:
                m = re.match(r"^(.*?산)(?:\s+(\d+(?:\.\d+)?%))?$", x)
                if not m:
                    cleaned_valid.append(x)
                    continue
                b = str(m.group(1) or "").strip()
                a = str(m.group(2) or "").strip()
                amounts = base_only.get(b, set())
                if a == "100%" and "" in amounts:
                    # 같은 base의 무비율 표기가 이미 있으면 100%는 제거
                    continue
                cleaned_valid.append(x)
            valid = cleaned_valid
            cleaned = ", ".join(valid) if valid else None
            if cleaned != (current or None):
                node["origin"] = cleaned
                stats["origin_cleaned"] += 1

        def _is_non_ingredient_qualifier(token: str) -> bool:
            t = _norm_token(token)
            if not t:
                return False
            if t in {
                "세균수기준", "규격기준", "품질기준", "검사기준", "관리기준",
                "강화성분제외", "강화성분제외함", "강화성분제외제품",
                "포함", "함유",
            }:
                return True
            # 예: "...기준", "...기준치" 는 설명성 문구로 간주
            if re.search(r"(기준|기준치)$", t):
                return True
            return False

        def _append_allergen_token(text: str) -> None:
            tok = str(text or "").strip()
            if not tok:
                return
            vals = payload.get("allergen_tokens")
            if not isinstance(vals, list):
                vals = []
            if tok not in vals:
                vals.append(tok)
            payload["allergen_tokens"] = vals

        def _strip_include_suffix(token: str) -> tuple[str, bool]:
            s = str(token or "").strip()
            if not s:
                return s, False
            # "우유포함", "대두 함유" -> "우유", "대두"
            m = re.search(r"\s*(포함|함유)\s*$", s)
            if not m:
                return s, False
            s2 = re.sub(r"\s*(포함|함유)\s*$", "", s).strip()
            return (s2 if s2 else s), True

        def _append_ignored_fragment(text: str) -> None:
            frag = str(text or "").strip()
            if not frag:
                return
            ignored = payload.get("ignored_fragments")
            if not isinstance(ignored, list):
                ignored = []
            if frag not in ignored:
                ignored.append(frag)
            payload["ignored_fragments"] = ignored

        def _clean_amount_value(amount: str) -> tuple[str | None, list[str]]:
            raw = str(amount or "").strip()
            if not raw:
                return None, []
            # "-1" 같은 이름 suffix 오인값은 amount로 보지 않는다.
            if re.fullmatch(r"-\d+(?:\.\d+)?", raw):
                return None, [raw]
            parts = [p.strip() for p in re.split(r"[,/;|]+", raw) if str(p).strip()]
            kept: list[str] = []
            dropped: list[str] = []
            for p in parts:
                # Brix는 함량(%)이 아니라 당도/규격 표기이므로 amount에서 제외
                if re.fullmatch(r"\d+(?:\.\d+)?\s*Brix", p, flags=re.IGNORECASE):
                    dropped.append(p)
                    continue
                if _is_non_ingredient_qualifier(p):
                    dropped.append(p)
                else:
                    kept.append(p)
            if not parts:
                return None, []
            # 분리되지 않은 단일 토큰도 검사
            if len(parts) == 1 and _is_non_ingredient_qualifier(raw):
                return None, [raw]
            cleaned = ", ".join(kept).strip() if kept else None
            return cleaned, dropped

        def _is_empty_name(name: str) -> bool:
            n = str(name or "").strip().lower()
            return (not n) or (n in {"null", "none", "없음", "(없음)"})

        valid_single_char_ingredients = {
            # 실제 식품 원재료로 빈번히 등장하는 1글자 토큰
            "파", "쌀", "콩", "차", "꿀", "무", "밀", "팥", "감",
        }
        valid_single_char_ingredients_norm = {_norm_token(x) for x in valid_single_char_ingredients}

        def _can_merge_suffix_token(parent_name: str, child_name: str) -> bool:
            """
            parent-child 이름 결합은 매우 제한적으로 허용:
            1) 원문에 실제로 `parent-child` 표기가 존재
            2) child가 1글자 꼬리 토큰(예: 엔)
            3) 알레르겐/원산지/수량/정상 1글자 원재료는 제외
            """
            p = str(parent_name or "").strip()
            c = str(child_name or "").strip()
            if (not p) or (not c):
                return False
            c_norm = _norm_token(c)
            if len(c_norm) != 1:
                return False
            if c_norm in valid_single_char_ingredients_norm:
                return False
            if c in allergen_words:
                return False
            if _looks_origin_token(c):
                return False
            if re.fullmatch(r"\d+(?:\.\d+)?%?", c_norm):
                return False
            if not raw_compact:
                return False
            compound = re.sub(r"\s+", "", f"{p}-{c}")
            return bool(compound and (compound in raw_compact))

        def _try_merge_chemical_prefix(parent_name: str, child_name: str) -> str | None:
            """
            화학 접두 표기 복원:
            - DL-알파토코페롤, L-시스틴, D-소르비톨, d-토코페롤 등
            - 원문에 실제 compound가 있을 때만 복원
            """
            p = str(parent_name or "").strip()
            c = str(child_name or "").strip()
            if (not p) or (not c) or (not raw_compact):
                return None
            p_norm = _norm_token(p)
            c_norm = _norm_token(c)
            if (not p_norm) or (not c_norm):
                return None
            # 접두어만 부모명으로 분리된 경우만 허용
            if p_norm.lower() not in {"dl", "d", "l"}:
                return None
            # 너무 짧은/노이즈 child 제외
            if len(c_norm) <= 1:
                return None
            # 원산지/알레르겐/수량 토큰과 결합 금지
            if _looks_origin_token(c):
                return None
            if c in allergen_words:
                return None
            if re.fullmatch(r"\d+(?:\.\d+)?%?", c_norm):
                return None
            compound = re.sub(r"\s+", "", f"{p}-{c}")
            if compound in raw_compact:
                return f"{p}-{c}"
            return None

        def _origin_name_to_country_label(name: str) -> str:
            t = _norm_token(name)
            if t.endswith("산"):
                core = t[:-1]
                if core in country_words:
                    return core
            return str(name or "").strip()

        def _parse_origin_amount_token(text: str) -> tuple[str | None, str | None]:
            s = _norm_token(text)
            if not s:
                return None, None
            m = re.match(r"^(.+?산)(\d+(?:\.\d+)?%)$", s)
            if m:
                return m.group(1), m.group(2)
            return None, None

        def _split_name_percent_suffix(name: str) -> tuple[str, str | None]:
            """
            예: 유자과즙3% -> (유자과즙, 3%)
            """
            s = str(name or "").strip()
            if not s:
                return s, None
            m = re.match(r"^(.*?)(\d+(?:\.\d+)?%)$", s)
            if not m:
                return s, None
            base = str(m.group(1) or "").strip()
            pct = str(m.group(2) or "").strip()
            if (not base) or (not pct):
                return s, None
            return base, pct

        def _walk(node: dict[str, Any]) -> None:
            stats["nodes"] += 1
            # 이름에 원산지 꼬리(괄호 미닫힘 포함)가 붙은 경우 분리
            name_key = "ingredient_name" if ("ingredient_name" in node) else ("name" if ("name" in node) else None)
            node_name_text = ""
            if name_key:
                raw_name_input = str(node.get(name_key) or "").strip()
                raw_name, had_include_suffix = _strip_include_suffix(raw_name_input)
                # 알레르기 안내(우유포함/대두함유 등)는 별도 필드에도 저장하고 트리에도 유지
                if had_include_suffix and (raw_name in allergen_words):
                    _append_allergen_token(raw_name)
                node[name_key] = raw_name or None
                if raw_name:
                    # name 끝 %는 amount로 분리
                    base_name, inline_pct = _split_name_percent_suffix(raw_name)
                    if inline_pct:
                        # amount가 이미 있어도 이름의 % 꼬리는 항상 제거
                        node[name_key] = base_name
                        if not str(node.get("amount") or "").strip():
                            node["amount"] = inline_pct
                        raw_name = base_name
                    # "에티오피아산" 같은 원산지명이 원재료명으로 들어오면
                    # origin으로 승격하고, name은 국가명으로 축약해 중복 트리 생성을 줄인다.
                    if _looks_origin_token(raw_name):
                        node_origin = str(node.get("origin") or "").strip()
                        if not node_origin:
                            _merge_origin_tokens(node, [raw_name])
                        node[name_key] = _origin_name_to_country_label(raw_name) or raw_name

                    # 예: 야자유(외국산, 야자유(외국산), 야자유[외국산]
                    m = re.match(r"^(?P<base>.+?)[\(\[]\s*(?P<orig>[^\)\]]+)\s*[\)\]]?\s*$", raw_name)
                    if m:
                        base_name = str(m.group("base") or "").strip()
                        origin_cand = str(m.group("orig") or "").strip()
                        origin_parts = _split_origin_chunks(origin_cand)
                        if base_name and origin_parts and all(_looks_origin_token(x) for x in origin_parts):
                            node[name_key] = base_name
                            _merge_origin_tokens(node, origin_parts)
                node_name_text = str(node.get(name_key) or "").strip()

            amount_val = str(node.get("amount") or "").strip()
            if amount_val:
                cleaned_amount, dropped_amount = _clean_amount_value(amount_val)
                if cleaned_amount != (amount_val or None):
                    node["amount"] = cleaned_amount
                for d in dropped_amount:
                    _append_ignored_fragment(d)
                    stats["dropped_qualifier_sub"] += 1

            origin_val = str(node.get("origin") or "").strip()
            if origin_val:
                tokens = _split_origin_chunks(origin_val)
                valid: list[str] = []
                invalid: list[str] = []
                for tok in tokens:
                    if _looks_origin_token(tok):
                        t = _norm_token(tok)
                        amt = ""
                        m_amt = re.search(r"(\d+(?:\.\d+)?%)$", t)
                        if m_amt:
                            amt = m_amt.group(1)
                            t = t[: -len(amt)]
                        if t in country_words and (not t.endswith("산")):
                            t = f"{t}산"
                        elif t in {"국내", "외국", "수입", "기타외국"}:
                            t = f"{t}산"
                        if t:
                            valid.append(f"{t} {amt}".strip() if amt else t)
                    else:
                        invalid.append(str(tok).strip())
                # 중복 제거(순서 유지)
                seen: set[str] = set()
                valid = [x for x in valid if (not (x in seen or seen.add(x)))]
                invalid = [x for x in invalid if x]
                cleaned = ", ".join(valid) if valid else None
                if cleaned != (origin_val or None):
                    node["origin"] = cleaned
                    stats["origin_cleaned"] += 1
                for bad in invalid:
                    _push_sub(node, bad)
                    stats["moved_to_sub"] += 1
            children = node.get("sub_ingredients")
            if isinstance(children, list):
                kept: list[dict[str, Any]] = []
                lifted_tokens: list[str] = []
                for c in children:
                    if isinstance(c, dict):
                        child_name_key = "name" if ("name" in c) else ("ingredient_name" if ("ingredient_name" in c) else None)
                        child_name_raw = str(c.get("name") or c.get("ingredient_name") or "").strip()
                        child_name, child_had_include_suffix = _strip_include_suffix(child_name_raw)
                        if child_had_include_suffix and (child_name in allergen_words):
                            _append_allergen_token(child_name)
                        if child_name in allergen_words:
                            _append_allergen_token(child_name)
                        if child_name_key:
                            c[child_name_key] = child_name or None
                        # child name 끝 %는 amount로 분리
                        if child_name:
                            c_base_name, c_inline_pct = _split_name_percent_suffix(child_name)
                            if c_inline_pct:
                                # amount가 이미 있어도 이름의 % 꼬리는 항상 제거
                                c[child_name_key] = c_base_name
                                if not str(c.get("amount") or "").strip():
                                    c["amount"] = c_inline_pct
                                child_name = c_base_name
                        child_origin = str(c.get("origin") or "").strip()
                        child_amount = str(c.get("amount") or "").strip()
                        child_sub = c.get("sub_ingredients")
                        child_has_nested = isinstance(child_sub, list) and any(isinstance(x, dict) for x in child_sub)

                        # 부모와 동일명 wrapper 노드 평탄화
                        # 예: 식물성크림[식물성크림(...)] -> 내부 하위항목을 부모 직계로 승격
                        if (
                            child_name
                            and node_name_text
                            and (_norm_token(child_name) == _norm_token(node_name_text))
                            and (not child_origin)
                            and (not child_amount)
                            and isinstance(child_sub, list)
                            and any(isinstance(x, dict) for x in child_sub)
                        ):
                            for gc in child_sub:
                                if isinstance(gc, dict):
                                    _walk(gc)
                                    kept.append(gc)
                            stats["lifted_from_sub"] += 1
                            continue

                        # 부모와 완전히 같은 자기복제 하위노드는 제거
                        # 예: 대두유[대두유, d-토코페롤, ...]
                        if (
                            child_name
                            and node_name_text
                            and (_norm_token(child_name) == _norm_token(node_name_text))
                            and (not child_origin)
                            and (not child_amount)
                            and (not child_has_nested)
                        ):
                            stats["dropped_qualifier_sub"] += 1
                            continue

                        # 잘못 분해된 접미 토큰 복원(원문 근거 기반)
                        # 예: 코치닐색소-엔 -> parent=코치닐색소, child=엔
                        if (
                            child_name_key
                            and child_name
                            and (not child_origin)
                            and (not child_amount)
                            and (not child_has_nested)
                            and node_name_text
                            and (not str(node_name_text).endswith(f"-{child_name}"))
                            and _can_merge_suffix_token(node_name_text, child_name)
                        ):
                            merged_name = f"{node_name_text}-{child_name}".strip("-")
                            if name_key and merged_name:
                                node[name_key] = merged_name
                                node_name_text = merged_name
                                stats["origin_cleaned"] += 1
                                continue

                        # 화학 접두어 결합 복원(원문 근거 기반)
                        # 예: DL + 알파토코페롤 -> DL-알파토코페롤
                        merged_chem = _try_merge_chemical_prefix(node_name_text, child_name)
                        if (
                            child_name_key
                            and merged_chem
                            and (not child_origin)
                            and (not child_amount)
                            and (not child_has_nested)
                            and name_key
                        ):
                            node[name_key] = merged_chem
                            node_name_text = merged_chem
                            stats["origin_cleaned"] += 1
                            continue

                        # 원산지+함량 결합 토큰 처리
                        # 예: 중국산75% -> name=중국, origin=중국산, amount=75%
                        p_origin, p_amt = _parse_origin_amount_token(child_name)
                        if p_origin and p_amt:
                            if not child_origin:
                                _merge_origin_tokens(c, [p_origin])
                            c["amount"] = p_amt
                            child_amount = p_amt
                            if child_name_key:
                                c[child_name_key] = _origin_name_to_country_label(p_origin) or child_name
                            child_name = str(c.get(child_name_key) or "").strip() if child_name_key else child_name

                        # 퍼센트 단독 토큰 + 부모 origin 존재 시 첫 origin에 귀속
                        # 예: (국산:25%,중국산75%)의 "25%" 처리
                        if (not p_origin) and re.fullmatch(r"\d+(?:\.\d+)?%", _norm_token(child_name or "")) and (not child_origin):
                            parent_origin_tokens = _split_origin_chunks(str(node.get("origin") or ""))
                            if parent_origin_tokens:
                                base_origin = parent_origin_tokens[0]
                                _merge_origin_tokens(c, [base_origin])
                                c["amount"] = _norm_token(child_name)
                                child_amount = str(c.get("amount") or "").strip()
                                if child_name_key:
                                    c[child_name_key] = _origin_name_to_country_label(base_origin) or child_name
                                child_name = str(c.get(child_name_key) or "").strip() if child_name_key else child_name

                        # leaf 원산지 토큰은 부모 origin으로 승격
                        if child_name and (not child_amount) and (not child_has_nested):
                            child_origin_parts = _split_origin_chunks(child_name)
                            if child_origin_parts and all(_looks_origin_token(x) for x in child_origin_parts):
                                lifted_tokens.extend(child_origin_parts)
                                stats["lifted_from_sub"] += 1
                                continue

                        # 자기복제 하위노드 제거:
                        # 예) 에티오피아산(44.4%) -> 하위 [에티오피아산]
                        if isinstance(child_sub, list) and len(child_sub) == 1 and isinstance(child_sub[0], dict):
                            only = child_sub[0]
                            only_name = str(only.get("name") or only.get("ingredient_name") or "").strip()
                            only_origin = str(only.get("origin") or "").strip()
                            only_amount = str(only.get("amount") or "").strip()
                            only_sub = only.get("sub_ingredients")
                            if (
                                only_name
                                and child_name
                                and (_norm_token(only_name) == _norm_token(child_name))
                                and (not only_origin)
                                and (not only_amount)
                                and (not (isinstance(only_sub, list) and any(isinstance(x, dict) for x in only_sub)))
                            ):
                                c["sub_ingredients"] = []
                                child_sub = []
                                child_has_nested = False

                        # "국가산 + 함량" 패턴은 원산지 분포 노드로 정규화
                        # name은 국가명으로 축약, origin은 국가산으로 유지
                        if child_name and _looks_origin_token(child_name):
                            if not str(c.get("origin") or "").strip():
                                _merge_origin_tokens(c, [child_name])
                            if child_name_key:
                                c[child_name_key] = _origin_name_to_country_label(child_name) or child_name
                            child_name = str(c.get(child_name_key) or "").strip() if child_name_key else child_name

                        # 원산지 분포 노드는 부모 origin으로 승격하고 하위에서는 제거
                        # 예: 포도과즙(이탈리아산67%, 스페인산33%)에서
                        #     하위 [{name:이탈리아, origin:이탈리아산, amount:67%}, ...] 제거
                        child_origin_now = str(c.get("origin") or "").strip()
                        child_amount_now = str(c.get("amount") or "").strip()
                        child_sub_now = c.get("sub_ingredients")
                        child_has_nested_now = isinstance(child_sub_now, list) and any(isinstance(x, dict) for x in child_sub_now)
                        if (
                            child_origin_now
                            and child_name
                            and _looks_origin_token(child_name)
                            and _looks_origin_token(child_origin_now)
                            and re.fullmatch(r"\d+(?:\.\d+)?%", _norm_token(child_amount_now or ""))
                            and (not child_has_nested_now)
                        ):
                            lifted_tokens.append(f"{_norm_token(child_origin_now)}{_norm_token(child_amount_now)}")
                            stats["lifted_from_sub"] += 1
                            continue

                        # "세균수기준" 같은 설명성 문구는 하위 원재료에서 제거
                        if child_name and _is_non_ingredient_qualifier(child_name) and (not child_origin) and (not child_amount) and (not child_has_nested):
                            _append_ignored_fragment(child_name)
                            stats["dropped_qualifier_sub"] += 1
                            continue
                        _walk(c)
                        kept.append(c)
                node["sub_ingredients"] = kept
                if lifted_tokens:
                    _merge_origin_tokens(node, lifted_tokens)

            # 최종 유효성 검사
            final_origin = str(node.get("origin") or "").strip()
            if final_origin:
                final_tokens = _split_origin_chunks(final_origin)
                if not final_tokens or any((not _looks_origin_token(t)) for t in final_tokens):
                    stats["invalid_remaining"] += 1

        def _prune_nodes(nodes: list[Any]) -> list[dict[str, Any]]:
            pruned: list[dict[str, Any]] = []
            for raw in nodes:
                if not isinstance(raw, dict):
                    continue
                node = raw
                subs_raw = node.get("sub_ingredients")
                subs = _prune_nodes(subs_raw) if isinstance(subs_raw, list) else []
                node["sub_ingredients"] = subs
                name = str(node.get("name") or node.get("ingredient_name") or "").strip()
                origin = str(node.get("origin") or "").strip()
                amount = str(node.get("amount") or "").strip()

                # "세균수기준", "강화성분제외" 같은 설명성 노드는 트리에서 제거
                if name and _is_non_ingredient_qualifier(name) and (not origin) and (not amount) and (not subs):
                    _append_ignored_fragment(name)
                    stats["dropped_qualifier_sub"] += 1
                    continue

                # 이름 없는/의미 없는 노드는 제거
                if _is_empty_name(name) and (not origin) and (not amount) and (not subs):
                    stats["dropped_qualifier_sub"] += 1
                    continue

                # 한 글자 꼬리 토큰(예: '-엔' 분리 잔여)은 노이즈로 제거
                name_norm = _norm_token(name)
                if (
                    len(name_norm) <= 1
                    and (name_norm not in valid_single_char_ingredients_norm)
                    and (not origin)
                    and (not amount)
                    and (not subs)
                ):
                    _append_ignored_fragment(name)
                    stats["dropped_qualifier_sub"] += 1
                    continue

                # 이름이 null/빈값이면 노드를 제거한다.
                # - 메타만 있는 경우: ignored_fragments로 이동
                # - 자식이 있는 경우: 자식을 부모 레벨로 승격(flatten)
                if _is_empty_name(name):
                    if origin:
                        _append_ignored_fragment(origin)
                    if amount:
                        _append_ignored_fragment(amount)
                    stats["dropped_qualifier_sub"] += 1
                    if subs:
                        pruned.extend(subs)
                    continue

                pruned.append(node)
            return pruned

        def _merge_duplicate_named_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
            merged: list[dict[str, Any]] = []
            index_by_name: dict[str, dict[str, Any]] = {}

            for raw in nodes:
                if not isinstance(raw, dict):
                    continue
                node = raw
                subs = node.get("sub_ingredients")
                if isinstance(subs, list):
                    node["sub_ingredients"] = _merge_duplicate_named_nodes(
                        [x for x in subs if isinstance(x, dict)]
                    )
                else:
                    node["sub_ingredients"] = []

                nm = str(node.get("name") or node.get("ingredient_name") or "").strip()
                key = _norm_token(nm)
                if (not nm) or (not key):
                    merged.append(node)
                    continue

                existing = index_by_name.get(key)
                if existing is None:
                    index_by_name[key] = node
                    merged.append(node)
                    continue

                # origin 통합
                ex_origin = str(existing.get("origin") or "").strip()
                nd_origin = str(node.get("origin") or "").strip()
                if nd_origin:
                    _merge_origin_tokens(existing, [nd_origin])
                elif ex_origin:
                    existing["origin"] = ex_origin

                # amount 통합
                ex_amount = str(existing.get("amount") or "").strip()
                nd_amount = str(node.get("amount") or "").strip()
                if (not ex_amount) and nd_amount:
                    existing["amount"] = nd_amount
                elif ex_amount and nd_amount and (ex_amount != nd_amount):
                    _append_ignored_fragment(nd_amount)

                # sub 통합
                ex_sub = existing.get("sub_ingredients")
                if not isinstance(ex_sub, list):
                    ex_sub = []
                nd_sub = node.get("sub_ingredients")
                if isinstance(nd_sub, list) and nd_sub:
                    ex_sub.extend([x for x in nd_sub if isinstance(x, dict)])
                    existing["sub_ingredients"] = _merge_duplicate_named_nodes(ex_sub)
                else:
                    existing["sub_ingredients"] = _merge_duplicate_named_nodes(ex_sub)

            return merged

        def _iter_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            stack: list[dict[str, Any]] = list(nodes)
            while stack:
                n = stack.pop()
                out.append(n)
                children = n.get("sub_ingredients")
                if isinstance(children, list):
                    for c in children:
                        if isinstance(c, dict):
                            stack.append(c)
            return out

        def _append_sub_if_missing(parent: dict[str, Any], name: str, amount: str | None) -> bool:
            nm = str(name or "").strip()
            if not nm:
                return False
            subs = parent.get("sub_ingredients")
            if not isinstance(subs, list):
                subs = []
            for s in subs:
                if not isinstance(s, dict):
                    continue
                sname = str(s.get("name") or s.get("ingredient_name") or "").strip()
                if sname == nm:
                    if amount and (not str(s.get("amount") or "").strip()):
                        s["amount"] = amount
                    parent["sub_ingredients"] = subs
                    return True
            subs.append(
                {
                    "name": nm,
                    "origin": None,
                    "amount": amount,
                    "sub_ingredients": [],
                }
            )
            parent["sub_ingredients"] = subs
            return True

        def _recover_ignored_include_fragments() -> None:
            ignored = payload.get("ignored_fragments")
            if not isinstance(ignored, list) or not ignored:
                return
            items = payload.get("ingredients_items")
            if not isinstance(items, list):
                return

            nodes = [n for n in _iter_nodes(items) if isinstance(n, dict)]
            name_index: dict[str, dict[str, Any]] = {}
            for n in nodes:
                nm = str(n.get("ingredient_name") or n.get("name") or "").strip()
                if nm and nm not in name_index:
                    name_index[nm] = n

            kept_ignored: list[str] = []
            for raw_frag in ignored:
                frag = str(raw_frag or "").strip()
                if not frag:
                    continue
                compact = re.sub(r"\s+", "", frag)
                # 예: 특정성분및함량:분말스프중된장분말9%함유
                if "함유" not in compact and "포함" not in compact:
                    kept_ignored.append(frag)
                    continue

                body = re.sub(r"^특정성분및함량[:：]?", "", compact)
                body = re.sub(r"(함유|포함)$", "", body)
                if not body:
                    kept_ignored.append(frag)
                    continue

                amount_match = re.search(r"(\d+(?:\.\d+)?%)", body)
                amount = amount_match.group(1) if amount_match else None
                if amount:
                    body = body.replace(amount, "")

                parent_hint = ""
                candidate = body
                if "중" in body:
                    parent_hint, candidate = body.split("중", 1)
                candidate = re.sub(r"^[\[\(\{]+|[\]\)\}]+$", "", candidate).strip()
                parent_hint = re.sub(r"^[\[\(\{]+|[\]\)\}]+$", "", parent_hint).strip()

                if (not candidate) or _is_non_ingredient_qualifier(candidate):
                    kept_ignored.append(frag)
                    continue

                recovered = False
                if parent_hint:
                    pnode = name_index.get(parent_hint)
                    if pnode is None and ("스프" in parent_hint):
                        for nname, nnode in name_index.items():
                            if "스프" in nname:
                                pnode = nnode
                                break
                    if pnode is not None:
                        recovered = _append_sub_if_missing(pnode, candidate, amount)

                if not recovered:
                    # 부모를 못 찾으면 상위 항목으로라도 복구
                    if candidate not in name_index:
                        items.append(
                            {
                                "ingredient_name": candidate,
                                "origin": None,
                                "amount": amount,
                                "sub_ingredients": [],
                            }
                        )
                        name_index[candidate] = items[-1]
                        recovered = True
                    else:
                        existing = name_index[candidate]
                        if amount and (not str(existing.get("amount") or "").strip()):
                            existing["amount"] = amount
                        recovered = True

                if not recovered:
                    kept_ignored.append(frag)

            payload["ignored_fragments"] = kept_ignored

        items = payload.get("ingredients_items")
        if not isinstance(items, list):
            return payload, stats
        for n in items:
            if isinstance(n, dict):
                _walk(n)
        payload["ingredients_items"] = _merge_duplicate_named_nodes(_prune_nodes(items))
        _recover_ignored_include_fragments()
        if not isinstance(payload.get("allergen_tokens"), list):
            payload["allergen_tokens"] = []
        # ignored_fragments와 allergen_tokens가 중복되면 allergen 쪽만 유지
        ignored = payload.get("ignored_fragments")
        allergens = payload.get("allergen_tokens")
        if isinstance(ignored, list) and isinstance(allergens, list):
            allergen_set = {str(x).strip() for x in allergens if str(x).strip()}
            payload["ignored_fragments"] = [
                x for x in ignored if str(x or "").strip() and (str(x).strip() not in allergen_set)
            ]
        return payload, stats

    def _build_processed_nutrition_payload(row: dict[str, Any]) -> dict[str, Any]:

        # processed_food_info의 구조화 컬럼을 nutrition_items로 매핑
        field_defs = [
            ("enerc", "열량", "kcal"),
            ("chocdf", "탄수화물", "g"),
            ("prot", "단백질", "g"),
            ("fatce", "지방", "g"),
            ("sugar", "당류", "g"),
            ("nat", "나트륨", "mg"),
            ("water", "수분", "g"),
            ("ash", "회분", "g"),
            ("fibtg", "식이섬유", "g"),
            ("ca", "칼슘", "mg"),
            ("fe", "철", "mg"),
            ("p", "인", "mg"),
            ("k", "칼륨", "mg"),
            ("vitaRae", "비타민A", "μg RAE"),
            ("retol", "레티놀", "μg"),
            ("cartb", "베타카로틴", "μg"),
            ("thia", "비타민B1", "mg"),
            ("ribf", "비타민B2", "mg"),
            ("nia", "나이아신", "mg"),
            ("vitc", "비타민C", "mg"),
            ("vitd", "비타민D", "μg"),
            ("chole", "콜레스테롤", "mg"),
            ("fasat", "포화지방산", "g"),
            ("fatrn", "트랜스지방산", "g"),
        ]
        items: list[dict[str, Any]] = []
        for col, name, unit in field_defs:
            val = str(row[col] or "").strip()
            if not val:
                continue
            items.append(
                {
                    "name": name,
                    "value": val,
                    "unit": unit,
                    "daily_value": None,
                }
            )
        base_txt = str(row["nutConSrtrQua"] or "").strip()
        serv_size = str(row.get("servSize") or "").strip()
        food_size = str(row.get("foodSize") or "").strip()
        if base_txt:
            items.insert(
                0,
                {
                    "name": "기준량",
                    "value": base_txt,
                    "unit": None,
                    "daily_value": None,
                },
            )
        basis = _extract_nutrition_basis(base_txt) if base_txt else None
        if basis is None and serv_size:
            basis = _extract_nutrition_basis(f"1회 제공량 {serv_size}")
        if basis is None and food_size:
            basis = _extract_nutrition_basis(f"총내용량 {food_size}")
        return {
            "nutrition_items": items,
            "nutrition_basis": basis,
            "parser": "processed_food_db_v1",
            "serv_size": serv_size or None,
            "food_size": food_size or None,
        }

    def _upsert_haccp_parsed_with_retry(
        conn: sqlite3.Connection,
        *,
        report_no: str,
        ingredients_items_json: str | None,
        nutrition_items_json: str | None,
        parse_status: str,
        parse_error: str | None,
        parser_version: str,
        source_rawmtrl: str | None,
        source_nutrient: str | None,
        max_retries: int = 8,
    ) -> None:
        for attempt in range(max_retries):
            try:
                upsert_haccp_parsed_row(
                    conn,
                    report_no=report_no,
                    ingredients_items_json=ingredients_items_json,
                    nutrition_items_json=nutrition_items_json,
                    parse_status=parse_status,
                    parse_error=parse_error,
                    parser_version=parser_version,
                    source_rawmtrl=source_rawmtrl,
                    source_nutrient=source_nutrient,
                )
                return
            except sqlite3.OperationalError as exc:
                msg = str(exc).lower()
                if "database is locked" not in msg:
                    raise
                if attempt >= max_retries - 1:
                    raise
                # 짧은 지터 백오프로 잠금 해제 대기
                backoff = min(2.0, 0.15 * (2 ** attempt)) + random.uniform(0.0, 0.12)
                time.sleep(backoff)

    def _sample_stratified_report_keys(
        conn: sqlite3.Connection,
        *,
        per_bucket: int,
        bucket_limit: int,
        skip_done_ok: bool,
    ) -> tuple[list[str], dict[str, int]]:
        norm_expr = "REPLACE(REPLACE(REPLACE(h.prdlstReportNo,'-',''),' ',''),'.','')"
        where: list[str] = [
            "COALESCE(TRIM(h.prdlstReportNo),'') != ''",
            "COALESCE(TRIM(h.rawmtrl),'') != ''",
        ]
        if skip_done_ok:
            where.append(
                "NOT EXISTS ("
                "SELECT 1 FROM haccp_parsed_cache p "
                "WHERE REPLACE(REPLACE(REPLACE(p.prdlstReportNo,'-',''),' ',''),'.','') = "
                f"{norm_expr} "
                "AND p.parse_status='ok' AND p.parser_version LIKE 'pass4_%'"
                ")"
            )
        sql = f"""
        WITH base AS (
            SELECT {norm_expr} AS report_key,
                   COALESCE(NULLIF(TRIM(h.prdkind),''), '미분류') AS bucket,
                   MAX(h.id) AS max_id
            FROM haccp_product_info h
            WHERE {' AND '.join(where)}
            GROUP BY report_key, bucket
        ),
        ranked AS (
            SELECT report_key, bucket,
                   ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY RANDOM()) AS rn,
                   DENSE_RANK() OVER (ORDER BY bucket) AS bidx
            FROM base
        )
        SELECT report_key, bucket
        FROM ranked
        WHERE rn <= ? AND bidx <= ?
        ORDER BY bucket, rn
        """
        rows = conn.execute(sql, (per_bucket, bucket_limit)).fetchall()
        keys: list[str] = []
        bucket_counts: dict[str, int] = {}
        for r in rows:
            k = str(r[0] or "").strip()
            b = str(r[1] or "").strip() or "미분류"
            if not k:
                continue
            keys.append(k)
            bucket_counts[b] = bucket_counts.get(b, 0) + 1
        return keys, bucket_counts

    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        init_haccp_tables(conn)
        if use_stratified and not target_report_keys:
            sampled_keys, sampled_buckets = _sample_stratified_report_keys(
                conn,
                per_bucket=strat_per_bucket,
                bucket_limit=strat_bucket_limit,
                skip_done_ok=skip_done,
            )
            if not sampled_keys:
                print("  ℹ️ 층화 샘플링 결과가 없습니다. (스킵 조건 포함)")
                return
            target_report_keys = sampled_keys
            print(
                f"  📌 층화 샘플링 선택: 카테고리 {len(sampled_buckets):,}개, "
                f"총 대상 {len(target_report_keys):,}개"
            )
            for b, c in list(sampled_buckets.items())[:12]:
                print(f"    - {b}: {c}개")
            if len(sampled_buckets) > 12:
                print(f"    ... 외 {len(sampled_buckets) - 12}개 카테고리")

        where: list[str] = ["COALESCE(TRIM(h.prdlstReportNo),'') != ''"]
        where.append("COALESCE(TRIM(h.rawmtrl),'') != ''")
        where.append(
            "REPLACE(REPLACE(REPLACE(h.prdlstReportNo,'-',''),' ',''),'.','') NOT IN ("
            "SELECT REPLACE(REPLACE(REPLACE(b.prdlstReportNo,'-',''),' ',''),'.','') "
            "FROM haccp_parse_blocklist b WHERE COALESCE(b.is_blocked,1)=1)"
        )
        params: list[Any] = []
        if target_report_keys:
            placeholders = ",".join(["?"] * len(target_report_keys))
            where.append(
                "REPLACE(REPLACE(REPLACE(h.prdlstReportNo,'-',''),' ',''),'.','') "
                f"IN ({placeholders})"
            )
            params.extend(target_report_keys)
        if skip_done:
            where.append(
                "h.prdlstReportNo NOT IN ("
                "SELECT p.prdlstReportNo FROM haccp_parsed_cache p "
                "WHERE p.parse_status='ok' AND p.parser_version LIKE 'pass4_%'"
                ")"
            )
        sql = (
            "SELECT h.prdlstReportNo, h.prdlstNm, h.rawmtrl, h.nutrient "
            "FROM haccp_product_info h "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY h.id DESC"
        )
        if (limit > 0) and (not target_report_keys):
            sql += f" LIMIT {limit}"
        rows = conn.execute(sql, tuple(params)).fetchall()
        if not rows:
            print("  ℹ️ 처리할 대상이 없습니다. (스킵 조건 포함)")
            return
        if target_report_keys:
            print(f"  📌 지정 품목보고번호 모드: 요청 {len(target_report_keys):,}개 / 대상 조회 {len(rows):,}개")

        processed_rows = conn.execute(
            """
            SELECT itemMnftrRptNo, foodNm, nutConSrtrQua, enerc, water, prot, fatce, ash,
                   chocdf, sugar, fibtg, ca, fe, p, k, nat,
                   vitaRae, retol, cartb, thia, ribf, nia, vitc, vitd,
                   chole, fasat, fatrn, servSize, foodSize
            FROM processed_food_info
            WHERE COALESCE(TRIM(itemMnftrRptNo),'') != ''
            ORDER BY id DESC
            """
        ).fetchall()
        processed_by_report: dict[str, dict[str, Any]] = {}
        for prow in processed_rows:
            key = _normalize_report_no_key(prow["itemMnftrRptNo"])
            if not key or key in processed_by_report:
                continue
            processed_by_report[key] = {k: prow[k] for k in prow.keys()}
        prompt_dir = Path(__file__).resolve().parent / "analyzer" / "prompts"
        ing_prompt_path = prompt_dir / "analyze_haccp_pass4_ingredients_prompt.txt"
        nut_prompt_path = prompt_dir / "analyze_haccp_pass4_nutrition_prompt.txt"
        if not ing_prompt_path.exists() or not nut_prompt_path.exists():
            print("  ❌ HACCP 전용 프롬프트 파일이 없습니다.")
            print(f"    - {ing_prompt_path}")
            print(f"    - {nut_prompt_path}")
            return
        ing_prompt_template = ing_prompt_path.read_text(encoding="utf-8")
        nut_prompt_template = nut_prompt_path.read_text(encoding="utf-8")

        ok = 0
        fail = 0
        skip = 0
        skip_no_haccp_nutrient = 0
        ing_success = 0
        nut_from_processed = 0
        nut_from_haccp_pass4 = 0
        total_tasks = len(rows)
        analyzer_local = threading.local()

        def _get_worker_analyzer() -> URLIngredientAnalyzer:
            az = getattr(analyzer_local, "analyzer", None)
            if az is None:
                az = URLIngredientAnalyzer(api_key=openai_api_key)
                setattr(analyzer_local, "analyzer", az)
            return az

        def _openai_batch_chat_json(
            *,
            stage: str,
            requests_payloads: list[dict[str, Any]],
            model_name: str,
            poll_sec: int,
            max_wait_min: int,
        ) -> dict[str, dict[str, Any]]:
            if not requests_payloads:
                return {}
            sess = requests.Session()
            auth_headers = {"Authorization": f"Bearer {openai_api_key}"}
            jsonl_data = "\n".join(
                json.dumps(x, ensure_ascii=False, separators=(",", ":"))
                for x in requests_payloads
            ) + "\n"
            upload = sess.post(
                "https://api.openai.com/v1/files",
                headers=auth_headers,
                data={"purpose": "batch"},
                files={"file": (f"haccp_{stage}_input.jsonl", jsonl_data.encode("utf-8"), "application/jsonl")},
                timeout=60,
            )
            upload.raise_for_status()
            input_file_id = str((upload.json() or {}).get("id") or "").strip()
            if not input_file_id:
                raise RuntimeError(f"{stage}_batch_upload_missing_file_id")

            batch_resp = sess.post(
                "https://api.openai.com/v1/batches",
                headers={**auth_headers, "Content-Type": "application/json"},
                data=json.dumps(
                    {
                        "input_file_id": input_file_id,
                        "endpoint": "/v1/chat/completions",
                        "completion_window": "24h",
                        "metadata": {"scope": "haccp_pass4", "stage": stage},
                    },
                    ensure_ascii=False,
                ),
                timeout=60,
            )
            batch_resp.raise_for_status()
            batch_id = str((batch_resp.json() or {}).get("id") or "").strip()
            if not batch_id:
                raise RuntimeError(f"{stage}_batch_create_missing_batch_id")
            print(f"  📦 [{stage.upper()}-BATCH] 제출 완료: batch_id={batch_id}")

            terminal = {"completed", "failed", "cancelled", "expired"}
            deadline = time.time() + (max_wait_min * 60)
            output_file_id = ""
            last_status = ""
            while True:
                stat_resp = sess.get(
                    f"https://api.openai.com/v1/batches/{batch_id}",
                    headers=auth_headers,
                    timeout=30,
                )
                stat_resp.raise_for_status()
                stat = stat_resp.json() or {}
                st = str(stat.get("status") or "").strip().lower()
                if st != last_status:
                    print(f"  ⏳ [{stage.upper()}-BATCH] 상태: {st or '-'}")
                    last_status = st
                if st in terminal:
                    output_file_id = str(stat.get("output_file_id") or "").strip()
                    if st != "completed":
                        err_file = str(stat.get("error_file_id") or "").strip()
                        raise RuntimeError(
                            f"{stage}_batch_not_completed: status={st}, output_file_id={output_file_id or '-'}, error_file_id={err_file or '-'}"
                        )
                    break
                if time.time() >= deadline:
                    raise RuntimeError(f"{stage}_batch_timeout_after_{max_wait_min}min")
                time.sleep(poll_sec)

            if not output_file_id:
                raise RuntimeError(f"{stage}_batch_completed_without_output_file")

            out_resp = sess.get(
                f"https://api.openai.com/v1/files/{output_file_id}/content",
                headers=auth_headers,
                timeout=120,
            )
            out_resp.raise_for_status()
            results: dict[str, dict[str, Any]] = {}
            for raw_line in out_resp.text.splitlines():
                line = str(raw_line or "").strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                custom_id = str(row.get("custom_id") or "").strip()
                if not custom_id:
                    continue
                resp = row.get("response") if isinstance(row, dict) else None
                if not isinstance(resp, dict):
                    err = row.get("error")
                    results[custom_id] = {"ok": False, "error": f"batch_line_no_response: {err}"}
                    continue
                code = int(resp.get("status_code") or 0)
                body = resp.get("body") or {}
                if code >= 400:
                    results[custom_id] = {"ok": False, "error": f"batch_http_{code}: {str(body)[:800]}"}
                    continue
                try:
                    model_text = _extract_openai_text(body if isinstance(body, dict) else {})
                    parsed = _extract_first_json_object(model_text)
                    results[custom_id] = {
                        "ok": True,
                        "raw_text": model_text,
                        "parsed": parsed,
                    }
                except Exception as exc:
                    results[custom_id] = {"ok": False, "error": f"batch_parse_error: {exc}"}
            return results

        def _process_one(task: tuple[int, dict[str, Any]]) -> dict[str, Any]:
            idx, row = task
            report_no = str(row.get("prdlstReportNo") or "").strip()
            product_name = str(row.get("prdlstNm") or "").strip() or None
            rawmtrl = str(row.get("rawmtrl") or "").strip()
            nutrient = str(row.get("nutrient") or "").strip()
            report_key = _normalize_report_no_key(report_no)
            processed_match = processed_by_report.get(report_key) if report_key else None
            log_lines: list[str] = []
            if verbose:
                log_lines.append("\n" + "-" * 96)
                log_lines.append(f"[대상 {idx}/{total_tasks}] report_no={report_no} | product={product_name or '-'}")
                log_lines.append(f"  - processed_match={'Y' if processed_match is not None else 'N'}")

            if not rawmtrl:
                if verbose:
                    log_lines.append("  - SKIP: rawmtrl 없음")
                return {"status": "skip_raw", "log": "\n".join(log_lines)}

            haccp_nutrient_missing = (not nutrient) or (nutrient == "알수없음")
            if (processed_match is None) and haccp_nutrient_missing:
                if verbose:
                    log_lines.append("  - STOP: processed 미매칭 + HACCP nutrient 없음(또는 알수없음)")
                return {"status": "skip_no_nutrient", "log": "\n".join(log_lines)}

            try:
                az = _get_worker_analyzer()
                if processed_match is not None:
                    nut_payload = _build_processed_nutrition_payload(processed_match)
                    parser_version = "pass4_policy_v3_processed_nut"
                    p_base = str(processed_match.get("nutConSrtrQua") or "").strip()
                    p_serv = str(processed_match.get("servSize") or "").strip()
                    p_food = str(processed_match.get("foodSize") or "").strip()
                    source_nutrient = (
                        f"basis={p_base or '-'} | servSize={p_serv or '-'} | foodSize={p_food or '-'} | processed_food_info"
                    )
                    nut_source = "processed"
                    if verbose:
                        log_lines.append("  [NUT] source=processed_food_info (모델 호출 없음)")
                        log_lines.append(f"    raw(base): {source_nutrient}")
                        log_lines.append(f"    parsed: {dumps_json(nut_payload)}")
                else:
                    nut_prompt = nut_prompt_template.replace("__NUTRIENT_TEXT__", nutrient or "")
                    if verbose:
                        log_lines.append("  [NUT] source=haccp_nutrient (모델 호출)")
                        log_lines.append(f"    raw(input): {nutrient or '-'}")
                    try:
                        nut_raw_text, nut_parsed, _nut_api_raw = az._call_text_model_openai(
                            nut_prompt,
                            model_name=getattr(az, "pass4_openai_model", None),
                            timeout_sec=getattr(az, "pass4_request_timeout_sec", None),
                            max_retries=getattr(az, "pass4_model_retries", None),
                            retry_backoff_sec=getattr(az, "pass4_retry_backoff_sec", None),
                        )
                    except Exception as exc:  # pylint: disable=broad-except
                        if verbose:
                            log_lines.append(f"    FAIL: haccp_pass4_nut_error: {exc}")
                        return {
                            "status": "error",
                            "report_no": report_no,
                            "ingredients_items_json": None,
                            "nutrition_items_json": None,
                            "parse_error": f"haccp_pass4_nut_error: {exc}",
                            "parser_version": "pass4_policy_v3_error",
                            "source_rawmtrl": rawmtrl or None,
                            "source_nutrient": nutrient or None,
                            "log": "\n".join(log_lines),
                        }
                    nutrition_items = nut_parsed.get("nutrition_items") or []
                    if not isinstance(nutrition_items, list):
                        nutrition_items = []
                    nutrition_items = _ensure_kcal_nutrition_item(nutrition_items, nutrient)
                    basis_from_model = nut_parsed.get("nutrition_basis")
                    if not isinstance(basis_from_model, dict):
                        basis_from_model = None
                    if isinstance(basis_from_model, dict):
                        basis_from_model = {
                            "per_amount": (str(basis_from_model.get("per_amount") or "").strip() or None),
                            "per_unit": (str(basis_from_model.get("per_unit") or "").strip().lower() or None),
                            "basis_text": (str(basis_from_model.get("basis_text") or "").strip() or None),
                        }
                    text_basis = _extract_nutrition_basis(nutrient)
                    # 규칙 보정:
                    # - 1회 제공량에서 g/ml 추출 가능하면 그 기준 우선
                    # - g/ml가 없고 개수 단위만 있으면 개수 단위 사용
                    if text_basis and isinstance(text_basis, dict):
                        t_unit = str(text_basis.get("per_unit") or "").strip().lower()
                        m_unit = str((basis_from_model or {}).get("per_unit") or "").strip().lower()
                        if (not basis_from_model):
                            basis_from_model = text_basis
                        elif t_unit in {"g", "ml"} and m_unit not in {"g", "ml"}:
                            basis_from_model = text_basis
                        elif (t_unit not in {"g", "ml"}) and (not m_unit):
                            basis_from_model = text_basis
                    nut_payload = {
                        "nutrition_items": nutrition_items,
                        "nutrition_basis": basis_from_model,
                        "parser": "haccp_pass4_nut_v1",
                        "raw_model_text": nut_raw_text,
                    }
                    parser_version = "pass4_policy_v3_haccp_nut"
                    source_nutrient = nutrient or None
                    nut_source = "haccp"
                    if verbose:
                        log_lines.append(f"    raw(model): {nut_raw_text or '-'}")
                        log_lines.append(f"    parsed: {dumps_json(nut_payload)}")

                ing_prompt = ing_prompt_template.replace("__RAWMTRL_TEXT__", rawmtrl or "")
                if verbose:
                    log_lines.append("  [ING] source=haccp_rawmtrl (모델 호출)")
                    log_lines.append(f"    raw(input): {rawmtrl or '-'}")
                try:
                    ing_raw_text, ing_parsed, _ing_api_raw = az._call_text_model_openai(
                        ing_prompt,
                        model_name=getattr(az, "pass4_openai_model", None),
                        timeout_sec=getattr(az, "pass4_request_timeout_sec", None),
                        max_retries=getattr(az, "pass4_model_retries", None),
                        retry_backoff_sec=getattr(az, "pass4_retry_backoff_sec", None),
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    if verbose:
                        log_lines.append(f"    FAIL: haccp_pass4_ing_error: {exc}")
                    return {
                        "status": "error",
                        "report_no": report_no,
                        "ingredients_items_json": None,
                        "nutrition_items_json": dumps_json(nut_payload),
                        "parse_error": f"haccp_pass4_ing_error: {exc}",
                        "parser_version": "pass4_policy_v3_error",
                        "source_rawmtrl": rawmtrl or None,
                        "source_nutrient": source_nutrient,
                        "log": "\n".join(log_lines),
                    }
                ingredient_items = ing_parsed.get("ingredients_items") or []
                if not isinstance(ingredient_items, list):
                    ingredient_items = []
                if not ingredient_items:
                    if verbose:
                        log_lines.append("    FAIL: haccp_pass4_ing_empty_or_invalid")
                    return {
                        "status": "error",
                        "report_no": report_no,
                        "ingredients_items_json": None,
                        "nutrition_items_json": dumps_json(nut_payload),
                        "parse_error": "haccp_pass4_ing_empty_or_invalid",
                        "parser_version": "pass4_policy_v3_error",
                        "source_rawmtrl": rawmtrl or None,
                        "source_nutrient": source_nutrient,
                        "log": "\n".join(log_lines),
                    }

                ing_payload = {
                    "ingredients_items": ingredient_items,
                    "ignored_fragments": list(ing_parsed.get("ignored_fragments") or []),
                    "parser": "haccp_pass4_ing_v1",
                    "raw_model_text": ing_raw_text,
                }
                ing_payload, origin_stats = _sanitize_ingredients_origin_fields(
                    ing_payload,
                    raw_ingredients_text=rawmtrl,
                )
                if int(origin_stats.get("invalid_remaining") or 0) > 0:
                    return {
                        "status": "error",
                        "report_no": report_no,
                        "ingredients_items_json": dumps_json(ing_payload),
                        "nutrition_items_json": dumps_json(nut_payload),
                        "parse_error": "haccp_pass4_ing_invalid_origin_field",
                        "parser_version": "pass4_policy_v3_error",
                        "source_rawmtrl": rawmtrl or None,
                        "source_nutrient": source_nutrient,
                        "log": "\n".join(log_lines),
                    }
                if verbose:
                    log_lines.append(f"    raw(model): {ing_raw_text or '-'}")
                    log_lines.append(f"    parsed: {dumps_json(ing_payload)}")
                    log_lines.append("  - SAVE: ok")
                return {
                    "status": "ok",
                    "report_no": report_no,
                    "ingredients_items_json": dumps_json(ing_payload),
                    "nutrition_items_json": dumps_json(nut_payload),
                    "parse_error": None,
                    "parser_version": parser_version,
                    "source_rawmtrl": rawmtrl or None,
                    "source_nutrient": source_nutrient,
                    "nut_source": nut_source,
                    "ing_has_items": bool(ing_payload.get("ingredients_items")),
                    "log": "\n".join(log_lines),
                }
            except Exception as exc:  # pylint: disable=broad-except
                if verbose:
                    log_lines.append(f"  - FAIL: unexpected_error: {exc}")
                return {
                    "status": "error",
                    "report_no": report_no,
                    "ingredients_items_json": None,
                    "nutrition_items_json": None,
                    "parse_error": str(exc),
                    "parser_version": "pass4_policy_v3_error",
                    "source_rawmtrl": rawmtrl or None,
                    "source_nutrient": nutrient or None,
                    "log": "\n".join(log_lines),
                }

        def _print_progress_bar(completed_count: int) -> None:
            width = 34
            ratio = (completed_count / total_tasks) if total_tasks > 0 else 1.0
            filled = int(width * ratio)
            bar = ("█" * filled) + ("-" * (width - filled))
            stage = "NUT->ING->SAVE"
            sys.stdout.write(
                f"\r  진행률 [{bar}] {completed_count:,}/{total_tasks:,} ({ratio * 100:5.1f}%) | 단계: {stage}"
            )
            sys.stdout.flush()

        def _handle_result(result: dict[str, Any], completed_count: int) -> None:
            nonlocal ok, fail, skip, skip_no_haccp_nutrient, ing_success, nut_from_processed, nut_from_haccp_pass4
            status = str(result.get("status") or "")
            if verbose:
                log_text = str(result.get("log") or "").strip()
                if log_text:
                    print(log_text)
            if status == "skip_raw":
                skip += 1
            elif status == "skip_no_nutrient":
                skip_no_haccp_nutrient += 1
            elif status == "ok":
                _upsert_haccp_parsed_with_retry(
                    conn,
                    report_no=str(result.get("report_no") or ""),
                    ingredients_items_json=result.get("ingredients_items_json"),
                    nutrition_items_json=result.get("nutrition_items_json"),
                    parse_status="ok",
                    parse_error=None,
                    parser_version=str(result.get("parser_version") or "pass4_policy_v3_haccp_nut"),
                    source_rawmtrl=result.get("source_rawmtrl"),
                    source_nutrient=result.get("source_nutrient"),
                )
                ok += 1
                if bool(result.get("ing_has_items")):
                    ing_success += 1
                if str(result.get("nut_source") or "") == "processed":
                    nut_from_processed += 1
                elif str(result.get("nut_source") or "") == "haccp":
                    nut_from_haccp_pass4 += 1
            else:
                _upsert_haccp_parsed_with_retry(
                    conn,
                    report_no=str(result.get("report_no") or ""),
                    ingredients_items_json=result.get("ingredients_items_json"),
                    nutrition_items_json=result.get("nutrition_items_json"),
                    parse_status="error",
                    parse_error=str(result.get("parse_error") or "unknown_error"),
                    parser_version=str(result.get("parser_version") or "pass4_policy_v3_error"),
                    source_rawmtrl=result.get("source_rawmtrl"),
                    source_nutrient=result.get("source_nutrient"),
                )
                fail += 1

            _print_progress_bar(completed_count)

        tasks = [
            (
                idx,
                {
                    "prdlstReportNo": row[0],
                    "prdlstNm": row[1],
                    "rawmtrl": row[2],
                    "nutrient": row[3],
                },
            )
            for idx, row in enumerate(rows, start=1)
        ]
        completed = 0
        if use_batch:
            az = _get_worker_analyzer()
            batch_model = str(getattr(az, "pass4_openai_model", None) or az.model).strip()
            pending_items: list[dict[str, Any]] = []
            nut_batch_requests: list[dict[str, Any]] = []

            for idx, row in tasks:
                report_no = str(row.get("prdlstReportNo") or "").strip()
                rawmtrl = str(row.get("rawmtrl") or "").strip()
                nutrient = str(row.get("nutrient") or "").strip()
                report_key = _normalize_report_no_key(report_no)
                processed_match = processed_by_report.get(report_key) if report_key else None

                if not rawmtrl:
                    completed += 1
                    _handle_result({"status": "skip_raw"}, completed)
                    continue

                haccp_nutrient_missing = (not nutrient) or (nutrient == "알수없음")
                if (processed_match is None) and haccp_nutrient_missing:
                    completed += 1
                    _handle_result({"status": "skip_no_nutrient"}, completed)
                    continue

                item: dict[str, Any] = {
                    "idx": idx,
                    "report_no": report_no,
                    "rawmtrl": rawmtrl,
                    "nutrient": nutrient,
                    "processed_match": processed_match,
                    "nut_payload": None,
                    "source_nutrient": None,
                    "nut_source": None,
                    "nut_custom_id": None,
                    "ing_custom_id": f"ing:{idx}:{report_no}",
                }
                if processed_match is not None:
                    nut_payload = _build_processed_nutrition_payload(processed_match)
                    p_base = str(processed_match.get("nutConSrtrQua") or "").strip()
                    p_serv = str(processed_match.get("servSize") or "").strip()
                    p_food = str(processed_match.get("foodSize") or "").strip()
                    source_nutrient = (
                        f"basis={p_base or '-'} | servSize={p_serv or '-'} | foodSize={p_food or '-'} | processed_food_info"
                    )
                    item["nut_payload"] = nut_payload
                    item["source_nutrient"] = source_nutrient
                    item["nut_source"] = "processed"
                else:
                    nut_prompt = nut_prompt_template.replace("__NUTRIENT_TEXT__", nutrient or "")
                    cid = f"nut:{idx}:{report_no}"
                    item["nut_custom_id"] = cid
                    nut_batch_requests.append(
                        {
                            "custom_id": cid,
                            "method": "POST",
                            "url": "/v1/chat/completions",
                            "body": {
                                "model": batch_model,
                                "messages": [{"role": "user", "content": nut_prompt}],
                                "temperature": 0,
                                "response_format": {"type": "json_object"},
                            },
                        }
                    )
                pending_items.append(item)

            if nut_batch_requests:
                print(f"  📦 NUT Batch 요청 생성: {len(nut_batch_requests):,}건")
                try:
                    nut_outputs = _openai_batch_chat_json(
                        stage="nut",
                        requests_payloads=nut_batch_requests,
                        model_name=batch_model,
                        poll_sec=batch_poll_sec,
                        max_wait_min=batch_max_wait_min,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    for item in pending_items:
                        if item.get("nut_payload") is not None:
                            continue
                        completed += 1
                        _handle_result(
                            {
                                "status": "error",
                                "report_no": str(item.get("report_no") or ""),
                                "ingredients_items_json": None,
                                "nutrition_items_json": None,
                                "parse_error": f"haccp_pass4_nut_batch_error: {exc}",
                                "parser_version": "pass4_policy_v3_error",
                                "source_rawmtrl": str(item.get("rawmtrl") or ""),
                                "source_nutrient": str(item.get("nutrient") or ""),
                            },
                            completed,
                        )
                    pending_items = [x for x in pending_items if x.get("nut_payload") is not None]
                    nut_outputs = {}

                for item in pending_items:
                    if item.get("nut_payload") is not None:
                        continue
                    cid = str(item.get("nut_custom_id") or "")
                    out = nut_outputs.get(cid) or {}
                    if not bool(out.get("ok")):
                        completed += 1
                        _handle_result(
                            {
                                "status": "error",
                                "report_no": str(item.get("report_no") or ""),
                                "ingredients_items_json": None,
                                "nutrition_items_json": None,
                                "parse_error": f"haccp_pass4_nut_error: {str(out.get('error') or 'batch_result_missing')}",
                                "parser_version": "pass4_policy_v3_error",
                                "source_rawmtrl": str(item.get("rawmtrl") or ""),
                                "source_nutrient": str(item.get("nutrient") or ""),
                            },
                            completed,
                        )
                        item["failed"] = True
                        continue
                    nut_parsed = out.get("parsed") if isinstance(out.get("parsed"), dict) else {}
                    nut_raw_text = str(out.get("raw_text") or "").strip()
                    nutrition_items = nut_parsed.get("nutrition_items") or []
                    if not isinstance(nutrition_items, list):
                        nutrition_items = []
                    nutrition_items = _ensure_kcal_nutrition_item(nutrition_items, str(item.get("nutrient") or ""))
                    basis_from_model = nut_parsed.get("nutrition_basis")
                    if not isinstance(basis_from_model, dict):
                        basis_from_model = None
                    if isinstance(basis_from_model, dict):
                        basis_from_model = {
                            "per_amount": (str(basis_from_model.get("per_amount") or "").strip() or None),
                            "per_unit": (str(basis_from_model.get("per_unit") or "").strip().lower() or None),
                            "basis_text": (str(basis_from_model.get("basis_text") or "").strip() or None),
                        }
                    text_basis = _extract_nutrition_basis(str(item.get("nutrient") or ""))
                    if text_basis and isinstance(text_basis, dict):
                        t_unit = str(text_basis.get("per_unit") or "").strip().lower()
                        m_unit = str((basis_from_model or {}).get("per_unit") or "").strip().lower()
                        if (not basis_from_model):
                            basis_from_model = text_basis
                        elif t_unit in {"g", "ml"} and m_unit not in {"g", "ml"}:
                            basis_from_model = text_basis
                        elif (t_unit not in {"g", "ml"}) and (not m_unit):
                            basis_from_model = text_basis
                    item["nut_payload"] = {
                        "nutrition_items": nutrition_items,
                        "nutrition_basis": basis_from_model,
                        "parser": "haccp_pass4_nut_v1",
                        "raw_model_text": nut_raw_text,
                    }
                    item["source_nutrient"] = str(item.get("nutrient") or "") or None
                    item["nut_source"] = "haccp"

            ing_batch_requests: list[dict[str, Any]] = []
            ready_items = [x for x in pending_items if (not x.get("failed")) and isinstance(x.get("nut_payload"), dict)]
            for item in ready_items:
                ing_prompt = ing_prompt_template.replace("__RAWMTRL_TEXT__", str(item.get("rawmtrl") or ""))
                ing_batch_requests.append(
                    {
                        "custom_id": str(item.get("ing_custom_id") or ""),
                        "method": "POST",
                        "url": "/v1/chat/completions",
                        "body": {
                            "model": batch_model,
                            "messages": [{"role": "user", "content": ing_prompt}],
                            "temperature": 0,
                            "response_format": {"type": "json_object"},
                        },
                    }
                )

            ing_outputs: dict[str, dict[str, Any]] = {}
            if ing_batch_requests:
                print(f"  📦 ING Batch 요청 생성: {len(ing_batch_requests):,}건")
                try:
                    ing_outputs = _openai_batch_chat_json(
                        stage="ing",
                        requests_payloads=ing_batch_requests,
                        model_name=batch_model,
                        poll_sec=batch_poll_sec,
                        max_wait_min=batch_max_wait_min,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    for item in ready_items:
                        completed += 1
                        _handle_result(
                            {
                                "status": "error",
                                "report_no": str(item.get("report_no") or ""),
                                "ingredients_items_json": None,
                                "nutrition_items_json": dumps_json(item.get("nut_payload") or {}),
                                "parse_error": f"haccp_pass4_ing_batch_error: {exc}",
                                "parser_version": "pass4_policy_v3_error",
                                "source_rawmtrl": str(item.get("rawmtrl") or ""),
                                "source_nutrient": item.get("source_nutrient"),
                            },
                            completed,
                        )
                    ready_items = []

            for item in ready_items:
                out = ing_outputs.get(str(item.get("ing_custom_id") or "")) or {}
                nut_payload = item.get("nut_payload") or {}
                if not bool(out.get("ok")):
                    completed += 1
                    _handle_result(
                        {
                            "status": "error",
                            "report_no": str(item.get("report_no") or ""),
                            "ingredients_items_json": None,
                            "nutrition_items_json": dumps_json(nut_payload),
                            "parse_error": f"haccp_pass4_ing_error: {str(out.get('error') or 'batch_result_missing')}",
                            "parser_version": "pass4_policy_v3_error",
                            "source_rawmtrl": str(item.get("rawmtrl") or ""),
                            "source_nutrient": item.get("source_nutrient"),
                        },
                        completed,
                    )
                    continue
                ing_parsed = out.get("parsed") if isinstance(out.get("parsed"), dict) else {}
                ing_raw_text = str(out.get("raw_text") or "").strip()
                ingredient_items = ing_parsed.get("ingredients_items") or []
                if not isinstance(ingredient_items, list):
                    ingredient_items = []
                if not ingredient_items:
                    completed += 1
                    _handle_result(
                        {
                            "status": "error",
                            "report_no": str(item.get("report_no") or ""),
                            "ingredients_items_json": None,
                            "nutrition_items_json": dumps_json(nut_payload),
                            "parse_error": "haccp_pass4_ing_empty_or_invalid",
                            "parser_version": "pass4_policy_v3_error",
                            "source_rawmtrl": str(item.get("rawmtrl") or ""),
                            "source_nutrient": item.get("source_nutrient"),
                        },
                        completed,
                    )
                    continue
                ing_payload = {
                    "ingredients_items": ingredient_items,
                    "ignored_fragments": list(ing_parsed.get("ignored_fragments") or []),
                    "parser": "haccp_pass4_ing_v1",
                    "raw_model_text": ing_raw_text,
                }
                ing_payload, origin_stats = _sanitize_ingredients_origin_fields(
                    ing_payload,
                    raw_ingredients_text=str(item.get("rawmtrl") or ""),
                )
                if int(origin_stats.get("invalid_remaining") or 0) > 0:
                    completed += 1
                    _handle_result(
                        {
                            "status": "error",
                            "report_no": str(item.get("report_no") or ""),
                            "ingredients_items_json": dumps_json(ing_payload),
                            "nutrition_items_json": dumps_json(nut_payload),
                            "parse_error": "haccp_pass4_ing_invalid_origin_field",
                            "parser_version": "pass4_policy_v3_error",
                            "source_rawmtrl": str(item.get("rawmtrl") or ""),
                            "source_nutrient": item.get("source_nutrient"),
                        },
                        completed,
                    )
                    continue
                parser_version = (
                    "pass4_policy_v3_processed_nut"
                    if str(item.get("nut_source") or "") == "processed"
                    else "pass4_policy_v3_haccp_nut"
                )
                completed += 1
                _handle_result(
                    {
                        "status": "ok",
                        "report_no": str(item.get("report_no") or ""),
                        "ingredients_items_json": dumps_json(ing_payload),
                        "nutrition_items_json": dumps_json(nut_payload),
                        "parse_error": None,
                        "parser_version": parser_version,
                        "source_rawmtrl": str(item.get("rawmtrl") or ""),
                        "source_nutrient": item.get("source_nutrient"),
                        "nut_source": str(item.get("nut_source") or ""),
                        "ing_has_items": bool(ing_payload.get("ingredients_items")),
                    },
                    completed,
                )
            if completed < total_tasks:
                # 배치 결과가 비정상 누락된 경우를 대비해 남은 건을 에러 집계로 마감
                for _ in range(total_tasks - completed):
                    completed += 1
                    _handle_result(
                        {
                            "status": "error",
                            "report_no": "",
                            "ingredients_items_json": None,
                            "nutrition_items_json": None,
                            "parse_error": "batch_unresolved_item",
                            "parser_version": "pass4_policy_v3_error",
                            "source_rawmtrl": None,
                            "source_nutrient": None,
                        },
                        completed,
                    )
        elif workers == 1:
            for task in tasks:
                completed += 1
                _handle_result(_process_one(task), completed)
        else:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                pending = {ex.submit(_process_one, task): task for task in tasks}
                while pending:
                    done, _ = wait(tuple(pending.keys()), return_when=FIRST_COMPLETED)
                    for fut in done:
                        task_ref = pending.pop(fut, None)
                        completed += 1
                        try:
                            result = fut.result()
                        except Exception as exc:  # pylint: disable=broad-except
                            idx, row = task_ref if task_ref is not None else (completed, tasks[min(completed - 1, len(tasks) - 1)][1])
                            result = {
                                "status": "error",
                                "report_no": str(row.get("prdlstReportNo") or ""),
                                "ingredients_items_json": None,
                                "nutrition_items_json": None,
                                "parse_error": f"worker_unexpected_error: {exc}",
                                "parser_version": "pass4_policy_v3_error",
                                "source_rawmtrl": str(row.get("rawmtrl") or ""),
                                "source_nutrient": str(row.get("nutrient") or ""),
                                "log": "",
                            }
                        _handle_result(result, completed)

        sys.stdout.write("\n")
        sys.stdout.flush()
        mode_label = "Batch" if use_batch else "실시간"
        print(f"\n  ✅ HACCP Pass4 파싱 완료 ({mode_label}, HACCP 전용 프롬프트)")
        print(f"  - 대상        : {len(rows):,}")
        print(f"  - 성공        : {ok:,}")
        print(f"  - 실패        : {fail:,}")
        print(f"  - 스킵(rawmtrl없음): {skip:,}")
        print(f"  - 중단(미매칭+HACCP nutrient 없음): {skip_no_haccp_nutrient:,}")
        print(f"  - ING 저장성공: {ing_success:,}")
        print(f"  - NUT 저장(가공식품DB): {nut_from_processed:,}")
        print(f"  - NUT 저장(HACCP Pass4): {nut_from_haccp_pass4:,}")
        print("  - parser_version: pass4_policy_v3_*")


def run_haccp_parsed_reset_menu() -> None:
    answer = input("  ⚠️ haccp_parsed_cache 전체 삭제합니다. 계속할까요? [y/N]: ").strip().lower()
    if answer != "y":
        print("  취소했습니다.")
        return
    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_tables(conn)
        deleted = clear_haccp_parsed_cache(conn)
    print(f"\n  ✅ HACCP 파싱 캐시 삭제 완료: {deleted:,}건 삭제")


def _build_haccp_ingredient_preview(
    ingredients_items_json: str | None,
    *,
    token_product_count: dict[str, int] | None = None,
) -> str:
    try:
        payload = json.loads(str(ingredients_items_json or "{}"))
    except Exception:
        return "<span class='muted'>파싱 JSON 읽기 실패</span>"
    items = payload.get("ingredients_items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        return "<span class='muted'>원재료 파싱 없음</span>"
    items = [it for it in items if isinstance(it, dict)]
    if not items:
        return "<span class='muted'>원재료 파싱 없음</span>"
    ignored_fragments = payload.get("ignored_fragments") if isinstance(payload, dict) else None
    ignored_fragments = [str(x).strip() for x in (ignored_fragments or []) if str(x).strip()]
    allergen_tokens = payload.get("allergen_tokens") if isinstance(payload, dict) else None
    allergen_tokens = [str(x).strip() for x in (allergen_tokens or []) if str(x).strip()]

    def _merge_origin(origin: str, origin_detail: str) -> str:
        base = origin.strip()
        extra = origin_detail.strip()
        if not extra:
            return base
        if not base:
            return extra
        if extra in base:
            return base
        return f"{base}, {extra}"

    def _norm_tok(text: str) -> str:
        t = re.sub(r"[\s\(\)\[\]\{\}]+", "", str(text or "").strip().lower())
        t = re.sub(r"[·\.,;:]+$", "", t)
        return t

    def _meta_badges(*, name: str, origin: str, amount: str) -> str:
        badges: list[str] = []
        if token_product_count:
            k = _norm_tok(name)
            pc = int(token_product_count.get(k, 0)) if k else 0
            other = max(0, pc - 1)
            if k:
                badges.append(f"<span class='meta-chip'><b>other</b> {other:,}</span>")
        if origin:
            badges.append(f"<span class='meta-chip'><b>origin</b> {html.escape(origin)}</span>")
        if amount:
            badges.append(f"<span class='meta-chip amount'><b>amount</b> {html.escape(amount)}</span>")
        return "".join(badges)

    def _render_subtree(nodes: list[dict[str, Any]], depth: int = 0) -> str:
        parts: list[str] = [f"<ul class='ing-tree depth-{depth}'>"]
        for node in nodes:
            name = str(node.get("ingredient_name") or node.get("name") or "").strip()
            origin = _merge_origin(
                str(node.get("origin") or "").strip(),
                str(node.get("origin_detail") or "").strip(),
            )
            amount = str(node.get("amount") or "").strip()
            sub_nodes_raw = node.get("sub_ingredients") or []
            sub_nodes = [c for c in sub_nodes_raw if isinstance(c, dict)] if isinstance(sub_nodes_raw, list) else []
            has_any_field = bool(name or origin or amount or sub_nodes)
            if not has_any_field:
                continue
            parts.append("<li class='ing-node'>")
            parts.append(
                "<div class='ing-row'>"
                f"<span class='name'>{html.escape(name or '(미분류 노드)')}</span>"
                f"<span class='meta'>{_meta_badges(name=name or '', origin=origin, amount=amount)}</span>"
                "</div>"
            )
            if sub_nodes:
                parts.append(_render_subtree(sub_nodes, depth + 1))
            parts.append("</li>")
        parts.append("</ul>")
        return "".join(parts)

    tree_html = _render_subtree(items)
    ignored_html = ""
    if ignored_fragments:
        ignored_html = (
            "<div class='ignored-wrap'><h5>ignored_fragments</h5>"
            "<div class='ignored-chips'>"
            + "".join(f"<span class='meta-chip ignored'>{html.escape(x)}</span>" for x in ignored_fragments)
            + "</div></div>"
        )
    allergen_html = ""
    if allergen_tokens:
        allergen_html = (
            "<div class='ignored-wrap'><h5>allergen_tokens</h5>"
            "<div class='ignored-chips'>"
            + "".join(f"<span class='meta-chip allergen'>{html.escape(x)}</span>" for x in allergen_tokens)
            + "</div></div>"
        )
    return tree_html + allergen_html + ignored_html


def _build_haccp_nutrition_preview(nutrition_items_json: str | None) -> str:
    try:
        payload = json.loads(str(nutrition_items_json or "{}"))
    except Exception:
        return "<span class='muted'>영양 파싱 JSON 읽기 실패</span>"
    items = payload.get("nutrition_items") if isinstance(payload, dict) else None
    basis = payload.get("nutrition_basis") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        return "<span class='muted'>영양 파싱 없음</span>"
    rows: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        value = str(item.get("value") or "").strip()
        unit = str(item.get("unit") or "").strip()
        daily_value = str(item.get("daily_value") or "").strip()
        if not name:
            continue
        rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td>"
            f"<td class='num'>{html.escape(value or '-')}</td>"
            f"<td>{html.escape(unit or '-')}</td>"
            f"<td>{html.escape(daily_value or '-')}</td>"
            "</tr>"
        )
    if not rows:
        return "<span class='muted'>영양 파싱 없음</span>"
    basis_html = ""
    if isinstance(basis, dict):
        btxt = str(basis.get("basis_text") or "").strip()
        bamt = str(basis.get("per_amount") or "").strip()
        bunit = str(basis.get("per_unit") or "").strip()
        if btxt or (bamt and bunit):
            label = btxt or f"{bamt}{bunit}당"
            basis_html = f"<div class='meta'><span class='meta-chip'><b>기준량</b> {html.escape(label)}</span></div>"
    return (
        basis_html +
        "<table class='nut-table'>"
        "<thead><tr><th>성분명</th><th>값</th><th>단위</th><th>일일기준치(%)</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _pretty_json_for_report(raw_json_text: str | None) -> str:
    src = str(raw_json_text or "").strip()
    if not src:
        return ""
    try:
        obj = json.loads(src)
    except Exception:
        # 파싱 실패 시 최소한 줄바꿈 escape는 완화해서 표시
        return src.replace("\\n", "\n")

    if isinstance(obj, dict):
        # 상세 비교 화면에서는 구조 확인이 목적이므로 긴 raw 모델 텍스트는 숨김
        obj = {k: v for k, v in obj.items() if k not in {"raw_model_text"}}
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _query_haccp_parsed_rows(*, limit: int, report_no: str, status_filter: str) -> tuple[int, list[sqlite3.Row], int, int]:
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        init_haccp_tables(conn)
        where: list[str] = []
        params: list[Any] = []
        if report_no:
            digits = re.sub(r"[^0-9]", "", report_no)
            if digits:
                where.append("REPLACE(REPLACE(REPLACE(p.prdlstReportNo,'-',''),' ',''),'.','') = ?")
                params.append(digits)
        if status_filter == "ok":
            where.append("p.parse_status = 'ok'")
        elif status_filter == "error":
            where.append("p.parse_status != 'ok'")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        total = int(conn.execute(
            f"SELECT COUNT(*) FROM haccp_parsed_cache p {where_sql}",
            tuple(params),
        ).fetchone()[0] or 0)
        rows = conn.execute(
            f"""
            SELECT p.prdlstReportNo, p.parse_status, p.parser_version, p.parsed_at,
                   COALESCE(h.prdlstNm, '') AS prdlstNm,
                   p.ingredients_items_json, p.nutrition_items_json, p.parse_error,
                   p.source_rawmtrl, p.source_nutrient
            FROM haccp_parsed_cache p
            LEFT JOIN haccp_product_info h ON h.prdlstReportNo = p.prdlstReportNo
            {where_sql}
            ORDER BY p.parsed_at DESC
            LIMIT ?
            """,
            tuple(params + [limit]),
        ).fetchall()
    ok_cnt = sum(1 for r in rows if str(r[1]) == "ok")
    err_cnt = len(rows) - ok_cnt
    return total, rows, ok_cnt, err_cnt


def _delete_haccp_parsed_cache_by_report_no(report_no: str) -> int:
    digits = re.sub(r"[^0-9]", "", str(report_no or ""))
    if not digits:
        return 0
    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_tables(conn)
        cur = conn.execute(
            """
            DELETE FROM haccp_parsed_cache
            WHERE REPLACE(REPLACE(REPLACE(prdlstReportNo,'-',''),' ',''),'.','') = ?
            """,
            (digits,),
        )
        conn.commit()
        return int(cur.rowcount or 0)


def _delete_and_block_haccp_parsed_by_report_no(report_no: str, *, reason: str | None = None) -> tuple[int, bool]:
    digits = re.sub(r"[^0-9]", "", str(report_no or ""))
    if not digits:
        return 0, False
    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_tables(conn)
        cur = conn.execute(
            """
            DELETE FROM haccp_parsed_cache
            WHERE REPLACE(REPLACE(REPLACE(prdlstReportNo,'-',''),' ',''),'.','') = ?
            """,
            (digits,),
        )
        deleted = int(cur.rowcount or 0)
        upsert_haccp_parse_block(conn, report_no=digits, reason=reason or "audit_browser_block")
        return deleted, True


def _build_haccp_parsed_browser_html(
    *,
    limit: int,
    report_no: str,
    status_filter: str,
    flash_msg: str | None = None,
) -> str:
    total, rows, ok_cnt, err_cnt = _query_haccp_parsed_rows(
        limit=limit,
        report_no=report_no,
        status_filter=status_filter,
    )
    cards: list[str] = []
    for idx, row in enumerate(rows, start=1):
        rpt = str(row[0] or "")
        status = str(row[1] or "")
        parser_ver = str(row[2] or "")
        parsed_at = str(row[3] or "")
        product_name = str(row[4] or "").strip()
        ing_json = str(row[5] or "")
        nut_json = str(row[6] or "")
        ing_json_pretty = _pretty_json_for_report(ing_json)
        nut_json_pretty = _pretty_json_for_report(nut_json)
        parse_err = str(row[7] or "")
        src_rawmtrl = str(row[8] or "")
        src_nutrient = str(row[9] or "")
        status_cls = "ok" if status == "ok" else "err"
        src_nut_l = src_nutrient.lower()
        if ("processed_food_info" in src_nut_l) or ("processed_nut" in parser_ver):
            nutrition_source_label = "영양 출처: 가공식품 DB"
            nutrition_source_cls = "src-processed"
        elif ("haccp_nut" in parser_ver) or ("haccp_pass4_nut_v1" in nut_json):
            nutrition_source_label = "영양 출처: HACCP Pass4-NUT"
            nutrition_source_cls = "src-haccp"
        else:
            nutrition_source_label = "영양 출처: 기타/미확인"
            nutrition_source_cls = "src-unknown"

        delete_query = urlencode(
            {
                "target_report_no": rpt,
                "limit": str(limit),
                "report_no": report_no,
                "status": status_filter,
            }
        )
        delete_href = f"/delete?{delete_query}"
        cards.append(
            "<article class='card' data-status='{status}'>"
            "<div class='card-head'>"
            "<div class='left'><span class='idx'>#{idx}</span><span class='rpt'>{rpt}</span>"
            "<span class='pname'>{pname}</span></div>"
            "<div class='right'><span class='badge {status_cls}'>{status}</span>"
            "<span class='badge src {nutrition_source_cls}'>{nutrition_source_label}</span>"
            "<span class='ver'>{ver}</span><span class='at'>{at}</span>"
            "<a class='btn-del' href='{delete_href}' onclick=\"return confirm('해당 품목보고번호 파싱 캐시를 삭제하고 재파싱 대상으로 돌릴까요?');\">삭제</a>"
            "</div>"
            "</div>"
            "<section class='compare-wrap'>"
            "<h4>원재료: RAW vs 구조화</h4>"
            "<div class='compare-grid'>"
            "<div class='pane pane-raw'><h5>원재료 RAW</h5><pre>{rawmtrl}</pre></div>"
            "<div class='pane pane-parsed'><h5>원재료 구조화 GUI</h5><div class='ingredient-tree'>{ing_preview}</div></div>"
            "</div>"
            "</section>"
            "<section class='compare-wrap'>"
            "<h4>영양성분: RAW vs 구조화 <span class='badge src {nutrition_source_cls}'>{nutrition_source_label}</span></h4>"
            "<div class='compare-grid'>"
            "<div class='pane pane-raw'><h5>영양성분 RAW</h5><pre>{nutrient}</pre></div>"
            "<div class='pane pane-parsed'><h5>영양성분 구조화 GUI</h5><div class='chips'>{nut_preview}</div></div>"
            "</div>"
            "</section>"
            "<details><summary>파싱 JSON 보기</summary>"
            "<div class='cols'><div><h5>ingredients_items_json</h5><pre>{ing_json}</pre></div>"
            "<div><h5>nutrition_items_json</h5><pre>{nut_json}</pre></div></div>"
            "{err_block}</details>"
            "</article>".format(
                status=html.escape(status),
                idx=idx,
                rpt=html.escape(rpt),
                pname=html.escape(product_name or "(상품명 없음)"),
                status_cls=status_cls,
                nutrition_source_cls=nutrition_source_cls,
                nutrition_source_label=html.escape(nutrition_source_label),
                ver=html.escape(parser_ver),
                at=html.escape(parsed_at),
                delete_href=html.escape(delete_href),
                ing_preview=_build_haccp_ingredient_preview(ing_json),
                nut_preview=_build_haccp_nutrition_preview(nut_json),
                rawmtrl=html.escape(src_rawmtrl),
                nutrient=html.escape(src_nutrient),
                ing_json=html.escape(ing_json_pretty),
                nut_json=html.escape(nut_json_pretty),
                err_block=(
                    f"<div class='errbox'><h5>parse_error</h5><pre>{html.escape(parse_err)}</pre></div>"
                    if parse_err
                    else ""
                ),
            )
        )

    flash_html = ""
    if flash_msg:
        flash_html = f"<div class='flash'>{html.escape(flash_msg)}</div>"
    if not cards:
        cards.append("<article class='card'><span class='muted'>조건에 맞는 파싱 결과가 없습니다.</span></article>")

    html_doc = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>HACCP 파싱 결과 리포트</title>
  <style>
    :root {{
      --bg:#f3f7f8; --card:#ffffff; --ink:#0e1f29; --muted:#5f7280; --line:#d8e2e7;
      --ok:#0f766e; --ok-bg:#d8f3ef; --err:#9f1239; --err-bg:#ffe4ec; --chip:#eef4f7;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:linear-gradient(180deg,#e8f1f3 0%, var(--bg) 35%); color:var(--ink); font-family:'Pretendard','Noto Sans KR',sans-serif; }}
    .wrap {{ max-width:1280px; margin:0 auto; padding:24px; }}
    .hero {{ background:var(--card); border:1px solid var(--line); border-radius:18px; padding:20px; box-shadow:0 8px 24px rgba(9,30,45,.06); }}
    .hero h1 {{ margin:0 0 10px; font-size:28px; letter-spacing:-.3px; }}
    .meta {{ color:var(--muted); font-size:14px; }}
    .kpi {{ display:flex; gap:10px; margin-top:12px; flex-wrap:wrap; }}
    .pill {{ background:#e9f0f3; border:1px solid var(--line); border-radius:999px; padding:6px 12px; font-size:13px; }}
    .controls {{ margin:16px 0; display:flex; gap:10px; flex-wrap:wrap; }}
    .controls input, .controls select {{
      background:var(--card); border:1px solid var(--line); border-radius:10px; padding:10px 12px; min-width:220px; color:var(--ink);
    }}
    .grid {{ display:grid; gap:14px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:14px; box-shadow:0 4px 14px rgba(9,30,45,.05); }}
    .card-head {{ display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap:wrap; margin-bottom:8px; }}
    .left {{ display:flex; align-items:center; gap:10px; }}
    .idx {{ font-weight:700; color:#0b7285; }}
    .rpt {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:14px; background:#edf4f7; padding:4px 8px; border-radius:8px; }}
    .pname {{ font-size:13px; color:#1e3a4a; background:#eef7ec; border:1px solid #d8e8d4; border-radius:8px; padding:4px 8px; max-width:560px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .right {{ display:flex; gap:8px; align-items:center; color:var(--muted); font-size:12px; }}
    .badge {{ padding:3px 8px; border-radius:999px; font-weight:700; text-transform:uppercase; font-size:11px; }}
    .badge.ok {{ color:var(--ok); background:var(--ok-bg); }}
    .badge.err {{ color:var(--err); background:var(--err-bg); }}
    .badge.src {{ text-transform:none; font-size:12px; border:1px solid transparent; }}
    .badge.src-processed {{ color:#0a5b27; background:#def6e5; border-color:#9fd7ae; }}
    .badge.src-haccp {{ color:#7a3f00; background:#fff1d9; border-color:#ebc68f; }}
    .badge.src-unknown {{ color:#5b6670; background:#edf2f6; border-color:#cfdbe4; }}
    .btn-del {{ text-decoration:none; font-size:12px; font-weight:700; color:#8a112f; background:#ffe6ee; border:1px solid #f2b4c6; border-radius:8px; padding:4px 8px; }}
    .btn-del:hover {{ background:#ffdce7; }}
    .flash {{ margin-top:10px; background:#e8f7ed; border:1px solid #b7e1c3; color:#15623a; border-radius:10px; padding:10px 12px; font-size:13px; }}
    .compare-wrap {{ margin-top:10px; }}
    .compare-wrap h4 {{ display:flex; align-items:center; gap:8px; flex-wrap:wrap; }}
    .compare-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
    .pane {{ border:1px solid var(--line); border-radius:12px; background:#f8fcfd; padding:10px; }}
    .pane-raw {{ background:#fbfdff; }}
    .pane-parsed {{ background:#f7fbf8; }}
    section h4 {{ margin:10px 0 6px; font-size:14px; }}
    .chips {{ display:flex; flex-wrap:wrap; gap:6px; }}
    .chip {{ background:var(--chip); border:1px solid #d9e6ec; border-radius:999px; padding:5px 10px; font-size:12px; }}
    .chip.nut {{ background:#fff6e8; border-color:#f0d9b0; }}
    .ingredient-tree {{ border:1px solid var(--line); border-radius:12px; background:#f9fcfd; padding:10px; }}
    .ing-tree {{ list-style:none; margin:0; padding-left:16px; border-left:2px solid #dce8ee; }}
    .ing-tree.depth-0 {{ padding-left:8px; border-left:none; }}
    .ing-node {{ margin:8px 0; }}
    .ing-row {{ display:flex; flex-wrap:wrap; gap:8px; align-items:flex-start; }}
    .ing-row .name {{ font-weight:700; color:#0f2c3a; background:#edf6fa; border:1px solid #d8e8f1; border-radius:8px; padding:4px 8px; }}
    .ing-row .meta {{ display:flex; flex-wrap:wrap; gap:6px; }}
    .meta-chip {{ font-size:11px; background:#f2f5f7; border:1px solid #dde6eb; border-radius:999px; padding:3px 8px; color:#30424e; }}
    .meta-chip b {{ margin-right:4px; color:#4b5f6e; font-weight:700; }}
    .meta-chip.amount {{ background:#fff8e9; border-color:#efd9ad; }}
    .ignored-wrap {{ margin-top:10px; border-top:1px dashed var(--line); padding-top:8px; }}
    .ignored-chips {{ display:flex; flex-wrap:wrap; gap:6px; }}
    .meta-chip.ignored {{ background:#fff1f3; border-color:#f0c8cf; color:#7f2d3a; }}
    .meta-chip.allergen {{ background:#fff7d9; border-color:#f1db86; color:#6a4c00; }}
    .nut-table {{ width:100%; border-collapse:collapse; background:#fffdf8; border:1px solid #efdfbf; border-radius:10px; overflow:hidden; font-size:12px; }}
    .nut-table th, .nut-table td {{ border-bottom:1px solid #f1e5cb; padding:8px 10px; text-align:left; }}
    .nut-table th {{ background:#fff4dc; color:#5d4a22; font-weight:700; }}
    .nut-table td.num {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-weight:700; color:#7a4310; }}
    .nut-table tbody tr:last-child td {{ border-bottom:none; }}
    .muted {{ color:var(--muted); }}
    details {{ margin-top:10px; border-top:1px dashed var(--line); padding-top:8px; }}
    summary {{ cursor:pointer; color:#1d4e89; font-weight:600; }}
    .cols {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-top:8px; }}
    h5 {{ margin:4px 0; font-size:12px; color:var(--muted); }}
    pre {{ margin:0; background:#f8fbfc; border:1px solid var(--line); border-radius:10px; padding:10px; font-size:12px; line-height:1.45; white-space:pre-wrap; word-break:break-word; max-height:260px; overflow:auto; }}
    .errbox pre {{ border-color:#f3b4c6; background:#fff2f6; }}
    @media (max-width: 900px) {{ .cols {{ grid-template-columns:1fr; }} .compare-grid {{ grid-template-columns:1fr; }} .wrap {{ padding:14px; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>HACCP 파싱 결과 브라우저</h1>
      <div class="meta">DB: food_data.db · table: haccp_parsed_cache · filter: status={html.escape(status_filter)}, report_no={html.escape(report_no or '-')}</div>
      {flash_html}
      <div class="kpi">
        <span class="pill">총 표시 건수: <b>{len(rows):,}</b></span>
        <span class="pill">조건 일치 전체: <b>{total:,}</b></span>
        <span class="pill">OK: <b>{ok_cnt:,}</b></span>
        <span class="pill">ERROR: <b>{err_cnt:,}</b></span>
      </div>
    </div>
    <div class="controls">
      <input id="q" type="text" placeholder="품목보고번호/텍스트 검색">
      <select id="st">
        <option value="all">상태 전체</option>
        <option value="ok">ok만</option>
        <option value="error">error만</option>
      </select>
    </div>
    <div id="list" class="grid">
      {''.join(cards)}
    </div>
  </div>
  <script>
    const q = document.getElementById('q');
    const st = document.getElementById('st');
    const cards = Array.from(document.querySelectorAll('.card'));
    st.value = {json.dumps(status_filter)};
    function applyFilter() {{
      const kw = (q.value || '').toLowerCase();
      const sv = st.value;
      cards.forEach(card => {{
        const t = card.textContent.toLowerCase();
        const status = (card.dataset.status || '').toLowerCase();
        const okStatus = (sv === 'all') || (sv === 'ok' && status === 'ok') || (sv === 'error' && status !== 'ok');
        const okText = !kw || t.includes(kw);
        card.style.display = (okStatus && okText) ? '' : 'none';
      }});
    }}
    q.addEventListener('input', applyFilter);
    st.addEventListener('change', applyFilter);
    applyFilter();
  </script>
</body>
</html>"""
    return html_doc


def _ensure_haccp_parsed_server() -> int:
    global _HACCP_PARSED_WEB_SERVER, _HACCP_PARSED_WEB_THREAD
    if _HACCP_PARSED_WEB_SERVER is not None and _HACCP_PARSED_WEB_THREAD is not None and _HACCP_PARSED_WEB_THREAD.is_alive():
        port = _HACCP_PARSED_WEB_SERVER.server_port
        return int(port)

    class _HaccpParsedHandler(BaseHTTPRequestHandler):
        def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_OPTIONS(self):  # noqa: N802
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query or "")

            def _q(name: str, default: str = "") -> str:
                return str((qs.get(name) or [default])[0] or default)

            limit = int(_q("limit", "200")) if _q("limit", "200").isdigit() else 200
            limit = max(1, min(1000, limit))
            report_no = _q("report_no", "")
            status_filter = _q("status", "all").lower()
            if status_filter not in ("all", "ok", "error"):
                status_filter = "all"

            if parsed.path == "/cache_api":
                action = _q("action", "").lower()
                target = _q("report_no", "")
                if action == "delete":
                    deleted = _delete_haccp_parsed_cache_by_report_no(target)
                    self._send_json(
                        {
                            "ok": True,
                            "action": "delete",
                            "report_no": re.sub(r"[^0-9]", "", target),
                            "deleted": int(deleted),
                        },
                        status=200,
                    )
                    return
                if action == "delete_block":
                    deleted, blocked = _delete_and_block_haccp_parsed_by_report_no(
                        target,
                        reason="audit_manual_block",
                    )
                    self._send_json(
                        {
                            "ok": True,
                            "action": "delete_block",
                            "report_no": re.sub(r"[^0-9]", "", target),
                            "deleted": int(deleted),
                            "blocked": bool(blocked),
                        },
                        status=200,
                    )
                    return
                self._send_json({"ok": False, "error": "invalid_action"}, status=400)
                return

            if parsed.path == "/delete":
                target = _q("target_report_no", "")
                deleted = _delete_haccp_parsed_cache_by_report_no(target)
                msg = f"삭제 완료: report_no={target} / {deleted}건"
                redirect_qs = urlencode(
                    {
                        "limit": str(limit),
                        "report_no": report_no,
                        "status": status_filter,
                        "msg": msg,
                    }
                )
                self.send_response(302)
                self.send_header("Location", f"/?{redirect_qs}")
                self.end_headers()
                return

            msg = _q("msg", "")
            html_doc = _build_haccp_parsed_browser_html(
                limit=limit,
                report_no=report_no,
                status_filter=status_filter,
                flash_msg=msg or None,
            )
            data = html_doc.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    last_exc: Exception | None = None
    for p in [HACCP_PARSED_WEB_PORT] + list(range(HACCP_PARSED_WEB_PORT + 1, HACCP_PARSED_WEB_PORT + 20)):
        try:
            server = HTTPServer(("127.0.0.1", p), _HaccpParsedHandler)
            th = threading.Thread(target=server.serve_forever, daemon=True)
            th.start()
            _HACCP_PARSED_WEB_SERVER = server
            _HACCP_PARSED_WEB_THREAD = th
            return p
        except OSError as exc:
            last_exc = exc
            continue
    raise RuntimeError(f"HACCP parsed browser server start failed: {last_exc}")


def run_haccp_parsed_view_menu() -> None:
    print("\n  🌐 [HACCP 파싱 결과 브라우저 열기]")
    raw_limit = input("  🔹 조회 건수 [기본 200]: ").strip()
    limit = int(raw_limit) if raw_limit.isdigit() else 200
    limit = max(1, min(1000, limit))
    report_no = input("  🔹 품목보고번호 필터(선택): ").strip()
    status_filter = input("  🔹 상태 필터 [all/ok/error, 기본 all]: ").strip().lower() or "all"
    if status_filter not in ("all", "ok", "error"):
        status_filter = "all"
    port = _ensure_haccp_parsed_server()
    url = f"http://127.0.0.1:{port}/?{urlencode({'limit': str(limit), 'report_no': report_no, 'status': status_filter})}"
    print(f"  🌐 HACCP 브라우저 URL: {url}")
    try:
        webbrowser.open(url)
        print("  🖥️ 브라우저 자동 열기 완료")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"  ⚠️ 브라우저 자동 열기 실패: {exc}")


def _print_duplicate_stats(stats: dict[str, int]) -> None:
    print("  📊 [중복 현황]")
    print(f"    총 레코드                : {stats['total_rows']:,}")
    print(f"    A(foodCd) 그룹/초과행    : {stats['foodCd_groups']:,} / {stats['foodCd_extra']:,}")
    print(f"    B(이름+용량+카테고리)    : {stats['h1_groups']:,} / {stats['h1_extra']:,}")
    print(f"    C(이름+영양+카테고리)    : {stats['h2_groups']:,} / {stats['h2_extra']:,}")
    print(f"    D(이름+카테고리)         : {stats['h3_groups']:,} / {stats['h3_extra']:,}")


def run_duplicate_menu() -> None:
    while True:
        print("\n  🧹 [중복 관리]")
        print("    [1] 🔍 중복 조건/현황 보기")
        print("    [2] 🗑️ 중복 삭제 실행")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            print("\n  📐 [중복 판정 조건]")
            for condition in duplicate_conditions():
                print(f"    • {condition}")
            with sqlite3.connect(DB_FILE) as conn:
                stats = get_duplicate_stats(conn)
                _print_duplicate_stats(stats)
                print("\n  🧾 [중복 의심 샘플 10개]")
                samples = get_duplicate_samples(conn, limit=10)
                if not samples:
                    print("    ✅ 없음")
                else:
                    for row in samples:
                        food_nm, food_size, serv_size, lv3, lv4, cnt, foodcd_cnt = row
                        print(
                            f"    - {food_nm} | cnt={cnt} foodCd={foodcd_cnt} | "
                            f"size={food_size}, serv={serv_size}, cat={lv3}>{lv4}"
                        )
        elif sub == "2":
            print("\n  ⚠️ [삭제 실행 전 안내]")
            for condition in duplicate_conditions():
                print(f"    • {condition}")
            with sqlite3.connect(DB_FILE) as conn:
                before = get_duplicate_stats(conn)
            print("\n  📌 [실행 전 통계]")
            _print_duplicate_stats(before)
            confirm = input("\n  ❓ 위 조건으로 중복 삭제를 실행할까요? [y/N]: ").strip().lower()
            if confirm != "y":
                print("  🛑 삭제를 취소했습니다.")
                continue

            try:
                backup_path = create_backup(DB_FILE, label="pre_dedupe")
                print(f"\n  💾 안전 백업 생성 완료: {backup_path}")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"\n  ❌ 백업 생성 실패: {exc}")
                continue

            with sqlite3.connect(DB_FILE) as conn:
                result = run_dedupe(conn)
                after = get_duplicate_stats(conn)

            print("\n  ✅ [삭제 결과]")
            print(f"    - 규칙 A 삭제: {result['removed_a']:,}건")
            print(f"    - 규칙 B 삭제: {result['removed_b']:,}건")
            print(f"    - 규칙 C 삭제: {result['removed_c']:,}건")
            print(f"    - 규칙 D 삭제: {result['removed_d']:,}건")
            print(f"    - 총 삭제   : {result['removed_total']:,}건")
            print(f"    - 삭제 목록 CSV : {result['csv_path']}")

            print("\n  📌 [실행 후 통계]")
            _print_duplicate_stats(after)
        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_backup_menu() -> None:
    while True:
        print("\n  💾 [백업/복원 관리]")
        print("    [1] 백업 생성")
        print("    [2] 백업 목록 보기")
        print("    [3] 백업 복원")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            label = input("  🔹 백업 라벨 [기본 manual]: ").strip() or "manual"
            try:
                path = create_backup(DB_FILE, label=label)
                print(f"  ✅ 백업 생성 완료: {path}")
                drive_dir = os.getenv("GOOGLE_DRIVE_BACKUP_DIR", "").strip()
                if drive_dir:
                    print(f"  ☁️ Google Drive 복사 완료: {drive_dir}")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"  ❌ 백업 생성 실패: {exc}")

        elif sub == "2":
            backups = list_backups(DB_FILE)
            print("\n  📚 [백업 목록]")
            if not backups:
                print("    (백업 파일 없음)")
            else:
                for idx, path in enumerate(backups, 1):
                    meta = read_backup_metadata(path)
                    if meta:
                        size_mb = (meta.get("backup_size_bytes") or 0) / (1024 * 1024)
                        mtime = meta.get("backup_mtime") or "-"
                        print(f"    [{idx}] {path}")
                        print(f"         size={size_mb:.1f}MB | mtime={mtime} | meta=있음")
                    else:
                        print(f"    [{idx}] {path}  (meta 없음)")

        elif sub == "3":
            backups = list_backups(DB_FILE)
            if not backups:
                print("  ⚠️ 복원 가능한 백업이 없습니다.")
                continue
            print("\n  📚 [복원 대상 선택]")
            for idx, path in enumerate(backups, 1):
                print(f"    [{idx}] {path}")
            raw = input("  👉 복원할 번호 입력 (b: 취소): ").strip().lower()
            if raw == "b":
                continue
            if not raw.isdigit():
                print("  ⚠️ 숫자로 입력해주세요.")
                continue
            pick = int(raw)
            if pick < 1 or pick > len(backups):
                print("  ⚠️ 범위를 벗어난 번호입니다.")
                continue

            target = backups[pick - 1]
            confirm = input(
                "  ❗ 현재 DB를 해당 백업으로 덮어씁니다. 계속할까요? [y/N]: "
            ).strip().lower()
            if confirm != "y":
                print("  🛑 복원을 취소했습니다.")
                continue

            try:
                check = verify_backup(target)
                print(f"  🔎 백업 검증: integrity={check['sqlite_integrity_ok']} checksum={check['checksum_match']}")
                restored = restore_backup(
                    target,
                    DB_FILE,
                    keep_current_snapshot=True,
                    verify_before_restore=True,
                )
                print(f"  ✅ 복원 완료: {restored}")
                print("  💾 기존 DB는 pre_restore 라벨로 자동 백업되었습니다.")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"  ❌ 복원 실패: {exc}")

        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_query_pipeline_menu() -> None:
    while True:
        print("\n  🧩 [검색어 파이프라인]")
        print("    [1] ➕ 검색어 직접 추가")
        print("    [2] 🌐 검색어 풀 브라우저 보기")
        print("    [b] ↩️ 뒤로가기")
        sub = input("  👉 선택 : ").strip().lower()

        if sub == "1":
            query_text = input("  🔹 검색어 입력: ").strip()
            if not query_text:
                print("  ⚠️ 검색어가 비어 있습니다.")
                continue
            raw_pri = input("  🔹 score(점수) [기본 0]: ").strip()
            notes = input("  🔹 메모(선택): ").strip() or None
            try:
                pri = float(raw_pri) if raw_pri else 0.0
            except ValueError:
                print("  ⚠️ 점수는 숫자여야 합니다.")
                continue

            with sqlite3.connect(DB_FILE) as conn:
                init_query_pipeline_tables(conn)
                query_id = upsert_query(
                    conn,
                    query_text,
                    source="manual",
                    priority_score=pri,
                    target_segment_score=0.0,
                    status="pending",
                    notes=notes,
                )
            print(f"  ✅ 저장 완료: query_id={query_id}")

        elif sub == "2":
            run_query_pool_browser_view()

        elif sub == "b":
            break
        else:
            print("  ⚠️ 올바른 메뉴 번호를 입력해주세요.")


def run_query_pool_browser_view() -> None:
    with sqlite3.connect(DB_FILE) as conn:
        out_path = viewer.open_query_pool_browser_report(conn)
    print(f"\n  ✅ 검색어 풀 브라우저 리포트 생성: {out_path}")


def _query_exec_log(logger: Callable[[str], None] | None, text: str) -> None:
    if logger is not None:
        logger(text)
    else:
        print(text)


def _yn_flag(v: bool | None) -> str:
    if v is True:
        return "✅"
    if v is False:
        return "❌"
    return "-"


def _compact_reason(reason: str | None, max_len: int = 72) -> str:
    value = str(reason or "").strip().replace("\n", " ")
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"


def _extract_image_size(image_bytes: bytes, mime_type: str | None = None) -> tuple[int | None, int | None]:
    b = image_bytes or b""
    if len(b) < 10:
        return (None, None)
    mime = str(mime_type or "").lower()
    # PNG
    if mime == "image/png" or b.startswith(b"\x89PNG\r\n\x1a\n"):
        if len(b) >= 24:
            w = int.from_bytes(b[16:20], "big", signed=False)
            h = int.from_bytes(b[20:24], "big", signed=False)
            return (w, h)
        return (None, None)
    # GIF
    if mime == "image/gif" or b.startswith(b"GIF87a") or b.startswith(b"GIF89a"):
        if len(b) >= 10:
            w = int.from_bytes(b[6:8], "little", signed=False)
            h = int.from_bytes(b[8:10], "little", signed=False)
            return (w, h)
        return (None, None)
    # WebP (VP8X/VP8/VP8L)
    if mime == "image/webp" or (len(b) >= 12 and b[0:4] == b"RIFF" and b[8:12] == b"WEBP"):
        try:
            if len(b) >= 30 and b[12:16] == b"VP8X":
                w = 1 + int.from_bytes(b[24:27], "little", signed=False)
                h = 1 + int.from_bytes(b[27:30], "little", signed=False)
                return (w, h)
            if len(b) >= 30 and b[12:16] == b"VP8L":
                val = int.from_bytes(b[21:25], "little", signed=False)
                w = (val & 0x3FFF) + 1
                h = ((val >> 14) & 0x3FFF) + 1
                return (w, h)
            # VP8 lossy width/height in frame header
            if len(b) >= 30 and b[12:16] == b"VP8 ":
                w = int.from_bytes(b[26:28], "little", signed=False) & 0x3FFF
                h = int.from_bytes(b[28:30], "little", signed=False) & 0x3FFF
                return (w, h)
        except Exception:  # pylint: disable=broad-except
            return (None, None)
        return (None, None)
    # JPEG
    if mime in ("image/jpeg", "image/jpg") or (len(b) >= 2 and b[0:2] == b"\xff\xd8"):
        i = 2
        try:
            while i + 9 < len(b):
                if b[i] != 0xFF:
                    i += 1
                    continue
                marker = b[i + 1]
                i += 2
                if marker in (0xD8, 0xD9):  # SOI/EOI
                    continue
                if i + 1 >= len(b):
                    break
                seg_len = int.from_bytes(b[i:i + 2], "big", signed=False)
                if seg_len < 2:
                    break
                if marker in (
                    0xC0, 0xC1, 0xC2, 0xC3,
                    0xC5, 0xC6, 0xC7,
                    0xC9, 0xCA, 0xCB,
                    0xCD, 0xCE, 0xCF,
                ):
                    if i + 7 < len(b):
                        h = int.from_bytes(b[i + 3:i + 5], "big", signed=False)
                        w = int.from_bytes(b[i + 5:i + 7], "big", signed=False)
                        return (w, h)
                    break
                i += seg_len
        except Exception:  # pylint: disable=broad-except
            return (None, None)
    return (None, None)


def _estimate_vision_tokens(width: int | None, height: int | None) -> int | None:
    """
    이미지 해상도 기반 매우 러프한 토큰 추정치.
    - 모델/서버 내부 타일링 정책에 따라 실제 청구와 다를 수 있음.
    """
    if not width or not height:
        return None
    pixels = max(1, int(width) * int(height))
    # 대략치: 1토큰 ~= 1024px 가정
    return max(1, int(round(pixels / 1024.0)))


def _correct_ingredients_boundaries(raw_text: str | None) -> dict[str, Any]:
    """
    Pass3 원재료 문자열의 괄호/경계 오독을 완화한다.
    - top-level 콤마 기준 분리
    - 괄호 불균형 진단
    - `), <재료명>[` 패턴에서 상위 항목 강제 분리
    """
    text = str(raw_text or "").strip()
    if not text:
        return {
            "corrected_text": "",
            "boundary_corrected": False,
            "boundary_confidence": 100,
            "issue_codes": [],
            "actions": [],
        }
    text = (
        text.replace("（", "(").replace("）", ")")
        .replace("［", "[").replace("］", "]")
        .replace("｛", "{").replace("｝", "}")
    )
    issue_codes: list[str] = []
    actions: list[str] = []
    items: list[str] = []
    buf: list[str] = []
    round_depth = 0
    square_depth = 0

    def _flush() -> None:
        token = "".join(buf).strip(" ,")
        buf.clear()
        if token:
            items.append(re.sub(r"\s+", " ", token).strip())

    for i, ch in enumerate(text):
        if ch == "(":
            round_depth += 1
            buf.append(ch)
            continue
        if ch == "[":
            square_depth += 1
            buf.append(ch)
            continue
        if ch == ")":
            if round_depth > 0:
                round_depth -= 1
            elif square_depth > 0:
                # 교차 닫힘: square를 닫고 이슈 기록
                square_depth -= 1
                issue_codes.append("cross_closer_paren")
            else:
                issue_codes.append("unexpected_closer_paren")
            buf.append(ch)
            continue
        if ch == "]":
            if square_depth > 0:
                square_depth -= 1
            elif round_depth > 0:
                round_depth -= 1
                issue_codes.append("cross_closer_square")
            else:
                issue_codes.append("unexpected_closer_square")
            buf.append(ch)
            continue
        if ch == ",":
            nxt = text[i + 1:].lstrip()
            is_top = (round_depth == 0 and square_depth == 0)
            forced_split = False
            if (not is_top) and square_depth == 0 and round_depth > 0:
                # 예: "...), 코코아가공품[...]" -> sibling 분리
                if re.match(r"^[A-Za-z가-힣0-9][^,\[\]\(\)]{0,40}\[", nxt):
                    forced_split = True
                    actions.append("forced_split_after_comma_before_bracket_item")
                    issue_codes.append("missing_closer_paren_recovered")
                    if round_depth > 0:
                        buf.append(")" * round_depth)
                    round_depth = 0
            if is_top or forced_split:
                _flush()
                continue
            buf.append(ch)
            continue
        buf.append(ch)
    _flush()

    if round_depth > 0:
        issue_codes.append("missing_closer_paren")
    if square_depth > 0:
        issue_codes.append("missing_closer_square")

    corrected = ", ".join(x for x in items if x)
    corrected = re.sub(r"\s+,", ",", corrected).strip(" ,")
    boundary_corrected = corrected != text or bool(issue_codes) or bool(actions)
    confidence = 100 - (len(issue_codes) * 12) - (len(actions) * 8)
    confidence = max(0, min(100, confidence))
    return {
        "corrected_text": corrected or text,
        "boundary_corrected": boundary_corrected,
        "boundary_confidence": confidence,
        "issue_codes": sorted(set(issue_codes)),
        "actions": actions,
    }


def _resize_image_to_target_tokens(
    image_bytes: bytes,
    mime_type: str | None,
    *,
    target_tokens: int,
    min_short_side: int = 512,
    min_long_side: int = 768,
) -> tuple[bytes, str, int | None, int | None, int | None, bool, str]:
    """
    토큰 목표에 맞춰 다운스케일만 수행한다.
    - 업스케일은 금지한다.
    - 원본 토큰이 이미 목표 이하이면 원본 유지.
    """
    ow, oh = _extract_image_size(image_bytes, mime_type)
    est = _estimate_vision_tokens(ow, oh)
    if not ow or not oh or not est:
        return (image_bytes, str(mime_type or "image/jpeg"), ow, oh, est, False, "size_unknown_keep_original")
    if est <= max(1, int(target_tokens)):
        return (image_bytes, str(mime_type or "image/jpeg"), ow, oh, est, False, "target_already_met")

    try:
        from PIL import Image  # type: ignore
    except Exception as exc:  # pylint: disable=broad-except
        raise RuntimeError(f"pil_import_failed:{exc}") from exc

    try:
        img = Image.open(io.BytesIO(image_bytes))
        img.load()
    except Exception as exc:  # pylint: disable=broad-except
        raise RuntimeError(f"image_decode_failed:{exc}") from exc

    ratio = (max(1, int(target_tokens)) / float(est)) ** 0.5
    nw = max(1, int(round(ow * ratio)))
    nh = max(1, int(round(oh * ratio)))
    # 업스케일 금지
    nw = min(nw, ow)
    nh = min(nh, oh)

    short_side = min(nw, nh)
    long_side = max(nw, nh)
    if short_side < min_short_side:
        s = float(min_short_side) / float(max(1, short_side))
        nw = int(round(nw * s))
        nh = int(round(nh * s))
    short_side = min(nw, nh)
    long_side = max(nw, nh)
    if long_side < min_long_side:
        s = float(min_long_side) / float(max(1, long_side))
        nw = int(round(nw * s))
        nh = int(round(nh * s))

    # 최소값 보정 후에도 업스케일 금지
    nw = min(max(1, nw), ow)
    nh = min(max(1, nh), oh)
    if nw == ow and nh == oh:
        return (image_bytes, str(mime_type or "image/jpeg"), ow, oh, est, False, "no_downscale_possible_keep_original")

    try:
        resized = img.resize((nw, nh), Image.Resampling.LANCZOS)
        fmt = "JPEG"
        out_mime = "image/jpeg"
        if str(mime_type or "").lower() == "image/png":
            fmt = "PNG"
            out_mime = "image/png"
        elif str(mime_type or "").lower() == "image/webp":
            fmt = "WEBP"
            out_mime = "image/webp"
        buf = io.BytesIO()
        if fmt == "JPEG":
            if resized.mode not in ("RGB", "L"):
                resized = resized.convert("RGB")
            resized.save(buf, format=fmt, quality=92, optimize=True)
        elif fmt == "PNG":
            if resized.mode not in ("RGB", "RGBA", "L"):
                resized = resized.convert("RGB")
            resized.save(buf, format=fmt, optimize=True)
        else:
            if resized.mode not in ("RGB", "RGBA", "L"):
                resized = resized.convert("RGB")
            resized.save(buf, format=fmt, quality=92, method=6)
        out_bytes = buf.getvalue()
    except Exception as exc:  # pylint: disable=broad-except
        raise RuntimeError(f"image_resize_encode_failed:{exc}") from exc

    est2 = _estimate_vision_tokens(nw, nh)
    trace = f"{ow}x{oh} tok~{est} -> {nw}x{nh} tok~{est2}"
    return (out_bytes, out_mime, nw, nh, est2, True, trace)


def execute_query_pipeline_run(
    *,
    mode: str = "1",
    provider: str = "google",
    query_limit: int = 3,
    max_pages: int = 1,
    max_images: int = 0,
    pass_workers: int = 5,
    direct_query: str | None = None,
    open_browser_report: bool = True,
    show_pass3_raw: bool = False,
    logger: Callable[[str], None] | None = None,
) -> str | None:
    from app import config as app_config
    app_config.reload_dotenv()

    serp_key = os.getenv("SERPAPI_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not openai_key:
        _query_exec_log(logger, "  ❌ OPENAI_API_KEY가 필요합니다.")
        return None

    if provider not in ("naver_official", "naver_blog", "naver_shop") and not serp_key:
        _query_exec_log(logger, "  ❌ SERPAPI_KEY가 필요합니다.")
        return None
    if provider in ("naver_official", "naver_blog", "naver_shop"):
        naver_client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
        naver_client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
        if not (naver_client_id and naver_client_secret):
            _query_exec_log(logger, "  ❌ NAVER_CLIENT_ID / NAVER_CLIENT_SECRET이 필요합니다.")
            return None

    mode = (mode or "1").strip()
    provider = (provider or "google").strip()
    query_limit = int(query_limit)
    max_pages = int(max_pages)
    max_images = int(max_images)
    pass_workers = int(pass_workers)
    query_limit = max(1, query_limit)
    max_pages = max(1, min(20, max_pages))
    max_images = max(0, max_images)
    pass_workers = max(1, min(50, pass_workers))
    try:
        vision_reject_aspect_ratio = float(os.getenv("VISION_REJECT_ASPECT_RATIO", "4.0"))
    except Exception:
        vision_reject_aspect_ratio = 4.0
    try:
        vision_target_tokens_pass2 = int(os.getenv("VISION_TARGET_TOKENS_PASS2", "300"))
    except Exception:
        vision_target_tokens_pass2 = 300
    try:
        vision_target_tokens_pass3 = int(os.getenv("VISION_TARGET_TOKENS_PASS3", "1000"))
    except Exception:
        vision_target_tokens_pass3 = 1000
    try:
        vision_min_short_side = int(os.getenv("VISION_MIN_SHORT_SIDE", "512"))
    except Exception:
        vision_min_short_side = 512
    try:
        vision_min_long_side = int(os.getenv("VISION_MIN_LONG_SIDE", "768"))
    except Exception:
        vision_min_long_side = 768
    vision_target_tokens_pass2 = max(64, vision_target_tokens_pass2)
    vision_target_tokens_pass3 = max(128, vision_target_tokens_pass3)
    vision_min_short_side = max(64, vision_min_short_side)
    vision_min_long_side = max(64, vision_min_long_side)
    pass3_gemini_concurrency = max(1, int(os.getenv("PASS3_GEMINI_CONCURRENCY", "1") or "1"))
    pass3_gemini_sem = threading.BoundedSemaphore(pass3_gemini_concurrency)

    from app.query_image_benchmark import _search_images_all

    reports: list[dict] = []
    use_terminal_dashboard = logger is None
    can_redraw_terminal = bool(
        use_terminal_dashboard
        and getattr(sys.stdout, "isatty", lambda: False)()
        and str(os.getenv("TERM", "")).lower() not in ("", "dumb")
    )

    def _print_pass3_raw_for_query(query_report: dict) -> None:
        if not show_pass3_raw:
            return
        rows = list(query_report.get("images") or [])
        targets = [r for r in rows if str(r.get("raw_pass3") or "").strip()]
        _query_exec_log(
            logger,
            f"\n  🧾 [Pass3 RAW] query='{query_report.get('query_text')}' | {len(targets)}/{len(rows)}건",
        )
        if not targets:
            _query_exec_log(logger, "    (Pass3 RAW 없음)")
            return
        for r in targets:
            idx = int(r.get("idx") or 0)
            url = str(r.get("url") or "")
            raw3 = str(r.get("raw_pass3") or "")
            _query_exec_log(logger, f"\n    [#{idx}] {url}")
            _query_exec_log(logger, "    [PASS3 RAW]")
            _query_exec_log(logger, raw3)

    def _stage_to_gauge(stage: str) -> str:
        s = str(stage or "").upper()
        if s.startswith("PASS1"):
            return "[█---]"
        if s.startswith("RESIZE"):
            return "[█---]"
        if s.startswith("PASS2"):
            return "[██--]"
        if s.startswith("PASS3"):
            return "[███-]"
        if s.startswith("PASS4") or s.startswith("DONE"):
            return "[████]"
        return "[----]"

    def _format_eta(remain: int, speed: float) -> str:
        if remain <= 0:
            return "0s"
        if speed <= 1e-9:
            return "-"
        sec = int(remain / speed)
        if sec < 60:
            return f"{sec}s"
        m, s = divmod(sec, 60)
        if m < 60:
            return f"{m}m {s}s"
        h, m = divmod(m, 60)
        return f"{h}h {m}m"

    class _PipelineDash:
        def __init__(self, query_text: str, total: int, slots: int):
            self.query_text = query_text
            self.total = max(0, int(total))
            self.slots = max(1, int(slots))
            self.done = 0
            self.saved = 0
            self.api_calls = 0
            self.skipped_existing = 0
            self.fail_stage_counter = Counter()
            self.fail_reason_counter = Counter()
            self.policy_fail_counter = Counter()
            self.error_fail_counter = Counter()
            self.aspect_ratio_blocked = 0
            self.orig_tokens_sum = 0
            self.pass2_tokens_sum = 0
            self.pass3_tokens_sum = 0
            self.started_at = time.time()
            self.slot_state = {
                i: {"idx": "-", "url": "", "stage": "IDLE", "msg": "-"}
                for i in range(1, self.slots + 1)
            }
            self.pass3_waiting = 0
            self._lock = threading.Lock()
            self._last_render_at = 0.0

        @staticmethod
        def _is_system_error(fail_stage: str, fail_reason: str) -> bool:
            stage = str(fail_stage or "").lower()
            reason = str(fail_reason or "").lower()
            # 통신/쿼터/타임아웃/예외/AI 호출 실패 계열은 시스템 오류로 분류
            error_keys = (
                "http_",
                "timeout",
                "timed out",
                "resource_exhausted",
                "connection",
                "name resolution",
                "exception",
                "traceback",
                "error:",
                "pass3_nutrition_error",
                "pass4_ai_error",
                "image_download_failed",
                "openai_http_",
                "gemini_http_",
                "empty_model_response",
            )
            if any(k in reason for k in error_keys):
                return True
            if stage in ("download",):
                return True
            return False

        def on_skip_existing(self, reason: str) -> None:
            with self._lock:
                self.done += 1
                self.skipped_existing += 1
                self.fail_stage_counter["skipped_existing"] += 1
                if reason:
                    self.fail_reason_counter[str(reason)] += 1
                self.render()

        def on_slot_stage(self, slot_id: int, idx: int, url: str, stage: str, msg: str = "") -> None:
            with self._lock:
                self.slot_state[slot_id] = {
                    "idx": idx,
                    "url": str(url or ""),
                    "stage": str(stage or ""),
                    "msg": str(msg or "-"),
                }
                self.render()

        def on_pass3_wait_start(self, slot_id: int, idx: int, url: str, kind: str) -> None:
            with self._lock:
                self.pass3_waiting += 1
                self.slot_state[slot_id] = {
                    "idx": idx,
                    "url": str(url or ""),
                    "stage": "PASS3-QUEUE",
                    "msg": f"gemini {kind} queued",
                }
                self.render(force=True)

        def on_pass3_wait_end(self, slot_id: int, idx: int, url: str, kind: str) -> None:
            with self._lock:
                self.pass3_waiting = max(0, self.pass3_waiting - 1)
                self.slot_state[slot_id] = {
                    "idx": idx,
                    "url": str(url or ""),
                    "stage": "PASS3-RUN",
                    "msg": f"gemini {kind} running",
                }
                self.render(force=True)

        def on_finish(
            self,
            slot_id: int,
            idx: int,
            url: str,
            *,
            pass4_ok: bool,
            fail_stage: str,
            fail_reason: str,
            api_calls: int,
            orig_tokens: int | None = None,
            pass2_tokens: int | None = None,
            pass3_tokens: int | None = None,
        ) -> None:
            with self._lock:
                self.done += 1
                self.api_calls += int(api_calls or 0)
                if orig_tokens is not None:
                    self.orig_tokens_sum += int(orig_tokens)
                if pass2_tokens is not None:
                    self.pass2_tokens_sum += int(pass2_tokens)
                if pass3_tokens is not None:
                    self.pass3_tokens_sum += int(pass3_tokens)
                if pass4_ok:
                    self.saved += 1
                    self.slot_state[slot_id] = {
                        "idx": idx,
                        "url": str(url or ""),
                        "stage": "DONE-SAVED",
                        "msg": "🏆 최종통과",
                    }
                    self.fail_stage_counter["saved"] += 1
                else:
                    stage = str(fail_stage or "failed")
                    self.fail_stage_counter[stage] += 1
                    if fail_reason:
                        self.fail_reason_counter[str(fail_reason)] += 1
                    if self._is_system_error(stage, fail_reason):
                        self.error_fail_counter[stage] += 1
                    else:
                        self.policy_fail_counter[stage] += 1
                    if "policy_excessive_aspect_ratio" in str(fail_reason):
                        self.aspect_ratio_blocked += 1
                    self.slot_state[slot_id] = {
                        "idx": idx,
                        "url": str(url or ""),
                        "stage": "DONE-FAIL",
                        "msg": f"⛔ {stage}",
                    }
                self.render(force=True)

        def release_slot(self, slot_id: int) -> None:
            with self._lock:
                self.slot_state[slot_id] = {"idx": "-", "url": "", "stage": "IDLE", "msg": "-"}
                self.render()

        def render(self, force: bool = False) -> None:
            if not use_terminal_dashboard:
                return
            now = time.time()
            if not force and (now - self._last_render_at) < 0.35:
                return
            self._last_render_at = now
            elapsed = max(1e-9, time.time() - self.started_at)
            speed = self.done / elapsed if self.done > 0 else 0.0
            remain = max(0, self.total - self.done)
            eta = _format_eta(remain, speed)
            yield_ratio = (self.saved / self.done * 100.0) if self.done else 0.0
            top3 = self.fail_reason_counter.most_common(3)
            top3_txt = " | ".join(f"{k[:36]}:{v}" for k, v in top3) if top3 else "-"
            saved_p2 = max(0, self.orig_tokens_sum - self.pass2_tokens_sum)
            saved_p3 = max(0, self.orig_tokens_sum - self.pass3_tokens_sum)
            saved_p2_rate = (saved_p2 / self.orig_tokens_sum * 100.0) if self.orig_tokens_sum else 0.0
            saved_p3_rate = (saved_p3 / self.orig_tokens_sum * 100.0) if self.orig_tokens_sum else 0.0
            fail_txt = (
                f"P1fail:{self.fail_stage_counter.get('pass1', 0)}"
                f"(정책{self.policy_fail_counter.get('pass1',0)}/오류{self.error_fail_counter.get('pass1',0)}) "
                f"P2fail:{self.fail_stage_counter.get('pass2', 0) + self.fail_stage_counter.get('pass2b', 0)}"
                f"(정책{self.policy_fail_counter.get('pass2',0)+self.policy_fail_counter.get('pass2b',0)}/"
                f"오류{self.error_fail_counter.get('pass2',0)+self.error_fail_counter.get('pass2b',0)}) "
                f"P3fail:{self.fail_stage_counter.get('pass3', 0)}"
                f"(정책{self.policy_fail_counter.get('pass3',0)}/오류{self.error_fail_counter.get('pass3',0)}) "
                f"P4fail:{self.fail_stage_counter.get('pass4', 0)}"
                f"(정책{self.policy_fail_counter.get('pass4',0)}/오류{self.error_fail_counter.get('pass4',0)}) "
                f"saved:{self.fail_stage_counter.get('saved', 0)}"
            )
            policy_total = sum(self.policy_fail_counter.values())
            error_total = sum(self.error_fail_counter.values())

            if can_redraw_terminal:
                print("\033[2J\033[H", end="")
            else:
                # 리다이렉트/비TTY 환경에선 과다 출력 방지용 구분선만 사용
                print("\n" + "=" * 80)
            print(f"검색어: {self.query_text}")
            print(f"total: {self.total} done: {self.done} 최종 저장: {self.saved}")
            print(f"API 호출 횟수: {self.api_calls}회")
            print(f"단계별 카운터: {fail_txt}")
            print(f"실패 구분: 정책 실패 {policy_total}건 | 시스템 오류 {error_total}건")
            print(f"유효 처리율(saved/done): {yield_ratio:.1f}%")
            print(f"중복 스킵: {self.skipped_existing}건")
            print(f"Pass3 대기중(sem): {self.pass3_waiting}건")
            print(f"평균 속도: {speed:.2f} img/s | ETA: {eta}")
            print(
                f"토큰합(원본/P2/P3): {self.orig_tokens_sum:,}/{self.pass2_tokens_sum:,}/{self.pass3_tokens_sum:,} "
                f"| 절감 P2 {saved_p2:,}({saved_p2_rate:.1f}%) P3 {saved_p3:,}({saved_p3_rate:.1f}%)"
            )
            print(f"세로비 정책 컷(aspect>{vision_reject_aspect_ratio:.2f}): {self.aspect_ratio_blocked}")
            print(f"최근 실패 TOP3: {top3_txt}")
            print("-" * 120)
            for i in range(1, self.slots + 1):
                s = self.slot_state[i]
                idx = s["idx"]
                url = str(s["url"] or "")
                stage = str(s["stage"] or "IDLE").upper()
                msg = str(s["msg"] or "-")
                gauge = _stage_to_gauge(stage)
                url_short = (url[:70] + "…") if len(url) > 71 else (url or "-")
                print(f"[{i:02}] [{idx}] {url_short:<72} {gauge} {stage:<8} {msg}")
            if can_redraw_terminal:
                sys.stdout.flush()
    with sqlite3.connect(DB_FILE) as conn:
        init_query_pipeline_tables(conn)
        conn.row_factory = sqlite3.Row
        public_food_index = _build_public_food_index(conn)
        _query_exec_log(logger, "\n  📋 [실행 설정]")
        _query_exec_log(logger, f"    - provider           : {provider}")
        _query_exec_log(logger, f"    - query limit        : {query_limit}")
        _query_exec_log(logger, f"    - max pages          : {max_pages} (항상 1페이지부터 수집)")
        _query_exec_log(logger, f"    - max images/query   : {max_images if max_images > 0 else '전체'}")
        _query_exec_log(logger, f"    - pass workers       : {pass_workers}")
        _query_exec_log(logger, f"    - pass3 gemini conc  : {pass3_gemini_concurrency}")
        _query_exec_log(logger, f"    - reject aspect ratio: h/w > {vision_reject_aspect_ratio:.2f}")
        _query_exec_log(logger, f"    - pass2 token target : ~{vision_target_tokens_pass2}")
        _query_exec_log(logger, f"    - pass3 token target : ~{vision_target_tokens_pass3}")
        _query_exec_log(logger, f"    - 공공DB 번호 인덱스    : {len(public_food_index):,}개")
        queries = []
        if mode == "2":
            if not str(direct_query or "").strip():
                _query_exec_log(logger, "  ⚠️ 직접 입력 모드인데 검색어가 비어 있습니다.")
                return None
            qid = upsert_query(
                conn,
                str(direct_query).strip(),
                source="manual_direct",
                priority_score=1000.0,
                target_segment_score=0.0,
                status="pending",
                notes="direct_run",
            )
            queries = conn.execute(
                "SELECT id, query_text, query_norm, priority_score, status FROM query_pool WHERE id=?",
                (qid,),
            ).fetchall()
        else:
            queries = conn.execute(
                """
                SELECT id, query_text, query_norm, priority_score, status
                FROM query_pool
                WHERE status IN ('pending', 'done', 'failed')
                ORDER BY priority_score DESC, id ASC
                LIMIT ?
                """,
                (query_limit,),
            ).fetchall()

        if not queries:
            _query_exec_log(logger, "  ⚠️ 실행할 검색어가 없습니다.")
            return None

        _query_exec_log(logger, f"\n  🚀 실행 시작: 대상 {len(queries)}개")

        for q in queries:
            query_id = int(q["id"])
            query_text = str(q["query_text"] or "").strip()
            query_norm = str(q["query_norm"] or "").strip()
            query_report: dict = {
                "query_id": query_id,
                "query_text": query_text,
                "provider": provider,
                "run_id": None,
                "status": "running",
                "reason": "",
                "total_images": 0,
                "analyzed_images": 0,
                "final_saved_count": 0,
                "api_calls": 0,
                "images": [],
            }
            max_page_done = get_provider_max_page_done(
                conn,
                query_norm=query_norm,
                provider=provider,
            )
            if max_pages <= max_page_done:
                _query_exec_log(logger, f"\n  ⏭️ query_id={query_id} 스킵")
                _query_exec_log(logger, f"    q={query_text}")
                _query_exec_log(
                    logger,
                    f"    사유: provider={provider} 기준 기존 max_page_done={max_page_done} "
                    f">= 요청 max_pages={max_pages}"
                )
                query_report["status"] = "skipped_by_provider_max_page"
                query_report["reason"] = (
                    f"provider={provider}, max_page_done={max_page_done}, requested={max_pages}"
                )
                reports.append(query_report)
                continue
            run_id = start_query_run(conn, query_id=query_id)
            _query_exec_log(logger, f"\n  ▶ query_id={query_id} run_id={run_id}")
            _query_exec_log(logger, f"    q={query_text}")
            query_report["run_id"] = run_id

            analyzed_images = 0
            pass2b_pass_count = 0
            pass4_pass_count = 0
            final_saved_count = 0
            api_calls = 0
            total_images = 0

            try:
                images = _search_images_all(
                    query=query_text,
                    api_key=serp_key,
                    max_pages=max_pages,
                    per_page=100,
                    provider=provider,
                )
                if max_images > 0 and len(images) > max_images:
                    images = images[:max_images]
                total_images = len(images)

                # SERP 캐시 저장
                page_map: dict[int, list[dict]] = {}
                for img in images:
                    page_map.setdefault(int(img.page_no), []).append(
                        {
                            "image_url": img.url,
                            "title": img.title,
                            "source": img.source,
                            "rank_in_page": img.rank_in_page,
                        }
                    )
                for page_no, items in page_map.items():
                    cache_serp_images(
                        conn,
                        query_id=query_id,
                        page=page_no,
                        page_size=100,
                        images=items,
                        run_id=run_id,
                    )

                _query_exec_log(logger, f"    수집 이미지: {total_images}개 | pass 동시호출: {pass_workers}")
                query_report["total_images"] = total_images
                dash = _PipelineDash(query_text=query_text, total=total_images, slots=pass_workers)
                if use_terminal_dashboard:
                    dash.render()

                to_process: list[tuple[int, object]] = []
                for idx, img in enumerate(images, 1):
                    cached = get_image_analysis_cache(conn, img.url)
                    if cached:
                        fail_stage = str(cached["fail_stage"] or "").strip() if "fail_stage" in cached.keys() else ""
                        stage_txt = fail_stage or ("pass4" if int(cached["pass4_ok"] or 0) == 1 else "attempted")

                        if not use_terminal_dashboard:
                            _query_exec_log(logger, f"    [{idx}/{total_images}] 기존 분석이력 스킵 (stage={stage_txt})")
                        dash.on_skip_existing(f"existing_history(stage={stage_txt})")
                        # 실행 결과 리포트는 "이번 실행에서 실제 검수한 이미지"만 노출한다.
                        continue
                    to_process.append((idx, img))

                thread_local = threading.local()

                def _get_analyzer() -> URLIngredientAnalyzer:
                    az = getattr(thread_local, "analyzer", None)
                    if az is None:
                        az = URLIngredientAnalyzer(api_key=openai_key)
                        thread_local.analyzer = az
                    return az

                def _analyze_one(idx: int, img_obj: object, slot_id: int) -> dict:
                    img_url = str(getattr(img_obj, "url"))
                    az = _get_analyzer()
                    dash.on_slot_stage(slot_id, idx, img_url, "PASS1", "start")
                    result: dict = {
                        "idx": idx,
                        "slot_id": slot_id,
                        "url": img_url,
                        "api_calls": 0,
                        "pass1_ok": False,
                        "pass2_ok": False,
                        "pass3_ok": False,
                        "pass4_ok": False,
                        "fail_stage": None,
                        "fail_reason": None,
                        "p2a_ok": False,
                        "p2b_ok": False,
                        "raw_pass2a": None,
                        "raw_pass2b": None,
                        "raw_pass3": None,
                        "raw_pass4": None,
                        "validation_log_json": None,
                        "product_name": None,
                        "report_no": None,
                        "report_no_candidates": None,
                        "report_no_selected_from": None,
                        "ingredients_text": None,
                        "ingredients_text_raw": None,
                        "ingredients_text_corrected": None,
                        "pass3_boundary_corrected": False,
                        "pass3_boundary_confidence": None,
                        "pass3_boundary_issue_codes_json": None,
                        "nutrition_text": None,
                        "data_source_path": None,
                        "nutrition_data_source": "none",
                        "public_food_matched": False,
                        "pass1_attempted": False,
                        "pass2a_attempted": False,
                        "pass2b_attempted": False,
                        "pass3_ing_attempted": False,
                        "pass3_nut_attempted": False,
                        "pass4_ing_attempted": False,
                        "pass4_nut_attempted": False,
                        "orig_w": None,
                        "orig_h": None,
                        "orig_est_tokens": None,
                        "pass2_w": None,
                        "pass2_h": None,
                        "pass2_est_tokens": None,
                        "pass2_resized": False,
                        "pass3_w": None,
                        "pass3_h": None,
                        "pass3_est_tokens": None,
                        "pass3_resized": False,
                        "resize_policy_code": "ok",
                        "resize_policy_reason": None,
                        "resize_trace_json": None,
                    }

                    try:
                        image_bytes, mime_type = az._download_image(img_url)  # pylint: disable=protected-access
                    except Exception as exc:  # pylint: disable=broad-except
                        result["fail_stage"] = "download"
                        result["fail_reason"] = str(exc) or "image_download_failed"
                        return result
                    ow, oh = _extract_image_size(image_bytes, mime_type)
                    result["orig_w"] = ow
                    result["orig_h"] = oh
                    est_tok = _estimate_vision_tokens(ow, oh)
                    result["orig_est_tokens"] = est_tok
                    ratio = (float(oh) / float(ow)) if ow and oh else None
                    if ratio is not None and ratio > float(vision_reject_aspect_ratio):
                        result["fail_stage"] = "pass1"
                        result["fail_reason"] = (
                            f"policy_excessive_aspect_ratio: aspect={ratio:.2f}>{vision_reject_aspect_ratio:.2f}"
                        )
                        result["resize_policy_code"] = "policy_excessive_aspect_ratio"
                        result["resize_policy_reason"] = result["fail_reason"]
                        dash.on_slot_stage(
                            slot_id,
                            idx,
                            img_url,
                            "PASS1",
                            f"policy_block aspect={ratio:.2f}>{vision_reject_aspect_ratio:.2f}",
                        )
                        return result

                    pass2_bytes = image_bytes
                    pass2_mime = mime_type
                    pass3_bytes = image_bytes
                    pass3_mime = mime_type
                    resize_trace: dict[str, Any] = {
                        "aspect_ratio": ratio,
                        "aspect_limit": float(vision_reject_aspect_ratio),
                        "pass2_target_tokens": int(vision_target_tokens_pass2),
                        "pass3_target_tokens": int(vision_target_tokens_pass3),
                    }
                    try:
                        dash.on_slot_stage(slot_id, idx, img_url, "RESIZE-P2", "prepare")
                        (
                            pass2_bytes,
                            pass2_mime,
                            p2w,
                            p2h,
                            p2tok,
                            p2resized,
                            p2trace,
                        ) = _resize_image_to_target_tokens(
                            image_bytes=image_bytes,
                            mime_type=mime_type,
                            target_tokens=vision_target_tokens_pass2,
                            min_short_side=vision_min_short_side,
                            min_long_side=vision_min_long_side,
                        )
                        result["pass2_w"] = p2w
                        result["pass2_h"] = p2h
                        result["pass2_est_tokens"] = p2tok
                        result["pass2_resized"] = bool(p2resized)
                        resize_trace["pass2"] = p2trace
                        dash.on_slot_stage(
                            slot_id,
                            idx,
                            img_url,
                            "RESIZE-P2",
                            f"{p2trace} | {'resized' if p2resized else 'keep'}",
                        )
                    except Exception as exc:  # pylint: disable=broad-except
                        result["fail_stage"] = "pass1"
                        result["fail_reason"] = f"policy_resize_failed_pass2:{exc}"
                        result["resize_policy_code"] = "policy_resize_failed_pass2"
                        result["resize_policy_reason"] = str(exc)
                        resize_trace["pass2"] = f"error:{exc}"
                        result["resize_trace_json"] = json.dumps(resize_trace, ensure_ascii=False)
                        dash.on_slot_stage(slot_id, idx, img_url, "RESIZE-P2", f"error:{exc}")
                        return result

                    try:
                        dash.on_slot_stage(slot_id, idx, img_url, "RESIZE-P3", "prepare")
                        (
                            pass3_bytes,
                            pass3_mime,
                            p3w,
                            p3h,
                            p3tok,
                            p3resized,
                            p3trace,
                        ) = _resize_image_to_target_tokens(
                            image_bytes=image_bytes,
                            mime_type=mime_type,
                            target_tokens=vision_target_tokens_pass3,
                            min_short_side=vision_min_short_side,
                            min_long_side=vision_min_long_side,
                        )
                        result["pass3_w"] = p3w
                        result["pass3_h"] = p3h
                        result["pass3_est_tokens"] = p3tok
                        result["pass3_resized"] = bool(p3resized)
                        resize_trace["pass3"] = p3trace
                        dash.on_slot_stage(
                            slot_id,
                            idx,
                            img_url,
                            "RESIZE-P3",
                            f"{p3trace} | {'resized' if p3resized else 'keep'}",
                        )
                    except Exception as exc:  # pylint: disable=broad-except
                        result["fail_stage"] = "pass1"
                        result["fail_reason"] = f"policy_resize_failed_pass3:{exc}"
                        result["resize_policy_code"] = "policy_resize_failed_pass3"
                        result["resize_policy_reason"] = str(exc)
                        resize_trace["pass3"] = f"error:{exc}"
                        result["resize_trace_json"] = json.dumps(resize_trace, ensure_ascii=False)
                        dash.on_slot_stage(slot_id, idx, img_url, "RESIZE-P3", f"error:{exc}")
                        return result

                    result["resize_policy_code"] = "ok"
                    result["resize_policy_reason"] = "ok"
                    result["resize_trace_json"] = json.dumps(resize_trace, ensure_ascii=False)
                    dash.on_slot_stage(
                        slot_id,
                        idx,
                        img_url,
                        "PASS1",
                        (
                            f"orig {ow}x{oh} tok~{est_tok or '-'} | "
                            f"p2 {result.get('pass2_w')}x{result.get('pass2_h')} tok~{result.get('pass2_est_tokens') or '-'} | "
                            f"p3 {result.get('pass3_w')}x{result.get('pass3_h')} tok~{result.get('pass3_est_tokens') or '-'}"
                            if ow and oh
                            else "size=unknown"
                        ),
                    )

                    result["pass1_attempted"] = True
                    pass1 = run_pass1_precheck(
                        az,
                        image_bytes=pass2_bytes,
                        mime_type=pass2_mime,
                        image_url=img_url,
                    )
                    pass1_ok = bool(pass1.get("precheck_pass"))
                    result["pass1_ok"] = pass1_ok
                    if not pass1_ok:
                        result["fail_stage"] = "pass1"
                        result["fail_reason"] = str(pass1.get("precheck_reason") or "pass1_precheck_failed")
                        return result

                    result["pass2a_attempted"] = True
                    dash.on_slot_stage(slot_id, idx, img_url, "PASS2A", "running")
                    pass2 = az.analyze_pass2_from_bytes(pass2_bytes, pass2_mime, target_item_rpt_no=None)
                    result["api_calls"] += 1
                    qf = pass2.get("quality_flags") or {}
                    p2a_ok = bool(qf.get("pass2a_ok"))
                    p2b_ok = bool(qf.get("pass2b_pass"))
                    p2b_executed = bool(qf.get("pass2b_executed"))
                    gate_ok = bool(pass2.get("quality_gate_pass"))
                    decision = str(pass2.get("ai_decision") or "").upper()
                    pass2_ok = gate_ok and p2a_ok and p2b_ok and decision == "READ"
                    result["p2a_ok"] = p2a_ok
                    result["p2b_ok"] = p2b_ok
                    result["pass2b_attempted"] = p2b_executed
                    result["raw_pass2a"] = pass2.get("raw_model_text_pass2a")
                    result["raw_pass2b"] = pass2.get("raw_model_text_pass2b")

                    if not pass2_ok:
                        result["fail_stage"] = "pass2"
                        result["fail_reason"] = (
                            "|".join(str(x) for x in (pass2.get("quality_fail_reasons") or []))
                            or str(pass2.get("ai_decision_reason") or "pass2_fail")
                        )
                        return result

                    result["pass2_ok"] = True
                    result["pass3_ing_attempted"] = True
                    result["pass3_nut_attempted"] = False
                    dash.on_slot_stage(slot_id, idx, img_url, "PASS3", "ingredients")
                    if str(getattr(az, "pass3_provider", "")).lower() == "gemini":
                        dash.on_pass3_wait_start(slot_id, idx, img_url, "ingredients")
                        pass3_gemini_sem.acquire()
                        try:
                            dash.on_pass3_wait_end(slot_id, idx, img_url, "ingredients")
                            pass3_ing = az.analyze_pass3_from_bytes(
                                image_bytes=pass3_bytes,
                                mime_type=pass3_mime,
                                target_item_rpt_no=None,
                                include_nutrition=False,
                            )
                        finally:
                            pass3_gemini_sem.release()
                    else:
                        pass3_ing = az.analyze_pass3_from_bytes(
                            image_bytes=pass3_bytes,
                            mime_type=pass3_mime,
                            target_item_rpt_no=None,
                            include_nutrition=False,
                        )
                    result["api_calls"] += 1
                    raw_pass3_ing = pass3_ing.get("raw_model_text_pass3")
                    # Pass3 조기 차단 시에도 리포트에서 RAW를 확인할 수 있도록 즉시 보관한다.
                    result["raw_pass3"] = raw_pass3_ing
                    pass3_err = str(pass3_ing.get("error") or "").strip()
                    if pass3_err:
                        result["fail_stage"] = "pass3"
                        result["fail_reason"] = pass3_err
                        return result

                    product_name = (pass3_ing.get("product_name_in_image") or "").strip()
                    report_raw = (pass3_ing.get("product_report_number") or "").strip()
                    ingredients_complete_flag = bool(pass3_ing.get("ingredients_complete"))
                    report_number_complete_flag = bool(pass3_ing.get("report_number_complete"))
                    has_report_label_flag = bool(pass3_ing.get("has_report_label"))
                    product_name_complete_flag = bool(pass3_ing.get("product_name_complete"))
                    report_candidates = _extract_report_no_candidates(report_raw)
                    selected_report_no, public_row, selected_reason = _select_best_report_candidate(
                        report_candidates=report_candidates,
                        public_food_index=public_food_index,
                    )
                    ingredients_text = (pass3_ing.get("ingredients_text") or "").strip()
                    ingredients_evidence_text = (pass3_ing.get("ingredients_evidence_text") or "").strip()
                    boundary_fix = _correct_ingredients_boundaries(ingredients_text)
                    ingredients_text_corrected = str(boundary_fix.get("corrected_text") or ingredients_text).strip()
                    result["ingredients_text_raw"] = ingredients_text
                    result["ingredients_text_corrected"] = ingredients_text_corrected
                    result["pass3_boundary_corrected"] = bool(boundary_fix.get("boundary_corrected"))
                    result["pass3_boundary_confidence"] = int(boundary_fix.get("boundary_confidence") or 0)
                    result["pass3_boundary_issue_codes_json"] = json.dumps(
                        {
                            "issues": boundary_fix.get("issue_codes") or [],
                            "actions": boundary_fix.get("actions") or [],
                        },
                        ensure_ascii=False,
                    )
                    validation = _validate_pass3_ingredients_verbatim(
                        ingredients_text=ingredients_text,
                        evidence_text=ingredients_evidence_text,
                    )
                    result["validation_log_json"] = json.dumps(validation, ensure_ascii=False)
                    if str(validation.get("status")) == "fail":
                        result["fail_stage"] = "pass3"
                        result["fail_reason"] = (
                            f"{validation.get('code')}:{validation.get('message')}"
                        )
                        return result
                    nutrition_text = None
                    if not ingredients_text:
                        result["fail_stage"] = "pass3"
                        guard_issues = pass3_ing.get("pass3_guard_issues") or []
                        if isinstance(guard_issues, list) and guard_issues:
                            result["fail_reason"] = "required_fields_missing(ingredients)|" + ",".join(
                                str(x) for x in guard_issues
                            )
                        else:
                            result["fail_reason"] = "required_fields_missing(ingredients)"
                        return result
                    if not ingredients_complete_flag:
                        result["fail_stage"] = "pass3"
                        result["fail_reason"] = "pass3_incomplete_ingredients"
                        return result
                    if (not has_report_label_flag) or (not report_number_complete_flag):
                        result["fail_stage"] = "pass3"
                        result["fail_reason"] = "pass3_incomplete_report_number_or_label"
                        return result
                    report_no = selected_report_no
                    public_complete = bool(
                        public_row
                        and public_row.get("has_name")
                        and public_row.get("has_nutrition")
                    )
                    result["report_no_candidates"] = json.dumps(report_candidates, ensure_ascii=False)
                    result["report_no_selected_from"] = selected_reason
                    result["public_food_matched"] = bool(public_row)
                    result["data_source_path"] = "public_food_enriched" if public_complete else "image_full_extraction"
                    if not report_no:
                        result["fail_stage"] = "pass3"
                        result["fail_reason"] = "no_valid_report_candidates"
                        return result

                    # Case B: Pass3 번호 기준 공공DB 미완전이면, Pass2B에서 제품명/영양성분 존재가 둘 다 필요
                    if not public_complete:
                        has_product_in_p2b = bool(qf.get("has_product_name"))
                        has_nutri_in_p2b = bool(qf.get("has_nutrition_section"))
                        if not (has_product_in_p2b and has_nutri_in_p2b):
                            result["fail_stage"] = "pass2b"
                            result["fail_reason"] = "case_b_requires_product_and_nutrition_at_pass2b"
                            return result
                        if (not product_name) or (not product_name_complete_flag):
                            result["fail_stage"] = "pass3"
                            result["fail_reason"] = "case_b_requires_product_name_at_pass3_ingredients"
                            return result
                        # Case B에서만 Pass3-Nutrition 별도 호출
                        result["pass3_nut_attempted"] = True
                        dash.on_slot_stage(slot_id, idx, img_url, "PASS3", "nutrition")
                        prompt_nut = az._build_prompt_pass3_nutrition(target_item_rpt_no=None)  # pylint: disable=protected-access
                        raw_nut = None
                        parsed_nut = {}
                        last_nut_err = None
                        max_attempts = max(1, int(getattr(az, "model_retries", 0)) + 1)
                        max_429_attempts = max(
                            max_attempts,
                            int(getattr(az, "pass3_retry_on_429_max_attempts", 6)),
                        )
                        base_429 = float(getattr(az, "pass3_retry_on_429_base_sec", 2.0))
                        cap_429 = float(getattr(az, "pass3_retry_on_429_max_sec", 30.0))
                        default_backoff = float(getattr(az, "retry_backoff_sec", 0.8))
                        attempt = 0
                        while attempt < max_attempts:
                            try:
                                if str(getattr(az, "pass3_provider", "")).lower() == "gemini":
                                    dash.on_pass3_wait_start(slot_id, idx, img_url, "nutrition")
                                    pass3_gemini_sem.acquire()
                                    try:
                                        dash.on_pass3_wait_end(slot_id, idx, img_url, "nutrition")
                                        raw_nut, parsed_nut, _raw_api_nut = az._call_model_pass3(  # pylint: disable=protected-access
                                            image_bytes=pass3_bytes,
                                            mime_type=pass3_mime,
                                            prompt=prompt_nut,
                                        )
                                    finally:
                                        pass3_gemini_sem.release()
                                else:
                                    raw_nut, parsed_nut, _raw_api_nut = az._call_model_pass3(  # pylint: disable=protected-access
                                        image_bytes=pass3_bytes,
                                        mime_type=pass3_mime,
                                        prompt=prompt_nut,
                                    )
                                break
                            except Exception as exc:  # pylint: disable=broad-except
                                last_nut_err = exc
                                msg = str(exc).lower()
                                is_429 = ("429" in msg) or ("resource_exhausted" in msg)
                                retryable = any(
                                    k in msg
                                    for k in (
                                        "429",
                                        "resource_exhausted",
                                        "503",
                                        "502",
                                        "504",
                                        "deadline",
                                        "timeout",
                                        "temporarily unavailable",
                                    )
                                )
                                # 429는 더 길게/많이 재시도
                                limit = max_429_attempts if is_429 else max_attempts
                                if retryable and attempt < (limit - 1):
                                    if is_429:
                                        delay = min(cap_429, base_429 * (2 ** attempt))
                                        dash.on_slot_stage(
                                            slot_id,
                                            idx,
                                            img_url,
                                            "PASS3",
                                            f"nutrition retry {attempt+1}/{limit} in {delay:.1f}s (429)",
                                        )
                                    else:
                                        delay = default_backoff * (attempt + 1)
                                        dash.on_slot_stage(
                                            slot_id,
                                            idx,
                                            img_url,
                                            "PASS3",
                                            f"nutrition retry {attempt+1}/{limit} in {delay:.1f}s",
                                        )
                                    time.sleep(delay)
                                    # 429 추가 재시도를 위해 loop length 동적 확장
                                    if is_429 and max_attempts < max_429_attempts:
                                        max_attempts = max_429_attempts
                                    attempt += 1
                                    continue
                            attempt += 1
                        if raw_nut is None and parsed_nut == {}:
                            result["fail_stage"] = "pass3"
                            result["fail_reason"] = f"pass3_nutrition_error:{last_nut_err}"
                            return result
                        result["api_calls"] += 1
                        nutrition_text = (parsed_nut.get("nutrition_text") or "").strip() or None
                        if not nutrition_text:
                            result["fail_stage"] = "pass3"
                            result["fail_reason"] = "case_b_requires_nutrition_at_pass3_nutrition"
                            return result
                        result["raw_pass3"] = (raw_pass3_ing or "") + "\n\n[PASS3-NUTRITION]\n" + (raw_nut or "")
                    else:
                        result["raw_pass3"] = raw_pass3_ing

                    result["pass3_ok"] = True
                    if public_complete and public_row:
                        result["product_name"] = str(public_row.get("product_name") or "").strip() or product_name
                        result["nutrition_text"] = str(public_row.get("nutrition_text") or "").strip() or None
                        result["nutrition_data_source"] = "public_food_db"
                    else:
                        result["product_name"] = product_name
                        result["nutrition_text"] = nutrition_text
                        result["nutrition_data_source"] = "image_pass4"
                    result["report_no"] = report_no
                    result["ingredients_text"] = ingredients_text_corrected or ingredients_text

                    result["pass4_ing_attempted"] = True
                    result["pass4_nut_attempted"] = bool((not public_complete) and nutrition_text)
                    dash.on_slot_stage(slot_id, idx, img_url, "PASS4", "parse")
                    pass3_for_pass4 = dict(pass3_ing)
                    pass3_for_pass4["ingredients_text"] = ingredients_text_corrected or ingredients_text
                    pass3_for_pass4["product_report_number"] = report_no
                    if public_complete:
                        # Case A: 영양성분은 공공DB 사용, Pass4 영양 파싱 호출 차단
                        pass3_for_pass4["nutrition_text"] = None
                        pass3_for_pass4["nutrition_complete"] = False
                        result["pass4_nut_attempted"] = False
                    else:
                        pass3_for_pass4["nutrition_text"] = nutrition_text
                        pass3_for_pass4["nutrition_complete"] = True if nutrition_text else False
                    pass4 = None
                    ingredient_items = []
                    pass4_raw_parts: list[str] = []
                    for pass4_try in range(2):
                        if pass4_try == 1:
                            dash.on_slot_stage(slot_id, idx, img_url, "PASS4", "retry-empty-ingredients")
                        current_pass4 = az.analyze_pass4_normalize(
                            pass2_result=pass2,
                            pass3_result=pass3_for_pass4,
                            target_item_rpt_no=None,
                        )
                        result["api_calls"] += 1
                        current_raw = str(current_pass4.get("raw_model_text_pass4") or "").strip()
                        if current_raw:
                            pass4_raw_parts.append(
                                f"[ATTEMPT {pass4_try + 1}]\n{current_raw}"
                            )

                        pass4_err = str(current_pass4.get("pass4_ai_error") or "").strip()
                        if pass4_err:
                            result["raw_pass4"] = "\n\n".join(pass4_raw_parts) if pass4_raw_parts else None
                            result["fail_stage"] = "pass4"
                            result["fail_reason"] = pass4_err or "pass4_fail"
                            return result

                        current_items = current_pass4.get("ingredient_items") or []
                        pass4 = current_pass4
                        ingredient_items = current_items if isinstance(current_items, list) else []
                        if ingredient_items:
                            break

                    result["raw_pass4"] = "\n\n".join(pass4_raw_parts) if pass4_raw_parts else (pass4.get("raw_model_text_pass4") if pass4 else None)
                    if not isinstance(ingredient_items, list) or len(ingredient_items) == 0:
                        result["fail_stage"] = "pass4"
                        ingredient_reason = str(pass4.get("ingredient_items_reason") or "").strip().lower()
                        # Pass4 내부에서 필수 필드 부족으로 API 호출 자체가 스킵된 경우를 명확히 구분
                        if ingredient_reason in ("pass4_skipped_missing_required_fields", "pass4_skipped_missing_ingredients_text"):
                            result["fail_reason"] = "pass4_skipped_no_api_call_missing_required_fields"
                        else:
                            result["fail_reason"] = "pass4_no_structured_ingredients"
                        return result

                    raw_pass4_ing = str(pass4.get("raw_model_text_pass4_ingredients") or "").strip()
                    if raw_pass4_ing:
                        result["ingredients_text"] = raw_pass4_ing
                    else:
                        result["ingredients_text"] = json.dumps(
                            {"ingredients_items": ingredient_items},
                            ensure_ascii=False,
                        )

                    if not public_complete:
                        raw_pass4_nut = str(pass4.get("raw_model_text_pass4_nutrition") or "").strip()
                        if raw_pass4_nut:
                            result["nutrition_text"] = raw_pass4_nut
                        else:
                            nut_items = pass4.get("nutrition_items") or []
                            if isinstance(nut_items, list) and nut_items:
                                result["nutrition_text"] = json.dumps(
                                    {"nutrition_items": nut_items},
                                    ensure_ascii=False,
                                )
                            else:
                                result["nutrition_text"] = None

                    result["pass4_ok"] = True
                    return result

                done_count = 0
                with ThreadPoolExecutor(max_workers=pass_workers) as ex:
                    pending = list(to_process)
                    free_slots = list(range(1, pass_workers + 1))
                    fut_map: dict = {}

                    while pending or fut_map:
                        while pending and free_slots:
                            idx, img = pending.pop(0)
                            slot_id = free_slots.pop(0)
                            fut = ex.submit(_analyze_one, idx, img, slot_id)
                            fut_map[fut] = slot_id

                        if not fut_map:
                            break

                        done_futs, _ = wait(set(fut_map.keys()), return_when=FIRST_COMPLETED)
                        for fut in done_futs:
                            slot_id = int(fut_map.pop(fut))
                            res = fut.result()
                            free_slots.append(slot_id)
                            free_slots.sort()

                        idx = int(res["idx"])
                        done_count += 1
                        analyzed_images += 1
                        api_calls += int(res.get("api_calls", 0))
                        p1 = _yn_flag(res.get("pass1_ok"))
                        p2a = _yn_flag(res.get("p2a_ok"))
                        p2b = _yn_flag(res.get("p2b_ok"))
                        p3 = _yn_flag(res.get("pass3_ok"))
                        p4 = _yn_flag(res.get("pass4_ok"))
                        fail_stage = str(res.get("fail_stage") or "")
                        fail_reason = _compact_reason(str(res.get("fail_reason") or ""))
                        url_short = str(res.get("url") or "")
                        if bool(res.get("pass4_ok")):
                            status_txt = "🏆 PASS4 최종통과"
                            reason_txt = ""
                        else:
                            last_ok = "pass0"
                            if res.get("pass1_ok"):
                                last_ok = "pass1"
                            if res.get("p2a_ok") and res.get("p2b_ok"):
                                last_ok = "pass2"
                            if res.get("pass3_ok"):
                                last_ok = "pass3"
                            status_txt = f"⛔ {last_ok}까지 통과"
                            reason_txt = f" | fail={fail_stage}:{fail_reason}" if fail_stage else ""
                        if not use_terminal_dashboard:
                            _query_exec_log(
                                logger,
                                (
                                    f"    [{idx}/{total_images}] {status_txt} "
                                    f"| P1 {p1} P2A {p2a} P2B {p2b} P3 {p3} P4 {p4}"
                                    f"{reason_txt} | url={url_short}"
                                ),
                            )
                        dash.on_finish(
                            int(res.get("slot_id") or slot_id),
                            idx,
                            str(res.get("url") or ""),
                            pass4_ok=bool(res.get("pass4_ok")),
                            fail_stage=str(res.get("fail_stage") or ""),
                            fail_reason=str(res.get("fail_reason") or ""),
                            api_calls=int(res.get("api_calls") or 0),
                            orig_tokens=(
                                int(res.get("orig_est_tokens"))
                                if res.get("orig_est_tokens") is not None
                                else None
                            ),
                            pass2_tokens=(
                                int(res.get("pass2_est_tokens"))
                                if res.get("pass2_est_tokens") is not None
                                else None
                            ),
                            pass3_tokens=(
                                int(res.get("pass3_est_tokens"))
                                if res.get("pass3_est_tokens") is not None
                                else None
                            ),
                        )
                        image_status = "saved" if bool(res.get("pass4_ok")) else "failed"
                        query_report["images"].append(
                            {
                                "idx": idx,
                                "url": str(res.get("url") or ""),
                                "status": image_status,
                                "fail_reason": str(res.get("fail_reason") or ""),
                                "pass1_attempted": bool(res.get("pass1_attempted")),
                                "pass2a_attempted": bool(res.get("pass2a_attempted")),
                                "pass2b_attempted": bool(res.get("pass2b_attempted")),
                                "pass3_ing_attempted": bool(res.get("pass3_ing_attempted")),
                                "pass3_nut_attempted": bool(res.get("pass3_nut_attempted")),
                                "pass4_ing_attempted": bool(res.get("pass4_ing_attempted")),
                                "pass4_nut_attempted": bool(res.get("pass4_nut_attempted")),
                                "pass1_ok": res.get("pass1_ok"),
                                "p2a_ok": res.get("p2a_ok"),
                                "p2b_ok": res.get("p2b_ok"),
                                "pass3_ok": res.get("pass3_ok"),
                                "pass4_ok": res.get("pass4_ok"),
                                "data_source_path": res.get("data_source_path"),
                                "nutrition_data_source": res.get("nutrition_data_source"),
                                "public_food_matched": bool(res.get("public_food_matched")),
                                "report_no": res.get("report_no"),
                                "report_no_candidates": res.get("report_no_candidates"),
                                "report_no_selected_from": res.get("report_no_selected_from"),
                                "product_name": res.get("product_name"),
                                "ingredients_text": res.get("ingredients_text"),
                                "ingredients_text_raw": res.get("ingredients_text_raw"),
                                "ingredients_text_corrected": res.get("ingredients_text_corrected"),
                                "pass3_boundary_corrected": bool(res.get("pass3_boundary_corrected")),
                                "pass3_boundary_confidence": res.get("pass3_boundary_confidence"),
                                "pass3_boundary_issue_codes_json": res.get("pass3_boundary_issue_codes_json"),
                                "nutrition_text": res.get("nutrition_text"),
                                "raw_pass2a": res.get("raw_pass2a"),
                                "raw_pass2b": res.get("raw_pass2b"),
                                "raw_pass3": res.get("raw_pass3"),
                                "raw_pass4": res.get("raw_pass4"),
                                "validation_log_json": res.get("validation_log_json"),
                                "orig_w": res.get("orig_w"),
                                "orig_h": res.get("orig_h"),
                                "orig_est_tokens": res.get("orig_est_tokens"),
                                "pass2_w": res.get("pass2_w"),
                                "pass2_h": res.get("pass2_h"),
                                "pass2_est_tokens": res.get("pass2_est_tokens"),
                                "pass2_resized": bool(res.get("pass2_resized")),
                                "pass3_w": res.get("pass3_w"),
                                "pass3_h": res.get("pass3_h"),
                                "pass3_est_tokens": res.get("pass3_est_tokens"),
                                "pass3_resized": bool(res.get("pass3_resized")),
                                "resize_policy_code": res.get("resize_policy_code"),
                                "resize_policy_reason": res.get("resize_policy_reason"),
                                "resize_trace_json": res.get("resize_trace_json"),
                            }
                        )

                        if not bool(res.get("pass2_ok")):
                            upsert_image_analysis_cache(
                                conn,
                                image_url=str(res["url"]),
                                run_id=run_id,
                                pass1_ok=bool(res.get("pass1_ok")),
                                pass2a_ok=bool(res.get("p2a_ok")),
                                pass2b_ok=bool(res.get("p2b_ok")),
                                pass3_ok=False,
                                pass4_ok=False,
                                fail_stage=str(res.get("fail_stage") or "pass2"),
                                fail_reason=str(res.get("fail_reason") or "pass2_fail"),
                                raw_pass2a=res.get("raw_pass2a"),
                                raw_pass2b=res.get("raw_pass2b"),
                                validation_log_json=res.get("validation_log_json"),
                                pass1_attempted=bool(res.get("pass1_attempted")),
                                pass2a_attempted=bool(res.get("pass2a_attempted")),
                                pass2b_attempted=bool(res.get("pass2b_attempted")),
                                pass3_ing_attempted=False,
                                pass3_nut_attempted=False,
                                pass4_ing_attempted=False,
                                pass4_nut_attempted=False,
                                data_source_path=res.get("data_source_path"),
                                nutrition_data_source=res.get("nutrition_data_source"),
                                public_food_matched=bool(res.get("public_food_matched")),
                                orig_w=res.get("orig_w"),
                                orig_h=res.get("orig_h"),
                                orig_est_tokens=res.get("orig_est_tokens"),
                                pass2_w=res.get("pass2_w"),
                                pass2_h=res.get("pass2_h"),
                                pass2_est_tokens=res.get("pass2_est_tokens"),
                                pass2_resized=bool(res.get("pass2_resized")),
                                pass3_w=res.get("pass3_w"),
                                pass3_h=res.get("pass3_h"),
                                pass3_est_tokens=res.get("pass3_est_tokens"),
                                pass3_resized=bool(res.get("pass3_resized")),
                                resize_policy_code=res.get("resize_policy_code"),
                                resize_policy_reason=res.get("resize_policy_reason"),
                                resize_trace_json=res.get("resize_trace_json"),
                                pass3_ingredients_raw=res.get("ingredients_text_raw"),
                                pass3_ingredients_corrected=res.get("ingredients_text_corrected"),
                                pass3_boundary_corrected=bool(res.get("pass3_boundary_corrected")),
                                pass3_boundary_confidence=res.get("pass3_boundary_confidence"),
                                pass3_boundary_issue_codes_json=res.get("pass3_boundary_issue_codes_json"),
                            )
                            continue

                        pass2b_pass_count += 1

                        if not bool(res.get("pass3_ok")):
                            upsert_image_analysis_cache(
                                conn,
                                image_url=str(res["url"]),
                                run_id=run_id,
                                pass1_ok=bool(res.get("pass1_ok")),
                                pass2a_ok=bool(res.get("p2a_ok")),
                                pass2b_ok=bool(res.get("p2b_ok")),
                                pass3_ok=False,
                                pass4_ok=False,
                                fail_stage=str(res.get("fail_stage") or "pass3"),
                                fail_reason=str(res.get("fail_reason") or "pass3_fail"),
                                raw_pass2a=res.get("raw_pass2a"),
                                raw_pass2b=res.get("raw_pass2b"),
                                raw_pass3=res.get("raw_pass3"),
                                validation_log_json=res.get("validation_log_json"),
                                pass1_attempted=bool(res.get("pass1_attempted")),
                                pass2a_attempted=bool(res.get("pass2a_attempted")),
                                pass2b_attempted=bool(res.get("pass2b_attempted")),
                                pass3_ing_attempted=bool(res.get("pass3_ing_attempted")),
                                pass3_nut_attempted=bool(res.get("pass3_nut_attempted")),
                                pass4_ing_attempted=False,
                                pass4_nut_attempted=False,
                                data_source_path=res.get("data_source_path"),
                                nutrition_data_source=res.get("nutrition_data_source"),
                                public_food_matched=bool(res.get("public_food_matched")),
                                orig_w=res.get("orig_w"),
                                orig_h=res.get("orig_h"),
                                orig_est_tokens=res.get("orig_est_tokens"),
                                pass2_w=res.get("pass2_w"),
                                pass2_h=res.get("pass2_h"),
                                pass2_est_tokens=res.get("pass2_est_tokens"),
                                pass2_resized=bool(res.get("pass2_resized")),
                                pass3_w=res.get("pass3_w"),
                                pass3_h=res.get("pass3_h"),
                                pass3_est_tokens=res.get("pass3_est_tokens"),
                                pass3_resized=bool(res.get("pass3_resized")),
                                resize_policy_code=res.get("resize_policy_code"),
                                resize_policy_reason=res.get("resize_policy_reason"),
                                resize_trace_json=res.get("resize_trace_json"),
                                pass3_ingredients_raw=res.get("ingredients_text_raw"),
                                pass3_ingredients_corrected=res.get("ingredients_text_corrected"),
                                pass3_boundary_corrected=bool(res.get("pass3_boundary_corrected")),
                                pass3_boundary_confidence=res.get("pass3_boundary_confidence"),
                                pass3_boundary_issue_codes_json=res.get("pass3_boundary_issue_codes_json"),
                            )
                            continue

                        if not bool(res.get("pass4_ok")):
                            upsert_image_analysis_cache(
                                conn,
                                image_url=str(res["url"]),
                                run_id=run_id,
                                pass1_ok=bool(res.get("pass1_ok")),
                                pass2a_ok=bool(res.get("p2a_ok")),
                                pass2b_ok=bool(res.get("p2b_ok")),
                                pass3_ok=True,
                                pass4_ok=False,
                                fail_stage=str(res.get("fail_stage") or "pass4"),
                                fail_reason=str(res.get("fail_reason") or "pass4_fail"),
                                raw_pass2a=res.get("raw_pass2a"),
                                raw_pass2b=res.get("raw_pass2b"),
                                raw_pass3=res.get("raw_pass3"),
                                raw_pass4=res.get("raw_pass4"),
                                validation_log_json=res.get("validation_log_json"),
                                pass1_attempted=bool(res.get("pass1_attempted")),
                                pass2a_attempted=bool(res.get("pass2a_attempted")),
                                pass2b_attempted=bool(res.get("pass2b_attempted")),
                                pass3_ing_attempted=bool(res.get("pass3_ing_attempted")),
                                pass3_nut_attempted=bool(res.get("pass3_nut_attempted")),
                                pass4_ing_attempted=bool(res.get("pass4_ing_attempted")),
                                pass4_nut_attempted=bool(res.get("pass4_nut_attempted")),
                                data_source_path=res.get("data_source_path"),
                                nutrition_data_source=res.get("nutrition_data_source"),
                                public_food_matched=bool(res.get("public_food_matched")),
                                orig_w=res.get("orig_w"),
                                orig_h=res.get("orig_h"),
                                orig_est_tokens=res.get("orig_est_tokens"),
                                pass2_w=res.get("pass2_w"),
                                pass2_h=res.get("pass2_h"),
                                pass2_est_tokens=res.get("pass2_est_tokens"),
                                pass2_resized=bool(res.get("pass2_resized")),
                                pass3_w=res.get("pass3_w"),
                                pass3_h=res.get("pass3_h"),
                                pass3_est_tokens=res.get("pass3_est_tokens"),
                                pass3_resized=bool(res.get("pass3_resized")),
                                resize_policy_code=res.get("resize_policy_code"),
                                resize_policy_reason=res.get("resize_policy_reason"),
                                resize_trace_json=res.get("resize_trace_json"),
                                pass3_ingredients_raw=res.get("ingredients_text_raw"),
                                pass3_ingredients_corrected=res.get("ingredients_text_corrected"),
                                pass3_boundary_corrected=bool(res.get("pass3_boundary_corrected")),
                                pass3_boundary_confidence=res.get("pass3_boundary_confidence"),
                                pass3_boundary_issue_codes_json=res.get("pass3_boundary_issue_codes_json"),
                            )
                            continue

                        pass4_pass_count += 1
                        upsert_food_final(
                            conn,
                            product_name=str(res.get("product_name") or ""),
                            item_mnftr_rpt_no=str(res.get("report_no") or ""),
                            all_report_nos_json=str(res.get("report_no_candidates") or ""),
                            report_no_selected_from=str(res.get("report_no_selected_from") or ""),
                            ingredients_text=str(res.get("ingredients_text") or ""),
                            nutrition_text=res.get("nutrition_text"),
                            nutrition_source=str(res.get("nutrition_data_source") or "none"),
                            source_image_url=str(res["url"]),
                            source_query_id=query_id,
                            source_run_id=run_id,
                        )
                        final_saved_count += 1

                        upsert_image_analysis_cache(
                            conn,
                            image_url=str(res["url"]),
                            run_id=run_id,
                            pass1_ok=bool(res.get("pass1_ok")),
                            pass2a_ok=bool(res.get("p2a_ok")),
                            pass2b_ok=bool(res.get("p2b_ok")),
                            pass3_ok=True,
                            pass4_ok=True,
                            fail_stage=None,
                            fail_reason=None,
                            raw_pass2a=res.get("raw_pass2a"),
                            raw_pass2b=res.get("raw_pass2b"),
                            raw_pass3=res.get("raw_pass3"),
                            raw_pass4=res.get("raw_pass4"),
                            validation_log_json=res.get("validation_log_json"),
                            pass1_attempted=bool(res.get("pass1_attempted")),
                            pass2a_attempted=bool(res.get("pass2a_attempted")),
                            pass2b_attempted=bool(res.get("pass2b_attempted")),
                            pass3_ing_attempted=bool(res.get("pass3_ing_attempted")),
                            pass3_nut_attempted=bool(res.get("pass3_nut_attempted")),
                            pass4_ing_attempted=bool(res.get("pass4_ing_attempted")),
                            pass4_nut_attempted=bool(res.get("pass4_nut_attempted")),
                            data_source_path=res.get("data_source_path"),
                            nutrition_data_source=res.get("nutrition_data_source"),
                            public_food_matched=bool(res.get("public_food_matched")),
                            orig_w=res.get("orig_w"),
                            orig_h=res.get("orig_h"),
                            orig_est_tokens=res.get("orig_est_tokens"),
                            pass2_w=res.get("pass2_w"),
                            pass2_h=res.get("pass2_h"),
                            pass2_est_tokens=res.get("pass2_est_tokens"),
                            pass2_resized=bool(res.get("pass2_resized")),
                            pass3_w=res.get("pass3_w"),
                            pass3_h=res.get("pass3_h"),
                            pass3_est_tokens=res.get("pass3_est_tokens"),
                            pass3_resized=bool(res.get("pass3_resized")),
                            resize_policy_code=res.get("resize_policy_code"),
                            resize_policy_reason=res.get("resize_policy_reason"),
                            resize_trace_json=res.get("resize_trace_json"),
                            pass3_ingredients_raw=res.get("ingredients_text_raw"),
                            pass3_ingredients_corrected=res.get("ingredients_text_corrected"),
                            pass3_boundary_corrected=bool(res.get("pass3_boundary_corrected")),
                            pass3_boundary_confidence=res.get("pass3_boundary_confidence"),
                            pass3_boundary_issue_codes_json=res.get("pass3_boundary_issue_codes_json"),
                        )

                if total_images > 0:
                    upsert_provider_max_page_done(
                        conn,
                        query_norm=query_norm,
                        provider=provider,
                        max_page_done=max_pages,
                    )
                else:
                    _query_exec_log(logger, "    ℹ️ 수집 0건으로 provider max_page_done은 갱신하지 않음")
                finish_query_run(
                    conn,
                    run_id=run_id,
                    status="done",
                    total_images=total_images,
                    analyzed_images=analyzed_images,
                    pass2b_pass_count=pass2b_pass_count,
                    pass4_pass_count=pass4_pass_count,
                    final_saved_count=final_saved_count,
                    api_calls=api_calls,
                )
                _query_exec_log(
                    logger,
                    f"    ✅ 완료: analyzed={analyzed_images}, pass2b={pass2b_pass_count}, "
                    f"pass4={pass4_pass_count}, saved={final_saved_count}"
                )
                query_report["status"] = "done"
                query_report["analyzed_images"] = analyzed_images
                query_report["final_saved_count"] = final_saved_count
                query_report["api_calls"] = api_calls
                _print_pass3_raw_for_query(query_report)
                reports.append(query_report)
            except Exception as exc:  # pylint: disable=broad-except
                finish_query_run(
                    conn,
                    run_id=run_id,
                    status="failed",
                    total_images=total_images,
                    analyzed_images=analyzed_images,
                    pass2b_pass_count=pass2b_pass_count,
                    pass4_pass_count=pass4_pass_count,
                    final_saved_count=final_saved_count,
                    api_calls=api_calls,
                    error_message=str(exc),
                )
                _query_exec_log(logger, f"    ❌ 실패: {exc}")
                query_report["status"] = "failed"
                query_report["reason"] = str(exc)
                query_report["analyzed_images"] = analyzed_images
                query_report["final_saved_count"] = final_saved_count
                query_report["api_calls"] = api_calls
                _print_pass3_raw_for_query(query_report)
                reports.append(query_report)

    if not reports:
        return None

    report_path = _write_query_execution_html_report(reports)
    _query_exec_log(logger, f"\n  🌐 실행 결과 브라우저 리포트: {report_path}")
    if open_browser_report:
        try:
            webbrowser.open(report_path.resolve().as_uri())
            _query_exec_log(logger, "  🖥️ 브라우저 자동 열기 완료")
        except Exception as exc:  # pylint: disable=broad-except
            _query_exec_log(logger, f"  ⚠️ 브라우저 자동 열기 실패: {exc}")
    return str(report_path)


def run_query_pipeline_execute() -> None:
    print("\n  🔹 실행 대상 선택")
    print("    [1] 검색어 풀에서 선택 실행")
    print("    [2] 직접 검색어 입력 실행")
    mode = input("  선택 > ").strip() or "1"

    raw_pages = input("  🔹 최대 페이지 수 [기본 1]: ").strip()
    raw_workers = input("  🔹 Pass 동시호출 수 [기본 5]: ").strip()
    print("  🔹 이미지 검색 엔진")
    print("    [1] Google Images")
    print("    [2] Naver Images (SerpAPI)")
    print("    [3] Naver Images (Official OpenAPI)")
    print("    [4] Naver Images (Blog source only)")
    print("    [5] Naver Shop Detail Images")
    raw_provider = input("  선택 > ").strip()
    if raw_provider == "2":
        provider = "naver_serpapi"
    elif raw_provider == "3":
        provider = "naver_official"
    elif raw_provider == "4":
        provider = "naver_blog"
    elif raw_provider == "5":
        provider = "naver_shop"
    else:
        provider = "google"

    if mode == "2":
        query_limit = 1
    else:
        raw_q = input("  🔹 실행할 검색어 개수 [기본 3]: ").strip()
        query_limit = int(raw_q) if raw_q.isdigit() else 3
    max_pages = int(raw_pages) if raw_pages.isdigit() else 1
    max_images = 0
    pass_workers = int(raw_workers) if raw_workers.isdigit() else 5

    direct_query = None
    if mode == "2":
        direct_query = input("  🔹 직접 실행할 검색어 입력: ").strip()

    execute_query_pipeline_run(
        mode=mode,
        provider=provider,
        query_limit=query_limit,
        max_pages=max_pages,
        max_images=max_images,
        pass_workers=pass_workers,
        direct_query=direct_query,
        open_browser_report=True,
        show_pass3_raw=False,
        logger=None,
    )


def run_haccp_backend_import_menu() -> None:
    print("\n  📤 [HACCP backend Import 전송]")
    default_import_url = os.getenv("BACKEND_IMPORT_URL", "http://localhost:3000/api/import").strip()
    import_url = input(f"  🔹 Import URL [기본 {default_import_url}]: ").strip() or default_import_url

    raw_batch_size = input("  🔹 배치 크기 [기본 50]: ").strip()
    raw_limit = input("  🔹 최대 처리 건수 [기본 전체]: ").strip()
    raw_min_id = input("  🔹 시작 id(min, 선택): ").strip()
    raw_max_id = input("  🔹 끝 id(max, 선택): ").strip()
    raw_exclude_blocked = input("  🔹 재파싱 차단(blocklist) 제외? [Y/n]: ").strip().lower()
    dry_run = input("  🔹 Dry-run(전송 없이 미리보기)? [y/N]: ").strip().lower() == "y"

    batch_size = int(raw_batch_size) if raw_batch_size.isdigit() else 50
    limit = int(raw_limit) if raw_limit.isdigit() else None
    min_id = int(raw_min_id) if raw_min_id.isdigit() else None
    max_id = int(raw_max_id) if raw_max_id.isdigit() else None
    exclude_blocked = raw_exclude_blocked != "n"

    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_tables(conn)

    rows = fetch_haccp_parsed_rows(
        DB_FILE,
        min_id=min_id,
        max_id=max_id,
        limit=limit,
        exclude_blocked=exclude_blocked,
    )
    print(f"\n  📦 조회 건수: {len(rows):,}건")

    payloads: list[dict[str, Any]] = []
    transform_errors = 0
    for row in rows:
        transformed = transform_haccp_parsed_row(row)
        if transformed.payload is not None:
            payloads.append(transformed.payload)
        else:
            transform_errors += 1
            print(f"  ⚠️ row id={row['id']} 변환 스킵: {transformed.error}")

    print(f"  🔧 변환 성공: {len(payloads):,}건 | 변환 실패: {transform_errors:,}건")
    if not payloads:
        print("  ℹ️ 전송할 데이터가 없습니다.")
        return

    if dry_run:
        print("\n  🧪 [Dry-run 첫 payload]")
        print(json.dumps(payloads[0], ensure_ascii=False, indent=2))
        return

    sent_count, send_errors = send_batches(
        payloads,
        import_url=import_url,
        batch_size=max(1, batch_size),
        timeout_sec=30,
    )
    print(
        f"\n  ✅ 전송 완료: 대상={len(payloads):,} sent={sent_count:,} "
        f"send_errors={send_errors:,} transform_errors={transform_errors:,}"
    )


def run_haccp_backend_import_payload_view_menu() -> None:
    print("\n  🧾 [HACCP backend Import payload JSON 파일 생성]")
    raw_limit = input("  🔹 최대 조회 건수 [기본 전체]: ").strip()
    raw_min_id = input("  🔹 시작 id(min, 선택): ").strip()
    raw_max_id = input("  🔹 끝 id(max, 선택): ").strip()
    raw_exclude_blocked = input("  🔹 재파싱 차단(blocklist) 제외? [Y/n]: ").strip().lower()

    limit = int(raw_limit) if raw_limit.isdigit() else None
    min_id = int(raw_min_id) if raw_min_id.isdigit() else None
    max_id = int(raw_max_id) if raw_max_id.isdigit() else None
    exclude_blocked = raw_exclude_blocked != "n"

    with sqlite3.connect(DB_FILE) as conn:
        init_haccp_tables(conn)

    rows = fetch_haccp_parsed_rows(
        DB_FILE,
        min_id=min_id,
        max_id=max_id,
        limit=(max(1, limit) if isinstance(limit, int) else None),
        exclude_blocked=exclude_blocked,
    )
    print(f"\n  📦 조회 건수: {len(rows):,}건")

    payload_rows: list[tuple[int, dict[str, Any]]] = []
    transform_errors = 0
    for row in rows:
        transformed = transform_haccp_parsed_row(row)
        if transformed.payload is not None:
            payload_rows.append((int(row["id"]), transformed.payload))
        else:
            transform_errors += 1
            print(f"  ⚠️ row id={row['id']} 변환 스킵: {transformed.error}")

    print(f"  🔧 변환 성공: {len(payload_rows):,}건 | 변환 실패: {transform_errors:,}건")
    if not payload_rows:
        print("  ℹ️ 저장할 payload가 없습니다.")
        return

    base_dir = Path(__file__).resolve().parent.parent / "reports" / "backend_import_payloads_haccp"
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_dir = base_dir / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    payloads = [p for _, p in payload_rows]
    manifest = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "haccp_parsed_cache",
        "count": len(payload_rows),
        "transform_errors": transform_errors,
        "min_id": min_id,
        "max_id": max_id,
        "limit": (max(1, limit) if isinstance(limit, int) else None),
        "exclude_blocked": exclude_blocked,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "payloads_all.json").write_text(json.dumps(payloads, ensure_ascii=False, indent=2), encoding="utf-8")
    with (out_dir / "payloads.ndjson").open("w", encoding="utf-8") as f:
        for p in payloads:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    items_dir = out_dir / "items"
    items_dir.mkdir(parents=True, exist_ok=True)
    for i, (row_id, payload) in enumerate(payload_rows, 1):
        item_no = str(payload.get("itemMnftrRptNo") or payload.get("item_mnftr_rpt_no") or "")
        safe_item_no = re.sub(r"[^0-9A-Za-z_-]", "_", item_no)[:40] or "no_report_no"
        fname = f"{i:04d}_row{row_id}_{safe_item_no}.json"
        (items_dir / fname).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n  ✅ JSON 파일 생성 완료")
    print(f"  - 폴더: {out_dir}")
    print(f"  - 전체(JSON array): {out_dir / 'payloads_all.json'}")
    print(f"  - 전체(NDJSON): {out_dir / 'payloads.ndjson'}")
    print(f"  - 개별 파일: {items_dir} ({len(payload_rows):,}개)")


def run_backend_import_menu() -> None:
    print("\n  📤 [backend Import 전송]")
    default_import_url = os.getenv("BACKEND_IMPORT_URL", "http://localhost:3000/api/import").strip()
    import_url = input(f"  🔹 Import URL [기본 {default_import_url}]: ").strip() or default_import_url

    raw_batch_size = input("  🔹 배치 크기 [기본 50]: ").strip()
    raw_limit = input("  🔹 최대 처리 건수 [기본 전체]: ").strip()
    raw_min_id = input("  🔹 시작 id(min, 선택): ").strip()
    raw_max_id = input("  🔹 끝 id(max, 선택): ").strip()
    dry_run = input("  🔹 Dry-run(전송 없이 미리보기)? [y/N]: ").strip().lower() == "y"

    batch_size = int(raw_batch_size) if raw_batch_size.isdigit() else 50
    limit = int(raw_limit) if raw_limit.isdigit() else None
    min_id = int(raw_min_id) if raw_min_id.isdigit() else None
    max_id = int(raw_max_id) if raw_max_id.isdigit() else None

    rows = fetch_food_final_rows(
        DB_FILE,
        min_id=min_id,
        max_id=max_id,
        limit=limit,
    )
    print(f"\n  📦 조회 건수: {len(rows):,}건")

    payloads: list[dict[str, Any]] = []
    transform_errors = 0
    for row in rows:
        transformed = transform_food_final_row(row)
        if transformed.payload is not None:
            payloads.append(transformed.payload)
        else:
            transform_errors += 1
            print(f"  ⚠️ row id={row['id']} 변환 스킵: {transformed.error}")

    print(f"  🔧 변환 성공: {len(payloads):,}건 | 변환 실패: {transform_errors:,}건")
    if not payloads:
        print("  ℹ️ 전송할 데이터가 없습니다.")
        return

    if dry_run:
        print("\n  🧪 [Dry-run 첫 payload]")
        print(json.dumps(payloads[0], ensure_ascii=False, indent=2))
        return

    sent_count, send_errors = send_batches(
        payloads,
        import_url=import_url,
        batch_size=max(1, batch_size),
        timeout_sec=30,
    )
    print(
        f"\n  ✅ 전송 완료: 대상={len(payloads):,} sent={sent_count:,} "
        f"send_errors={send_errors:,} transform_errors={transform_errors:,}"
    )


def run_backend_import_payload_view_menu() -> None:
    print("\n  🧾 [backend Import payload JSON 파일 생성]")
    raw_limit = input("  🔹 최대 조회 건수 [기본 전체]: ").strip()
    raw_min_id = input("  🔹 시작 id(min, 선택): ").strip()
    raw_max_id = input("  🔹 끝 id(max, 선택): ").strip()

    limit = int(raw_limit) if raw_limit.isdigit() else None
    min_id = int(raw_min_id) if raw_min_id.isdigit() else None
    max_id = int(raw_max_id) if raw_max_id.isdigit() else None

    rows = fetch_food_final_rows(
        DB_FILE,
        min_id=min_id,
        max_id=max_id,
        limit=(max(1, limit) if isinstance(limit, int) else None),
    )
    print(f"\n  📦 조회 건수: {len(rows):,}건")

    payload_rows: list[tuple[int, dict[str, Any]]] = []
    transform_errors = 0
    for row in rows:
        transformed = transform_food_final_row(row)
        if transformed.payload is not None:
            payload_rows.append((int(row["id"]), transformed.payload))
        else:
            transform_errors += 1
            print(f"  ⚠️ row id={row['id']} 변환 스킵: {transformed.error}")

    print(f"  🔧 변환 성공: {len(payload_rows):,}건 | 변환 실패: {transform_errors:,}건")
    if not payload_rows:
        print("  ℹ️ 저장할 payload가 없습니다.")
        return

    base_dir = Path(__file__).resolve().parent.parent / "reports" / "backend_import_payloads"
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_dir = base_dir / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    payloads = [p for _, p in payload_rows]
    manifest = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(payload_rows),
        "transform_errors": transform_errors,
        "min_id": min_id,
        "max_id": max_id,
        "limit": (max(1, limit) if isinstance(limit, int) else None),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "payloads_all.json").write_text(json.dumps(payloads, ensure_ascii=False, indent=2), encoding="utf-8")
    with (out_dir / "payloads.ndjson").open("w", encoding="utf-8") as f:
        for p in payloads:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    items_dir = out_dir / "items"
    items_dir.mkdir(parents=True, exist_ok=True)
    for i, (row_id, payload) in enumerate(payload_rows, 1):
        item_no = str(payload.get("itemMnftrRptNo") or payload.get("item_mnftr_rpt_no") or "")
        safe_item_no = re.sub(r"[^0-9A-Za-z_-]", "_", item_no)[:40] or "no_report_no"
        fname = f"{i:04d}_row{row_id}_{safe_item_no}.json"
        (items_dir / fname).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n  ✅ JSON 파일 생성 완료")
    print(f"  - 폴더: {out_dir}")
    print(f"  - 전체(JSON array): {out_dir / 'payloads_all.json'}")
    print(f"  - 전체(NDJSON): {out_dir / 'payloads.ndjson'}")
    print(f"  - 개별 파일: {items_dir} ({len(payload_rows):,}개)")


def main() -> None:
    try:
        with sqlite3.connect(DB_FILE) as _conn:
            ensure_processed_food_table(_conn)
    except sqlite3.Error:
        pass

    while True:
        print_header()
        print(_bar())
        print("  🎛️ [ 메인 메뉴 ]")
        print("    [1] 👀 데이터 조회/탐색 (신규 viewer)")
        print("    [2] 🌐 공공 API 관리 (가공식품)")
        print("    [3] 💾 백업/복원 관리")
        print("    [4] 📊 analyze 벤치마크 도우미")
        print("    [5] 🧩 검색어 관리")
        print("    [6] 🚀 파이프라인 실행")
        print("    [7] 📤 backend Import 전송")
        print("    [8] 🧾 backend Import payload JSON 파일 생성")
        print("    [q] 🚪 종료")
        print(_bar())
        choice = input("  👉 선택 : ").strip().lower()

        if choice == "1":
            run_data_viewer()
        elif choice == "2":
            run_public_api_menu()
        elif choice == "3":
            run_backup_menu()
        elif choice == "4":
            run_benchmark_menu()
        elif choice == "5":
            run_query_pipeline_menu()
        elif choice == "6":
            run_query_pipeline_execute()
        elif choice == "7":
            run_backend_import_menu()
        elif choice == "8":
            run_backend_import_payload_view_menu()
        elif choice == "q":
            print("\n  👋 실행기를 종료합니다.\n")
            break
        else:
            print("\n  ⚠️ 올바른 메뉴 번호를 입력해주세요.\n")


if __name__ == "__main__":
    main()
