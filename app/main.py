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

import sqlite3
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
from app.ingredient_enricher import (
    diagnose_analysis,
    get_priority_subcategories,
    run_enricher,
    run_enricher_for_report_no,
)
from app.analyzer import URLIngredientAnalyzer
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

W = 68
WEB_UI_PORT = 8501
WEB_UI_URL = f"http://localhost:{WEB_UI_PORT}"
QUERY_WEB_UI_PORT = 8502
QUERY_WEB_UI_URL = f"http://localhost:{QUERY_WEB_UI_PORT}"
ADMIN_WEB_UI_PORT = 8503
ADMIN_WEB_UI_URL = f"http://localhost:{ADMIN_WEB_UI_PORT}"


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
    print("    [2] Naver Images (Official OpenAPI)")
    print("    [3] Naver Images (Blog source only)")
    print("    [4] Naver Shop Detail Images")
    raw_provider = input("  선택 > ").strip()
    if raw_provider == "2":
        provider = "naver_official"
    elif raw_provider == "3":
        provider = "naver_blog"
    elif raw_provider == "4":
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
        elif choice == "q":
            print("\n  👋 실행기를 종료합니다.\n")
            break
        else:
            print("\n  ⚠️ 올바른 메뉴 번호를 입력해주세요.\n")


if __name__ == "__main__":
    main()
