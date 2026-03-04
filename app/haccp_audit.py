"""
HACCP parsed cache quality audit.

대상: haccp_parsed_cache(parse_status='ok')의 ingredients_items_json
출력: token 통계, 상품별 점수, 상세 이슈
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from typing import Any


AUDIT_TOKEN_TABLE = "haccp_audit_token_stats"
AUDIT_RESULT_TABLE = "haccp_audit_result"
AUDIT_ISSUE_TABLE = "haccp_audit_issue"


_VALID_SINGLE_CHAR = {"파", "쌀", "콩", "차", "꿀", "무", "밀", "팥", "감"}
_ORIGIN_GENERIC = {"국산", "국내산", "수입산", "외국산", "기타외국산"}
_NON_ORIGIN_WORDS = {
    "합성보존료", "향미증진제", "산도조절제", "유화제", "증점제", "감미료",
    "대두", "밀", "우유", "돼지고기", "설탕", "정제소금",
}


def _norm_token(text: str | None) -> str:
    t = str(text or "").strip().lower()
    t = re.sub(r"[\s\(\)\[\]\{\}]+", "", t)
    t = re.sub(r"[·\.,;:]+$", "", t)
    return t


def _looks_origin_token(token: str) -> bool:
    t = _norm_token(token)
    if not t:
        return False
    t = re.sub(r"\d+(?:\.\d+)?%$", "", t)
    if t in _ORIGIN_GENERIC:
        return True
    if t.endswith("산"):
        return True
    return False


def _split_origin_chunks(text: str) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    out: list[str] = []
    for part in re.split(r"[,/;|]+", raw):
        part = str(part or "").strip()
        if not part:
            continue
        if ":" in part:
            h, t = part.split(":", 1)
            if h.strip():
                out.append(h.strip())
            if t.strip():
                out.extend([x.strip() for x in re.split(r"[,/;|·ㆍ]+", t) if x.strip()])
        else:
            out.extend([x.strip() for x in re.split(r"[·ㆍ]+", part) if x.strip()])
    return [x for x in out if x]


@dataclass
class FlatNode:
    report_no: str
    path: str
    depth: int
    name: str
    origin: str
    amount: str


def init_haccp_audit_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_TOKEN_TABLE} (
            audit_version TEXT NOT NULL,
            token_norm TEXT NOT NULL,
            token_sample TEXT,
            product_count INTEGER NOT NULL DEFAULT 0,
            occurrence_count INTEGER NOT NULL DEFAULT 0,
            last_audited_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (audit_version, token_norm)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_RESULT_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prdlstReportNo TEXT NOT NULL,
            audit_version TEXT NOT NULL,
            suspicion_score INTEGER NOT NULL DEFAULT 0,
            node_count INTEGER NOT NULL DEFAULT 0,
            max_depth INTEGER NOT NULL DEFAULT 0,
            low_freq_node_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            warn_count INTEGER NOT NULL DEFAULT 0,
            rule_hits_json TEXT,
            audited_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(prdlstReportNo, audit_version)
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_ISSUE_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prdlstReportNo TEXT NOT NULL,
            audit_version TEXT NOT NULL,
            severity TEXT NOT NULL,
            rule_code TEXT NOT NULL,
            path TEXT,
            node_name TEXT,
            origin TEXT,
            amount TEXT,
            message TEXT,
            evidence TEXT,
            audited_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def clear_haccp_audit_tables(conn: sqlite3.Connection, audit_version: str | None = None) -> dict[str, int]:
    if audit_version:
        c1 = conn.execute(f"DELETE FROM {AUDIT_TOKEN_TABLE} WHERE audit_version=?", (audit_version,))
        c2 = conn.execute(f"DELETE FROM {AUDIT_RESULT_TABLE} WHERE audit_version=?", (audit_version,))
        c3 = conn.execute(f"DELETE FROM {AUDIT_ISSUE_TABLE} WHERE audit_version=?", (audit_version,))
    else:
        c1 = conn.execute(f"DELETE FROM {AUDIT_TOKEN_TABLE}")
        c2 = conn.execute(f"DELETE FROM {AUDIT_RESULT_TABLE}")
        c3 = conn.execute(f"DELETE FROM {AUDIT_ISSUE_TABLE}")
    conn.commit()
    return {"token_stats": int(c1.rowcount), "results": int(c2.rowcount), "issues": int(c3.rowcount)}


def _flatten_items(report_no: str, items: list[dict[str, Any]]) -> list[FlatNode]:
    out: list[FlatNode] = []

    def walk(node: dict[str, Any], path: str, depth: int) -> None:
        name = str(node.get("ingredient_name") or node.get("name") or "").strip()
        origin = str(node.get("origin") or "").strip()
        amount = str(node.get("amount") or "").strip()
        out.append(
            FlatNode(
                report_no=report_no,
                path=path,
                depth=depth,
                name=name,
                origin=origin,
                amount=amount,
            )
        )
        subs = node.get("sub_ingredients")
        if isinstance(subs, list):
            for i, c in enumerate(subs):
                if isinstance(c, dict):
                    walk(c, f"{path}/sub[{i}]", depth + 1)

    for i, n in enumerate(items):
        if isinstance(n, dict):
            walk(n, f"root[{i}]", 1)
    return out


def _severity_score(severity: str) -> int:
    return 10 if severity == "error" else 3


def run_haccp_parsed_audit(
    conn: sqlite3.Connection,
    *,
    audit_version: str = "haccp_audit_v1",
    limit: int = 0,
) -> dict[str, Any]:
    init_haccp_audit_tables(conn)
    clear_haccp_audit_tables(conn, audit_version=audit_version)

    sql = (
        "SELECT prdlstReportNo, ingredients_items_json "
        "FROM haccp_parsed_cache WHERE parse_status='ok' "
        "AND COALESCE(TRIM(ingredients_items_json),'') != '' "
        "ORDER BY id DESC"
    )
    if limit > 0:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()

    per_report_nodes: dict[str, list[FlatNode]] = {}
    parse_json_errors = 0
    all_nodes: list[FlatNode] = []
    for r in rows:
        report_no = str(r[0] or "").strip()
        raw = str(r[1] or "").strip()
        if (not report_no) or (not raw):
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            parse_json_errors += 1
            continue
        items = payload.get("ingredients_items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            parse_json_errors += 1
            continue
        nodes = _flatten_items(report_no, items)
        per_report_nodes[report_no] = nodes
        all_nodes.extend(nodes)

    token_occ = Counter()
    token_prod: dict[str, set[str]] = {}
    token_sample: dict[str, str] = {}
    for n in all_nodes:
        k = _norm_token(n.name)
        if not k:
            continue
        token_occ[k] += 1
        token_prod.setdefault(k, set()).add(n.report_no)
        if k not in token_sample:
            token_sample[k] = n.name

    token_rows = [
        (
            audit_version,
            k,
            token_sample.get(k, ""),
            len(token_prod.get(k, set())),
            int(token_occ.get(k, 0)),
        )
        for k in token_occ.keys()
    ]
    conn.executemany(
        f"""
        INSERT INTO {AUDIT_TOKEN_TABLE}
        (audit_version, token_norm, token_sample, product_count, occurrence_count, last_audited_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(audit_version, token_norm) DO UPDATE SET
            token_sample=excluded.token_sample,
            product_count=excluded.product_count,
            occurrence_count=excluded.occurrence_count,
            last_audited_at=CURRENT_TIMESTAMP
        """,
        token_rows,
    )

    result_rows: list[tuple[Any, ...]] = []
    issue_rows: list[tuple[Any, ...]] = []

    for report_no, nodes in per_report_nodes.items():
        score = 0
        err = 0
        warn = 0
        low_freq_count = 0
        max_depth = 0
        rule_hits = Counter()

        for n in nodes:
            max_depth = max(max_depth, int(n.depth))
            name = n.name
            origin = n.origin
            amount = n.amount
            name_norm = _norm_token(name)

            def add_issue(sev: str, code: str, msg: str, ev: str) -> None:
                nonlocal score, err, warn
                score += _severity_score(sev)
                if sev == "error":
                    err += 1
                else:
                    warn += 1
                rule_hits[code] += 1
                issue_rows.append(
                    (
                        report_no,
                        audit_version,
                        sev,
                        code,
                        n.path,
                        name or None,
                        origin or None,
                        amount or None,
                        msg,
                        ev[:400],
                    )
                )

            if (not name) or (name_norm in {"null", "none", "없음"}):
                add_issue("error", "NODE_EMPTY_NAME", "이름이 비어있거나 null 계열입니다.", name)

            if name and re.search(r"\d+(?:\.\d+)?%", name):
                add_issue("warn", "ING_NAME_HAS_PERCENT", "name에 퍼센트가 포함되어 있습니다.", name)
            if name and re.search(r"\d+(?:\.\d+)?\s*brix", name, flags=re.IGNORECASE):
                add_issue("warn", "ING_NAME_HAS_BRIX", "name에 Brix가 포함되어 있습니다.", name)
            if name and re.search(r"(포함|함유)", name):
                add_issue("warn", "ING_NAME_HAS_INCLUDE_WORD", "name에 포함/함유가 들어있습니다.", name)
            if name and re.search(r"-\d+(?:\.\d+)?$", name):
                add_issue("warn", "ING_NAME_NUMERIC_SUFFIX", "name에 숫자 접미가 있습니다.", name)
            if name and (("산" in name) and _looks_origin_token(name)):
                add_issue("warn", "ING_NAME_HAS_ORIGIN_WORD", "name에 원산지 표기가 섞였습니다.", name)

            if origin:
                parts = _split_origin_chunks(origin)
                if not parts:
                    add_issue("error", "ORIGIN_NON_ORIGIN_TOKEN", "origin이 비어있거나 파싱 불가입니다.", origin)
                for p in parts:
                    pn = _norm_token(p)
                    if pn in {_norm_token(x) for x in _NON_ORIGIN_WORDS}:
                        add_issue("error", "ORIGIN_NON_ORIGIN_TOKEN", "origin에 비원산지 단어가 있습니다.", p)
                    elif not _looks_origin_token(p):
                        add_issue("error", "ORIGIN_NON_ORIGIN_TOKEN", "origin 토큰이 원산지 형식이 아닙니다.", p)
                # base 중복 체크: 이스라엘산 + 이스라엘산100%
                base_seen = Counter()
                for p in parts:
                    m = re.match(r"^(.*?산)(?:\s*\d+(?:\.\d+)?%)?$", _norm_token(p))
                    if m:
                        base_seen[str(m.group(1) or "")] += 1
                if any(v >= 2 for v in base_seen.values()):
                    add_issue("warn", "ORIGIN_DUPLICATE_BASE", "origin에 동일 국가 base 중복이 있습니다.", origin)

            if amount:
                if not re.fullmatch(r"\d+(?:\.\d+)?%", amount):
                    add_issue("warn", "AMOUNT_NOT_PERCENT", "amount가 % 형식이 아닙니다.", amount)
                if re.search(r"(기준|제외|함유|포함|brix)", amount, flags=re.IGNORECASE):
                    add_issue("warn", "AMOUNT_HAS_QUALIFIER", "amount에 설명성 키워드가 있습니다.", amount)

            if name_norm:
                pc = len(token_prod.get(name_norm, set()))
                if pc == 1:
                    low_freq_count += 1
                    add_issue("warn", "LOW_FREQ_TOKEN_1", "1개 상품에서만 등장한 토큰입니다.", name)
                elif 2 <= pc <= 3:
                    low_freq_count += 1
                    add_issue("warn", "LOW_FREQ_TOKEN_2_3", "2~3개 상품에서만 등장한 토큰입니다.", name)

            if len(name_norm) <= 1 and name_norm and (name not in _VALID_SINGLE_CHAR):
                add_issue("warn", "NODE_SINGLE_CHAR_NOISE", "한 글자 노이즈 가능성이 있습니다.", name)
            if n.depth > 6:
                add_issue("warn", "NODE_TOO_DEEP", "트리 깊이가 과도합니다.", str(n.depth))

        if max_depth > 6:
            score += 3

        result_rows.append(
            (
                report_no,
                audit_version,
                int(score),
                len(nodes),
                int(max_depth),
                int(low_freq_count),
                int(err),
                int(warn),
                json.dumps(dict(rule_hits), ensure_ascii=False, separators=(",", ":")),
            )
        )

    conn.executemany(
        f"""
        INSERT INTO {AUDIT_RESULT_TABLE}
        (prdlstReportNo, audit_version, suspicion_score, node_count, max_depth, low_freq_node_count,
         error_count, warn_count, rule_hits_json, audited_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(prdlstReportNo, audit_version) DO UPDATE SET
            suspicion_score=excluded.suspicion_score,
            node_count=excluded.node_count,
            max_depth=excluded.max_depth,
            low_freq_node_count=excluded.low_freq_node_count,
            error_count=excluded.error_count,
            warn_count=excluded.warn_count,
            rule_hits_json=excluded.rule_hits_json,
            audited_at=CURRENT_TIMESTAMP
        """,
        result_rows,
    )

    conn.executemany(
        f"""
        INSERT INTO {AUDIT_ISSUE_TABLE}
        (prdlstReportNo, audit_version, severity, rule_code, path, node_name, origin, amount, message, evidence, audited_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        issue_rows,
    )
    conn.commit()

    return {
        "audit_version": audit_version,
        "target_products": len(per_report_nodes),
        "token_count": len(token_rows),
        "result_count": len(result_rows),
        "issue_count": len(issue_rows),
        "parse_json_errors": parse_json_errors,
    }


def fetch_haccp_audit_summary(conn: sqlite3.Connection, *, audit_version: str = "haccp_audit_v1") -> dict[str, Any]:
    row = conn.execute(
        f"""
        SELECT COUNT(*),
               COALESCE(SUM(error_count), 0),
               COALESCE(SUM(warn_count), 0),
               COALESCE(AVG(suspicion_score), 0),
               COALESCE(MAX(suspicion_score), 0)
        FROM {AUDIT_RESULT_TABLE}
        WHERE audit_version=?
        """,
        (audit_version,),
    ).fetchone()
    r = row or (0, 0, 0, 0, 0)
    top_rules = conn.execute(
        f"""
        SELECT rule_code, COUNT(*) AS cnt
        FROM {AUDIT_ISSUE_TABLE}
        WHERE audit_version=?
        GROUP BY rule_code
        ORDER BY cnt DESC
        LIMIT 15
        """,
        (audit_version,),
    ).fetchall()
    return {
        "rows": int(r[0] or 0),
        "errors": int(r[1] or 0),
        "warns": int(r[2] or 0),
        "avg_score": float(r[3] or 0.0),
        "max_score": int(r[4] or 0),
        "top_rules": [(str(x[0] or ""), int(x[1] or 0)) for x in top_rules],
    }


def fetch_haccp_audit_top(
    conn: sqlite3.Connection,
    *,
    audit_version: str = "haccp_audit_v1",
    limit: int = 30,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT prdlstReportNo, suspicion_score, error_count, warn_count, node_count, max_depth, low_freq_node_count, rule_hits_json, audited_at
        FROM {AUDIT_RESULT_TABLE}
        WHERE audit_version=?
        ORDER BY suspicion_score DESC, error_count DESC, warn_count DESC, prdlstReportNo
        LIMIT ?
        """,
        (audit_version, int(limit)),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "prdlstReportNo": str(r[0] or ""),
                "suspicion_score": int(r[1] or 0),
                "error_count": int(r[2] or 0),
                "warn_count": int(r[3] or 0),
                "node_count": int(r[4] or 0),
                "max_depth": int(r[5] or 0),
                "low_freq_node_count": int(r[6] or 0),
                "rule_hits_json": str(r[7] or ""),
                "audited_at": str(r[8] or ""),
            }
        )
    return out

