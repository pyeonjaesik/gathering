"""HACCP raw 텍스트를 규칙 기반으로 파싱한다 (LLM 미사용)."""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any


_NUTRIENT_FIELD_PATTERNS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "열량(kcal)": (
        "kcal",
        "kcal",
        (
            r"열량\s*\(?\s*k\s*cal\s*\)?\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)",
            r"열량\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)\s*k\s*cal",
        ),
    ),
    "탄수화물(g)": (
        "g",
        "g",
        (
            r"탄수화물\s*\(?\s*g\s*\)?\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)",
            r"탄수화물\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)\s*g",
        ),
    ),
    "단백질(g)": (
        "g",
        "g",
        (
            r"단백질\s*\(?\s*g\s*\)?\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)",
            r"단백질\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)\s*g",
        ),
    ),
    "지방(g)": (
        "g",
        "g",
        (
            r"지방\s*\(?\s*g\s*\)?\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)",
            r"지방\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)\s*g",
        ),
    ),
    "당류(g)": (
        "g",
        "g",
        (
            r"당류\s*\(?\s*g\s*\)?\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)",
            r"당류\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)\s*g",
        ),
    ),
    "나트륨(mg)": (
        "mg",
        "mg",
        (
            r"나트륨\s*\(?\s*mg\s*\)?\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)",
            r"나트륨\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)\s*mg",
        ),
    ),
}

_NON_ORIGIN_ROLE_HINT_RE = re.compile(
    r"(향미증진제|합성보존료|보존료|산도조절제|유화제|감미료|착색료|착향료|증점제|팽창제|표백제|응고제|피막제|소포제)"
)


def _split_top_level(text: str, sep: str = ",") -> list[str]:
    text = str(text or "").replace("，", ",")
    out: list[str] = []
    buf: list[str] = []
    depth_round = 0
    depth_square = 0
    depth_curly = 0
    for ch in text:
        if ch == "(":
            depth_round += 1
        elif ch == ")":
            depth_round = max(0, depth_round - 1)
        elif ch == "[":
            depth_square += 1
        elif ch == "]":
            depth_square = max(0, depth_square - 1)
        elif ch == "{":
            depth_curly += 1
        elif ch == "}":
            depth_curly = max(0, depth_curly - 1)
        if ch == sep and depth_round == 0 and depth_square == 0 and depth_curly == 0:
            token = "".join(buf).strip()
            if token:
                out.append(token)
            buf = []
            continue
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


def _has_unbalanced_brackets(text: str) -> bool:
    pairs = {"(": ")", "[": "]", "{": "}"}
    opens = "([{"
    closes = ")]}"
    stack: list[str] = []
    for ch in str(text or ""):
        if ch in opens:
            stack.append(ch)
        elif ch in closes:
            if not stack:
                return True
            top = stack.pop()
            if pairs.get(top) != ch:
                return True
    return bool(stack)


def _split_top_level_tolerant(text: str, sep: str = ",") -> list[str]:
    """괄호 불균형 데이터도 최대한 토큰 분리."""
    base = _split_top_level(text, sep=sep)
    if len(base) > 1:
        return base
    src = str(text or "")
    if sep not in src:
        return base
    if not _has_unbalanced_brackets(src):
        return base
    # 괄호가 비정상일 때는 강제 쉼표 분리로 복구
    return [p.strip() for p in src.split(sep) if p and p.strip()]


def _merge_continuation_tokens(tokens: list[str]) -> list[str]:
    if not tokens:
        return tokens
    merged: list[str] = []
    for tok in tokens:
        t = str(tok or "").strip()
        if not t:
            continue
        if merged and _has_unbalanced_brackets(merged[-1]):
            merged[-1] = merged[-1].rstrip() + ", " + t
            continue
        merged.append(t)
    return merged


def _split_first_top_level(text: str, seps: tuple[str, ...]) -> tuple[str, str | None]:
    src = str(text or "")
    buf: list[str] = []
    depth_round = 0
    depth_square = 0
    depth_curly = 0
    for i, ch in enumerate(src):
        if ch == "(":
            depth_round += 1
        elif ch == ")":
            depth_round = max(0, depth_round - 1)
        elif ch == "[":
            depth_square += 1
        elif ch == "]":
            depth_square = max(0, depth_square - 1)
        elif ch == "{":
            depth_curly += 1
        elif ch == "}":
            depth_curly = max(0, depth_curly - 1)
        if ch in seps and depth_round == 0 and depth_square == 0 and depth_curly == 0:
            return ("".join(buf).strip(), src[i + 1 :].strip())
        buf.append(ch)
    return (src.strip(), None)


def _extract_inline_amount(text: str) -> tuple[str, str | None]:
    t = str(text or "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)\s*%$", t)
    if m:
        amount = m.group(0).strip()
        base = t[: m.start()].strip()
        return base, amount
    m2 = re.search(r"(\d{1,2})\s*종$", t)
    if m2:
        # "1종"은 의미가 약해 원재료명 일부로 취급하고,
        # "2종" 이상만 amount로 분리한다.
        try:
            kind_count = int(m2.group(1))
        except Exception:
            kind_count = 0
        if kind_count >= 2:
            amount = f"{kind_count}종"
            base = t[: m2.start()].strip()
            return base, amount
    return t, None


def _extract_origin_detail(name_text: str) -> tuple[str, str | None]:
    t = str(name_text or "").strip()
    m = re.search(
        r"(국산|수입산|외국산(?:\s*[:：]\s*[가-힣A-Za-z,\s/·ㆍ]+)?|[가-힣A-Za-z]+산(?:[,/\s]*[가-힣A-Za-z]+산)*)$",
        t,
    )
    if not m:
        return t, None
    base = t[: m.start()].strip(" :")
    origin_detail = m.group(1).strip()
    if not base:
        return t, None
    return base, origin_detail


def _is_origin_descriptor(text: str | None) -> bool:
    t = str(text or "").strip()
    if not t:
        return False
    t = re.sub(r"\s+", "", t)
    return bool(
        re.fullmatch(
            r"(국산|수입산|외국산(?::[가-힣A-Za-z,/·ㆍ]+)?|[가-힣A-Za-z]+산(?:[,/·ㆍ][가-힣A-Za-z]+산)*)",
            t,
        )
    )


def _split_sub_tokens(text: str) -> list[str]:
    # 원문 오탈자/비정형 구분자 보정:
    # - "/(" -> ",("
    # - ")-원재료" -> "),원재료"
    normalized = str(text or "")
    normalized = normalized.replace("/(", ",(")
    normalized = re.sub(r"\)\s*-\s*(?=[가-힣A-Za-z])", "),", normalized)
    parts = _split_top_level_tolerant(normalized, sep=",")
    out: list[str] = []
    for p in parts:
        token = str(p or "").strip()
        if not token:
            continue
        # "5-이노신산이나트륨 5-구아닐산이나트륨" 형태 보정
        additive_matches = list(re.finditer(r"(?:\d+\-)?[가-힣A-Za-z]+나트륨", token))
        if len(additive_matches) >= 2:
            spans = []
            for i, m in enumerate(additive_matches):
                s = m.start()
                e = additive_matches[i + 1].start() if i + 1 < len(additive_matches) else len(token)
                spans.append(token[s:e].strip())
            if all(spans):
                out.extend(spans)
                continue
        out.append(token)
    return out


def _split_head_and_group(raw: str) -> tuple[str, str | None]:
    """원재료 본문에서 최상위 괄호/대괄호/중괄호 그룹을 분리한다."""
    text = str(raw or "").strip()
    if not text:
        return "", None
    first_open = None
    open_ch = ""
    close_ch = ""
    close_map = {"[": "]", "(": ")", "{": "}"}
    for i, ch in enumerate(text):
        if ch in ("[", "(", "{"):
            first_open = i
            open_ch = ch
            close_ch = close_map[ch]
            break
    if first_open is None:
        return text, None
    depth = 0
    close_idx = None
    for i in range(first_open, len(text)):
        ch = text[i]
        if ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                close_idx = i
                break
    if close_idx is None:
        # 닫는 괄호 누락 데이터 보정
        head = text[:first_open].strip()
        inner = text[first_open + 1 :].strip()
        return (head, inner if inner else None)
    tail = text[close_idx + 1 :].strip()
    if tail:
        # 그룹 뒤 꼬리가 단순 함량 표기면 허용
        if re.fullmatch(r"\d+(?:\.\d+)?\s*%", tail):
            head = (text[:first_open] + " " + tail).strip()
            inner = text[first_open + 1 : close_idx].strip()
            return head, inner
        # 괄호 뒤에 문자가 더 있으면 그룹 분리하지 않는다(오탐 방지).
        return text, None
    head = text[:first_open].strip()
    inner = text[first_open + 1 : close_idx].strip()
    return head, inner


def _clean_name_token(text: str | None) -> str:
    t = str(text or "").strip()
    t = re.sub(r"^[\[\(\{]+", "", t)
    t = re.sub(r"[\]\)\}]+$", "", t)
    t = re.sub(r"[\s/,:;\-]+$", "", t)
    return t.strip()


def _extract_origin_prefix(token: str) -> tuple[str | None, str]:
    """
    '중국산/고춧가루' -> ('중국산', '고춧가루')
    '외국산:미국,인도/원재료' -> ('외국산:미국,인도', '원재료')
    """
    t = str(token or "").strip()
    if "/" not in t:
        return (None, t)
    left, right = t.split("/", 1)
    left = left.strip()
    right = right.strip()
    if _is_origin_descriptor(left) and right:
        return (left, right)
    return (None, t)


def _parse_sub_ingredient(token: str) -> dict[str, Any] | None:
    raw = str(token or "").strip()
    if not raw:
        return None
    if _is_origin_descriptor(raw):
        return None

    origin_prefix, raw = _extract_origin_prefix(raw)
    name_part, sub_raw = _split_head_and_group(raw)
    # "마스킹시즈닝-M(0.1%):양파분말..." 지원
    head2, tail2 = _split_first_top_level(raw, (":", "："))
    if tail2:
        name_part, sub_raw = _split_head_and_group(head2)
        sub_raw = ((sub_raw + ", " + tail2) if sub_raw else tail2).strip()

    # "배타믹스5.5%-밀가루(강력분)" -> name=배타믹스, amount=5.5%, sub=밀가루(강력분)
    amount_from_head: str | None = None
    m_amount_hyphen = re.match(r"^(.*?)(\d+(?:\.\d+)?)%\s*-\s*(.+)$", name_part)
    if m_amount_hyphen:
        name_part = (m_amount_hyphen.group(1) or "").strip()
        amount_from_head = f"{m_amount_hyphen.group(2)}%"
        rhs = (m_amount_hyphen.group(3) or "").strip()
        sub_raw = ((rhs + ", " + sub_raw) if sub_raw else rhs).strip()

    origin_hint: str | None = None
    if "-" in name_part:
        left, right = name_part.split("-", 1)
        left = left.strip()
        right = right.strip()
        if left and _is_origin_descriptor(right):
            name_part = left
            origin_hint = right
    if not name_part and sub_raw:
        if _is_origin_descriptor(sub_raw):
            # "(국산)"만 있는 토큰 등은 하위 원재료로 만들지 않음.
            return None
        # "[물엿(옥분:옥수수)]" 같이 껍데기 괄호만 있는 경우 내부를 본문으로 승격.
        name_part, sub_raw = _split_head_and_group(sub_raw)
        if not name_part:
            name_part = sub_raw or ""
            sub_raw = None

    name_part, amount = _extract_inline_amount(name_part)
    if amount_from_head:
        amount = amount or amount_from_head
    name_part, origin_detail = _extract_origin_detail(name_part)
    if origin_prefix:
        origin_detail = (origin_detail + ", " if origin_detail else "") + origin_prefix
    if origin_hint:
        origin_detail = (origin_detail + ", " if origin_detail else "") + origin_hint
    sub_ingredients: list[dict[str, Any]] = []

    if sub_raw:
        if _is_origin_descriptor(sub_raw):
            # 원유(국산) 처럼 괄호 내용이 원산지면 부모 메타로 흡수.
            origin_detail = (origin_detail + ", " if origin_detail else "") + sub_raw.strip()
        else:
            for child in _split_sub_tokens(sub_raw):
                if _is_origin_descriptor(child):
                    if sub_ingredients:
                        prev = sub_ingredients[-1]
                        prev_od = str(prev.get("origin_detail") or "").strip()
                        prev["origin_detail"] = (prev_od + ", " if prev_od else "") + str(child).strip()
                    continue
                parsed = _parse_sub_ingredient(child)
                if parsed is not None:
                    sub_ingredients.append(parsed)

    name = _clean_name_token(name_part)
    if not name:
        return None
    return {
        "name": name,
        "origin": None,
        "origin_detail": origin_detail,
        "amount": amount,
        "sub_ingredients": sub_ingredients,
    }


def _parse_one_ingredient(token: str) -> dict[str, Any]:
    raw = str(token or "").strip()
    if not raw:
        return {
            "ingredient_name": None,
            "origin": None,
            "origin_detail": None,
            "amount": None,
            "sub_ingredients": [],
        }

    origin_prefix, raw = _extract_origin_prefix(raw)
    name_part, sub_raw = _split_head_and_group(raw)
    # "마스킹시즈닝-M(0.1%):양파분말..." 지원
    head2, tail2 = _split_first_top_level(raw, (":", "："))
    if tail2:
        name_part, sub_raw = _split_head_and_group(head2)
        sub_raw = ((sub_raw + ", " + tail2) if sub_raw else tail2).strip()

    # "배타믹스5.5%-밀가루" / "바니라향파우다4.5%-바나린"
    amount_from_head: str | None = None
    m_amount_hyphen = re.match(r"^(.*?)(\d+(?:\.\d+)?)%\s*-\s*(.+)$", name_part)
    if m_amount_hyphen:
        name_part = (m_amount_hyphen.group(1) or "").strip()
        amount_from_head = f"{m_amount_hyphen.group(2)}%"
        rhs = (m_amount_hyphen.group(3) or "").strip()
        sub_raw = ((rhs + ", " + sub_raw) if sub_raw else rhs).strip()

    origin_hint: str | None = None
    if "-" in name_part:
        left, right = name_part.split("-", 1)
        left = left.strip()
        right = right.strip()
        if left and _is_origin_descriptor(right):
            name_part = left
            origin_hint = right

    name_part, amount = _extract_inline_amount(name_part)
    if amount_from_head:
        amount = amount or amount_from_head
    name_part, origin_detail = _extract_origin_detail(name_part)
    if origin_prefix:
        origin_detail = (origin_detail + ", " if origin_detail else "") + origin_prefix
    if origin_hint:
        origin_detail = (origin_detail + ", " if origin_detail else "") + origin_hint

    sub_ingredients: list[dict[str, Any]] = []
    if sub_raw:
        if _is_origin_descriptor(sub_raw):
            # 원유(국산) 등: 하위원재료로 만들지 않고 부모 원산지 메타로 흡수
            origin_detail = (origin_detail + ", " if origin_detail else "") + sub_raw.strip()
        else:
            for child in _split_sub_tokens(sub_raw):
                if _is_origin_descriptor(child):
                    if sub_ingredients:
                        prev = sub_ingredients[-1]
                        prev_od = str(prev.get("origin_detail") or "").strip()
                        prev["origin_detail"] = (prev_od + ", " if prev_od else "") + str(child).strip()
                    continue
                parsed = _parse_sub_ingredient(child)
                if parsed is not None:
                    sub_ingredients.append(parsed)

    return {
        "ingredient_name": _clean_name_token(name_part) or None,
        "origin": None,
        "origin_detail": origin_detail,
        "amount": amount,
        "sub_ingredients": sub_ingredients,
    }


def _append_detail(base: str | None, extra: str | None) -> str | None:
    b = str(base or "").strip()
    e = str(extra or "").strip()
    if not e:
        return b or None
    if not b:
        return e
    parts = [p.strip() for p in re.split(r"\s*,\s*", b) if p and p.strip()]
    if e in parts:
        return b
    return f"{b}, {e}"


def _split_detail_tokens(value: str | None) -> list[str]:
    t = str(value or "").strip()
    if not t:
        return []
    return [p.strip() for p in re.split(r"\s*,\s*", t) if p and p.strip()]


def _separate_origin_detail_tokens(origin_detail: str | None) -> tuple[str | None, list[str]]:
    """
    origin_detail 안의 토큰을 분해해서
    - 원산지로 보이는 토큰은 유지
    - 기능성/첨가물 역할 토큰은 하위 원재료 후보로 분리
    """
    kept: list[str] = []
    roles: list[str] = []
    for tok in _split_detail_tokens(origin_detail):
        if _is_origin_descriptor(tok):
            kept.append(tok)
            continue
        if _NON_ORIGIN_ROLE_HINT_RE.search(tok):
            roles.append(tok)
            continue
        kept.append(tok)
    return (", ".join(kept) if kept else None, roles)


def _separate_amount_token(amount: str | None) -> tuple[str | None, list[str]]:
    """
    amount 오기입 교정:
    - '향미증진제', '합성보존료' 같은 역할 토큰은 amount가 아니라 하위 성분으로 이동
    """
    t = str(amount or "").strip()
    if not t:
        return (None, [])
    if _NON_ORIGIN_ROLE_HINT_RE.search(t):
        return (None, [t])
    return (t, [])


def _normalize_label_prefix(name: str) -> str:
    """
    OCR/라벨 표기 접두 제거:
    - '*면:소맥분' -> '소맥분'
    - '면-소맥분'  -> '소맥분'
    """
    n = str(name or "").replace("\n", " ").strip()
    m = re.match(r"^[^가-힣A-Za-z0-9]*면\s*[:\-]\s*(.+)$", n)
    if m:
        return _clean_name_token(m.group(1))
    # OCR 결합 오염: "매운양념분말 *농심사리면-소맥분" -> "소맥분"
    m2 = re.search(r"(?:^|[\s*])(?:[가-힣A-Za-z0-9]+)?면\s*[:\-]\s*(소맥분(?:\([^)]+\))?)$", n)
    if m2:
        return _clean_name_token(m2.group(1))
    return n


def _expand_compound_name_tokens(name: str) -> list[str]:
    """
    구분자 누락 결합 토큰 보정:
    - '감자전분(독일산,덴마크산)감미유S' -> ['감자전분(독일산,덴마크산)', '감미유S']
    """
    n = str(name or "").strip()
    if not n:
        return []
    # 닫는 괄호 뒤 바로 한글/영문이 이어지면 쉼표 누락으로 간주
    fixed = re.sub(r"([\)\]\}])(?=[가-힣A-Za-z])", r"\1,", n)
    if fixed != n:
        return [p.strip() for p in _split_top_level_tolerant(fixed, sep=",") if p and p.strip()]
    return [n]


def _to_sub_node(node: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(node, dict):
        return None
    name = _clean_name_token(node.get("ingredient_name") or node.get("name"))
    if not name:
        return None
    out_children: list[dict[str, Any]] = []
    for child in node.get("sub_ingredients") or []:
        c = _to_sub_node(child if isinstance(child, dict) else None)
        if c is not None:
            out_children.append(c)
    return {
        "name": name,
        "origin": node.get("origin"),
        "origin_detail": node.get("origin_detail"),
        "amount": node.get("amount"),
        "sub_ingredients": out_children,
    }


def _from_sub_to_top(node: dict[str, Any]) -> dict[str, Any]:
    return {
        "ingredient_name": _clean_name_token(node.get("name")) or None,
        "origin": node.get("origin"),
        "origin_detail": node.get("origin_detail"),
        "amount": node.get("amount"),
        "sub_ingredients": node.get("sub_ingredients") or [],
    }


def _normalize_sub_nodes(nodes: list[dict[str, Any]], stats: Counter[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    pending_origin: str | None = None
    for raw_node in nodes:
        if not isinstance(raw_node, dict):
            stats["dropped_invalid_node"] += 1
            continue
        name = _clean_name_token(raw_node.get("name"))
        origin = raw_node.get("origin")
        origin_detail = raw_node.get("origin_detail")
        amount = raw_node.get("amount")
        children = [c for c in (raw_node.get("sub_ingredients") or []) if isinstance(c, dict)]

        if not name and children:
            out.extend(_normalize_sub_nodes(children, stats))
            stats["promoted_empty_parent"] += 1
            continue
        if not name:
            stats["dropped_empty_name"] += 1
            continue

        if _is_origin_descriptor(name):
            pending_origin = _append_detail(pending_origin, name)
            stats["origin_descriptor_promoted"] += 1
            continue

        name = _normalize_label_prefix(name)
        if name != _clean_name_token(raw_node.get("name")):
            stats["strip.label_prefix"] += 1

        expanded_name_tokens = _expand_compound_name_tokens(name)
        if len(expanded_name_tokens) > 1:
            expanded_nodes: list[dict[str, Any]] = []
            for tok in expanded_name_tokens:
                parsed = _parse_one_ingredient(tok)
                pname = _clean_name_token(parsed.get("ingredient_name"))
                if not pname:
                    continue
                pchildren = [_to_sub_node(s) for s in parsed.get("sub_ingredients") or []]
                pchildren = [s for s in pchildren if s is not None]
                expanded_nodes.append(
                    {
                        "name": pname,
                        "origin": parsed.get("origin"),
                        "origin_detail": parsed.get("origin_detail"),
                        "amount": parsed.get("amount"),
                        "sub_ingredients": pchildren,
                    }
                )
            if expanded_nodes:
                expanded_nodes[0]["origin_detail"] = _append_detail(expanded_nodes[0].get("origin_detail"), origin_detail)
                expanded_nodes[0]["amount"] = expanded_nodes[0].get("amount") or amount
                expanded_nodes[0]["sub_ingredients"] = expanded_nodes[0]["sub_ingredients"] + children
                out.extend(_normalize_sub_nodes(expanded_nodes, stats))
                stats["split.compound_name_missing_sep"] += 1
                continue

        # 이름 안에 통짜 구조가 남은 경우(고추장{...}, 맛소금[...] 등) 재파싱
        if any(ch in name for ch in "[]{}()") or "," in name:
            reparsed = _parse_one_ingredient(name)
            reparsed_name = _clean_name_token(reparsed.get("ingredient_name"))
            if reparsed_name:
                reparsed_sub = [_to_sub_node(s) for s in reparsed.get("sub_ingredients") or []]
                reparsed_sub = [s for s in reparsed_sub if s is not None]
                if reparsed_sub or reparsed_name != name:
                    name = reparsed_name
                    amount = amount or reparsed.get("amount")
                    origin_detail = _append_detail(origin_detail, reparsed.get("origin_detail"))
                    if reparsed_sub:
                        children = reparsed_sub + children
                    stats["reparsed_compound_name"] += 1

        # "배타믹스5.5%-밀가루" 같은 잔여 패턴 최종 보정
        m_amount_hyphen = re.match(r"^(.*?)(\d+(?:\.\d+)?)%\s*-\s*(.+)$", name)
        if m_amount_hyphen:
            base = _clean_name_token(m_amount_hyphen.group(1))
            rhs = _clean_name_token(m_amount_hyphen.group(3))
            if base:
                name = base
                amount = amount or f"{m_amount_hyphen.group(2)}%"
                if rhs:
                    rhs_node = _parse_sub_ingredient(rhs)
                    if rhs_node is not None:
                        children = [rhs_node] + children
                stats["split_amount_hyphen"] += 1

        name2, amount2 = _extract_inline_amount(name)
        if amount2 and not amount:
            amount = amount2
            stats["amount_extracted"] += 1
        name = _clean_name_token(name2)

        name3, od2 = _extract_origin_detail(name)
        if od2:
            origin_detail = _append_detail(origin_detail, od2)
            stats["origin_from_name"] += 1
        name = _clean_name_token(name3)

        if pending_origin:
            origin_detail = _append_detail(origin_detail, pending_origin)
            pending_origin = None

        norm_children = _normalize_sub_nodes(children, stats)
        amount, amount_role_tokens = _separate_amount_token(amount)
        if amount_role_tokens:
            for tok in amount_role_tokens:
                role_node = _parse_sub_ingredient(tok)
                if role_node is None:
                    role_name = _clean_name_token(tok)
                    if not role_name:
                        continue
                    role_node = {
                        "name": role_name,
                        "origin": None,
                        "origin_detail": None,
                        "amount": None,
                        "sub_ingredients": [],
                    }
                norm_children.append(role_node)
                stats["amount_role_to_sub"] += 1
        origin_detail, role_tokens = _separate_origin_detail_tokens(origin_detail)
        if role_tokens:
            for tok in role_tokens:
                role_node = _parse_sub_ingredient(tok)
                if role_node is None:
                    role_name = _clean_name_token(tok)
                    if not role_name:
                        continue
                    role_node = {
                        "name": role_name,
                        "origin": None,
                        "origin_detail": None,
                        "amount": None,
                        "sub_ingredients": [],
                    }
                norm_children.append(role_node)
                stats["origin_detail_role_to_sub"] += 1
        if not name:
            if norm_children:
                out.extend(norm_children)
                stats["promoted_children_without_name"] += 1
            else:
                stats["dropped_empty_name"] += 1
            continue

        out.append(
            {
                "name": name,
                "origin": origin,
                "origin_detail": origin_detail,
                "amount": amount,
                "sub_ingredients": norm_children,
            }
        )
    return out


def normalize_haccp_ingredients_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int], bool]:
    base = payload if isinstance(payload, dict) else {}
    items_raw = base.get("ingredients_items") or []
    items = [it for it in items_raw if isinstance(it, dict)] if isinstance(items_raw, list) else []
    stats: Counter[str] = Counter()
    normalized: list[dict[str, Any]] = []
    pending_origin: str | None = None

    for raw in items:
        name = _clean_name_token(raw.get("ingredient_name") or raw.get("name"))
        origin = raw.get("origin")
        origin_detail = raw.get("origin_detail")
        amount = raw.get("amount")
        children_raw = [c for c in (raw.get("sub_ingredients") or []) if isinstance(c, dict)]

        if not name and children_raw:
            promoted = [_from_sub_to_top(c) for c in _normalize_sub_nodes(children_raw, stats)]
            normalized.extend(promoted)
            stats["promoted_empty_parent"] += 1
            continue
        if not name:
            stats["dropped_empty_name"] += 1
            continue
        if _is_origin_descriptor(name):
            pending_origin = _append_detail(pending_origin, name)
            stats["origin_descriptor_promoted"] += 1
            continue

        normalized_name = _normalize_label_prefix(name)
        if normalized_name != name:
            name = normalized_name
            stats["strip.label_prefix"] += 1

        expanded_name_tokens = _expand_compound_name_tokens(name)
        if len(expanded_name_tokens) > 1:
            expanded = []
            for tok in expanded_name_tokens:
                parsed = _parse_one_ingredient(tok)
                pname = _clean_name_token(parsed.get("ingredient_name"))
                if not pname:
                    continue
                pchildren = [_to_sub_node(s) for s in parsed.get("sub_ingredients") or []]
                pchildren = [s for s in pchildren if s is not None]
                expanded.append(
                    {
                        "ingredient_name": pname,
                        "origin": parsed.get("origin"),
                        "origin_detail": parsed.get("origin_detail"),
                        "amount": parsed.get("amount"),
                        "sub_ingredients": pchildren,
                    }
                )
            if expanded:
                expanded[0]["origin_detail"] = _append_detail(expanded[0].get("origin_detail"), origin_detail)
                expanded[0]["amount"] = expanded[0].get("amount") or amount
                if children_raw:
                    expanded[0]["sub_ingredients"] = expanded[0]["sub_ingredients"] + _normalize_sub_nodes(children_raw, stats)
                normalized.extend(expanded)
                stats["split.compound_name_missing_sep"] += 1
                continue

        # 이름 자체가 다중 토큰이면 top-level로 확장
        split_tokens = _split_top_level_tolerant(name, sep=",")
        if len(split_tokens) > 1:
            expanded = []
            for tok in split_tokens:
                parsed = _parse_one_ingredient(tok)
                pname = _clean_name_token(parsed.get("ingredient_name"))
                if not pname:
                    continue
                pchildren = [_to_sub_node(s) for s in parsed.get("sub_ingredients") or []]
                pchildren = [s for s in pchildren if s is not None]
                expanded.append(
                    {
                        "ingredient_name": pname,
                        "origin": parsed.get("origin"),
                        "origin_detail": parsed.get("origin_detail"),
                        "amount": parsed.get("amount"),
                        "sub_ingredients": pchildren,
                    }
                )
            if expanded:
                if children_raw:
                    expanded[0]["sub_ingredients"] = expanded[0]["sub_ingredients"] + _normalize_sub_nodes(children_raw, stats)
                normalized.extend(expanded)
                stats["expanded_top_level_name"] += 1
                continue

        if any(ch in name for ch in "[]{}()"):
            reparsed = _parse_one_ingredient(name)
            reparsed_name = _clean_name_token(reparsed.get("ingredient_name"))
            if reparsed_name:
                name = reparsed_name
                amount = amount or reparsed.get("amount")
                origin_detail = _append_detail(origin_detail, reparsed.get("origin_detail"))
                reparsed_sub = [_to_sub_node(s) for s in reparsed.get("sub_ingredients") or []]
                reparsed_sub = [s for s in reparsed_sub if s is not None]
                if reparsed_sub:
                    children_raw = reparsed_sub + children_raw
                stats["reparsed_compound_name"] += 1

        name2, amount2 = _extract_inline_amount(name)
        if amount2 and not amount:
            amount = amount2
            stats["amount_extracted"] += 1
        name = _clean_name_token(name2)
        name3, od2 = _extract_origin_detail(name)
        if od2:
            origin_detail = _append_detail(origin_detail, od2)
            stats["origin_from_name"] += 1
        name = _clean_name_token(name3)

        if pending_origin:
            origin_detail = _append_detail(origin_detail, pending_origin)
            pending_origin = None

        norm_children = _normalize_sub_nodes(children_raw, stats)
        amount, amount_role_tokens = _separate_amount_token(amount)
        if amount_role_tokens:
            for tok in amount_role_tokens:
                role_node = _parse_sub_ingredient(tok)
                if role_node is None:
                    role_name = _clean_name_token(tok)
                    if not role_name:
                        continue
                    role_node = {
                        "name": role_name,
                        "origin": None,
                        "origin_detail": None,
                        "amount": None,
                        "sub_ingredients": [],
                    }
                norm_children.append(role_node)
                stats["amount_role_to_sub"] += 1
        origin_detail, role_tokens = _separate_origin_detail_tokens(origin_detail)
        if role_tokens:
            for tok in role_tokens:
                role_node = _parse_sub_ingredient(tok)
                if role_node is None:
                    role_name = _clean_name_token(tok)
                    if not role_name:
                        continue
                    role_node = {
                        "name": role_name,
                        "origin": None,
                        "origin_detail": None,
                        "amount": None,
                        "sub_ingredients": [],
                    }
                norm_children.append(role_node)
                stats["origin_detail_role_to_sub"] += 1
        if not name:
            if norm_children:
                normalized.extend([_from_sub_to_top(c) for c in norm_children])
                stats["promoted_children_without_name"] += 1
            else:
                stats["dropped_empty_name"] += 1
            continue

        normalized.append(
            {
                "ingredient_name": name,
                "origin": origin,
                "origin_detail": origin_detail,
                "amount": amount,
                "sub_ingredients": norm_children,
            }
        )

    if pending_origin and normalized:
        normalized[-1]["origin_detail"] = _append_detail(normalized[-1].get("origin_detail"), pending_origin)
        stats["origin_descriptor_promoted"] += 1

    normalized_payload = {
        "ingredients_items": normalized,
        "ignored_fragments": list(base.get("ignored_fragments") or []),
        "parser": "code_v4",
    }
    before = dumps_json(base)
    after = dumps_json(normalized_payload)
    return normalized_payload, dict(stats), before != after


def enrich_ingredients_from_raw_text(payload: dict[str, Any], raw_text: str | None) -> tuple[dict[str, Any], bool]:
    """
    모델 구조화 결과에 원문 괄호 정보를 보강한다.
    예: 파라옥시안식향산에틸(합성보존료) -> sub_ingredients에 합성보존료 추가
    """
    base = payload if isinstance(payload, dict) else {}
    items_raw = base.get("ingredients_items") or []
    if not isinstance(items_raw, list) or not items_raw:
        return base, False

    src = str(raw_text or "").strip()
    if not src:
        return base, False

    raw_groups_by_name: dict[str, list[str]] = {}
    for tok in _split_top_level_tolerant(src, sep=","):
        head, group = _split_head_and_group(tok)
        name = _clean_name_token(head)
        if not name or not group:
            continue
        raw_groups_by_name.setdefault(name, []).append(group)

    changed = False
    out_items: list[dict[str, Any]] = []
    for item in items_raw:
        if not isinstance(item, dict):
            out_items.append(item)
            continue
        name = _clean_name_token(item.get("ingredient_name") or item.get("name"))
        if not name:
            out_items.append(item)
            continue

        sub_nodes = [c for c in (item.get("sub_ingredients") or []) if isinstance(c, dict)]
        existing_names = {str(c.get("name") or "").strip() for c in sub_nodes}

        groups = raw_groups_by_name.get(name) or []
        for group in groups:
            for part in _split_sub_tokens(group):
                p = str(part or "").strip()
                if not p:
                    continue
                if _is_origin_descriptor(p):
                    od = str(item.get("origin_detail") or "").strip()
                    merged = _append_detail(od, p)
                    if merged != od:
                        item["origin_detail"] = merged
                        changed = True
                    continue
                parsed = _parse_sub_ingredient(p)
                if parsed is None:
                    parsed = {
                        "name": _clean_name_token(p),
                        "origin": None,
                        "origin_detail": None,
                        "amount": None,
                        "sub_ingredients": [],
                    }
                cname = str(parsed.get("name") or "").strip()
                if not cname or cname in existing_names:
                    continue
                sub_nodes.append(parsed)
                existing_names.add(cname)
                changed = True

        item["sub_ingredients"] = sub_nodes
        out_items.append(item)

    out_payload = {
        "ingredients_items": out_items,
        "ignored_fragments": list(base.get("ignored_fragments") or []),
        "parser": str(base.get("parser") or "code_v4"),
    }
    return out_payload, changed


def parse_haccp_rawmtrl(rawmtrl_text: str | None) -> dict[str, Any]:
    src = str(rawmtrl_text or "").strip()
    # 비정형 구분자 보정
    src = src.replace("], [", ",").replace("],[", ",")
    if not src:
        return {"ingredients_items": [], "ignored_fragments": [], "parser": "code_v4"}
    tokens = _split_top_level_tolerant(src, sep=",")
    tokens = _merge_continuation_tokens(tokens)
    items = [_parse_one_ingredient(tok) for tok in tokens]
    items = [it for it in items if it.get("ingredient_name")]
    # 최후 보정: 비정형 데이터로 통짜 1건이 되면 강제 재분해
    if len(items) == 1:
        only = items[0]
        only_name = str(only.get("ingredient_name") or "")
        only_sub = only.get("sub_ingredients") or []
        if (not only_sub) and only_name.count(",") >= 2:
            force_tokens = [p.strip() for p in src.split(",") if p and p.strip()]
            force_items = [_parse_one_ingredient(tok) for tok in force_tokens]
            force_items = [it for it in force_items if it.get("ingredient_name")]
            if len(force_items) > 1:
                items = force_items
    payload = {
        "ingredients_items": items,
        "ignored_fragments": [],
        "parser": "code_v4",
    }
    normalized_payload, _, _ = normalize_haccp_ingredients_payload(payload)
    return normalized_payload


def parse_haccp_nutrient(nutrient_text: str | None) -> dict[str, Any]:
    src = str(nutrient_text or "").strip().replace(",", "")
    out_items: list[dict[str, Any]] = []
    for display_name, (_unit, canonical_unit, patterns) in _NUTRIENT_FIELD_PATTERNS.items():
        value: str | None = None
        for pat in patterns:
            m = re.search(pat, src, re.IGNORECASE)
            if m:
                value = str(m.group(1))
                break
        if value is None:
            continue
        out_items.append(
            {
                "name": display_name,
                "value": value,
                "unit": canonical_unit,
                "daily_value": None,
            }
        )
    return {"nutrition_items": out_items, "parser": "code_v4"}


def dumps_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
