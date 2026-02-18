"""
DB 데이터 조회 뷰어
  - 품목 보고 번호(itemMnftrRptNo) 기준 검색/탐색
  - 식품명 검색, 전체 목록 페이지 탐색, 통계 요약 제공
"""

import sqlite3
import sys
import webbrowser

from app.config import DB_FILE

W = 64  # 출력 너비


# ── 공통 유틸 ────────────────────────────────────────────────────


def _display_width(text: str) -> int:
    """터미널 표시 너비 계산 (한글 등 CJK 문자는 2칸)"""
    return sum(2 if ord(c) > 127 else 1 for c in (text or ""))


def _trunc(text: str, max_w: int) -> str:
    """표시 너비 기준으로 문자열 자르기"""
    result, width = [], 0
    for c in text or "":
        cw = 2 if ord(c) > 127 else 1
        if width + cw > max_w:
            break
        result.append(c)
        width += cw
    return "".join(result)


def _fixed(text: str, max_w: int) -> str:
    """표시 너비 기준으로 고정 폭 문자열 반환 (자르기 + 공백 패딩)"""
    t = _trunc(text or "—", max_w)
    return t + " " * (max_w - _display_width(t))


def _bar(char: str = "─") -> str:
    return "  " + char * (W - 4)


def _category_parts(row: dict) -> list[str]:
    """카테고리 레벨을 순서대로 수집하고 중복을 제거한다."""
    keys = ("foodLv3Nm", "foodLv4Nm", "foodLv5Nm", "foodLv6Nm", "foodLv7Nm")
    parts: list[str] = []
    seen: set[str] = set()
    for key in keys:
        value = (row.get(key) or "").strip()
        if not value or value in seen:
            continue
        parts.append(value)
        seen.add(value)
    return parts


def _category_text(row: dict, full: bool = False) -> str:
    """카테고리를 사용자 친화적인 텍스트로 변환한다."""
    parts = _category_parts(row)
    if not parts:
        return "분류 정보 없음"
    if full or len(parts) == 1:
        return " > ".join(parts)
    return f"{parts[-1]} ({parts[0]})"


def _category_level_text(row: dict, level: int) -> str:
    """대/중/소 분류를 '이름 (코드)' 형식으로 반환."""
    name_key = f"foodLv{level}Nm"
    code_key = f"foodLv{level}Cd"
    name = (row.get(name_key) or "").strip()
    code = (row.get(code_key) or "").strip()
    if not name and not code:
        return "—"
    if name and code:
        return f"{name} ({code})"
    return name or code


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row and row[0] > 0)


# ── 화면 출력 ────────────────────────────────────────────────────


def print_header() -> None:
    title = "식품 DB 데이터 뷰어"
    inner = W - 2
    dw = _display_width(title)
    pad_l = (inner - dw) // 2
    pad_r = inner - pad_l - dw
    print()
    print("╔" + "═" * inner + "╗")
    print("║" + " " * pad_l + title + " " * pad_r + "║")
    print("╚" + "═" * inner + "╝")
    print()


def print_section(title: str) -> None:
    print()
    print(_bar())
    print(f"  {title}")
    print(_bar())


# ── DB 현황 요약 ────────────────────────────────────────────────


def print_db_summary(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM food_info").fetchone()[0]
    unique_rpt = conn.execute(
        "SELECT COUNT(DISTINCT itemMnftrRptNo) FROM food_info "
        "WHERE itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != ''"
    ).fetchone()[0]
    missing_rpt = conn.execute(
        "SELECT COUNT(*) FROM food_info "
        "WHERE itemMnftrRptNo IS NULL OR itemMnftrRptNo = ''"
    ).fetchone()[0]
    unique_cat = conn.execute(
        "SELECT COUNT(DISTINCT foodLv3Nm) FROM food_info "
        "WHERE foodLv3Nm IS NOT NULL AND foodLv3Nm != ''"
    ).fetchone()[0]
    date_row = conn.execute(
        "SELECT MIN(crtYmd), MAX(crtYmd) FROM food_info "
        "WHERE crtYmd IS NOT NULL AND crtYmd != ''"
    ).fetchone()
    min_date = date_row[0] if date_row else "—"
    max_date = date_row[1] if date_row else "—"

    print_section("[ DB 현황 ]")
    print(f"  전체 레코드    : {total:,}건")
    print(f"  품목 보고 번호 : {unique_rpt:,}개 (고유값, 값 있는 데이터 기준)")
    print(f"  번호 없음      : {missing_rpt:,}건")
    print(f"  식품 분류      : {unique_cat:,}개")
    if min_date and max_date:
        print(f"  등록일 범위    : {min_date} ~ {max_date}")
    print()


# ── 단건 카드 출력 ───────────────────────────────────────────────


def print_food_card(row: dict, index: int | None = None, detail: bool = False) -> None:
    """품목 1건을 카드 형식으로 출력"""
    sep = "─" * (W - 4)
    print(f"  ┌{sep}┐")

    if index is not None:
        print(f"  │  [{index}]")

    # 핵심 정보 (품목 보고 번호 강조)
    rpt_no  = _trunc(row.get("itemMnftrRptNo") or "—", 40)
    food_nm = _trunc(row.get("foodNm")  or "—", 36)
    food_cd = _trunc(row.get("foodCd")  or "—", 36)
    cat     = _trunc(_category_text(row), 36)
    cat_all = _trunc(_category_text(row, full=True), 50)
    cat_lv3 = _trunc(_category_level_text(row, 3), 42)
    cat_lv4 = _trunc(_category_level_text(row, 4), 42)
    cat_lv5 = _trunc(_category_level_text(row, 5), 42)
    mfr     = _trunc(row.get("mfrNm")   or "—", 36)
    impt    = "수입" if row.get("imptYn") == "Y" else "국산"

    print(f"  │  품목 보고 번호 : {rpt_no}")
    print(f"  │  식품명          : {food_nm}")
    print(f"  │  식품 코드       : {food_cd}")
    print(f"  │  카테고리        : {cat}")
    print(f"  │  대분류(코드)    : {cat_lv3}")
    print(f"  │  중분류(코드)    : {cat_lv4}")
    print(f"  │  소분류(코드)    : {cat_lv5}")
    print(f"  │  제조사          : {mfr}")
    print(f"  │  구분            : {impt}")

    if detail:
        enerc  = row.get("enerc")  or "—"
        prot   = row.get("prot")   or "—"
        fatce  = row.get("fatce")  or "—"
        chocdf = row.get("chocdf") or "—"
        nat    = row.get("nat")    or "—"
        srv    = row.get("servSize") or "—"
        coo    = _trunc(row.get("cooNm") or "—", 30)
        crtYmd = row.get("crtYmd") or "—"
        impt_nm = _trunc(row.get("imptNm") or "—", 30)
        dist_nm = _trunc(row.get("distNm") or "—", 30)

        print("  │")
        print("  │  ── 영양 정보 (100g 또는 1회 제공량 기준) ──")
        print(f"  │    서빙 크기     : {srv}")
        print(f"  │    에너지        : {enerc} kcal")
        print(f"  │    단백질        : {prot} g")
        print(f"  │    지방          : {fatce} g")
        print(f"  │    탄수화물      : {chocdf} g")
        print(f"  │    나트륨        : {nat} mg")
        print("  │")
        print("  │  ── 추가 정보 ──")
        print(f"  │    카테고리(전체): {cat_all}")
        print(f"  │    원산지        : {coo}")
        if impt == "수입":
            print(f"  │    수입사        : {impt_nm}")
        print(f"  │    유통사        : {dist_nm}")
        print(f"  │    등록일        : {crtYmd}")

    print(f"  └{sep}┘")


# ── 메뉴 기능 ────────────────────────────────────────────────────


def _fetch_rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def search_by_report_no(conn: sqlite3.Connection) -> None:
    """★ 품목 보고 번호(키값)로 검색"""
    print_section("[ 품목 보고 번호 검색 ]  ★ 키값 기준")
    print("  일부만 입력해도 됩니다.  예) 2022,  20220000123456")
    query = input("  검색어 입력 : ").strip()
    if not query:
        print("  검색어를 입력해주세요.")
        return

    rows = _fetch_rows(
        conn,
        "SELECT * FROM food_info WHERE itemMnftrRptNo LIKE ? LIMIT 20",
        (f"%{query}%",),
    )

    if not rows:
        print(f"\n  결과 없음 : '{query}'와 일치하는 품목 보고 번호가 없습니다.")
        return

    print(f"\n  검색 결과 : {len(rows)}건 (최대 20건 표시)\n")
    for i, row in enumerate(rows, 1):
        print_food_card(row, index=i, detail=True)
        print()


def search_by_name(conn: sqlite3.Connection) -> None:
    """식품명으로 검색"""
    print_section("[ 식품명 검색 ]")
    query = input("  식품명 입력 : ").strip()
    if not query:
        print("  검색어를 입력해주세요.")
        return

    rows = _fetch_rows(
        conn,
        "SELECT * FROM food_info WHERE foodNm LIKE ? LIMIT 20",
        (f"%{query}%",),
    )

    if not rows:
        print(f"\n  결과 없음 : '{query}'에 해당하는 식품이 없습니다.")
        return

    print(f"\n  검색 결과 : {len(rows)}건 (최대 20건 표시)\n")
    for i, row in enumerate(rows, 1):
        print_food_card(row, index=i, detail=False)
        print()

    print("  번호를 입력하면 상세 정보를 볼 수 있습니다.")
    sel = input("  번호 입력 (엔터: 건너뜀) : ").strip()
    if sel.isdigit() and 1 <= int(sel) <= len(rows):
        print()
        print_food_card(rows[int(sel) - 1], detail=True)


def list_all(conn: sqlite3.Connection) -> None:
    """전체 목록 페이지 탐색"""
    PAGE = 10
    total = conn.execute("SELECT COUNT(*) FROM food_info").fetchone()[0]
    if total == 0:
        print("  저장된 데이터가 없습니다.")
        return

    page = 0
    while True:
        offset = page * PAGE
        rows = _fetch_rows(
            conn,
            "SELECT * FROM food_info LIMIT ? OFFSET ?",
            (PAGE, offset),
        )
        if not rows:
            print("\n  마지막 페이지입니다.")
            break

        start = offset + 1
        end = offset + len(rows)
        print_section(f"[ 전체 목록 ]  {start}~{end}건 / 총 {total:,}건")

        # 헤더 행
        h_no   = _fixed("번호", 4)
        h_rpt  = _fixed("품목 보고 번호", 22)
        h_nm   = _fixed("식품명", 20)
        h_cat  = _fixed("카테고리", 20)
        print(f"  {h_no}  {h_rpt}  {h_nm}  {h_cat}")
        print(f"  {'─'*4}  {'─'*22}  {'─'*20}  {'─'*20}")

        for i, row in enumerate(rows, start):
            rpt = _fixed(row.get("itemMnftrRptNo") or "—", 22)
            nm  = _fixed(row.get("foodNm") or "—", 20)
            cat = _fixed(_category_text(row), 20)
            print(f"  {i:>4}  {rpt}  {nm}  {cat}")

        # 내비게이션
        nav = []
        if page > 0:
            nav.append("[p] 이전")
        if len(rows) == PAGE and end < total:
            nav.append("[n] 다음")
        nav.append("[상세] 번호 입력")
        nav.append("[q] 메뉴")
        print(f"\n  {' / '.join(nav)}")

        choice = input("  선택 : ").strip().lower()
        if choice == "n" and end < total:
            page += 1
        elif choice == "p" and page > 0:
            page -= 1
        elif choice == "q":
            break
        elif choice.isdigit():
            idx = int(choice) - start
            if 0 <= idx < len(rows):
                print()
                print_food_card(rows[idx], detail=True)
            else:
                print("  해당 번호가 현재 페이지에 없습니다.")
        else:
            print("  올바른 키를 입력해주세요.")


def show_stats(conn: sqlite3.Connection) -> None:
    """통계/요약"""
    print_section("[ 통계 ]")

    # 분류별 현황
    rows = conn.execute("""
        SELECT foodLv3Nm, COUNT(*) AS cnt
        FROM food_info
        WHERE foodLv3Nm IS NOT NULL AND foodLv3Nm != ''
        GROUP BY foodLv3Nm
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()

    if rows:
        print("  ■ 분류별 상위 10개")
        max_cnt = rows[0][1] if rows else 1
        bar_w = 20
        for name, cnt in rows:
            label = _fixed(name or "—", 18)
            filled = int(bar_w * cnt / max_cnt) if max_cnt > 0 else 0
            bar = "█" * filled + "░" * (bar_w - filled)
            print(f"  {label}  [{bar}]  {cnt:,}건")

    print()

    # 국산/수입
    import_data: dict = {}
    for yn, cnt in conn.execute(
        "SELECT imptYn, COUNT(*) FROM food_info GROUP BY imptYn"
    ):
        import_data[yn] = cnt

    domestic = sum(v for k, v in import_data.items() if k != "Y")
    imported = import_data.get("Y", 0)
    print("  ■ 국산 / 수입 현황")
    print(f"    국산 : {domestic:,}건")
    print(f"    수입 : {imported:,}건")
    print()

    # 최근 등록 TOP 5
    recent = conn.execute("""
        SELECT foodNm, itemMnftrRptNo, crtYmd
        FROM food_info
        WHERE crtYmd IS NOT NULL AND crtYmd != ''
        ORDER BY crtYmd DESC
        LIMIT 5
    """).fetchall()

    if recent:
        print("  ■ 최근 등록 5건")
        for food_nm, rpt_no, date in recent:
            nm  = _fixed(food_nm or "—", 20)
            rpt = _trunc(rpt_no or "—", 22)
            print(f"    {date}  {nm}  {rpt}")


def show_all_categories(conn: sqlite3.Connection) -> None:
    """카테고리 계층 중 대분류/중분류만 상품 수와 함께 출력."""
    print_section("[ 전체 카테고리 보기 ]")

    rows = conn.execute("""
        SELECT
            COALESCE(NULLIF(foodLv3Nm, ''), '미분류') AS lv3,
            NULLIF(foodLv4Nm, '') AS lv4,
            COUNT(*) AS cnt
        FROM food_info
        GROUP BY lv3, lv4
        ORDER BY lv3, lv4
    """).fetchall()

    if not rows:
        print("  저장된 데이터가 없습니다.")
        return

    grouped: dict[str, dict[str, object]] = {}
    for lv3, lv4, cnt in rows:
        if lv3 not in grouped:
            grouped[lv3] = {"total": 0, "children": {}}
        grouped[lv3]["total"] = int(grouped[lv3]["total"]) + cnt
        mid = lv4 or "중분류 미지정"
        children = grouped[lv3]["children"]
        if not isinstance(children, dict):
            children = {}
            grouped[lv3]["children"] = children
        children[mid] = int(children.get(mid, 0)) + cnt

    top_category_count = len(grouped)
    total_products = conn.execute("SELECT COUNT(*) FROM food_info").fetchone()[0]
    print(f"  대분류 수 : {top_category_count:,}개")
    print(f"  전체 상품 : {total_products:,}건")
    print()

    print("  ■ 대분류 / 중분류")
    major_items = sorted(
        grouped.items(),
        key=lambda x: (-int(x[1]["total"]), x[0]),
    )
    for major, data in major_items:
        mid_map = data["children"]
        if not isinstance(mid_map, dict):
            mid_map = {}
        print(
            f"  - 대분류: {major}  "
            f"(중분류 {len(mid_map):,}개 / 상품 {int(data['total']):,}건)"
        )
        mid_items = sorted(mid_map.items(), key=lambda x: (-x[1], x[0]))
        for mid, cnt in mid_items:
            print(f"      · 중분류: {mid}  (상품 {cnt:,}건)")


def show_ingredient_dashboard(conn: sqlite3.Connection) -> None:
    """원재료 수집/매칭 파이프라인 운영 현황."""
    print_section("[ 원재료 수집 대시보드 ]")

    required_tables = ("ingredient_info", "ingredient_attempts", "ingredient_extractions")
    if not all(_table_exists(conn, t) for t in required_tables):
        print("  원재료 수집 테이블이 아직 없습니다.")
        print("  먼저 ingredient_enricher.py를 1회 실행해주세요.")
        return

    total_target = conn.execute(
        "SELECT COUNT(*) FROM food_info WHERE itemMnftrRptNo IS NOT NULL AND itemMnftrRptNo != ''"
    ).fetchone()[0]
    ingredient_done = conn.execute("SELECT COUNT(*) FROM ingredient_info").fetchone()[0]
    attempts_total = conn.execute("SELECT COUNT(*) FROM ingredient_attempts").fetchone()[0]
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
    pending = conn.execute(
        """
        SELECT COUNT(*)
        FROM food_info fi
        WHERE fi.itemMnftrRptNo IS NOT NULL
          AND fi.itemMnftrRptNo != ''
          AND NOT EXISTS (
              SELECT 1 FROM ingredient_info ii
              WHERE ii.itemMnftrRptNo = fi.itemMnftrRptNo
          )
          AND NOT EXISTS (
              SELECT 1 FROM ingredient_attempts ia
              WHERE ia.query_itemMnftrRptNo = fi.itemMnftrRptNo
          )
        """
    ).fetchone()[0]
    extraction_rows = conn.execute("SELECT COUNT(*) FROM ingredient_extractions").fetchone()[0]
    avg_images = conn.execute(
        "SELECT ROUND(AVG(images_analyzed), 2) FROM ingredient_attempts "
        "WHERE status IN ('matched', 'unmatched')"
    ).fetchone()[0]

    coverage = (ingredient_done / total_target * 100) if total_target else 0.0
    attempt_ratio = (attempts_total / total_target * 100) if total_target else 0.0

    print("  ■ 파이프라인 핵심 지표")
    print(f"    대상 상품 수         : {total_target:,}건")
    print(f"    원재료 확보 완료      : {ingredient_done:,}건 ({coverage:.1f}%)")
    print(f"    시도 이력(누적)       : {attempts_total:,}건 ({attempt_ratio:.1f}%)")
    print(f"    대기 상품(미시도)     : {pending:,}건")
    print(f"    분석 로그 누적        : {extraction_rows:,}건")
    print(f"    평균 분석 이미지 수   : {avg_images if avg_images is not None else '—'}")
    print()

    print("  ■ 시도 상태 분포")
    print(f"    matched      : {matched:,}건")
    print(f"    unmatched    : {unmatched:,}건")
    print(f"    failed       : {failed:,}건")
    print(f"    in_progress  : {in_progress:,}건")
    print()

    print("  ■ 최근 매칭 성공 5건 (출처 URL 포함)")
    rows = conn.execute(
        """
        SELECT ia.query_itemMnftrRptNo, ia.query_food_name, ii.source_image_url, ii.updated_at
        FROM ingredient_attempts ia
        JOIN ingredient_info ii
          ON ii.itemMnftrRptNo = ia.query_itemMnftrRptNo
        WHERE ia.status = 'matched'
        ORDER BY ii.updated_at DESC
        LIMIT 5
        """
    ).fetchall()
    if not rows:
        print("    (데이터 없음)")
    else:
        for rpt_no, food_nm, url, updated_at in rows:
            print(f"    - {food_nm} | {rpt_no} | {updated_at}")
            print(f"      url: {url or '—'}")
    print()

    print("  ■ 최근 실패 5건")
    fail_rows = conn.execute(
        """
        SELECT query_itemMnftrRptNo, query_food_name, error_message, started_at
        FROM ingredient_attempts
        WHERE status = 'failed'
        ORDER BY started_at DESC
        LIMIT 5
        """
    ).fetchall()
    if not fail_rows:
        print("    (데이터 없음)")
    else:
        for rpt_no, food_nm, err, started_at in fail_rows:
            print(f"    - {food_nm} | {rpt_no} | {started_at}")
            print(f"      err: {_trunc(err or '—', 86)}")


def show_ingredient_detail(conn: sqlite3.Connection) -> None:
    """품목보고번호 기준 원재료/시도이력/분석근거 상세 조회."""
    print_section("[ 원재료 상세 조회 ]")

    required_tables = ("ingredient_info", "ingredient_attempts", "ingredient_extractions")
    if not all(_table_exists(conn, t) for t in required_tables):
        print("  원재료 수집 테이블이 아직 없습니다.")
        print("  먼저 ingredient_enricher.py를 실행해주세요.")
        return

    query = input("  품목 보고 번호 입력 : ").strip()
    if not query:
        print("  품목 보고 번호를 입력해주세요.")
        return

    food = conn.execute(
        """
        SELECT foodNm, COALESCE(mfrNm, ''), foodLv3Nm, foodLv4Nm
        FROM food_info
        WHERE itemMnftrRptNo = ?
        LIMIT 1
        """,
        (query,),
    ).fetchone()
    if food:
        food_nm, mfr_nm, lv3, lv4 = food
        cat = lv4 or lv3 or "분류 정보 없음"
        print(f"\n  상품명      : {food_nm or '—'}")
        print(f"  제조사      : {mfr_nm or '—'}")
        print(f"  카테고리    : {cat}")
    else:
        print("\n  food_info에서 해당 품목 보고 번호를 찾지 못했습니다.")

    ing = conn.execute(
        """
        SELECT ingredients_text, source_image_url, source_query, updated_at
        FROM ingredient_info
        WHERE itemMnftrRptNo = ?
        """,
        (query,),
    ).fetchone()
    print("\n  ■ 원재료 저장 정보")
    if not ing:
        print("    원재료 저장 데이터 없음")
    else:
        ingredients, source_url, source_query, updated_at = ing
        print(f"    업데이트 시각 : {updated_at}")
        print(f"    출처 검색어   : {_trunc(source_query or '—', 76)}")
        print(f"    출처 URL      : {source_url or '—'}")
        print(f"    원재료명      : {_trunc(ingredients or '—', 76)}")

    attempt = conn.execute(
        """
        SELECT status, searched_query, images_requested, images_analyzed,
               matched_itemMnftrRptNo, error_message, started_at, finished_at
        FROM ingredient_attempts
        WHERE query_itemMnftrRptNo = ?
        """,
        (query,),
    ).fetchone()
    print("\n  ■ 검색 시도 이력")
    if not attempt:
        print("    시도 이력 없음")
    else:
        status, searched_query, img_req, img_ana, matched_no, err, st, fn = attempt
        print(f"    상태         : {status}")
        print(f"    검색어       : {_trunc(searched_query or '—', 76)}")
        print(f"    이미지 분석  : {img_ana}/{img_req}")
        print(f"    매칭 번호    : {matched_no or '—'}")
        print(f"    시작/종료    : {st or '—'} / {fn or '—'}")
        if err:
            print(f"    에러         : {_trunc(err, 76)}")

    logs = conn.execute(
        """
        SELECT image_rank, image_url, extracted_itemMnftrRptNo,
               matched_target, ingredients_text, created_at
        FROM ingredient_extractions
        WHERE query_itemMnftrRptNo = ?
        ORDER BY id DESC
        LIMIT 10
        """,
        (query,),
    ).fetchall()
    print("\n  ■ 최근 분석 로그 10건")
    if not logs:
        print("    로그 없음")
    else:
        openable_urls: list[str] = []
        for rank, url, extracted, matched_target, ingredients, created_at in logs:
            mark = "MATCH" if matched_target == 1 else "NO-MATCH"
            print(f"    - [{rank}] {mark} | {created_at} | 추출번호: {extracted or '미검출'}")
            if url:
                openable_urls.append(url)
                print(f"      열기번호: {len(openable_urls)}")
            else:
                print("      열기번호: -")
            print(f"      url: {url or '—'}")
            if ingredients:
                print(f"      원재료: {_trunc(ingredients, 76)}")

        if openable_urls:
            print("\n  ■ URL 바로 열기")
            raw_open = input(
                "    열 번호 입력 (예: 1 또는 1,3 / Enter=건너뛰기): "
            ).strip()
            if raw_open:
                picks: list[int] = []
                for token in raw_open.split(","):
                    token = token.strip()
                    if token.isdigit():
                        idx = int(token)
                        if 1 <= idx <= len(openable_urls):
                            picks.append(idx)
                if not picks:
                    print("    ⚠️ 유효한 번호가 없습니다.")
                else:
                    for idx in picks:
                        url = openable_urls[idx - 1]
                        ok = webbrowser.open_new_tab(url)
                        state = "OK" if ok else "요청됨(브라우저 확인)"
                        print(f"    - [{idx}] {state}: {url}")


# ── 진입점 ───────────────────────────────────────────────────────


def main() -> None:
    print_header()

    try:
        conn = sqlite3.connect(DB_FILE)
        conn.execute("SELECT 1 FROM food_info LIMIT 1")
    except sqlite3.OperationalError:
        print(f"  오류: {DB_FILE} 파일이 없거나 food_info 테이블이 없습니다.")
        print("  먼저 main.py를 실행해 데이터를 수집해주세요.")
        sys.exit(1)

    print_db_summary(conn)

    MENU: list[tuple[str, str, object]] = [
        ("1", "품목 보고 번호로 검색  ★ 키값", search_by_report_no),
        ("2", "식품명으로 검색",                search_by_name),
        ("3", "전체 목록 보기",                 list_all),
        ("4", "통계 보기",                       show_stats),
        ("5", "전체 카테고리 보기",              show_all_categories),
        ("6", "원재료 수집 대시보드",            show_ingredient_dashboard),
        ("7", "원재료 상세 조회(품목번호)",      show_ingredient_detail),
        ("q", "종료",                            None),
    ]
    menu_map = {k: fn for k, _, fn in MENU}

    while True:
        print(_bar())
        print("  [ 메뉴 ]")
        for key, label, _ in MENU:
            print(f"    [{key}]  {label}")
        print()
        choice = input("  선택 : ").strip().lower()

        if choice == "q":
            print("\n  뷰어를 종료합니다.\n")
            break
        elif choice in menu_map and menu_map[choice] is not None:
            menu_map[choice](conn)  # type: ignore[operator]
        else:
            print("  올바른 메뉴 번호를 입력해주세요.")

    conn.close()


if __name__ == "__main__":
    main()
