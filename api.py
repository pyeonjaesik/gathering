"""
공공 API 호출 기능
"""

import xml.etree.ElementTree as ET

import requests

from config import BASE_URL, SERVICE_KEY


def fetch_total_count() -> int:
    """API에서 전체 데이터 건수를 조회 (1건만 호출해 totalCount 파싱)"""
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": 1,
        "numOfRows": 1,
        "type": "xml",
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        root = ET.fromstring(response.content)
        total_el = root.find(".//totalCount")
        if total_el is not None and total_el.text:
            return int(total_el.text)
    except Exception as e:
        print(f"  [오류] 전체 건수 조회 실패: {e}", flush=True)
    return 0


def fetch_page(page_no: int, num_of_rows: int) -> list[dict]:
    """API 한 페이지 호출 후 item 목록 반환"""
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": page_no,
        "numOfRows": num_of_rows,
        "type": "xml",
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
    except requests.RequestException as e:
        print(f"  [오류] HTTP 요청 실패: {e}", flush=True)
        return []

    if response.status_code != 200:
        print(f"  [오류] HTTP {response.status_code} 응답", flush=True)
        return []

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as e:
        print(f"  [오류] XML 파싱 실패: {e}", flush=True)
        return []

    result_code = root.find(".//resultCode")
    if result_code is not None and result_code.text != "00":
        result_msg = root.find(".//resultMsg")
        msg = result_msg.text if result_msg is not None else "알 수 없음"
        print(f"  [오류] API 응답 오류 [{result_code.text}]: {msg}", flush=True)
        return []

    items = root.findall(".//item")
    rows = []
    for item in items:
        row = {child.tag: child.text for child in item}
        rows.append(row)
    return rows
