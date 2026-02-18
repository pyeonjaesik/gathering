"""
공공 API 호출 기능
"""

import xml.etree.ElementTree as ET

import requests

from config import BASE_URL, SERVICE_KEY


def fetch_page(page_no: int, num_of_rows: int) -> list[dict]:
    """API 한 페이지 호출 후 item 목록 반환"""
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": page_no,
        "numOfRows": num_of_rows,
        "type": "xml",
    }

    print(f"  [Page {page_no}] 요청 중 (numOfRows={num_of_rows})...", flush=True)
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
    except requests.RequestException as e:
        print(f"  HTTP 요청 실패: {e}", flush=True)
        return []

    if response.status_code != 200:
        print(f"  HTTP 오류: {response.status_code}", flush=True)
        return []

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as e:
        print(f"  XML 파싱 오류: {e}", flush=True)
        return []

    result_code = root.find(".//resultCode")
    if result_code is not None and result_code.text != "00":
        result_msg = root.find(".//resultMsg")
        msg = result_msg.text if result_msg is not None else "알 수 없음"
        print(f"  API 오류 [{result_code.text}]: {msg}", flush=True)
        return []

    items = root.findall(".//item")
    print(f"  → {len(items)}건 수신", flush=True)

    rows = []
    for item in items:
        row = {child.tag: child.text for child in item}
        rows.append(row)
    return rows
