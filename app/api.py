"""
공공 API 호출 기능
"""

import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from app.config import BASE_URL, SERVICE_KEY

REQUEST_TIMEOUT = 120
MAX_RETRIES = 6
RETRY_BACKOFF_BASE_SEC = 2.0


def fetch_total_count() -> int:
    """API에서 전체 데이터 건수를 조회 (1건만 호출해 totalCount 파싱)"""
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": 1,
        "numOfRows": 1,
        "type": "xml",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            root = ET.fromstring(response.content)
            total_el = root.find(".//totalCount")
            if total_el is not None and total_el.text:
                return int(total_el.text)
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"  [오류] 전체 건수 조회 실패: {e}", flush=True)
            else:
                time.sleep(RETRY_BACKOFF_BASE_SEC * (2 ** (attempt - 1)))
    return 0


def fetch_page(page_no: int, num_of_rows: int) -> tuple[list[dict], bool]:
    """API 한 페이지 호출 후 (item 목록, 성공 여부) 반환"""
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": page_no,
        "numOfRows": num_of_rows,
        "type": "xml",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            if attempt < MAX_RETRIES:
                print(
                    f"  [재시도] 페이지 {page_no} HTTP 실패 "
                    f"(시도 {attempt}/{MAX_RETRIES}): {e}",
                    flush=True,
                )
                time.sleep(RETRY_BACKOFF_BASE_SEC * (2 ** (attempt - 1)))
                continue
            print(f"  [오류] HTTP 요청 실패 (페이지 {page_no}): {e}", flush=True)
            return [], False

        if response.status_code != 200:
            if response.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                print(
                    f"  [재시도] 페이지 {page_no} HTTP {response.status_code} "
                    f"(시도 {attempt}/{MAX_RETRIES})",
                    flush=True,
                )
                time.sleep(RETRY_BACKOFF_BASE_SEC * (2 ** (attempt - 1)))
                continue
            print(
                f"  [오류] HTTP {response.status_code} 응답 (페이지 {page_no})",
                flush=True,
            )
            return [], False

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            if attempt < MAX_RETRIES:
                print(
                    f"  [재시도] 페이지 {page_no} XML 파싱 실패 "
                    f"(시도 {attempt}/{MAX_RETRIES}): {e}",
                    flush=True,
                )
                time.sleep(RETRY_BACKOFF_BASE_SEC * (2 ** (attempt - 1)))
                continue
            print(f"  [오류] XML 파싱 실패 (페이지 {page_no}): {e}", flush=True)
            return [], False

        result_code = root.find(".//resultCode")
        if result_code is not None and result_code.text != "00":
            result_msg = root.find(".//resultMsg")
            msg = result_msg.text if result_msg is not None else "알 수 없음"
            print(f"  [오류] API 응답 오류 [{result_code.text}]: {msg}", flush=True)
            return [], False

        items = root.findall(".//item")
        rows = []
        for item in items:
            row = {child.tag: child.text for child in item}
            rows.append(row)
        return rows, True

    return [], False


def fetch_pages_parallel(
    page_numbers: list[int],
    num_of_rows: int,
    max_workers: int,
) -> dict[int, tuple[list[dict], bool]]:
    """여러 페이지를 병렬로 가져온다. {page_no: (rows, ok)} 반환."""
    results: dict[int, tuple[list[dict], bool]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(fetch_page, page_no, num_of_rows): page_no
            for page_no in page_numbers
        }
        for future in as_completed(future_to_page):
            page_no = future_to_page[future]
            results[page_no] = future.result()
    return results
