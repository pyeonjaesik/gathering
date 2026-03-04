"""HACCP(Open API) 조회 유틸."""

from __future__ import annotations

import os
from typing import Any

import time

import requests

REQUEST_TIMEOUT_SEC = 30
MAX_RETRIES = 5
RETRY_BACKOFF_BASE_SEC = 1.5
DEFAULT_HACCP_API_URL = "https://apis.data.go.kr/B553748/CertImgListServiceV3/getCertImgListServiceV3"


def _as_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _extract_items_from_body(body: Any) -> list[dict[str, Any]]:
    """
    HACCP 응답의 items 구조 편차를 흡수한다.
    지원 형태 예:
    - body.items.item = {...} | [{...}]
    - body.items = {...} | [{...}]
    - body.item = {...} | [{...}]
    - body.Items / Item (대소문자 편차)
    """
    if not isinstance(body, dict):
        return []

    key_candidates = ("items", "Items", "item", "Item")
    out: list[dict[str, Any]] = []

    def _collect(value: Any) -> None:
        if isinstance(value, dict):
            # item 키를 한 번 더 감싸는 경우
            nested = (
                value.get("item")
                or value.get("Item")
                or value.get("items")
                or value.get("Items")
            )
            if nested is not None and nested is not value:
                _collect(nested)
                return
            out.append(value)
            return
        if isinstance(value, list):
            for v in value:
                _collect(v)

    for k in key_candidates:
        if k in body:
            _collect(body.get(k))
    # 후보 키가 없고 body 자체가 사실상 item 형태인 경우
    if not out and body:
        if any(k in body for k in ("prdlstReportNo", "prdlstNm", "manufacture", "seller")):
            out.append(body)
    return out


def _extract_header_body(payload: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}, {}
    header = payload.get("header")
    body = payload.get("body")
    if isinstance(header, list):
        header = header[0] if header else {}
    if isinstance(body, list):
        body = body[0] if body else {}
    return (
        header if isinstance(header, dict) else {},
        body if isinstance(body, dict) else {},
    )


def _normalize_api_url(value: str) -> str:
    v = str(value or "").strip()
    if not v:
        return ""
    if v.startswith("apis.data.go.kr/"):
        return "https://" + v
    if not v.startswith(("http://", "https://")):
        return "https://" + v
    return v


def _request_haccp(
    *,
    params: dict[str, Any],
) -> dict[str, Any]:
    api_url = _normalize_api_url(os.getenv("HACCP_API_URL", "").strip()) or DEFAULT_HACCP_API_URL
    api_key = os.getenv("HACCP_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("HACCP_API_KEY 환경변수가 필요합니다.")

    request_params = dict(params)
    request_params["serviceKey"] = api_key
    request_params["type"] = "json"
    request_params["returnType"] = "json"

    last_error: str | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(api_url, params=request_params, timeout=REQUEST_TIMEOUT_SEC)
            resp.raise_for_status()
            payload = resp.json()
            header, body = _extract_header_body(payload)
            result_code = str(header.get("resultCode") or "").strip().upper()
            # 공공API 관례상 정상코드가 "00" 또는 "OK"로 올 수 있다.
            if result_code and result_code not in ("00", "OK"):
                msg = header.get("resultMessage") or "unknown"
                raise RuntimeError(f"api_error[{result_code}] {msg}")
            items = _extract_items_from_body(body)
            return {"header": header, "body": body, "items": items, "raw": payload}
        except Exception as exc:  # pylint: disable=broad-except
            last_error = str(exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_BASE_SEC * (2 ** (attempt - 1)))
                continue
            break
    raise RuntimeError(f"HACCP API request failed: {last_error}")


def fetch_haccp_total_count() -> int:
    result = _request_haccp(
        params={
            "pageNo": 1,
            "numOfRows": 1,
        }
    )
    body = result.get("body") or {}
    try:
        return int(body.get("totalCount") or 0)
    except Exception:
        return 0


def fetch_haccp_page(
    *,
    page_no: int,
    num_of_rows: int,
) -> dict[str, Any]:
    return _request_haccp(
        params={
            "pageNo": max(1, int(page_no)),
            "numOfRows": max(1, int(num_of_rows)),
        }
    )


def fetch_haccp_products_by_report_no(
    *,
    report_no: str,
    page_no: int = 1,
    num_of_rows: int = 20,
) -> dict[str, Any]:
    """
    HACCP API를 호출해 품목보고번호 기준 제품정보를 조회한다.

    선택 환경변수:
    - HACCP_API_URL (미설정 시 기본 URL 사용)
    필수 환경변수:
    - HACCP_API_KEY
    """
    return _request_haccp(
        params={
            "prdlstReportNo": report_no,
            "pageNo": max(1, int(page_no)),
            "numOfRows": max(1, int(num_of_rows)),
        }
    )
