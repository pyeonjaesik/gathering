"""
설정값 및 상수
"""

from __future__ import annotations

import os
from pathlib import Path


def _load_dotenv() -> None:
    """프로젝트 루트의 .env를 읽어 환경변수로 주입한다."""
    root_dir = Path(__file__).resolve().parent.parent
    env_path = root_dir / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        # 이미 셸에 설정된 값이 있으면 우선 사용
        os.environ.setdefault(key, value)


_load_dotenv()

SERVICE_KEY = "67ff6f3fae63eb631f0f9b320fc98bb7f53d28d9fcad137f98d351a7db3e9e4d"
BASE_URL = "http://api.data.go.kr/openapi/tn_pubr_public_nutri_process_info_api"
DB_FILE = "food_data.db"
ROWS_PER_PAGE = 10000  # 페이지당 요청 수 (API 최대값)
MAX_WORKERS = 8        # 병렬 동시 요청 수

# 출력 컬럼 목록 (API 응답 필드)
COLUMNS = [
    "foodCd", "foodNm", "dataCd", "typeNm",
    "foodOriginCd", "foodOriginNm",
    "foodLv3Cd", "foodLv3Nm",
    "foodLv4Cd", "foodLv4Nm",
    "foodLv5Cd", "foodLv5Nm",
    "foodLv6Cd", "foodLv6Nm",
    "foodLv7Cd", "foodLv7Nm",
    "nutConSrtrQua",
    "enerc", "water", "prot", "fatce", "ash",
    "chocdf", "sugar", "fibtg",
    "ca", "fe", "p", "k", "nat",
    "vitaRae", "retol", "cartb",
    "thia", "ribf", "nia", "vitc", "vitd",
    "chole", "fasat", "fatrn",
    "srcCd", "srcNm",
    "servSize", "foodSize",
    "itemMnftrRptNo", "mfrNm", "imptNm", "distNm",
    "imptYn", "cooCd", "cooNm",
    "dataProdCd", "dataProdNm",
    "crtYmd", "crtrYmd",
    "instt_code", "instt_nm",
]
