"""
설정값 및 상수
"""

SERVICE_KEY = "67ff6f3fae63eb631f0f9b320fc98bb7f53d28d9fcad137f98d351a7db3e9e4d"
BASE_URL = "http://api.data.go.kr/openapi/tn_pubr_public_nutri_process_info_api"
DB_FILE = "food_data.db"
ROWS_PER_PAGE = 1000  # 페이지당 요청 수 (최대 10000)

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
