# gathering

## 원재료 분석 브라우저 모니터 실행

1. 의존성 설치

```bash
uv sync
```

2. `.env` 설정 (`/Users/dmyeon/gathering/.env`)

```env
SERPAPI_KEY=your_serpapi_key
GEMINI_API_KEY=your_gemini_api_key
```

3. 웹 UI 실행

```bash
uv run streamlit run app/web_ui.py
```

4. 브라우저에서 확인

- 기본 주소: `http://localhost:8501`
- 기능:
  - 카테고리 선택 후 원재료 수집 실행
  - 진행중 상품/이미지 분석 로그 실시간 확인
  - 이미지 URL 클릭으로 바로 열기

## analyze 검증셋 벤치마크

1. 템플릿 생성

```bash
python3 -m app.validation_benchmark --init-template validation/samples.template.jsonl
```

2. 템플릿을 복사해 `validation/samples.jsonl` 작성
  - 핵심 정답 필드(최소): `expected.itemMnftrRptNo`, `expected.ingredients_text`
  - 나머지 필드는 생략 가능

3. 벤치마크 실행

```bash
python3 -m app.validation_benchmark --dataset validation/samples.jsonl
```

4. 결과 확인

- 요약: `validation_reports/benchmark_YYYYMMDD_HHMMSS/summary.json`
- 상세: `validation_reports/benchmark_YYYYMMDD_HHMMSS/details.csv`

## backend(`/api/import`) 배치 전송

`food_final` 데이터를 `insidefood backend`의 `POST /api/import` 포맷으로 변환해 전송합니다.

1. 백엔드 실행 (`/Users/dmyeon/backend`)

```bash
npm run dev
```

2. Dry-run으로 변환 결과 확인

```bash
python3 -m app.export_to_backend --dry-run --limit 3
```

3. 실제 배치 전송

```bash
python3 -m app.export_to_backend --batch-size 50
```

주요 옵션:
- `--import-url` (기본: `http://localhost:3000/api/import`)
- `--min-id`, `--max-id` (특정 id 구간만 전송)
- `--limit` (최대 건수 제한)
- `--dry-run` (전송 없이 변환만 수행)
