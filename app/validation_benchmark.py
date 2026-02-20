"""
analyze 성능 검증셋 실행 도구.

사용 예:
  python3 -m app.validation_benchmark --dataset validation/samples.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any


def _has_text(value: Any) -> bool:
    return bool(value and str(value).strip())


def _safe_div(num: float, den: float) -> float:
    return num / den if den else 0.0


def _normalize_report_no(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or None


def _normalize_ingredients_text(text: Any) -> str:
    value = (str(text or "")).lower().strip()
    value = value.replace("원재료명", " ").replace("원재료", " ")
    value = value.replace("ingredients", " ")
    value = value.replace("\n", " ").replace("\t", " ")
    value = value.replace("(", " ").replace(")", " ")
    value = value.replace("[", " ").replace("]", " ")
    value = value.replace(":", " ").replace(";", " ")
    value = value.replace("·", ",").replace("，", ",").replace("/", ",")
    value = " ".join(value.split())
    # 비교 시 공백/개행 영향 제거(사용자 요청)
    value = re.sub(r"\s+", "", value)
    return value


def _compact_text(text: Any) -> str:
    """표시용: 공백/줄바꿈 제거."""
    return re.sub(r"\s+", "", str(text or "")).strip()


def _ingredients_similarity(a: Any, b: Any) -> float:
    ta = _normalize_ingredients_text(a)
    tb = _normalize_ingredients_text(b)
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    sa = {x.strip() for x in ta.split(",") if x.strip()}
    sb = {x.strip() for x in tb.split(",") if x.strip()}
    if not sa or not sb:
        return 1.0 if ta == tb else 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return _safe_div(inter, union)


def _build_reason(
    err: str | None,
    report_expected: bool,
    report_ok: bool,
    report_pred_has_value: bool,
    ing_expected: bool,
    ing_ok: bool,
    ing_pred_has_value: bool,
    ing_similarity: float | None,
    ingredients_threshold: float,
) -> str:
    if err:
        return f"분석 오류: {str(err)[:40]}"
    if (not report_expected) and report_pred_has_value and (not ing_expected) and ing_pred_has_value:
        return "정답은 번호/원재료 모두 없음(null)인데 AI가 값을 예측함"
    if (not report_expected) and report_pred_has_value:
        return "정답 번호는 없음(null)인데 AI가 번호를 예측함"
    if (not ing_expected) and ing_pred_has_value:
        return "정답 원재료는 없음(null)인데 AI가 원재료를 예측함"
    if report_expected and not report_ok and ing_expected and not ing_ok:
        sim_txt = f"{ing_similarity:.3f}" if ing_similarity is not None else "-"
        return f"번호 불일치 + 원재료 유사도 미달({sim_txt} < {ingredients_threshold:.3f})"
    if report_expected and not report_ok:
        return "품목보고번호 불일치"
    if ing_expected and not ing_ok:
        sim_txt = f"{ing_similarity:.3f}" if ing_similarity is not None else "-"
        return f"원재료 유사도 미달({sim_txt} < {ingredients_threshold:.3f})"
    return "번호/원재료 기준 충족"


def _load_dataset(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fp:
        for lineno, line in enumerate(fp, start=1):
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            try:
                item = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"JSONL 파싱 실패: line {lineno}: {exc}") from exc
            if not isinstance(item, dict):
                raise ValueError(f"JSONL 형식 오류: line {lineno} is not object")
            if not item.get("image"):
                raise ValueError(f"JSONL 형식 오류: line {lineno} missing image")
            rows.append(item)
    if not rows:
        raise ValueError("검증셋이 비어 있습니다.")
    return rows


def _write_template(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    samples = [
        {
            "id": "sample-01",
            "image": "https://example.com/image1.jpg",
            "expected": {
                "itemMnftrRptNo": "2010010681148",
                "ingredients_text": "밀가루, 설탕, 식물성유지, 코코아분말",
            },
        },
        {
            "id": "sample-02",
            "image": "https://example.com/photo.jpg",
            "expected": {
                "itemMnftrRptNo": None,
                "ingredients_text": None,
            },
        },
    ]
    with path.open("w", encoding="utf-8") as fp:
        for item in samples:
            fp.write(json.dumps(item, ensure_ascii=False) + "\n")


def run_benchmark(
    dataset_path: Path,
    output_dir: Path,
    delay_sec: float = 0.0,
    ingredients_threshold: float = 0.9,
) -> Path:
    from app.ingredient_analyzer import URLIngredientAnalyzer

    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise SystemExit("GEMINI_API_KEY(또는 GOOGLE_API_KEY) 환경변수를 설정해주세요.")

    dataset = _load_dataset(dataset_path)
    analyzer = URLIngredientAnalyzer(api_key=api_key, strict_mode=False)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / f"benchmark_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    total = len(dataset)
    correct_count = 0
    wrong_count = 0
    error_count = 0
    report_wrong_count = 0
    ingredients_wrong_count = 0
    details: list[dict[str, Any]] = []
    t0 = time.time()
    print("\n=== 벤치마크 시작 ===")
    print(f"검증셋: {dataset_path}")
    print(f"총 샘플: {total}건")
    print(f"원재료 유사도 기준: {ingredients_threshold:.3f}")

    for idx, sample in enumerate(dataset, start=1):
        sample_id = sample.get("id") or f"row-{idx}"
        image = str(sample["image"]).strip()
        expected = sample.get("expected") or {}
        exp_report = expected.get("itemMnftrRptNo")
        err: str | None = None

        try:
            pred = analyzer.analyze(image_url=image, target_item_rpt_no=exp_report)
        except Exception as exc:  # pylint: disable=broad-except
            pred = {
                "itemMnftrRptNo": None,
                "ingredients_text": None,
                "note": f"error:{type(exc).__name__}",
            }
            err = str(exc)

        exp_ingredients_text = expected.get("ingredients_text")
        pred_report = pred.get("itemMnftrRptNo")
        exp_report_norm = _normalize_report_no(exp_report)
        pred_report_norm = _normalize_report_no(pred_report)
        pred_ing_text = pred.get("ingredients_text")
        ing_similarity = (
            _ingredients_similarity(exp_ingredients_text, pred_ing_text)
            if exp_ingredients_text is not None
            else None
        )
        report_expected = exp_report is not None
        ing_expected = exp_ingredients_text is not None
        report_pred_has_value = pred_report_norm is not None
        ing_pred_has_value = _has_text(pred_ing_text)

        if report_expected:
            report_ok = (exp_report_norm is not None and pred_report_norm == exp_report_norm)
        else:
            report_ok = not report_pred_has_value

        if ing_expected:
            ing_ok = (ing_similarity is not None and ing_similarity >= ingredients_threshold)
        else:
            ing_ok = not ing_pred_has_value
        is_correct = bool(report_ok and ing_ok and err is None)
        reason = _build_reason(
            err=err,
            report_expected=report_expected,
            report_ok=report_ok,
            report_pred_has_value=report_pred_has_value,
            ing_expected=ing_expected,
            ing_ok=ing_ok,
            ing_pred_has_value=ing_pred_has_value,
            ing_similarity=ing_similarity,
            ingredients_threshold=ingredients_threshold,
        )
        if is_correct:
            correct_count += 1
        else:
            wrong_count += 1
            if err is not None:
                error_count += 1
            if report_expected and not report_ok:
                report_wrong_count += 1
            if ing_expected and not ing_ok:
                ingredients_wrong_count += 1

        details.append(
            {
                "id": sample_id,
                "image": image,
                "verdict": "정답" if is_correct else "오답",
                "reason": reason,
                "error": err,
                "expected_itemMnftrRptNo": exp_report,
                "pred_itemMnftrRptNo": pred_report,
                "expected_ingredients_text": exp_ingredients_text,
                "pred_ingredients_text": pred_ing_text,
                "ingredients_similarity": ing_similarity,
                "report_ok": report_ok,
                "ingredients_ok": ing_ok,
            }
        )

        elapsed = time.time() - t0
        report_label = "✅ 정답" if report_ok else "❌ 오답"
        report_acc = 100.0 if report_ok else 0.0
        ing_label = "✅ 정답" if ing_ok else "❌ 오답"
        if ing_expected:
            ing_acc = (ing_similarity * 100.0) if ing_similarity is not None else 0.0
        else:
            ing_acc = 100.0 if ing_ok else 0.0
        exp_report_compact = _compact_text(exp_report) or "-"
        pred_report_compact = _compact_text(pred_report) or "-"
        exp_ing_compact = _compact_text(exp_ingredients_text) or "-"
        pred_ing_compact = _compact_text(pred_ing_text) or "-"

        print(f"\n[{idx:03d}/{len(dataset):03d}] id={sample_id} | {'✅ 정답' if is_correct else '❌ 오답'} | {elapsed:.1f}s")
        print("  [품목보고번호 비교]")
        print("    │")
        print(f"    ├─ * 판정")
        print(f"    │   {report_label} | 정확도율: {report_acc:.1f}%")
        print("    │")
        print("    ├─ - 정답")
        print(f"    │   {exp_report_compact}")
        print("    │")
        print("    └─ - AI 예측")
        print(f"        {pred_report_compact}")
        print()
        print("  [원재료명 비교]")
        print("    │")
        print(f"    ├─ * 판정")
        print(f"    │   {ing_label} | 정확도율: {ing_acc:.1f}%")
        print("    │")
        print("    ├─ - 정답")
        print(f"    │   {exp_ing_compact}")
        print("    │")
        print("    └─ - AI 예측")
        print(f"        {pred_ing_compact}")
        print()
        print("  [최종 근거]")
        print(f"    {reason}")
        print("\n" + "-" * 78)
        if delay_sec > 0:
            time.sleep(delay_sec)

    summary = {
        "dataset_path": str(dataset_path),
        "samples": total,
        "threshold": ingredients_threshold,
        "correct_count": correct_count,
        "wrong_count": wrong_count,
        "accuracy": _safe_div(correct_count, total),
        "wrong_reasons": {
            "analysis_error": error_count,
            "report_mismatch": report_wrong_count,
            "ingredients_mismatch": ingredients_wrong_count,
        },
        "finished_at": datetime.now().isoformat(),
    }

    summary_path = run_dir / "summary.json"
    with summary_path.open("w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)

    print("\n=== 최종 결과 ===")
    print(f"정답: {correct_count}건")
    print(f"오답: {wrong_count}건")
    print(f"정확도: {_safe_div(correct_count, total):.3f}")
    print("오답 원인:")
    print(f"  - 분석 오류         : {error_count}")
    print(f"  - 품목보고번호 불일치: {report_wrong_count}")
    print(f"  - 원재료 불일치      : {ingredients_wrong_count}")
    print(f"summary: {summary_path}")
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="analyze 검증셋 벤치마크")
    parser.add_argument("--dataset", type=str, help="JSONL 검증셋 파일 경로")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="validation_reports",
        help="리포트 출력 디렉토리",
    )
    parser.add_argument(
        "--delay-sec",
        type=float,
        default=0.0,
        help="샘플 간 대기(초)",
    )
    parser.add_argument(
        "--ingredients-threshold",
        type=float,
        default=0.9,
        help="원재료 텍스트 유사도 임계값(0~1)",
    )
    parser.add_argument(
        "--init-template",
        type=str,
        help="샘플 JSONL 템플릿 생성 경로",
    )
    args = parser.parse_args()

    if args.init_template:
        target = Path(args.init_template)
        _write_template(target)
        print(f"template created: {target}")
        return

    if not args.dataset:
        raise SystemExit("--dataset 또는 --init-template 중 하나는 필요합니다.")

    run_benchmark(
        dataset_path=Path(args.dataset),
        output_dir=Path(args.output_dir),
        delay_sec=args.delay_sec,
        ingredients_threshold=args.ingredients_threshold,
    )


if __name__ == "__main__":
    main()
