"""
analyze 성능 검증셋 실행 도구.

사용 예:
  python3 -m app.validation_benchmark --dataset validation/samples.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "t", "yes", "y"):
        return True
    if text in ("0", "false", "f", "no", "n"):
        return False
    return None


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
    return value


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
    exp_strict: bool,
    pred_strict: bool,
    exp_report: Any,
    pred_report: Any,
    exp_ing_text: Any,
    pred_ing_text: Any,
    ing_similarity: float | None,
    ingredients_threshold: float,
) -> tuple[str, str]:
    if err:
        return ("판독실패", f"분석 예외: {str(err)[:40]}")
    if exp_strict and pred_strict:
        return ("정답", "번호 일치 + 원재료 유사도 기준 통과")
    if exp_strict and not pred_strict:
        report_missing = bool(exp_report and not pred_report)
        report_mismatch = bool(exp_report and pred_report and str(exp_report) != str(pred_report))
        ing_missing = bool(exp_ing_text and not _has_text(pred_ing_text))
        ing_low_sim = bool(ing_similarity is not None and ing_similarity < ingredients_threshold)
        if report_missing:
            return ("놓침", "정답 번호가 있는데 AI 번호 미검출")
        if report_mismatch:
            return ("놓침", "품목보고번호 불일치")
        if ing_missing:
            return ("놓침", "정답 원재료가 있는데 AI 원재료 미검출")
        if ing_low_sim:
            return ("놓침", f"원재료 유사도 부족({ing_similarity:.3f} < {ingredients_threshold:.3f})")
        return ("놓침", "strict 조건 불충족")
    if (not exp_strict) and pred_strict:
        return ("오탐", "정답은 비통과인데 AI가 통과 판정")
    return ("정답", "정답/예측 모두 비통과")


@dataclass
class FieldScore:
    name: str
    total: int = 0
    correct: int = 0

    def add(self, expected: Any, predicted: Any) -> None:
        if expected is None:
            return
        self.total += 1
        if expected == predicted:
            self.correct += 1

    @property
    def accuracy(self) -> float:
        return _safe_div(self.correct, self.total)


def _derive_strict_accept(record: dict[str, Any]) -> bool:
    report_no = record.get("itemMnftrRptNo")
    ingredients = record.get("ingredients_text")
    return bool(
        _has_text(report_no)
        and _has_text(ingredients)
        and record.get("is_flat") is True
        and record.get("has_rect_ingredient_box") is True
        and record.get("has_report_label") is True
        and record.get("is_designed_graphic") is True
        and record.get("has_real_world_objects") is False
    )


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
    analyzer = URLIngredientAnalyzer(api_key=api_key, strict_mode=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / f"benchmark_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    field_scores = {
        "itemMnftrRptNo": FieldScore("itemMnftrRptNo"),
        "ingredients_present": FieldScore("ingredients_present"),
        "is_flat": FieldScore("is_flat"),
        "has_rect_ingredient_box": FieldScore("has_rect_ingredient_box"),
        "has_report_label": FieldScore("has_report_label"),
        "is_designed_graphic": FieldScore("is_designed_graphic"),
        "has_real_world_objects": FieldScore("has_real_world_objects"),
        "strict_accept": FieldScore("strict_accept"),
    }

    tp = fp = tn = fn = 0
    miss_count = 0
    false_accept_count = 0
    unreadable_count = 0
    pair_evaluable = 0
    pair_success = 0
    accepted_count = 0
    accepted_evaluable = 0
    accepted_success = 0
    details: list[dict[str, Any]] = []
    t0 = time.time()

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
                "is_flat": None,
                "has_rect_ingredient_box": None,
                "has_report_label": None,
                "is_designed_graphic": None,
                "has_real_world_objects": None,
                "note": f"error:{type(exc).__name__}",
            }
            err = str(exc)

        pred_ingredients_present = _has_text(pred.get("ingredients_text"))
        pred_strict_accept = _derive_strict_accept(pred)

        exp_ingredients_text = expected.get("ingredients_text")
        exp_ingredients_present = _to_bool(expected.get("ingredients_present"))
        exp_is_flat = _to_bool(expected.get("is_flat"))
        exp_rect = _to_bool(expected.get("has_rect_ingredient_box"))
        exp_label = _to_bool(expected.get("has_report_label"))
        exp_graphic = _to_bool(expected.get("is_designed_graphic"))
        exp_real_obj = _to_bool(expected.get("has_real_world_objects"))
        exp_strict = _to_bool(expected.get("strict_accept"))

        if exp_strict is None:
            # 최소 라벨셋(번호+원재료)만 있어도 strict 정답을 유도한다.
            exp_strict = bool(_has_text(exp_report) and _has_text(exp_ingredients_text))

        pred_report = pred.get("itemMnftrRptNo")
        exp_report_norm = _normalize_report_no(exp_report)
        pred_report_norm = _normalize_report_no(pred_report)
        pred_ing_text = pred.get("ingredients_text")
        ing_similarity = (
            _ingredients_similarity(exp_ingredients_text, pred_ing_text)
            if exp_ingredients_text is not None
            else None
        )
        pair_ok = False
        if exp_report is not None and exp_ingredients_text is not None:
            pair_evaluable += 1
            report_ok = (exp_report_norm is not None and pred_report_norm == exp_report_norm)
            ing_ok = (ing_similarity is not None and ing_similarity >= ingredients_threshold)
            pair_ok = bool(report_ok and ing_ok)
            if pair_ok:
                pair_success += 1

        field_scores["itemMnftrRptNo"].add(exp_report, pred_report)
        field_scores["ingredients_present"].add(exp_ingredients_present, pred_ingredients_present)
        field_scores["is_flat"].add(exp_is_flat, pred.get("is_flat"))
        field_scores["has_rect_ingredient_box"].add(exp_rect, pred.get("has_rect_ingredient_box"))
        field_scores["has_report_label"].add(exp_label, pred.get("has_report_label"))
        field_scores["is_designed_graphic"].add(exp_graphic, pred.get("is_designed_graphic"))
        field_scores["has_real_world_objects"].add(exp_real_obj, pred.get("has_real_world_objects"))
        field_scores["strict_accept"].add(exp_strict, pred_strict_accept)

        if exp_strict:
            if pred_strict_accept:
                tp += 1
            else:
                fn += 1
                miss_count += 1
        else:
            if pred_strict_accept:
                fp += 1
                false_accept_count += 1
            else:
                tn += 1

        if err or (not _has_text(pred_report) and not _has_text(pred_ing_text)):
            unreadable_count += 1

        if pred_strict_accept:
            accepted_count += 1
            if exp_report is not None and exp_ingredients_text is not None:
                accepted_evaluable += 1
                if pair_ok:
                    accepted_success += 1

        verdict, reason = _build_reason(
            err=err,
            exp_strict=bool(exp_strict),
            pred_strict=bool(pred_strict_accept),
            exp_report=exp_report,
            pred_report=pred_report,
            exp_ing_text=exp_ingredients_text,
            pred_ing_text=pred_ing_text,
            ing_similarity=ing_similarity,
            ingredients_threshold=ingredients_threshold,
        )

        details.append(
            {
                "id": sample_id,
                "image": image,
                "verdict": verdict,
                "reason": reason,
                "error": err,
                "expected_itemMnftrRptNo": exp_report,
                "pred_itemMnftrRptNo": pred_report,
                "expected_ingredients_text": exp_ingredients_text,
                "pred_ingredients_text": pred_ing_text,
                "ingredients_similarity": ing_similarity,
                "pair_match": pair_ok,
                "expected_ingredients_present": exp_ingredients_present,
                "pred_ingredients_present": pred_ingredients_present,
                "expected_is_flat": exp_is_flat,
                "pred_is_flat": pred.get("is_flat"),
                "expected_has_rect_ingredient_box": exp_rect,
                "pred_has_rect_ingredient_box": pred.get("has_rect_ingredient_box"),
                "expected_has_report_label": exp_label,
                "pred_has_report_label": pred.get("has_report_label"),
                "expected_is_designed_graphic": exp_graphic,
                "pred_is_designed_graphic": pred.get("is_designed_graphic"),
                "expected_has_real_world_objects": exp_real_obj,
                "pred_has_real_world_objects": pred.get("has_real_world_objects"),
                "expected_strict_accept": exp_strict,
                "pred_strict_accept": pred_strict_accept,
                "pred_note": pred.get("note"),
                "pred_ingredients_preview": (pred_ing_text or "")[:200],
            }
        )

        elapsed = time.time() - t0
        simple_ok = bool(pair_ok) if (exp_report is not None and exp_ingredients_text is not None) else (verdict == "정답")
        print(f"\n[{idx:03d}/{len(dataset):03d}] id={sample_id} 결과={'정답' if simple_ok else '오답'} elapsed={elapsed:.1f}s")
        print(f"  정답 | 품목보고번호: {exp_report or '-'}")
        print(f"  정답 | 원재료명   : {exp_ingredients_text or '-'}")
        print(f"  예측 | 품목보고번호: {pred_report or '-'}")
        print(f"  예측 | 원재료명   : {pred_ing_text or '-'}")
        print(f"  비교 | 번호(정규화): {exp_report_norm or '-'} vs {pred_report_norm or '-'}")
        print(f"  근거 | {reason}")
        if delay_sec > 0:
            time.sleep(delay_sec)

    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2 * precision * recall, precision + recall) if (precision + recall) else 0.0

    summary = {
        "dataset_path": str(dataset_path),
        "samples": len(dataset),
        "핵심지표": {
            "정답_성공률": _safe_div(pair_success, pair_evaluable),
            "정답_성공건수": pair_success,
            "정답_평가가능건수": pair_evaluable,
            "안전_정확도": _safe_div(accepted_success, accepted_evaluable),
            "안전_정확도_성공건수": accepted_success,
            "안전_정확도_평가가능통과건수": accepted_evaluable,
            "통과율": _safe_div(accepted_count, len(dataset)),
            "통과건수": accepted_count,
            "전체건수": len(dataset),
            "원재료_유사도_임계값": ingredients_threshold,
        },
        "실패분류": {
            "놓침": miss_count,
            "오탐": false_accept_count,
            "판독실패": unreadable_count,
        },
        "strict_confusion(참고)": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
        "strict_metrics(참고)": {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "accuracy": _safe_div(tp + tn, tp + tn + fp + fn),
        },
        "field_accuracy": {
            k: {
                "total": v.total,
                "correct": v.correct,
                "accuracy": v.accuracy,
            }
            for k, v in field_scores.items()
        },
        "finished_at": datetime.now().isoformat(),
    }

    summary_path = run_dir / "summary.json"
    with summary_path.open("w", encoding="utf-8") as fp:
        json.dump(summary, fp, ensure_ascii=False, indent=2)

    print("\n=== benchmark done ===")
    print(f"전체: {len(dataset)}건")
    if pair_evaluable > 0:
        print(f"정답 판정: {pair_success}/{pair_evaluable}건")
    else:
        print("정답 판정: 평가 가능한 라벨(번호+원재료) 없음")
    print(f"오답 사유 집계: 놓침={miss_count}, 오탐={false_accept_count}, 판독실패={unreadable_count}")
    print(f"summary: {summary_path}")
    print("details: 파일 생성 안 함 (터미널 표로만 출력)")
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
