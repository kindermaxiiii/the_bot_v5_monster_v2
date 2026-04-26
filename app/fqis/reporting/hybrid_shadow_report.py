from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True, frozen=True)
class NumericSummary:
    field_name: str
    count: int
    mean: float | None
    minimum: float | None
    maximum: float | None


@dataclass(slots=True, frozen=True)
class HybridShadowBatchReport:
    status: str
    source_path: str
    batch_count: int
    match_count: int
    accepted_match_count: int
    rejected_match_count: int
    accepted_bet_count: int
    thesis_count: int
    hybrid_probability_count: int
    hybrid_count: int
    model_only_count: int
    acceptance_rate: float
    source_counts: dict[str, int]
    intent_counts: dict[str, int]
    numeric_summaries: dict[str, NumericSummary]

    @property
    def has_probabilities(self) -> bool:
        return self.hybrid_probability_count > 0


def load_hybrid_shadow_batch_records_from_jsonl(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        raise FileNotFoundError(f"hybrid shadow batch file not found: {path}")

    records: list[dict[str, Any]] = []

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()

        if not line:
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc

        if not isinstance(record, dict):
            raise ValueError(f"line {line_number}: record must be a JSON object")

        records.append(record)

    if not records:
        raise ValueError(f"hybrid shadow batch file is empty: {path}")

    return tuple(records)


def build_hybrid_shadow_batch_report_from_jsonl(path: Path) -> HybridShadowBatchReport:
    records = load_hybrid_shadow_batch_records_from_jsonl(path)

    return build_hybrid_shadow_batch_report_from_records(
        records,
        source_path=str(path),
    )


def build_hybrid_shadow_batch_report_from_records(
    records: tuple[dict[str, Any], ...],
    *,
    source_path: str,
) -> HybridShadowBatchReport:
    if not records:
        raise ValueError("records must not be empty")

    match_count = sum(_safe_int(record.get("match_count")) for record in records)
    accepted_match_count = sum(_safe_int(record.get("accepted_match_count")) for record in records)
    rejected_match_count = sum(_safe_int(record.get("rejected_match_count")) for record in records)
    accepted_bet_count = sum(_safe_int(record.get("accepted_bet_count")) for record in records)
    thesis_count = sum(_safe_int(record.get("thesis_count")) for record in records)
    hybrid_probability_count = sum(_safe_int(record.get("hybrid_probability_count")) for record in records)
    hybrid_count = sum(_safe_int(record.get("hybrid_count")) for record in records)
    model_only_count = sum(_safe_int(record.get("model_only_count")) for record in records)

    diagnostics = tuple(_iter_hybrid_probability_diagnostics(records))

    source_counts = _count_by_key(diagnostics, "source")
    intent_counts = _count_by_key(diagnostics, "intent_key")

    numeric_summaries = {
        "p_model": _summarize_numeric_field(diagnostics, "p_model"),
        "p_market_no_vig": _summarize_numeric_field(diagnostics, "p_market_no_vig"),
        "p_hybrid": _summarize_numeric_field(diagnostics, "p_hybrid"),
        "delta_model_market": _summarize_numeric_field(diagnostics, "delta_model_market"),
        "model_weight": _summarize_numeric_field(diagnostics, "model_weight"),
        "market_weight": _summarize_numeric_field(diagnostics, "market_weight"),
    }

    acceptance_rate = accepted_match_count / match_count if match_count else 0.0

    return HybridShadowBatchReport(
        status="ok",
        source_path=source_path,
        batch_count=len(records),
        match_count=match_count,
        accepted_match_count=accepted_match_count,
        rejected_match_count=rejected_match_count,
        accepted_bet_count=accepted_bet_count,
        thesis_count=thesis_count,
        hybrid_probability_count=hybrid_probability_count,
        hybrid_count=hybrid_count,
        model_only_count=model_only_count,
        acceptance_rate=acceptance_rate,
        source_counts=source_counts,
        intent_counts=intent_counts,
        numeric_summaries=numeric_summaries,
    )


def hybrid_shadow_batch_report_to_record(report: HybridShadowBatchReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "source": "fqis_hybrid_shadow_batch_report",
        "source_path": report.source_path,
        "batch_count": report.batch_count,
        "match_count": report.match_count,
        "accepted_match_count": report.accepted_match_count,
        "rejected_match_count": report.rejected_match_count,
        "accepted_bet_count": report.accepted_bet_count,
        "thesis_count": report.thesis_count,
        "hybrid_probability_count": report.hybrid_probability_count,
        "hybrid_count": report.hybrid_count,
        "model_only_count": report.model_only_count,
        "acceptance_rate": report.acceptance_rate,
        "source_counts": dict(report.source_counts),
        "intent_counts": dict(report.intent_counts),
        "numeric_summaries": {
            field_name: _numeric_summary_to_record(summary)
            for field_name, summary in report.numeric_summaries.items()
        },
    }


def write_hybrid_shadow_batch_report_json(
    report: HybridShadowBatchReport,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            hybrid_shadow_batch_report_to_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _iter_hybrid_probability_diagnostics(
    records: tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    diagnostics: list[dict[str, Any]] = []

    for record in records:
        for cycle in record.get("cycles", []) or []:
            if not isinstance(cycle, dict):
                continue

            for thesis_result in cycle.get("thesis_results", []) or []:
                if not isinstance(thesis_result, dict):
                    continue

                for diagnostic in thesis_result.get("hybrid_probability_diagnostics", []) or []:
                    if isinstance(diagnostic, dict):
                        diagnostics.append(diagnostic)

    return tuple(diagnostics)


def _count_by_key(records: tuple[dict[str, Any], ...], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}

    for record in records:
        value = record.get(key)

        if value in (None, ""):
            continue

        normalized = str(value)
        counts[normalized] = counts.get(normalized, 0) + 1

    return dict(sorted(counts.items()))


def _summarize_numeric_field(
    records: tuple[dict[str, Any], ...],
    field_name: str,
) -> NumericSummary:
    values = tuple(
        float(record[field_name])
        for record in records
        if record.get(field_name) is not None
    )

    if not values:
        return NumericSummary(
            field_name=field_name,
            count=0,
            mean=None,
            minimum=None,
            maximum=None,
        )

    return NumericSummary(
        field_name=field_name,
        count=len(values),
        mean=sum(values) / len(values),
        minimum=min(values),
        maximum=max(values),
    )


def _numeric_summary_to_record(summary: NumericSummary) -> dict[str, Any]:
    return {
        "field_name": summary.field_name,
        "count": summary.count,
        "mean": summary.mean,
        "min": summary.minimum,
        "max": summary.maximum,
    }


def _safe_int(value: Any) -> int:
    if value in (None, ""):
        return 0

    return int(value)

    