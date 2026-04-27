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
class MarketPerformance:
    group_key: str
    bet_count: int
    won_count: int
    lost_count: int
    push_count: int
    unsettled_count: int
    total_staked: float
    total_profit: float
    roi: float | None
    hit_rate: float | None
    average_odds: float | None
    average_p_real: float | None
    brier_score: float | None


@dataclass(slots=True, frozen=True)
class CalibrationBucket:
    bucket_label: str
    lower_bound: float
    upper_bound: float
    bet_count: int
    won_count: int
    lost_count: int
    mean_predicted_probability: float | None
    observed_win_rate: float | None
    brier_score: float | None
    total_profit: float
    roi: float | None


@dataclass(slots=True, frozen=True)
class PerformanceReport:
    status: str
    source_path: str
    report_count: int
    bet_count: int
    settled_bet_count: int
    graded_bet_count: int
    won_count: int
    lost_count: int
    push_count: int
    unsettled_count: int
    total_staked: float
    total_profit: float
    roi: float | None
    hit_rate: float | None
    brier_score: float | None
    average_odds: float | None
    average_p_real: float | None
    numeric_summaries: dict[str, NumericSummary]
    performance_by_family: dict[str, MarketPerformance]
    performance_by_market_key: dict[str, MarketPerformance]
    calibration_buckets: tuple[CalibrationBucket, ...]

    @property
    def has_graded_bets(self) -> bool:
        return self.graded_bet_count > 0


def load_settlement_report_records(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        raise FileNotFoundError(f"settlement report file not found: {path}")

    text = path.read_text(encoding="utf-8-sig").strip()

    if not text:
        raise ValueError(f"settlement report file is empty: {path}")

    if text.startswith("{"):
        record = json.loads(text)

        if not isinstance(record, dict):
            raise ValueError("settlement report JSON must be an object")

        return (record,)

    if text.startswith("["):
        records = json.loads(text)

        if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
            raise ValueError("settlement report JSON array must contain objects")

        if not records:
            raise ValueError("settlement report JSON array must not be empty")

        return tuple(records)

    records: list[dict[str, Any]] = []

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()

        if not line:
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc

        if not isinstance(record, dict):
            raise ValueError(f"line {line_number}: settlement report must be a JSON object")

        records.append(record)

    if not records:
        raise ValueError(f"settlement report file is empty: {path}")

    return tuple(records)


def build_performance_report_from_json(
    path: Path,
    *,
    bucket_size: float = 0.10,
) -> PerformanceReport:
    records = load_settlement_report_records(path)

    return build_performance_report_from_records(
        records,
        source_path=str(path),
        bucket_size=bucket_size,
    )


def build_performance_report_from_records(
    records: tuple[dict[str, Any], ...],
    *,
    source_path: str,
    bucket_size: float = 0.10,
) -> PerformanceReport:
    if not records:
        raise ValueError("records must not be empty")

    if bucket_size <= 0.0 or bucket_size > 1.0:
        raise ValueError("bucket_size must be in (0, 1]")

    bets = tuple(_iter_settled_bets(records))

    settled_bets = tuple(
        bet for bet in bets
        if bet.get("result") in {"WON", "LOST", "PUSH"}
    )
    graded_bets = tuple(
        bet for bet in bets
        if bet.get("result") in {"WON", "LOST"}
    )

    won_count = sum(1 for bet in bets if bet.get("result") == "WON")
    lost_count = sum(1 for bet in bets if bet.get("result") == "LOST")
    push_count = sum(1 for bet in bets if bet.get("result") == "PUSH")
    unsettled_count = sum(1 for bet in bets if bet.get("result") == "UNSETTLED")

    total_staked = sum(_safe_float(bet.get("stake")) for bet in settled_bets)
    total_profit = sum(_safe_float(bet.get("profit")) for bet in bets)

    roi = total_profit / total_staked if total_staked > 0.0 else None
    hit_rate = won_count / (won_count + lost_count) if (won_count + lost_count) > 0 else None
    brier_score = _brier_score(graded_bets)

    numeric_summaries = {
        "odds_decimal": _summarize_numeric_field(bets, "odds_decimal"),
        "p_real": _summarize_numeric_field(bets, "p_real"),
        "profit": _summarize_numeric_field(bets, "profit"),
    }

    return PerformanceReport(
        status="ok",
        source_path=source_path,
        report_count=len(records),
        bet_count=len(bets),
        settled_bet_count=len(settled_bets),
        graded_bet_count=len(graded_bets),
        won_count=won_count,
        lost_count=lost_count,
        push_count=push_count,
        unsettled_count=unsettled_count,
        total_staked=total_staked,
        total_profit=total_profit,
        roi=roi,
        hit_rate=hit_rate,
        brier_score=brier_score,
        average_odds=numeric_summaries["odds_decimal"].mean,
        average_p_real=numeric_summaries["p_real"].mean,
        numeric_summaries=numeric_summaries,
        performance_by_family=_build_group_performance(bets, group_field="family"),
        performance_by_market_key=_build_group_performance(bets, group_field="market_key"),
        calibration_buckets=_build_calibration_buckets(graded_bets, bucket_size=bucket_size),
    )


def performance_report_to_record(report: PerformanceReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "source": "fqis_performance_report",
        "source_path": report.source_path,
        "report_count": report.report_count,
        "bet_count": report.bet_count,
        "settled_bet_count": report.settled_bet_count,
        "graded_bet_count": report.graded_bet_count,
        "won_count": report.won_count,
        "lost_count": report.lost_count,
        "push_count": report.push_count,
        "unsettled_count": report.unsettled_count,
        "total_staked": report.total_staked,
        "total_profit": report.total_profit,
        "roi": report.roi,
        "hit_rate": report.hit_rate,
        "brier_score": report.brier_score,
        "average_odds": report.average_odds,
        "average_p_real": report.average_p_real,
        "numeric_summaries": {
            field_name: _numeric_summary_to_record(summary)
            for field_name, summary in report.numeric_summaries.items()
        },
        "performance_by_family": {
            key: _market_performance_to_record(value)
            for key, value in report.performance_by_family.items()
        },
        "performance_by_market_key": {
            key: _market_performance_to_record(value)
            for key, value in report.performance_by_market_key.items()
        },
        "calibration_buckets": [
            _calibration_bucket_to_record(bucket)
            for bucket in report.calibration_buckets
        ],
    }


def write_performance_report_json(report: PerformanceReport, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            performance_report_to_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _iter_settled_bets(records: tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    bets: list[dict[str, Any]] = []

    for record in records:
        for bet in record.get("settled_bets", []) or []:
            if isinstance(bet, dict):
                bets.append(bet)

    return tuple(bets)


def _build_group_performance(
    bets: tuple[dict[str, Any], ...],
    *,
    group_field: str,
) -> dict[str, MarketPerformance]:
    grouped: dict[str, list[dict[str, Any]]] = {}

    for bet in bets:
        key = str(bet.get(group_field, "UNKNOWN"))
        grouped.setdefault(key, []).append(bet)

    return {
        key: _summarize_market_group(key, tuple(group_bets))
        for key, group_bets in sorted(grouped.items())
    }


def _summarize_market_group(
    group_key: str,
    bets: tuple[dict[str, Any], ...],
) -> MarketPerformance:
    settled_bets = tuple(
        bet for bet in bets
        if bet.get("result") in {"WON", "LOST", "PUSH"}
    )
    graded_bets = tuple(
        bet for bet in bets
        if bet.get("result") in {"WON", "LOST"}
    )

    won_count = sum(1 for bet in bets if bet.get("result") == "WON")
    lost_count = sum(1 for bet in bets if bet.get("result") == "LOST")
    push_count = sum(1 for bet in bets if bet.get("result") == "PUSH")
    unsettled_count = sum(1 for bet in bets if bet.get("result") == "UNSETTLED")

    total_staked = sum(_safe_float(bet.get("stake")) for bet in settled_bets)
    total_profit = sum(_safe_float(bet.get("profit")) for bet in bets)

    roi = total_profit / total_staked if total_staked > 0.0 else None
    hit_rate = won_count / (won_count + lost_count) if (won_count + lost_count) > 0 else None

    return MarketPerformance(
        group_key=group_key,
        bet_count=len(bets),
        won_count=won_count,
        lost_count=lost_count,
        push_count=push_count,
        unsettled_count=unsettled_count,
        total_staked=total_staked,
        total_profit=total_profit,
        roi=roi,
        hit_rate=hit_rate,
        average_odds=_mean(_numeric_values(bets, "odds_decimal")),
        average_p_real=_mean(_numeric_values(bets, "p_real")),
        brier_score=_brier_score(graded_bets),
    )


def _build_calibration_buckets(
    graded_bets: tuple[dict[str, Any], ...],
    *,
    bucket_size: float,
) -> tuple[CalibrationBucket, ...]:
    buckets: dict[tuple[float, float], list[dict[str, Any]]] = {}

    for bet in graded_bets:
        p_real = bet.get("p_real")

        if p_real is None:
            continue

        probability = max(0.0, min(1.0, float(p_real)))

        lower = int(probability / bucket_size) * bucket_size

        if probability == 1.0:
            lower = 1.0 - bucket_size

        upper = min(1.0, lower + bucket_size)

        lower = round(lower, 10)
        upper = round(upper, 10)

        buckets.setdefault((lower, upper), []).append(bet)

    results: list[CalibrationBucket] = []

    for (lower, upper), bucket_bets in sorted(buckets.items()):
        bucket_tuple = tuple(bucket_bets)
        won_count = sum(1 for bet in bucket_tuple if bet.get("result") == "WON")
        lost_count = sum(1 for bet in bucket_tuple if bet.get("result") == "LOST")
        total_staked = sum(_safe_float(bet.get("stake")) for bet in bucket_tuple)
        total_profit = sum(_safe_float(bet.get("profit")) for bet in bucket_tuple)

        results.append(
            CalibrationBucket(
                bucket_label=f"{lower:.2f}-{upper:.2f}",
                lower_bound=lower,
                upper_bound=upper,
                bet_count=len(bucket_tuple),
                won_count=won_count,
                lost_count=lost_count,
                mean_predicted_probability=_mean(_numeric_values(bucket_tuple, "p_real")),
                observed_win_rate=won_count / len(bucket_tuple) if bucket_tuple else None,
                brier_score=_brier_score(bucket_tuple),
                total_profit=total_profit,
                roi=total_profit / total_staked if total_staked > 0.0 else None,
            )
        )

    return tuple(results)


def _brier_score(graded_bets: tuple[dict[str, Any], ...]) -> float | None:
    errors: list[float] = []

    for bet in graded_bets:
        if bet.get("p_real") is None:
            continue

        outcome = 1.0 if bet.get("result") == "WON" else 0.0
        prediction = float(bet["p_real"])

        errors.append((prediction - outcome) ** 2)

    return sum(errors) / len(errors) if errors else None


def _summarize_numeric_field(
    records: tuple[dict[str, Any], ...],
    field_name: str,
) -> NumericSummary:
    values = _numeric_values(records, field_name)

    return NumericSummary(
        field_name=field_name,
        count=len(values),
        mean=_mean(values),
        minimum=min(values) if values else None,
        maximum=max(values) if values else None,
    )


def _numeric_values(records: tuple[dict[str, Any], ...], field_name: str) -> tuple[float, ...]:
    return tuple(
        float(record[field_name])
        for record in records
        if record.get(field_name) is not None
    )


def _mean(values: tuple[float, ...]) -> float | None:
    if not values:
        return None

    return sum(values) / len(values)


def _safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0

    return float(value)


def _numeric_summary_to_record(summary: NumericSummary) -> dict[str, Any]:
    return {
        "field_name": summary.field_name,
        "count": summary.count,
        "mean": summary.mean,
        "min": summary.minimum,
        "max": summary.maximum,
    }


def _market_performance_to_record(performance: MarketPerformance) -> dict[str, Any]:
    return {
        "group_key": performance.group_key,
        "bet_count": performance.bet_count,
        "won_count": performance.won_count,
        "lost_count": performance.lost_count,
        "push_count": performance.push_count,
        "unsettled_count": performance.unsettled_count,
        "total_staked": performance.total_staked,
        "total_profit": performance.total_profit,
        "roi": performance.roi,
        "hit_rate": performance.hit_rate,
        "average_odds": performance.average_odds,
        "average_p_real": performance.average_p_real,
        "brier_score": performance.brier_score,
    }


def _calibration_bucket_to_record(bucket: CalibrationBucket) -> dict[str, Any]:
    return {
        "bucket_label": bucket.bucket_label,
        "lower_bound": bucket.lower_bound,
        "upper_bound": bucket.upper_bound,
        "bet_count": bucket.bet_count,
        "won_count": bucket.won_count,
        "lost_count": bucket.lost_count,
        "mean_predicted_probability": bucket.mean_predicted_probability,
        "observed_win_rate": bucket.observed_win_rate,
        "brier_score": bucket.brier_score,
        "total_profit": bucket.total_profit,
        "roi": bucket.roi,
    }

    