from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.fqis.performance.metrics import load_settlement_report_records


@dataclass(slots=True, frozen=True)
class ClosingOdds:
    event_id: int
    market_key: str
    closing_odds_decimal: float
    bookmaker_name: str | None = None
    source_timestamp_utc: str | None = None


@dataclass(slots=True, frozen=True)
class ClvBet:
    event_id: int
    market_key: str
    family: str
    bookmaker_name: str
    offered_odds_decimal: float
    closing_odds_decimal: float | None
    offered_implied_probability: float
    closing_implied_probability: float | None
    clv_odds_delta: float | None
    clv_percent: float | None
    clv_implied_probability_delta: float | None
    beat_closing_line: bool | None
    result: str | None
    profit: float | None
    p_real: float | None
    clv_status: str


@dataclass(slots=True, frozen=True)
class ClvGroupSummary:
    group_key: str
    bet_count: int
    priced_count: int
    missing_count: int
    beat_count: int
    beat_rate: float | None
    average_clv_odds_delta: float | None
    average_clv_percent: float | None
    average_clv_implied_probability_delta: float | None


@dataclass(slots=True, frozen=True)
class ClvReport:
    status: str
    settlement_path: str
    closing_path: str
    bet_count: int
    priced_count: int
    missing_count: int
    beat_count: int
    not_beat_count: int
    beat_rate: float | None
    average_clv_odds_delta: float | None
    average_clv_percent: float | None
    average_clv_implied_probability_delta: float | None
    clv_bets: tuple[ClvBet, ...]
    clv_by_family: dict[str, ClvGroupSummary]
    clv_by_market_key: dict[str, ClvGroupSummary]

    @property
    def has_priced_bets(self) -> bool:
        return self.priced_count > 0


def load_closing_odds_from_jsonl(path: Path) -> dict[tuple[int, str], ClosingOdds]:
    if not path.exists():
        raise FileNotFoundError(f"closing odds file not found: {path}")

    closing_odds: dict[tuple[int, str], ClosingOdds] = {}

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()

        if not line:
            continue

        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc

        if not isinstance(row, dict):
            raise ValueError(f"line {line_number}: closing odds row must be a JSON object")

        closing = _row_to_closing_odds(row, line_number=line_number)
        key = (closing.event_id, closing.market_key)

        if key in closing_odds:
            raise ValueError(f"line {line_number}: duplicate closing odds key {key}")

        closing_odds[key] = closing

    if not closing_odds:
        raise ValueError(f"closing odds file is empty: {path}")

    return closing_odds


def build_clv_report_from_json(
    *,
    settlement_path: Path,
    closing_path: Path,
) -> ClvReport:
    settlement_records = load_settlement_report_records(settlement_path)
    closing_odds = load_closing_odds_from_jsonl(closing_path)

    return build_clv_report_from_records(
        settlement_records,
        closing_odds,
        settlement_path=str(settlement_path),
        closing_path=str(closing_path),
    )


def build_clv_report_from_records(
    settlement_records: tuple[dict[str, Any], ...],
    closing_odds: dict[tuple[int, str], ClosingOdds],
    *,
    settlement_path: str,
    closing_path: str,
) -> ClvReport:
    if not settlement_records:
        raise ValueError("settlement_records must not be empty")

    settled_bets = tuple(_iter_settled_bets(settlement_records))

    clv_bets = tuple(
        build_clv_bet(
            bet,
            closing_odds.get((int(bet["event_id"]), str(bet["market_key"]))),
        )
        for bet in settled_bets
    )

    priced_bets = tuple(bet for bet in clv_bets if bet.closing_odds_decimal is not None)
    missing_count = len(clv_bets) - len(priced_bets)
    beat_count = sum(1 for bet in priced_bets if bet.beat_closing_line is True)
    not_beat_count = sum(1 for bet in priced_bets if bet.beat_closing_line is False)

    return ClvReport(
        status="ok",
        settlement_path=settlement_path,
        closing_path=closing_path,
        bet_count=len(clv_bets),
        priced_count=len(priced_bets),
        missing_count=missing_count,
        beat_count=beat_count,
        not_beat_count=not_beat_count,
        beat_rate=beat_count / len(priced_bets) if priced_bets else None,
        average_clv_odds_delta=_mean(_present_float_values(priced_bets, "clv_odds_delta")),
        average_clv_percent=_mean(_present_float_values(priced_bets, "clv_percent")),
        average_clv_implied_probability_delta=_mean(
            _present_float_values(priced_bets, "clv_implied_probability_delta")
        ),
        clv_bets=clv_bets,
        clv_by_family=_build_group_summary(clv_bets, group_field="family"),
        clv_by_market_key=_build_group_summary(clv_bets, group_field="market_key"),
    )


def build_clv_bet(
    bet_record: dict[str, Any],
    closing_odds: ClosingOdds | None,
) -> ClvBet:
    offered_odds = float(bet_record["odds_decimal"])
    offered_implied = implied_probability_from_decimal_odds(offered_odds)

    if closing_odds is None:
        return ClvBet(
            event_id=int(bet_record["event_id"]),
            market_key=str(bet_record["market_key"]),
            family=str(bet_record.get("family", "UNKNOWN")),
            bookmaker_name=str(bet_record.get("bookmaker_name", "UNKNOWN")),
            offered_odds_decimal=offered_odds,
            closing_odds_decimal=None,
            offered_implied_probability=offered_implied,
            closing_implied_probability=None,
            clv_odds_delta=None,
            clv_percent=None,
            clv_implied_probability_delta=None,
            beat_closing_line=None,
            result=bet_record.get("result"),
            profit=_optional_float(bet_record.get("profit")),
            p_real=_optional_float(bet_record.get("p_real")),
            clv_status="missing_closing_odds",
        )

    closing_implied = implied_probability_from_decimal_odds(closing_odds.closing_odds_decimal)
    clv_odds_delta = offered_odds - closing_odds.closing_odds_decimal
    clv_percent = (offered_odds / closing_odds.closing_odds_decimal) - 1.0
    clv_implied_probability_delta = closing_implied - offered_implied

    return ClvBet(
        event_id=int(bet_record["event_id"]),
        market_key=str(bet_record["market_key"]),
        family=str(bet_record.get("family", "UNKNOWN")),
        bookmaker_name=str(bet_record.get("bookmaker_name", "UNKNOWN")),
        offered_odds_decimal=offered_odds,
        closing_odds_decimal=closing_odds.closing_odds_decimal,
        offered_implied_probability=offered_implied,
        closing_implied_probability=closing_implied,
        clv_odds_delta=clv_odds_delta,
        clv_percent=clv_percent,
        clv_implied_probability_delta=clv_implied_probability_delta,
        beat_closing_line=clv_odds_delta > 0.0,
        result=bet_record.get("result"),
        profit=_optional_float(bet_record.get("profit")),
        p_real=_optional_float(bet_record.get("p_real")),
        clv_status="priced",
    )


def clv_report_to_record(report: ClvReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "source": "fqis_clv_report",
        "settlement_path": report.settlement_path,
        "closing_path": report.closing_path,
        "bet_count": report.bet_count,
        "priced_count": report.priced_count,
        "missing_count": report.missing_count,
        "beat_count": report.beat_count,
        "not_beat_count": report.not_beat_count,
        "beat_rate": report.beat_rate,
        "average_clv_odds_delta": report.average_clv_odds_delta,
        "average_clv_percent": report.average_clv_percent,
        "average_clv_implied_probability_delta": report.average_clv_implied_probability_delta,
        "clv_by_family": {
            key: _group_summary_to_record(value)
            for key, value in report.clv_by_family.items()
        },
        "clv_by_market_key": {
            key: _group_summary_to_record(value)
            for key, value in report.clv_by_market_key.items()
        },
        "clv_bets": [
            _clv_bet_to_record(bet)
            for bet in report.clv_bets
        ],
    }


def write_clv_report_json(report: ClvReport, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            clv_report_to_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def implied_probability_from_decimal_odds(odds_decimal: float) -> float:
    if odds_decimal <= 1.0:
        raise ValueError("decimal odds must be > 1.0")

    return 1.0 / odds_decimal


def _row_to_closing_odds(row: dict[str, Any], *, line_number: int) -> ClosingOdds:
    event_id = _required_int(row, "event_id", line_number=line_number)
    market_key = str(row.get("market_key") or _market_key_from_fields(row, line_number=line_number))
    closing_odds_decimal = _required_float(row, "closing_odds_decimal", line_number=line_number)

    return ClosingOdds(
        event_id=event_id,
        market_key=market_key,
        closing_odds_decimal=closing_odds_decimal,
        bookmaker_name=_optional_str(row.get("bookmaker_name")),
        source_timestamp_utc=_optional_str(row.get("source_timestamp_utc")),
    )


def _market_key_from_fields(row: dict[str, Any], *, line_number: int) -> str:
    family = _required_str(row, "family", line_number=line_number)
    side = _required_str(row, "side", line_number=line_number)
    team_role = str(row.get("team_role", "NONE"))
    line = row.get("line")
    line_label = "NA" if line in (None, "") else str(float(line))

    return f"{family}|{side}|{team_role}|{line_label}"


def _iter_settled_bets(records: tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    bets: list[dict[str, Any]] = []

    for record in records:
        for bet in record.get("settled_bets", []) or []:
            if isinstance(bet, dict):
                bets.append(bet)

    return tuple(bets)


def _build_group_summary(
    clv_bets: tuple[ClvBet, ...],
    *,
    group_field: str,
) -> dict[str, ClvGroupSummary]:
    grouped: dict[str, list[ClvBet]] = {}

    for bet in clv_bets:
        group_key = str(getattr(bet, group_field))
        grouped.setdefault(group_key, []).append(bet)

    return {
        key: _summarize_group(key, tuple(group_bets))
        for key, group_bets in sorted(grouped.items())
    }


def _summarize_group(group_key: str, bets: tuple[ClvBet, ...]) -> ClvGroupSummary:
    priced_bets = tuple(bet for bet in bets if bet.closing_odds_decimal is not None)
    missing_count = len(bets) - len(priced_bets)
    beat_count = sum(1 for bet in priced_bets if bet.beat_closing_line is True)

    return ClvGroupSummary(
        group_key=group_key,
        bet_count=len(bets),
        priced_count=len(priced_bets),
        missing_count=missing_count,
        beat_count=beat_count,
        beat_rate=beat_count / len(priced_bets) if priced_bets else None,
        average_clv_odds_delta=_mean(_present_float_values(priced_bets, "clv_odds_delta")),
        average_clv_percent=_mean(_present_float_values(priced_bets, "clv_percent")),
        average_clv_implied_probability_delta=_mean(
            _present_float_values(priced_bets, "clv_implied_probability_delta")
        ),
    )


def _clv_bet_to_record(bet: ClvBet) -> dict[str, Any]:
    return {
        "event_id": bet.event_id,
        "market_key": bet.market_key,
        "family": bet.family,
        "bookmaker_name": bet.bookmaker_name,
        "offered_odds_decimal": bet.offered_odds_decimal,
        "closing_odds_decimal": bet.closing_odds_decimal,
        "offered_implied_probability": bet.offered_implied_probability,
        "closing_implied_probability": bet.closing_implied_probability,
        "clv_odds_delta": bet.clv_odds_delta,
        "clv_percent": bet.clv_percent,
        "clv_implied_probability_delta": bet.clv_implied_probability_delta,
        "beat_closing_line": bet.beat_closing_line,
        "result": bet.result,
        "profit": bet.profit,
        "p_real": bet.p_real,
        "clv_status": bet.clv_status,
    }


def _group_summary_to_record(summary: ClvGroupSummary) -> dict[str, Any]:
    return {
        "group_key": summary.group_key,
        "bet_count": summary.bet_count,
        "priced_count": summary.priced_count,
        "missing_count": summary.missing_count,
        "beat_count": summary.beat_count,
        "beat_rate": summary.beat_rate,
        "average_clv_odds_delta": summary.average_clv_odds_delta,
        "average_clv_percent": summary.average_clv_percent,
        "average_clv_implied_probability_delta": summary.average_clv_implied_probability_delta,
    }


def _present_float_values(records: tuple[Any, ...], field_name: str) -> tuple[float, ...]:
    values: list[float] = []

    for record in records:
        value = getattr(record, field_name)

        if value is not None:
            values.append(float(value))

    return tuple(values)


def _mean(values: tuple[float, ...]) -> float | None:
    if not values:
        return None

    return sum(values) / len(values)


def _required_int(row: dict[str, Any], key: str, *, line_number: int) -> int:
    if row.get(key) in (None, ""):
        raise ValueError(f"line {line_number}: missing required field {key}")

    return int(row[key])


def _required_float(row: dict[str, Any], key: str, *, line_number: int) -> float:
    if row.get(key) in (None, ""):
        raise ValueError(f"line {line_number}: missing required field {key}")

    return float(row[key])


def _required_str(row: dict[str, Any], key: str, *, line_number: int) -> str:
    if row.get(key) in (None, ""):
        raise ValueError(f"line {line_number}: missing required field {key}")

    return str(row[key])


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None

    return float(value)


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None

    return str(value)

    