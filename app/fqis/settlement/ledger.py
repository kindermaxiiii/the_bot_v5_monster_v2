from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from app.fqis.reporting.hybrid_shadow_report import load_hybrid_shadow_batch_records_from_jsonl


SettlementResult = Literal["WON", "LOST", "PUSH", "UNSETTLED"]


@dataclass(slots=True, frozen=True)
class MatchResult:
    event_id: int
    home_goals: int
    away_goals: int

    @property
    def total_goals(self) -> int:
        return self.home_goals + self.away_goals

    @property
    def btts_yes(self) -> bool:
        return self.home_goals > 0 and self.away_goals > 0


@dataclass(slots=True, frozen=True)
class SettledBet:
    event_id: int
    market_key: str
    family: str
    side: str
    team_role: str
    line: float | None
    bookmaker_name: str
    odds_decimal: float
    stake: float
    p_real: float | None
    result: SettlementResult
    profit: float
    home_goals: int | None
    away_goals: int | None
    settlement_reason: str


@dataclass(slots=True, frozen=True)
class SettlementReport:
    status: str
    batch_path: str
    results_path: str
    accepted_bet_count: int
    settled_bet_count: int
    unsettled_bet_count: int
    won_count: int
    lost_count: int
    push_count: int
    total_staked: float
    total_profit: float
    roi: float | None
    settled_bets: tuple[SettledBet, ...]

    @property
    def has_settled_bets(self) -> bool:
        return self.settled_bet_count > 0


def load_match_results_from_jsonl(path: Path) -> dict[int, MatchResult]:
    if not path.exists():
        raise FileNotFoundError(f"match results file not found: {path}")

    results: dict[int, MatchResult] = {}

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()

        if not line:
            continue

        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc

        if not isinstance(row, dict):
            raise ValueError(f"line {line_number}: match result row must be a JSON object")

        result = _row_to_match_result(row, line_number=line_number)

        if result.event_id in results:
            raise ValueError(f"line {line_number}: duplicate event_id {result.event_id}")

        results[result.event_id] = result

    if not results:
        raise ValueError(f"match results file is empty: {path}")

    return results


def settle_hybrid_shadow_batch_from_jsonl(
    *,
    batch_path: Path,
    results_path: Path,
    stake: float = 1.0,
) -> SettlementReport:
    batch_records = load_hybrid_shadow_batch_records_from_jsonl(batch_path)
    match_results = load_match_results_from_jsonl(results_path)

    return settle_hybrid_shadow_batch_records(
        batch_records,
        match_results,
        batch_path=str(batch_path),
        results_path=str(results_path),
        stake=stake,
    )


def settle_hybrid_shadow_batch_records(
    batch_records: tuple[dict[str, Any], ...],
    match_results: dict[int, MatchResult],
    *,
    batch_path: str,
    results_path: str,
    stake: float = 1.0,
) -> SettlementReport:
    if stake <= 0.0:
        raise ValueError("stake must be > 0")

    accepted_bets = tuple(_iter_accepted_bet_records(batch_records))
    settled_bets = tuple(
        settle_bet_record(
            bet_record,
            match_results.get(int(bet_record["event_id"])),
            stake=stake,
        )
        for bet_record in accepted_bets
    )

    settled_bet_count = sum(1 for bet in settled_bets if bet.result != "UNSETTLED")
    unsettled_bet_count = sum(1 for bet in settled_bets if bet.result == "UNSETTLED")
    won_count = sum(1 for bet in settled_bets if bet.result == "WON")
    lost_count = sum(1 for bet in settled_bets if bet.result == "LOST")
    push_count = sum(1 for bet in settled_bets if bet.result == "PUSH")

    total_profit = sum(bet.profit for bet in settled_bets)
    total_staked = sum(bet.stake for bet in settled_bets if bet.result != "UNSETTLED")
    roi = total_profit / total_staked if total_staked > 0.0 else None

    return SettlementReport(
        status="ok",
        batch_path=batch_path,
        results_path=results_path,
        accepted_bet_count=len(accepted_bets),
        settled_bet_count=settled_bet_count,
        unsettled_bet_count=unsettled_bet_count,
        won_count=won_count,
        lost_count=lost_count,
        push_count=push_count,
        total_staked=total_staked,
        total_profit=total_profit,
        roi=roi,
        settled_bets=settled_bets,
    )


def settle_bet_record(
    bet_record: dict[str, Any],
    match_result: MatchResult | None,
    *,
    stake: float = 1.0,
) -> SettledBet:
    event_id = int(bet_record["event_id"])
    family = str(bet_record["family"])
    side = str(bet_record["side"])
    team_role = str(bet_record.get("team_role", "NONE"))
    line = _optional_float(bet_record.get("line"))
    odds_decimal = float(bet_record["odds_decimal"])

    if match_result is None:
        return _build_settled_bet(
            bet_record,
            result="UNSETTLED",
            profit=0.0,
            home_goals=None,
            away_goals=None,
            settlement_reason="missing match result",
            stake=stake,
        )

    result, reason = _evaluate_bet_result(
        family=family,
        side=side,
        team_role=team_role,
        line=line,
        match_result=match_result,
    )

    profit = _profit_for_result(result, odds_decimal=odds_decimal, stake=stake)

    return _build_settled_bet(
        bet_record,
        result=result,
        profit=profit,
        home_goals=match_result.home_goals,
        away_goals=match_result.away_goals,
        settlement_reason=reason,
        stake=stake,
    )


def settlement_report_to_record(report: SettlementReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "source": "fqis_settlement_report",
        "batch_path": report.batch_path,
        "results_path": report.results_path,
        "accepted_bet_count": report.accepted_bet_count,
        "settled_bet_count": report.settled_bet_count,
        "unsettled_bet_count": report.unsettled_bet_count,
        "won_count": report.won_count,
        "lost_count": report.lost_count,
        "push_count": report.push_count,
        "total_staked": report.total_staked,
        "total_profit": report.total_profit,
        "roi": report.roi,
        "settled_bets": [
            _settled_bet_to_record(bet)
            for bet in report.settled_bets
        ],
    }


def write_settlement_report_json(report: SettlementReport, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            settlement_report_to_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _evaluate_bet_result(
    *,
    family: str,
    side: str,
    team_role: str,
    line: float | None,
    match_result: MatchResult,
) -> tuple[SettlementResult, str]:
    if family == "BTTS":
        if side == "YES":
            return ("WON", "both teams scored") if match_result.btts_yes else ("LOST", "both teams did not score")

        if side == "NO":
            return ("LOST", "both teams scored") if match_result.btts_yes else ("WON", "both teams did not score")

    if family == "MATCH_TOTAL":
        if line is None:
            return "UNSETTLED", "missing line for MATCH_TOTAL"

        return _settle_over_under(
            observed=float(match_result.total_goals),
            line=line,
            side=side,
            reason_prefix="match total goals",
        )

    if family in {"TEAM_TOTAL_HOME", "TEAM_TOTAL_AWAY"}:
        if line is None:
            return "UNSETTLED", "missing line for TEAM_TOTAL"

        observed = match_result.home_goals if team_role == "HOME" else match_result.away_goals

        return _settle_over_under(
            observed=float(observed),
            line=line,
            side=side,
            reason_prefix=f"{team_role.lower()} team goals",
        )

    if family == "MATCH_RESULT":
        if match_result.home_goals > match_result.away_goals:
            actual = "HOME"
        elif match_result.home_goals < match_result.away_goals:
            actual = "AWAY"
        else:
            actual = "DRAW"

        return ("WON", f"match result {actual}") if side == actual else ("LOST", f"match result {actual}")

    return "UNSETTLED", f"unsupported family {family}"


def _settle_over_under(
    *,
    observed: float,
    line: float,
    side: str,
    reason_prefix: str,
) -> tuple[SettlementResult, str]:
    if observed == line:
        return "PUSH", f"{reason_prefix} equals line"

    if side == "OVER":
        return ("WON", f"{reason_prefix} over line") if observed > line else ("LOST", f"{reason_prefix} under line")

    if side == "UNDER":
        return ("WON", f"{reason_prefix} under line") if observed < line else ("LOST", f"{reason_prefix} over line")

    return "UNSETTLED", f"unsupported over/under side {side}"


def _profit_for_result(
    result: SettlementResult,
    *,
    odds_decimal: float,
    stake: float,
) -> float:
    if result == "WON":
        return stake * (odds_decimal - 1.0)

    if result == "LOST":
        return -stake

    return 0.0


def _iter_accepted_bet_records(records: tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    accepted_bets: list[dict[str, Any]] = []

    for record in records:
        cycles = record.get("cycles", []) or []

        for cycle in cycles:
            if not isinstance(cycle, dict):
                continue

            for bet in cycle.get("accepted_bets", []) or []:
                if isinstance(bet, dict):
                    accepted_bets.append(bet)

    return tuple(accepted_bets)


def _row_to_match_result(row: dict[str, Any], *, line_number: int) -> MatchResult:
    event_id = _resolve_event_id(row, line_number=line_number)
    home_goals = _resolve_score(
        row,
        line_number=line_number,
        keys=("home_score_final", "home_final_score", "home_goals", "home_score"),
    )
    away_goals = _resolve_score(
        row,
        line_number=line_number,
        keys=("away_score_final", "away_final_score", "away_goals", "away_score"),
    )

    return MatchResult(
        event_id=event_id,
        home_goals=home_goals,
        away_goals=away_goals,
    )


def _resolve_event_id(row: dict[str, Any], *, line_number: int) -> int:
    for key in ("event_id", "fixture_id", "match_id"):
        if row.get(key) not in (None, ""):
            return int(row[key])

    raise ValueError(f"line {line_number}: missing event_id")


def _resolve_score(
    row: dict[str, Any],
    *,
    line_number: int,
    keys: tuple[str, ...],
) -> int:
    for key in keys:
        if row.get(key) not in (None, ""):
            return int(row[key])

    raise ValueError(f"line {line_number}: missing score field, tried {keys}")


def _build_settled_bet(
    bet_record: dict[str, Any],
    *,
    result: SettlementResult,
    profit: float,
    home_goals: int | None,
    away_goals: int | None,
    settlement_reason: str,
    stake: float,
) -> SettledBet:
    family = str(bet_record["family"])
    side = str(bet_record["side"])
    team_role = str(bet_record.get("team_role", "NONE"))
    line = _optional_float(bet_record.get("line"))

    return SettledBet(
        event_id=int(bet_record["event_id"]),
        market_key=_market_key(family=family, side=side, team_role=team_role, line=line),
        family=family,
        side=side,
        team_role=team_role,
        line=line,
        bookmaker_name=str(bet_record.get("bookmaker_name", "UNKNOWN")),
        odds_decimal=float(bet_record["odds_decimal"]),
        stake=stake,
        p_real=_optional_float(bet_record.get("p_real")),
        result=result,
        profit=profit,
        home_goals=home_goals,
        away_goals=away_goals,
        settlement_reason=settlement_reason,
    )


def _settled_bet_to_record(bet: SettledBet) -> dict[str, Any]:
    return {
        "event_id": bet.event_id,
        "market_key": bet.market_key,
        "family": bet.family,
        "side": bet.side,
        "team_role": bet.team_role,
        "line": bet.line,
        "bookmaker_name": bet.bookmaker_name,
        "odds_decimal": bet.odds_decimal,
        "stake": bet.stake,
        "p_real": bet.p_real,
        "result": bet.result,
        "profit": bet.profit,
        "home_goals": bet.home_goals,
        "away_goals": bet.away_goals,
        "settlement_reason": bet.settlement_reason,
    }


def _market_key(
    *,
    family: str,
    side: str,
    team_role: str,
    line: float | None,
) -> str:
    line_label = "NA" if line is None else str(line)

    return f"{family}|{side}|{team_role}|{line_label}"


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None

    return float(value)

    