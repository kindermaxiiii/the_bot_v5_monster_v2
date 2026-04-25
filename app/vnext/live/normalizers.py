from __future__ import annotations

from statistics import mean
from typing import Any

from app.vnext.data.normalized_models import quality_flag_from_score
from app.vnext.live.models import LiveSnapshot


def _as_int(value: Any, *, default: int | None = None) -> int | None:
    if value in (None, "", "null"):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, *, default: float | None = None) -> float | None:
    if value in (None, "", "null"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_str(value: Any, *, default: str = "") -> str:
    return str(value or default).strip()


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _present(value: Any) -> float:
    return 0.0 if value in (None, "", "null") else 1.0


def _any_present(*values: Any) -> float:
    return 1.0 if any(value not in (None, "", "null") for value in values) else 0.0


def normalize_live_snapshot(
    fixture_row: dict[str, Any],
    stats_row: dict[str, Any] | None = None,
) -> LiveSnapshot:
    stats_row = stats_row or {}

    fixture_core_completeness = mean(
        [
            _present(fixture_row.get("fixture_id")),
            _present(fixture_row.get("home_team_id")),
            _present(fixture_row.get("away_team_id")),
            _present(fixture_row.get("minute")),
            _present(fixture_row.get("status")),
            _present(fixture_row.get("home_goals")),
            _present(fixture_row.get("away_goals")),
        ]
    )

    discipline_completeness = mean(
        [
            _any_present(fixture_row.get("home_red"), stats_row.get("home_red_cards")),
            _any_present(fixture_row.get("away_red"), stats_row.get("away_red_cards")),
        ]
    )

    stats_completeness = mean(
        [
            _any_present(stats_row.get("home_shots_on"), stats_row.get("home_shots_total")),
            _any_present(stats_row.get("away_shots_on"), stats_row.get("away_shots_total")),
            _any_present(stats_row.get("home_dangerous_attacks"), stats_row.get("home_attacks")),
            _any_present(stats_row.get("away_dangerous_attacks"), stats_row.get("away_attacks")),
            _any_present(stats_row.get("home_xg"), stats_row.get("home_possession"), stats_row.get("home_corners")),
            _any_present(stats_row.get("away_xg"), stats_row.get("away_possession"), stats_row.get("away_corners")),
        ]
    )

    quality_score = _clip(
        (fixture_core_completeness * 0.72)
        + (stats_completeness * 0.22)
        + (discipline_completeness * 0.06)
    )

    return LiveSnapshot(
        fixture_id=_as_int(fixture_row.get("fixture_id"), default=0) or 0,
        competition_id=_as_int(fixture_row.get("league_id"), default=0) or 0,
        season=_as_int(fixture_row.get("season"), default=0) or 0,
        kickoff_utc=fixture_row.get("start_time_utc"),
        minute=max(0, _as_int(fixture_row.get("minute"), default=0) or 0),
        status=_as_str(fixture_row.get("status"), default="UNKNOWN").upper(),  # type: ignore[arg-type]
        home_team_id=_as_int(fixture_row.get("home_team_id"), default=0) or 0,
        away_team_id=_as_int(fixture_row.get("away_team_id"), default=0) or 0,
        home_team_name=_as_str(fixture_row.get("home_team_name"), default="Home"),
        away_team_name=_as_str(fixture_row.get("away_team_name"), default="Away"),
        home_goals=_as_int(fixture_row.get("home_goals"), default=0) or 0,
        away_goals=_as_int(fixture_row.get("away_goals"), default=0) or 0,
        home_red_cards=_as_int(
            fixture_row.get("home_red"),
            default=_as_int(stats_row.get("home_red_cards"), default=0),
        )
        or 0,
        away_red_cards=_as_int(
            fixture_row.get("away_red"),
            default=_as_int(stats_row.get("away_red_cards"), default=0),
        )
        or 0,
        home_shots_total=_as_int(stats_row.get("home_shots_total")),
        away_shots_total=_as_int(stats_row.get("away_shots_total")),
        home_shots_on=_as_int(stats_row.get("home_shots_on")),
        away_shots_on=_as_int(stats_row.get("away_shots_on")),
        home_corners=_as_int(stats_row.get("home_corners")),
        away_corners=_as_int(stats_row.get("away_corners")),
        home_dangerous_attacks=_as_int(stats_row.get("home_dangerous_attacks")),
        away_dangerous_attacks=_as_int(stats_row.get("away_dangerous_attacks")),
        home_attacks=_as_int(stats_row.get("home_attacks")),
        away_attacks=_as_int(stats_row.get("away_attacks")),
        home_possession=_as_float(stats_row.get("home_possession")),
        away_possession=_as_float(stats_row.get("away_possession")),
        home_xg=_as_float(stats_row.get("home_xg")),
        away_xg=_as_float(stats_row.get("away_xg")),
        live_snapshot_quality_score=round(quality_score, 4),
        data_quality_flag=quality_flag_from_score(quality_score),
        payload={
            "fixture_row": fixture_row,
            "stats_row": stats_row,
        },
    )
    