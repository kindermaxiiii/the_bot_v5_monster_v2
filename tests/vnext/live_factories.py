from __future__ import annotations

from datetime import datetime
from statistics import mean

from app.vnext.data.normalized_models import quality_flag_from_score
from app.vnext.live.models import LiveSnapshot
from app.vnext.posterior.builder import build_scenario_posterior_result
from app.vnext.prior.builder import build_historical_prior_pack
from app.vnext.scenario.builder import build_scenario_prior_result
from tests.vnext.factories import build_reference_dataset


def make_live_snapshot(
    *,
    fixture_id: int = 999,
    competition_id: int = 100,
    season: int = 2025,
    kickoff_utc: datetime | None = datetime(2025, 12, 15, 19, 0),
    minute: int = 28,
    status: str = "1H",
    home_team_id: int = 1,
    away_team_id: int = 2,
    home_team_name: str = "Lions",
    away_team_name: str = "Falcons",
    home_goals: int = 0,
    away_goals: int = 0,
    home_red_cards: int = 0,
    away_red_cards: int = 0,
    home_shots_total: int | None = 8,
    away_shots_total: int | None = 5,
    home_shots_on: int | None = 3,
    away_shots_on: int | None = 1,
    home_corners: int | None = 4,
    away_corners: int | None = 2,
    home_dangerous_attacks: int | None = 28,
    away_dangerous_attacks: int | None = 16,
    home_attacks: int | None = 45,
    away_attacks: int | None = 33,
    home_possession: float | None = 54.0,
    away_possession: float | None = 46.0,
    home_xg: float | None = 0.75,
    away_xg: float | None = 0.22,
) -> LiveSnapshot:
    completeness = mean(
        [
            1.0 if minute is not None else 0.0,
            1.0 if home_goals is not None else 0.0,
            1.0 if away_goals is not None else 0.0,
            1.0 if home_shots_on is not None else 0.0,
            1.0 if away_shots_on is not None else 0.0,
            1.0 if home_dangerous_attacks is not None else 0.0,
            1.0 if away_dangerous_attacks is not None else 0.0,
            1.0 if home_shots_total is not None else 0.0,
            1.0 if away_shots_total is not None else 0.0,
        ]
    )
    return LiveSnapshot(
        fixture_id=fixture_id,
        competition_id=competition_id,
        season=season,
        kickoff_utc=kickoff_utc,
        minute=minute,
        status=status,  # type: ignore[arg-type]
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_team_name=home_team_name,
        away_team_name=away_team_name,
        home_goals=home_goals,
        away_goals=away_goals,
        home_red_cards=home_red_cards,
        away_red_cards=away_red_cards,
        home_shots_total=home_shots_total,
        away_shots_total=away_shots_total,
        home_shots_on=home_shots_on,
        away_shots_on=away_shots_on,
        home_corners=home_corners,
        away_corners=away_corners,
        home_dangerous_attacks=home_dangerous_attacks,
        away_dangerous_attacks=away_dangerous_attacks,
        home_attacks=home_attacks,
        away_attacks=away_attacks,
        home_possession=home_possession,
        away_possession=away_possession,
        home_xg=home_xg,
        away_xg=away_xg,
        live_snapshot_quality_score=round(completeness, 4),
        data_quality_flag=quality_flag_from_score(completeness),
    )


def build_reference_prior_result():
    dataset = build_reference_dataset()
    prior_pack = build_historical_prior_pack(dataset, fixture_id=999)
    return build_scenario_prior_result(prior_pack)


def build_reference_posterior_result():
    prior_result = build_reference_prior_result()
    previous = make_live_snapshot(minute=24, status="1H", home_goals=0, away_goals=0)
    current = make_live_snapshot()
    return build_scenario_posterior_result(prior_result, current, previous)
