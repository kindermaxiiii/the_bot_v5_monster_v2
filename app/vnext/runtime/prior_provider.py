from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.clients.api_football import APIFootballRequestError, api_client
from app.normalizers.fixtures import normalize_live_fixtures
from app.normalizers.statistics import normalize_fixture_statistics
from app.vnext.data.normalized_models import HistoricalDataset, NormalizedFixtureBundle
from app.vnext.data.normalizers import normalize_competition, normalize_fixture_bundle, normalize_team
from app.vnext.data.raw_models import (
    RawCardEventRecord,
    RawCompetitionRecord,
    RawFixtureRecord,
    RawFixtureTeamStatsRecord,
    RawGoalEventRecord,
    RawTeamRecord,
)
from app.vnext.live.models import LiveSnapshot
from app.vnext.prior.builder import build_historical_prior_pack
from app.vnext.scenario.builder import build_scenario_prior_result
from app.vnext.scenario.models import ScenarioPriorResult


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _kickoff(snapshot: LiveSnapshot) -> datetime:
    return snapshot.kickoff_utc or _utcnow_naive()


def _fixture_row_from_snapshot(snapshot: LiveSnapshot) -> dict[str, Any]:
    payload = snapshot.payload or {}
    fixture_row = payload.get("fixture_row")
    if isinstance(fixture_row, dict):
        row = dict(fixture_row)
        row.setdefault("league_id", snapshot.competition_id)
        row.setdefault("season", snapshot.season)
        row.setdefault("status", snapshot.status)
        row.setdefault("minute", snapshot.minute)
        row.setdefault("home_team_id", snapshot.home_team_id)
        row.setdefault("away_team_id", snapshot.away_team_id)
        row.setdefault("home_team_name", snapshot.home_team_name)
        row.setdefault("away_team_name", snapshot.away_team_name)
        row.setdefault("home_goals", snapshot.home_goals)
        row.setdefault("away_goals", snapshot.away_goals)
        row.setdefault("fixture_id", snapshot.fixture_id)
        row.setdefault("start_time_utc", _kickoff(snapshot))
        return row
    return {
        "fixture_id": snapshot.fixture_id,
        "league_id": snapshot.competition_id,
        "league_name": f"Competition {snapshot.competition_id}",
        "season": snapshot.season,
        "status": snapshot.status,
        "minute": snapshot.minute,
        "start_time_utc": _kickoff(snapshot),
        "home_team_id": snapshot.home_team_id,
        "home_team_name": snapshot.home_team_name,
        "away_team_id": snapshot.away_team_id,
        "away_team_name": snapshot.away_team_name,
        "home_goals": snapshot.home_goals,
        "away_goals": snapshot.away_goals,
        "home_red": snapshot.home_red_cards,
        "away_red": snapshot.away_red_cards,
    }


def _stats_row_from_snapshot(snapshot: LiveSnapshot) -> dict[str, Any]:
    payload = snapshot.payload or {}
    stats_row = payload.get("stats_row")
    if isinstance(stats_row, dict):
        return dict(stats_row)
    return {
        "home_shots_total": snapshot.home_shots_total,
        "away_shots_total": snapshot.away_shots_total,
        "home_shots_on": snapshot.home_shots_on,
        "away_shots_on": snapshot.away_shots_on,
        "home_corners": snapshot.home_corners,
        "away_corners": snapshot.away_corners,
        "home_dangerous_attacks": snapshot.home_dangerous_attacks,
        "away_dangerous_attacks": snapshot.away_dangerous_attacks,
        "home_attacks": snapshot.home_attacks,
        "away_attacks": snapshot.away_attacks,
        "home_possession": snapshot.home_possession,
        "away_possession": snapshot.away_possession,
        "home_xg": snapshot.home_xg,
        "away_xg": snapshot.away_xg,
        "home_red_cards": snapshot.home_red_cards,
        "away_red_cards": snapshot.away_red_cards,
        "home_saves": None,
        "away_saves": None,
    }


def _synthetic_goal_events(fixture_row: dict[str, Any]) -> list[RawGoalEventRecord]:
    fixture_id = int(fixture_row.get("fixture_id") or 0)
    as_of_date = (_kickoff_from_row(fixture_row)).date()
    home_goals = int(fixture_row.get("home_goals") or 0)
    away_goals = int(fixture_row.get("away_goals") or 0)
    home_team_id = int(fixture_row.get("home_team_id") or 0)
    away_team_id = int(fixture_row.get("away_team_id") or 0)
    events: list[RawGoalEventRecord] = []
    for index in range(home_goals):
        events.append(
            RawGoalEventRecord(
                fixture_id=fixture_id,
                team_id=home_team_id,
                minute=12 + (index * 17),
                as_of_date=as_of_date,
                event_id=f"{fixture_id}-home-{index}",
                detail="Goal",
                source="synthetic_goal_events.v1",
            )
        )
    for index in range(away_goals):
        events.append(
            RawGoalEventRecord(
                fixture_id=fixture_id,
                team_id=away_team_id,
                minute=18 + (index * 19),
                as_of_date=as_of_date,
                event_id=f"{fixture_id}-away-{index}",
                detail="Goal",
                source="synthetic_goal_events.v1",
            )
        )
    return events


def _synthetic_card_events(fixture_row: dict[str, Any], stats_row: dict[str, Any]) -> list[RawCardEventRecord]:
    fixture_id = int(fixture_row.get("fixture_id") or 0)
    as_of_date = (_kickoff_from_row(fixture_row)).date()
    events: list[RawCardEventRecord] = []
    home_reds = int(
        fixture_row.get("home_red")
        or stats_row.get("home_red_cards")
        or 0
    )
    away_reds = int(
        fixture_row.get("away_red")
        or stats_row.get("away_red_cards")
        or 0
    )
    for index in range(home_reds):
        events.append(
            RawCardEventRecord(
                fixture_id=fixture_id,
                team_id=int(fixture_row.get("home_team_id") or 0),
                minute=70 + index,
                as_of_date=as_of_date,
                card_type="RED",
                event_id=f"{fixture_id}-home-red-{index}",
                source="synthetic_card_events.v1",
            )
        )
    for index in range(away_reds):
        events.append(
            RawCardEventRecord(
                fixture_id=fixture_id,
                team_id=int(fixture_row.get("away_team_id") or 0),
                minute=70 + index,
                as_of_date=as_of_date,
                card_type="RED",
                event_id=f"{fixture_id}-away-red-{index}",
                source="synthetic_card_events.v1",
            )
        )
    return events


def _kickoff_from_row(fixture_row: dict[str, Any]) -> datetime:
    kickoff = fixture_row.get("start_time_utc")
    if isinstance(kickoff, datetime):
        return kickoff
    return _utcnow_naive()


def _fixture_bundle_from_rows(
    fixture_row: dict[str, Any],
    stats_row: dict[str, Any],
    *,
    is_finished: bool,
) -> NormalizedFixtureBundle:
    kickoff = _kickoff_from_row(fixture_row)
    as_of_date = kickoff.date()
    raw_fixture = RawFixtureRecord(
        fixture_id=fixture_row.get("fixture_id"),
        competition_id=fixture_row.get("league_id"),
        season=fixture_row.get("season"),
        kickoff_utc=kickoff,
        as_of_date=as_of_date,
        home_team_id=fixture_row.get("home_team_id"),
        away_team_id=fixture_row.get("away_team_id"),
        home_team_name=fixture_row.get("home_team_name"),
        away_team_name=fixture_row.get("away_team_name"),
        competition_name=fixture_row.get("league_name") or f"Competition {fixture_row.get('league_id')}",
        home_score=fixture_row.get("home_goals"),
        away_score=fixture_row.get("away_goals"),
        status=fixture_row.get("status") or ("FT" if is_finished else "LIVE"),
        is_finished=is_finished,
        market_depth_score=0.65,
        payload=fixture_row,
        source="runtime_live_fixture.v1",
    )
    raw_home_stats = RawFixtureTeamStatsRecord(
        fixture_id=fixture_row.get("fixture_id"),
        team_id=fixture_row.get("home_team_id"),
        team_name=fixture_row.get("home_team_name"),
        venue="HOME",
        xg=stats_row.get("home_xg"),
        shots=stats_row.get("home_shots_total"),
        shots_on=stats_row.get("home_shots_on"),
        corners=stats_row.get("home_corners"),
        dangerous_attacks=stats_row.get("home_dangerous_attacks"),
        possession=stats_row.get("home_possession"),
        saves=stats_row.get("home_saves"),
        red_cards=stats_row.get("home_red_cards") or fixture_row.get("home_red"),
        payload=stats_row,
        source="runtime_live_stats.v1",
    )
    raw_away_stats = RawFixtureTeamStatsRecord(
        fixture_id=fixture_row.get("fixture_id"),
        team_id=fixture_row.get("away_team_id"),
        team_name=fixture_row.get("away_team_name"),
        venue="AWAY",
        xg=stats_row.get("away_xg"),
        shots=stats_row.get("away_shots_total"),
        shots_on=stats_row.get("away_shots_on"),
        corners=stats_row.get("away_corners"),
        dangerous_attacks=stats_row.get("away_dangerous_attacks"),
        possession=stats_row.get("away_possession"),
        saves=stats_row.get("away_saves"),
        red_cards=stats_row.get("away_red_cards") or fixture_row.get("away_red"),
        payload=stats_row,
        source="runtime_live_stats.v1",
    )
    return normalize_fixture_bundle(
        raw_fixture,
        raw_home_stats=raw_home_stats,
        raw_away_stats=raw_away_stats,
        raw_goal_events=_synthetic_goal_events(fixture_row),
        raw_card_events=_synthetic_card_events(fixture_row, stats_row),
    )


@dataclass(slots=True)
class LivePriorResultProvider:
    team_history_limit: int = 8
    _prior_cache: dict[int, ScenarioPriorResult] = field(default_factory=dict)
    _bundle_cache: dict[int, NormalizedFixtureBundle] = field(default_factory=dict)

    def __call__(self, snapshot: LiveSnapshot) -> ScenarioPriorResult:
        cached = self._prior_cache.get(snapshot.fixture_id)
        if cached is not None:
            return cached

        dataset = self._build_dataset(snapshot)
        prior_pack = build_historical_prior_pack(dataset, fixture_id=snapshot.fixture_id)
        prior_result = build_scenario_prior_result(prior_pack)
        self._prior_cache[snapshot.fixture_id] = prior_result
        return prior_result

    def _build_dataset(self, snapshot: LiveSnapshot) -> HistoricalDataset:
        current_fixture_row = _fixture_row_from_snapshot(snapshot)
        current_stats_row = _stats_row_from_snapshot(snapshot)
        bundles: list[NormalizedFixtureBundle] = [
            _fixture_bundle_from_rows(current_fixture_row, current_stats_row, is_finished=False)
        ]

        history_rows = self._fetch_history_rows(snapshot)
        for fixture_row in history_rows:
            fixture_id = int(fixture_row.get("fixture_id") or 0)
            if fixture_id <= 0 or fixture_id == snapshot.fixture_id:
                continue
            bundle = self._bundle_cache.get(fixture_id)
            if bundle is None:
                stats_row = self._fetch_stats_row(fixture_row)
                bundle = _fixture_bundle_from_rows(fixture_row, stats_row, is_finished=True)
                self._bundle_cache[fixture_id] = bundle
            bundles.append(bundle)

        competitions = (
            normalize_competition(
                RawCompetitionRecord(
                    competition_id=snapshot.competition_id,
                    season=snapshot.season,
                    name=current_fixture_row.get("league_name") or f"Competition {snapshot.competition_id}",
                    country_name=(current_fixture_row.get("country_name") or "Unknown Country"),
                    as_of_date=_kickoff(snapshot).date(),
                    market_depth_score=0.65,
                    payload=current_fixture_row,
                    source="runtime_live_competition.v1",
                )
            ),
        )

        team_rows: dict[int, tuple[str, dict[str, Any]]] = {
            snapshot.home_team_id: (snapshot.home_team_name, current_fixture_row),
            snapshot.away_team_id: (snapshot.away_team_name, current_fixture_row),
        }
        for bundle in bundles:
            fixture = bundle.fixture
            team_rows.setdefault(fixture.home_team_id, (fixture.home_team_name, fixture.payload if hasattr(fixture, "payload") else {}))
            team_rows.setdefault(fixture.away_team_id, (fixture.away_team_name, fixture.payload if hasattr(fixture, "payload") else {}))

        teams = tuple(
            normalize_team(
                RawTeamRecord(
                    team_id=team_id,
                    name=team_name,
                    as_of_date=_kickoff(snapshot).date(),
                    payload=payload if isinstance(payload, dict) else {},
                    source="runtime_live_team.v1",
                )
            )
            for team_id, (team_name, payload) in sorted(team_rows.items(), key=lambda item: item[0])
        )

        return HistoricalDataset.from_bundles(
            competitions=competitions,
            teams=teams,
            bundles=tuple(bundles),
        )

    def _fetch_history_rows(self, snapshot: LiveSnapshot) -> list[dict[str, Any]]:
        fixture_rows: dict[int, dict[str, Any]] = {}
        for team_id in (snapshot.home_team_id, snapshot.away_team_id):
            try:
                payload = api_client.get_team_fixtures(
                    team_id,
                    season=snapshot.season,
                    league_id=snapshot.competition_id,
                    last=self.team_history_limit,
                    status="FT",
                )
            except APIFootballRequestError:
                continue
            for row in normalize_live_fixtures(payload):
                fixture_id = int(row.get("fixture_id") or 0)
                if fixture_id > 0:
                    fixture_rows[fixture_id] = row
        return sorted(
            fixture_rows.values(),
            key=lambda row: _kickoff_from_row(row),
            reverse=True,
        )

    def _fetch_stats_row(self, fixture_row: dict[str, Any]) -> dict[str, Any]:
        fixture_id = int(fixture_row.get("fixture_id") or 0)
        try:
            return normalize_fixture_statistics(
                api_client.get_fixture_statistics(fixture_id),
                expected_home_team_id=fixture_row.get("home_team_id"),
                expected_away_team_id=fixture_row.get("away_team_id"),
            )
        except APIFootballRequestError:
            return {}
