from __future__ import annotations

from datetime import date, datetime, time
from math import isfinite
from statistics import mean, pstdev

from app.vnext.data.normalized_models import (
    FixtureRecord,
    FixtureTeamStatsRecord,
    HistoricalDataset,
    quality_flag_from_score,
    worst_quality_flag,
)
from app.vnext.profiles.models import (
    CompetitionProfile,
    MatchupProfile,
    TeamRecentProfile,
    TeamStrengthProfile,
    TeamStyleProfile,
    TeamVenueProfile,
)


def _as_of_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, time.min)


def _round(value: float) -> float:
    return round(value, 4)


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _mean(values: list[float | None], *, default: float = 0.0) -> float:
    clean = [float(value) for value in values if value is not None and isfinite(float(value))]
    if not clean:
        return default
    return sum(clean) / len(clean)


def _rate(values: list[bool]) -> float:
    if not values:
        return 0.0
    return sum(1 for value in values if value) / len(values)


def _team_history(
    dataset: HistoricalDataset,
    *,
    team_id: int,
    as_of: date | datetime,
    competition_id: int | None = None,
) -> list[FixtureTeamStatsRecord]:
    cutoff = _as_of_datetime(as_of)
    records = [
        record
        for record in dataset.fixture_team_stats
        if record.team_id == team_id
        and record.kickoff_utc < cutoff
        and (competition_id is None or record.competition_id == competition_id)
    ]
    records.sort(key=lambda record: record.kickoff_utc, reverse=True)
    return records


def _competition_history(
    dataset: HistoricalDataset,
    *,
    competition_id: int,
    season: int | None,
    as_of: date | datetime,
) -> list[FixtureRecord]:
    cutoff = _as_of_datetime(as_of)
    fixtures = [
        fixture
        for fixture in dataset.fixtures
        if fixture.competition_id == competition_id
        and fixture.kickoff_utc < cutoff
        and (season is None or fixture.season == season)
    ]
    fixtures.sort(key=lambda fixture: fixture.kickoff_utc, reverse=True)
    return fixtures


def _team_quality(records: list[FixtureTeamStatsRecord]) -> float:
    if not records:
        return 0.0
    return _clip(_mean([record.data_completeness_score for record in records]))


def build_team_recent_profile(
    dataset: HistoricalDataset,
    *,
    team_id: int,
    as_of: date | datetime,
    competition_id: int | None = None,
) -> TeamRecentProfile:
    history = _team_history(dataset, team_id=team_id, as_of=as_of, competition_id=competition_id)
    primary = history[:8]
    control = history[:5]
    primary_weight = 0.7
    control_weight = 0.3 if control else 0.0
    denom = primary_weight + control_weight or 1.0

    def _blend(metric: str) -> float:
        primary_value = _mean([getattr(record, metric) for record in primary])
        control_value = _mean([getattr(record, metric) for record in control], default=primary_value)
        return ((primary_value * primary_weight) + (control_value * control_weight)) / denom

    points_per_match = _blend("points")
    goals_for = _blend("goals_for")
    goals_against = _blend("goals_against")
    xg_for = _blend("xg_for")
    xg_against = _blend("xg_against")
    shots_for = _blend("shots_for")
    shots_on_for = _blend("shots_on_for")
    shots_on_against = _blend("shots_on_against")
    clean_sheet_rate = (
        (_rate([record.clean_sheet for record in primary]) * primary_weight)
        + (_rate([record.clean_sheet for record in control]) * control_weight)
    ) / denom
    failed_to_score_rate = (
        (_rate([record.failed_to_score for record in primary]) * primary_weight)
        + (_rate([record.failed_to_score for record in control]) * control_weight)
    ) / denom
    form_score = _clip(
        (
            (points_per_match / 3.0) * 0.45
            + (_clip(xg_for / max(0.1, xg_for + xg_against)) * 0.35)
            + ((1.0 - failed_to_score_rate) * 0.20)
        ),
        low=0.0,
        high=1.0,
    )
    quality_score = _team_quality(primary)
    confidence_weight = _clip((len(primary) / 8.0) * quality_score)
    team = dataset.team_by_id(team_id)
    return TeamRecentProfile(
        team_id=team_id,
        team_name=team.name,
        as_of_date=_as_of_datetime(as_of).date(),
        sample_size=len(primary),
        primary_window=8,
        control_window=5,
        control_sample_size=len(control),
        confidence_weight=_round(confidence_weight),
        data_quality_flag=quality_flag_from_score(quality_score),
        source="team_recent_profile.v1",
        goals_for_per_match=_round(goals_for),
        goals_against_per_match=_round(goals_against),
        xg_for_per_match=_round(xg_for),
        xg_against_per_match=_round(xg_against),
        shots_for_per_match=_round(shots_for),
        shots_on_for_per_match=_round(shots_on_for),
        shots_on_against_per_match=_round(shots_on_against),
        clean_sheet_rate=_round(clean_sheet_rate),
        failed_to_score_rate=_round(failed_to_score_rate),
        points_per_match=_round(points_per_match),
        form_score=_round(form_score),
    )


def build_team_venue_profile(
    dataset: HistoricalDataset,
    *,
    team_id: int,
    venue: str,
    as_of: date | datetime,
    competition_id: int | None = None,
) -> TeamVenueProfile:
    history = _team_history(dataset, team_id=team_id, as_of=as_of, competition_id=competition_id)
    venue_history = [record for record in history if record.venue == venue][:8]
    season_history = history[:12]
    shrinkage_weight = _clip(len(venue_history) / 5.0)

    def _shrink(metric: str) -> float:
        venue_value = _mean([getattr(record, metric) for record in venue_history])
        season_value = _mean([getattr(record, metric) for record in season_history], default=venue_value)
        return (shrinkage_weight * venue_value) + ((1.0 - shrinkage_weight) * season_value)

    quality_score = (
        (_team_quality(venue_history) * shrinkage_weight)
        + (_team_quality(season_history) * (1.0 - shrinkage_weight))
    )
    confidence_weight = _clip((_clip(len(venue_history) / 5.0) * 0.6) + (quality_score * 0.4))
    team = dataset.team_by_id(team_id)
    return TeamVenueProfile(
        team_id=team_id,
        team_name=team.name,
        venue=venue,  # type: ignore[arg-type]
        as_of_date=_as_of_datetime(as_of).date(),
        sample_size=len(venue_history),
        season_sample_size=len(season_history),
        shrinkage_weight=_round(shrinkage_weight),
        confidence_weight=_round(confidence_weight),
        data_quality_flag=quality_flag_from_score(quality_score),
        source="team_venue_profile.v1",
        goals_for_per_match=_round(_shrink("goals_for")),
        goals_against_per_match=_round(_shrink("goals_against")),
        xg_for_per_match=_round(_shrink("xg_for")),
        xg_against_per_match=_round(_shrink("xg_against")),
        shots_for_per_match=_round(_shrink("shots_for")),
        shots_on_for_per_match=_round(_shrink("shots_on_for")),
        shots_on_against_per_match=_round(_shrink("shots_on_against")),
        clean_sheet_rate=_round(_shrink("clean_sheet")),
        failed_to_score_rate=_round(_shrink("failed_to_score")),
    )


def build_team_strength_profile(
    dataset: HistoricalDataset,
    *,
    team_id: int,
    as_of: date | datetime,
    competition_id: int | None = None,
) -> TeamStrengthProfile:
    history = _team_history(dataset, team_id=team_id, as_of=as_of, competition_id=competition_id)[:12]
    baseline_pool = [
        record
        for record in dataset.fixture_team_stats
        if record.kickoff_utc < _as_of_datetime(as_of)
        and (competition_id is None or record.competition_id == competition_id)
    ]
    baseline_xg_for = max(0.1, _mean([record.xg_for for record in baseline_pool], default=1.0))
    baseline_goals_for = max(0.1, _mean([record.goals_for for record in baseline_pool], default=1.0))
    baseline_shots_on_for = max(0.1, _mean([record.shots_on_for for record in baseline_pool], default=3.0))
    baseline_xg_against = max(0.1, _mean([record.xg_against for record in baseline_pool], default=1.0))
    baseline_goals_against = max(0.1, _mean([record.goals_against for record in baseline_pool], default=1.0))
    baseline_shots_on_against = max(0.1, _mean([record.shots_on_against for record in baseline_pool], default=3.0))

    team_xg_for = _mean([record.xg_for for record in history], default=baseline_xg_for)
    team_goals_for = _mean([record.goals_for for record in history], default=baseline_goals_for)
    team_shots_on_for = _mean([record.shots_on_for for record in history], default=baseline_shots_on_for)
    team_xg_against = _mean([record.xg_against for record in history], default=baseline_xg_against)
    team_goals_against = _mean([record.goals_against for record in history], default=baseline_goals_against)
    team_shots_on_against = _mean([record.shots_on_against for record in history], default=baseline_shots_on_against)

    attack_index = mean([
        team_xg_for / baseline_xg_for,
        team_goals_for / baseline_goals_for,
        team_shots_on_for / baseline_shots_on_for,
    ])
    defense_index = mean([
        baseline_xg_against / max(0.1, team_xg_against),
        baseline_goals_against / max(0.1, team_goals_against),
        baseline_shots_on_against / max(0.1, team_shots_on_against),
    ])
    offensive_rating = _clip(50.0 + ((attack_index - 1.0) * 40.0), low=0.0, high=100.0)
    defensive_rating = _clip(50.0 + ((defense_index - 1.0) * 40.0), low=0.0, high=100.0)
    goal_diffs = [record.goals_for - record.goals_against for record in history] or [0]
    stability_score = _clip(1.0 - (pstdev(goal_diffs) / 3.0))
    global_rating = _clip(
        (offensive_rating * 0.45) + (defensive_rating * 0.45) + (stability_score * 10.0),
        low=0.0,
        high=100.0,
    )
    quality_score = _team_quality(history)
    confidence_weight = _clip((len(history) / 12.0) * quality_score)
    team = dataset.team_by_id(team_id)
    return TeamStrengthProfile(
        team_id=team_id,
        team_name=team.name,
        as_of_date=_as_of_datetime(as_of).date(),
        sample_size=len(history),
        confidence_weight=_round(confidence_weight),
        data_quality_flag=quality_flag_from_score(quality_score),
        source="team_strength_profile.v1",
        global_rating=_round(global_rating),
        offensive_rating=_round(offensive_rating),
        defensive_rating=_round(defensive_rating),
        stability_score=_round(stability_score),
    )


def build_team_style_profile(
    dataset: HistoricalDataset,
    *,
    team_id: int,
    as_of: date | datetime,
    competition_id: int | None = None,
) -> TeamStyleProfile:
    history = _team_history(dataset, team_id=team_id, as_of=as_of, competition_id=competition_id)[:12]
    btts = _rate([record.goals_for > 0 and record.goals_against > 0 for record in history])
    over_2_5 = _rate([(record.goals_for + record.goals_against) > 2.5 for record in history])
    under_2_5 = _rate([(record.goals_for + record.goals_against) < 2.5 for record in history])
    team_over_0_5 = _rate([record.goals_for > 0.5 for record in history])
    team_over_1_5 = _rate([record.goals_for > 1.5 for record in history])
    team_over_2_5 = _rate([record.goals_for > 2.5 for record in history])
    clean_sheet_rate = _rate([record.clean_sheet for record in history])
    failed_to_score_rate = _rate([record.failed_to_score for record in history])
    quality_score = _team_quality(history)
    confidence_weight = _clip((len(history) / 12.0) * quality_score)
    team = dataset.team_by_id(team_id)
    return TeamStyleProfile(
        team_id=team_id,
        team_name=team.name,
        as_of_date=_as_of_datetime(as_of).date(),
        sample_size=len(history),
        confidence_weight=_round(confidence_weight),
        data_quality_flag=quality_flag_from_score(quality_score),
        source="team_style_profile.v1",
        btts_rate=_round(btts),
        under_2_5_rate=_round(under_2_5),
        over_2_5_rate=_round(over_2_5),
        team_total_over_0_5_rate=_round(team_over_0_5),
        team_total_over_1_5_rate=_round(team_over_1_5),
        team_total_over_2_5_rate=_round(team_over_2_5),
        clean_sheet_rate=_round(clean_sheet_rate),
        failed_to_score_rate=_round(failed_to_score_rate),
    )


def build_matchup_profile(
    dataset: HistoricalDataset,
    *,
    home_team_id: int,
    away_team_id: int,
    as_of: date | datetime,
) -> MatchupProfile:
    cutoff = _as_of_datetime(as_of)
    fixtures = [
        fixture
        for fixture in dataset.fixtures
        if fixture.kickoff_utc < cutoff
        and {fixture.home_team_id, fixture.away_team_id} == {home_team_id, away_team_id}
    ]
    fixtures.sort(key=lambda fixture: fixture.kickoff_utc, reverse=True)

    selected: list[FixtureRecord] = []
    seasons_seen: set[int] = set()
    for fixture in fixtures:
        if len(selected) >= 5:
            break
        new_seasons = seasons_seen | {fixture.season}
        if len(new_seasons) > 3:
            continue
        seasons_seen = new_seasons
        selected.append(fixture)

    weights = [1.0, 0.8, 0.65, 0.5, 0.35]
    total_weight = sum(weights[: len(selected)]) or 1.0
    home_goals = 0.0
    away_goals = 0.0
    btts = 0.0
    over_2_5 = 0.0
    draw_rate = 0.0
    quality_flags = []

    for index, fixture in enumerate(selected):
        weight = weights[index]
        if fixture.home_team_id == home_team_id:
            home_score = fixture.home_score
            away_score = fixture.away_score
        else:
            home_score = fixture.away_score
            away_score = fixture.home_score
        home_goals += home_score * weight
        away_goals += away_score * weight
        btts += float(home_score > 0 and away_score > 0) * weight
        over_2_5 += float((home_score + away_score) > 2.5) * weight
        draw_rate += float(home_score == away_score) * weight
        quality_flags.append(fixture.data_quality_flag)

    sample_factor = _clip(len(selected) / 5.0)
    confidence_weight = _round(sample_factor * 0.35)
    home_team = dataset.team_by_id(home_team_id)
    away_team = dataset.team_by_id(away_team_id)
    data_quality = worst_quality_flag(*quality_flags) if quality_flags else "LOW"
    return MatchupProfile(
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_team_name=home_team.name,
        away_team_name=away_team.name,
        as_of_date=_as_of_datetime(as_of).date(),
        sample_size=len(selected),
        seasons_covered=len(seasons_seen),
        confidence_weight=confidence_weight,
        data_quality_flag=data_quality,
        source="matchup_profile.v1",
        home_team_goals_per_match=_round(home_goals / total_weight),
        away_team_goals_per_match=_round(away_goals / total_weight),
        btts_rate=_round(btts / total_weight),
        over_2_5_rate=_round(over_2_5 / total_weight),
        draw_rate=_round(draw_rate / total_weight),
    )


def build_competition_profile(
    dataset: HistoricalDataset,
    *,
    competition_id: int,
    as_of: date | datetime,
    season: int | None = None,
) -> CompetitionProfile:
    fixtures = _competition_history(dataset, competition_id=competition_id, season=season, as_of=as_of)
    total_goals = [fixture.home_score + fixture.away_score for fixture in fixtures]
    btts = [fixture.home_score > 0 and fixture.away_score > 0 for fixture in fixtures]
    over_2_5 = [(fixture.home_score + fixture.away_score) > 2.5 for fixture in fixtures]
    quality_score = _clip(_mean([fixture.data_completeness_score for fixture in fixtures], default=0.0))
    market_depth_score = _clip(_mean([fixture.market_depth_score for fixture in fixtures], default=0.5))
    sample_factor = _clip(len(fixtures) / 20.0)
    competition_confidence = _clip((quality_score * 0.4) + (market_depth_score * 0.3) + (sample_factor * 0.3))
    variance_score = _clip((pstdev(total_goals) / 2.5) if len(total_goals) > 1 else 0.0)
    data_quality_flag = quality_flag_from_score((quality_score + market_depth_score) / 2.0)
    competition_name = dataset.competition_by_id(competition_id, season).name
    selected_season = season if season is not None else (fixtures[0].season if fixtures else 0)
    return CompetitionProfile(
        competition_id=competition_id,
        competition_name=competition_name,
        season=selected_season,
        as_of_date=_as_of_datetime(as_of).date(),
        sample_size=len(fixtures),
        confidence_weight=_round(competition_confidence),
        data_quality_flag=data_quality_flag,
        source="competition_profile.v1",
        avg_goals_per_match=_round(_mean(total_goals)),
        btts_rate=_round(_rate(btts)),
        over_2_5_rate=_round(_rate(over_2_5)),
        data_quality_score=_round(quality_score),
        market_depth_score=_round(market_depth_score),
        competition_confidence_score=_round(competition_confidence),
        variance_score=_round(variance_score),
    )
