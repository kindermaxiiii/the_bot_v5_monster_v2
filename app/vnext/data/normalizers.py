from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from statistics import mean
from typing import Any

from app.vnext.data.normalized_models import (
    CardEventRecord,
    CardEventType,
    CompetitionRecord,
    DataQualityFlag,
    FixtureRecord,
    FixtureTeamStatsRecord,
    GoalEventKind,
    GoalEventRecord,
    NormalizedFixtureBundle,
    TeamRecord,
    quality_flag_from_score,
)
from app.vnext.data.raw_models import (
    RawCardEventRecord,
    RawCompetitionRecord,
    RawFixtureRecord,
    RawFixtureTeamStatsRecord,
    RawGoalEventRecord,
    RawTeamRecord,
)


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


def _as_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, "", "null"):
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return default


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    text = _as_str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _bounded_score(value: float | None, *, default: float = 0.5) -> float:
    if value is None:
        return default
    return max(0.0, min(1.0, value))


def _event_sort_key(event: GoalEventRecord | CardEventRecord) -> tuple[int, int, str]:
    return (event.minute, event.extra_minute, event.event_id)


def _goal_event_kind(raw: RawGoalEventRecord) -> GoalEventKind:
    detail = _as_str(raw.detail).casefold()
    if "pen" in detail:
        return "PENALTY"
    if "own" in detail:
        return "OWN_GOAL"
    if "goal" in detail or not detail:
        return "GOAL"
    return "OTHER"


def _card_event_type(raw: RawCardEventRecord) -> CardEventType:
    detail = _as_str(raw.card_type).casefold()
    if "second" in detail:
        return "SECOND_YELLOW_RED"
    if "red" in detail:
        return "RED"
    return "YELLOW"


def _goal_event_signature(
    raw: RawGoalEventRecord,
    *,
    fixture_id: int,
    team_id: int,
    minute: int,
    extra_minute: int,
    kind: GoalEventKind,
) -> str:
    raw_event_id = _as_str(raw.event_id)
    if raw_event_id:
        return raw_event_id
    return f"{fixture_id}:{team_id}:{minute}:{extra_minute}:{kind}:{_as_str(raw.player_name)}"


def _card_event_signature(
    raw: RawCardEventRecord,
    *,
    fixture_id: int,
    team_id: int,
    minute: int,
    extra_minute: int,
    card_type: CardEventType,
) -> str:
    raw_event_id = _as_str(raw.event_id)
    if raw_event_id:
        return raw_event_id
    return f"{fixture_id}:{team_id}:{minute}:{extra_minute}:{card_type}:{_as_str(raw.player_name)}"


def _stats_completeness(raw_stats: RawFixtureTeamStatsRecord) -> float:
    fields = [
        raw_stats.xg,
        raw_stats.shots,
        raw_stats.shots_on,
        raw_stats.corners,
        raw_stats.dangerous_attacks,
        raw_stats.possession,
        raw_stats.saves,
        raw_stats.red_cards,
    ]
    present = sum(1 for value in fields if value not in (None, "", "null"))
    return present / len(fields)


def normalize_competition(raw: RawCompetitionRecord) -> CompetitionRecord:
    completeness = mean(
        [
            1.0 if _as_int(raw.competition_id) is not None else 0.0,
            1.0 if _as_int(raw.season) is not None else 0.0,
            1.0 if _as_str(raw.name) else 0.0,
            1.0 if _as_str(raw.country_name) else 0.0,
        ]
    )
    return CompetitionRecord(
        competition_id=_as_int(raw.competition_id) or 0,
        season=_as_int(raw.season) or 0,
        name=_as_str(raw.name, default="Unknown Competition"),
        country_name=_as_str(raw.country_name, default="Unknown Country"),
        as_of_date=raw.as_of_date,
        market_depth_score=_bounded_score(_as_float(raw.market_depth_score), default=0.5),
        data_quality_flag=quality_flag_from_score(completeness),
        data_completeness_score=round(completeness, 3),
        source=raw.source,
    )


def normalize_team(raw: RawTeamRecord) -> TeamRecord:
    completeness = mean(
        [
            1.0 if _as_int(raw.team_id) is not None else 0.0,
            1.0 if _as_str(raw.name) else 0.0,
        ]
    )
    return TeamRecord(
        team_id=_as_int(raw.team_id) or 0,
        name=_as_str(raw.name, default="Unknown Team"),
        as_of_date=raw.as_of_date,
        short_name=_as_str(raw.short_name) or None,
        country_name=_as_str(raw.country_name) or None,
        data_quality_flag=quality_flag_from_score(completeness),
        data_completeness_score=round(completeness, 3),
        source=raw.source,
    )


def normalize_fixture(raw: RawFixtureRecord) -> FixtureRecord:
    completeness = mean(
        [
            1.0 if _as_int(raw.fixture_id) is not None else 0.0,
            1.0 if _as_int(raw.competition_id) is not None else 0.0,
            1.0 if _as_int(raw.home_team_id) is not None else 0.0,
            1.0 if _as_int(raw.away_team_id) is not None else 0.0,
            1.0 if _as_str(raw.home_team_name) else 0.0,
            1.0 if _as_str(raw.away_team_name) else 0.0,
            1.0 if raw.kickoff_utc not in (None, "", "null") else 0.0,
            1.0 if _as_int(raw.home_score) is not None else 0.0,
            1.0 if _as_int(raw.away_score) is not None else 0.0,
        ]
    )
    return FixtureRecord(
        fixture_id=_as_int(raw.fixture_id) or 0,
        competition_id=_as_int(raw.competition_id) or 0,
        season=_as_int(raw.season) or 0,
        kickoff_utc=_as_datetime(raw.kickoff_utc),
        as_of_date=raw.as_of_date,
        home_team_id=_as_int(raw.home_team_id) or 0,
        away_team_id=_as_int(raw.away_team_id) or 0,
        home_team_name=_as_str(raw.home_team_name, default="Home"),
        away_team_name=_as_str(raw.away_team_name, default="Away"),
        competition_name=_as_str(raw.competition_name, default="Unknown Competition"),
        home_score=_as_int(raw.home_score, default=0) or 0,
        away_score=_as_int(raw.away_score, default=0) or 0,
        status=_as_str(raw.status, default="FT").upper(),
        is_finished=_as_bool(raw.is_finished, default=True),
        data_quality_flag=quality_flag_from_score(completeness),
        data_completeness_score=round(completeness, 3),
        market_depth_score=_bounded_score(_as_float(raw.market_depth_score), default=0.5),
        source=raw.source,
    )


def normalize_goal_events(
    raw_events: list[RawGoalEventRecord],
    fixture: FixtureRecord,
) -> tuple[GoalEventRecord, ...]:
    normalized: list[GoalEventRecord] = []
    seen: set[str] = set()
    for raw in raw_events:
        raw_team_id = _as_int(raw.team_id)
        if raw_team_id is None:
            continue
        kind = _goal_event_kind(raw)
        team_id = raw_team_id
        if kind == "OWN_GOAL":
            if raw_team_id == fixture.home_team_id:
                team_id = fixture.away_team_id
            elif raw_team_id == fixture.away_team_id:
                team_id = fixture.home_team_id
        minute = max(0, _as_int(raw.minute, default=0) or 0)
        extra_minute = max(0, _as_int(raw.extra_minute, default=0) or 0)
        event_id = _goal_event_signature(
            raw,
            fixture_id=fixture.fixture_id,
            team_id=team_id,
            minute=minute,
            extra_minute=extra_minute,
            kind=kind,
        )
        if event_id in seen:
            continue
        seen.add(event_id)
        is_home_team = team_id == fixture.home_team_id
        team_name = fixture.home_team_name if is_home_team else fixture.away_team_name
        normalized.append(
            GoalEventRecord(
                event_id=event_id,
                fixture_id=fixture.fixture_id,
                competition_id=fixture.competition_id,
                season=fixture.season,
                kickoff_utc=fixture.kickoff_utc,
                as_of_date=raw.as_of_date,
                team_id=team_id,
                team_name=team_name,
                minute=minute,
                extra_minute=extra_minute,
                event_kind=kind,
                is_home_team=is_home_team,
                source=raw.source,
            )
        )
    normalized.sort(key=_event_sort_key)
    return tuple(normalized)


def normalize_card_events(
    raw_events: list[RawCardEventRecord],
    fixture: FixtureRecord,
) -> tuple[CardEventRecord, ...]:
    normalized: list[CardEventRecord] = []
    seen: set[str] = set()
    for raw in raw_events:
        team_id = _as_int(raw.team_id)
        if team_id is None:
            continue
        card_type = _card_event_type(raw)
        minute = max(0, _as_int(raw.minute, default=0) or 0)
        extra_minute = max(0, _as_int(raw.extra_minute, default=0) or 0)
        event_id = _card_event_signature(
            raw,
            fixture_id=fixture.fixture_id,
            team_id=team_id,
            minute=minute,
            extra_minute=extra_minute,
            card_type=card_type,
        )
        if event_id in seen:
            continue
        seen.add(event_id)
        is_home_team = team_id == fixture.home_team_id
        team_name = fixture.home_team_name if is_home_team else fixture.away_team_name
        normalized.append(
            CardEventRecord(
                event_id=event_id,
                fixture_id=fixture.fixture_id,
                competition_id=fixture.competition_id,
                season=fixture.season,
                kickoff_utc=fixture.kickoff_utc,
                as_of_date=raw.as_of_date,
                team_id=team_id,
                team_name=team_name,
                minute=minute,
                extra_minute=extra_minute,
                card_type=card_type,
                is_home_team=is_home_team,
                source=raw.source,
            )
        )
    normalized.sort(key=_event_sort_key)
    return tuple(normalized)


def normalize_fixture_team_stats_pair(
    fixture: FixtureRecord,
    raw_home_stats: RawFixtureTeamStatsRecord,
    raw_away_stats: RawFixtureTeamStatsRecord,
) -> tuple[FixtureTeamStatsRecord, FixtureTeamStatsRecord]:
    home_team_id = _as_int(raw_home_stats.team_id)
    away_team_id = _as_int(raw_away_stats.team_id)
    if home_team_id != fixture.home_team_id:
        raise ValueError("home stats team_id does not match fixture.home_team_id")
    if away_team_id != fixture.away_team_id:
        raise ValueError("away stats team_id does not match fixture.away_team_id")
    if _as_str(raw_home_stats.venue).upper() != "HOME":
        raise ValueError("home stats venue must be HOME")
    if _as_str(raw_away_stats.venue).upper() != "AWAY":
        raise ValueError("away stats venue must be AWAY")

    def _points(goals_for: int, goals_against: int) -> int:
        if goals_for > goals_against:
            return 3
        if goals_for == goals_against:
            return 1
        return 0

    def _build(
        raw_stats: RawFixtureTeamStatsRecord,
        *,
        team_id: int,
        opponent_team_id: int,
        team_name: str,
        opponent_team_name: str,
        venue: str,
        goals_for: int,
        goals_against: int,
        opposing_raw: RawFixtureTeamStatsRecord,
    ) -> FixtureTeamStatsRecord:
        completeness = _stats_completeness(raw_stats)
        return FixtureTeamStatsRecord(
            fixture_id=fixture.fixture_id,
            competition_id=fixture.competition_id,
            season=fixture.season,
            kickoff_utc=fixture.kickoff_utc,
            as_of_date=fixture.as_of_date,
            team_id=team_id,
            opponent_team_id=opponent_team_id,
            team_name=team_name,
            opponent_team_name=opponent_team_name,
            venue=venue,  # type: ignore[arg-type]
            goals_for=goals_for,
            goals_against=goals_against,
            xg_for=_as_float(raw_stats.xg),
            xg_against=_as_float(opposing_raw.xg),
            shots_for=_as_int(raw_stats.shots),
            shots_against=_as_int(opposing_raw.shots),
            shots_on_for=_as_int(raw_stats.shots_on),
            shots_on_against=_as_int(opposing_raw.shots_on),
            corners_for=_as_int(raw_stats.corners),
            corners_against=_as_int(opposing_raw.corners),
            dangerous_attacks_for=_as_int(raw_stats.dangerous_attacks),
            dangerous_attacks_against=_as_int(opposing_raw.dangerous_attacks),
            possession=_as_float(raw_stats.possession),
            saves=_as_int(raw_stats.saves),
            red_cards=_as_int(raw_stats.red_cards, default=0) or 0,
            clean_sheet=goals_against == 0,
            failed_to_score=goals_for == 0,
            points=_points(goals_for, goals_against),
            data_quality_flag=quality_flag_from_score(completeness),
            data_completeness_score=round(completeness, 3),
            source=raw_stats.source,
        )

    home = _build(
        raw_home_stats,
        team_id=fixture.home_team_id,
        opponent_team_id=fixture.away_team_id,
        team_name=fixture.home_team_name,
        opponent_team_name=fixture.away_team_name,
        venue="HOME",
        goals_for=fixture.home_score,
        goals_against=fixture.away_score,
        opposing_raw=raw_away_stats,
    )
    away = _build(
        raw_away_stats,
        team_id=fixture.away_team_id,
        opponent_team_id=fixture.home_team_id,
        team_name=fixture.away_team_name,
        opponent_team_name=fixture.home_team_name,
        venue="AWAY",
        goals_for=fixture.away_score,
        goals_against=fixture.home_score,
        opposing_raw=raw_home_stats,
    )
    return home, away


def normalize_fixture_bundle(
    raw_fixture: RawFixtureRecord,
    *,
    raw_home_stats: RawFixtureTeamStatsRecord,
    raw_away_stats: RawFixtureTeamStatsRecord,
    raw_goal_events: list[RawGoalEventRecord] | None = None,
    raw_card_events: list[RawCardEventRecord] | None = None,
) -> NormalizedFixtureBundle:
    fixture = normalize_fixture(raw_fixture)
    goal_events = normalize_goal_events(raw_goal_events or [], fixture)
    card_events = normalize_card_events(raw_card_events or [], fixture)
    team_stats = normalize_fixture_team_stats_pair(fixture, raw_home_stats, raw_away_stats)

    home_goals_from_events = sum(1 for event in goal_events if event.team_id == fixture.home_team_id)
    away_goals_from_events = sum(1 for event in goal_events if event.team_id == fixture.away_team_id)
    goal_events_coherent = (
        home_goals_from_events == fixture.home_score
        and away_goals_from_events == fixture.away_score
    )
    notes: list[str] = []
    if fixture.home_score + fixture.away_score > 0 and not goal_events_coherent:
        notes.append("goal_event_score_mismatch")

    stats_completeness = mean(stat.data_completeness_score for stat in team_stats)
    event_score = 1.0 if goal_events_coherent else 0.0
    bundle_completeness = mean([fixture.data_completeness_score, stats_completeness, event_score])
    quality_flag: DataQualityFlag = quality_flag_from_score(
        bundle_completeness,
        inconsistent=not goal_events_coherent and (fixture.home_score + fixture.away_score > 0),
    )
    fixture = replace(
        fixture,
        goal_events_coherent=goal_events_coherent,
        data_quality_flag=quality_flag,
        data_completeness_score=round(bundle_completeness, 3),
        notes=tuple(notes),
    )
    return NormalizedFixtureBundle(
        fixture=fixture,
        team_stats=team_stats,
        goal_events=goal_events,
        card_events=card_events,
    )
