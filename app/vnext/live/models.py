from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from app.vnext.data.normalized_models import DataQualityFlag

MatchStatus = Literal["NS", "1H", "HT", "2H", "FT", "ET", "INT", "SUSP", "UNKNOWN"]
TimeBand = Literal["EARLY", "MID", "LATE"]
LeadingSide = Literal["HOME", "AWAY", "DRAW"]


@dataclass(slots=True)
class LiveSnapshot:
    fixture_id: int
    competition_id: int
    season: int
    kickoff_utc: datetime | None
    minute: int
    status: MatchStatus
    home_team_id: int
    away_team_id: int
    home_team_name: str
    away_team_name: str
    home_goals: int
    away_goals: int
    home_red_cards: int
    away_red_cards: int
    home_shots_total: int | None
    away_shots_total: int | None
    home_shots_on: int | None
    away_shots_on: int | None
    home_corners: int | None
    away_corners: int | None
    home_dangerous_attacks: int | None
    away_dangerous_attacks: int | None
    home_attacks: int | None
    away_attacks: int | None
    home_possession: float | None
    away_possession: float | None
    home_xg: float | None
    away_xg: float | None
    live_snapshot_quality_score: float
    data_quality_flag: DataQualityFlag
    source: str = "live_snapshot.v1"
    payload: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class LiveThreatBlock:
    home_threat_raw: float
    away_threat_raw: float
    threat_edge: float
    source: str = "live_threat.v1"


@dataclass(slots=True)
class LivePressureBlock:
    home_pressure_raw: float
    away_pressure_raw: float
    pressure_edge: float
    source: str = "live_pressure.v1"


@dataclass(slots=True)
class LiveBalanceBlock:
    home_balance_raw: float
    away_balance_raw: float
    balance_edge: float
    source: str = "live_balance.v1"


@dataclass(slots=True)
class LiveStateBlock:
    time_band: TimeBand
    leading_side: LeadingSide
    score_diff: int
    home_state_raw: float
    away_state_raw: float
    state_edge: float
    state_coherence_score: float
    source: str = "live_state.v1"


@dataclass(slots=True)
class LiveBreakEventsBlock:
    goal_scored: bool
    home_goal_scored: bool
    away_goal_scored: bool
    equalizer_event: bool
    lead_change_event: bool
    two_goal_gap_event: bool
    red_card_occurred: bool
    home_red_card: bool
    away_red_card: bool
    event_clarity_score: float
    source: str = "live_break_events.v1"


@dataclass(slots=True)
class LiveContextPack:
    current_snapshot: LiveSnapshot
    previous_snapshot: LiveSnapshot | None
    threat: LiveThreatBlock
    pressure: LivePressureBlock
    balance: LiveBalanceBlock
    state: LiveStateBlock
    break_events: LiveBreakEventsBlock
    source_version: str = "live_context.v1"
