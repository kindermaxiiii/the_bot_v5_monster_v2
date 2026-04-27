from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey


@dataclass(slots=True, frozen=True)
class StatisticalThesis:
    event_id: int
    thesis_key: ThesisKey
    strength: float
    confidence: float
    rationale: tuple[str, ...] = ()
    features: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class MarketIntent:
    event_id: int
    thesis_key: ThesisKey
    family: MarketFamily
    side: MarketSide
    period: Period
    team_role: TeamRole
    line: float | None
    rationale: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class BookOffer:
    event_id: int
    bookmaker_id: int | None
    bookmaker_name: str
    family: MarketFamily
    side: MarketSide
    period: Period
    team_role: TeamRole
    line: float | None
    odds_decimal: float
    source_timestamp_utc: str | None
    freshness_seconds: int | None
    raw_ref: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ExecutableBet:
    event_id: int
    thesis_key: ThesisKey
    family: MarketFamily
    side: MarketSide
    period: Period
    team_role: TeamRole
    line: float | None
    bookmaker_id: int | None
    bookmaker_name: str
    odds_decimal: float
    p_real: float
    p_implied: float
    edge: float
    ev: float
    score_stat: float
    score_exec: float
    score_final: float
    rationale: tuple[str, ...] = ()