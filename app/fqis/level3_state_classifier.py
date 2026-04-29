from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Level3State(str, Enum):
    REAL_TRADE_READY = "REAL_TRADE_READY"
    EVENTS_ONLY_RESEARCH_READY = "EVENTS_ONLY_RESEARCH_READY"
    SCORE_ONLY = "SCORE_ONLY"


@dataclass(frozen=True)
class Level3Classification:
    state: Level3State
    production_eligible: bool
    research_eligible: bool
    live_staking_allowed: bool
    reason: str


def classify_level3_state(
    *,
    events_available: bool,
    stats_available: bool,
    promotion_allowed: bool = False,
) -> Level3Classification:
    if events_available and stats_available:
        return Level3Classification(
            state=Level3State.REAL_TRADE_READY,
            production_eligible=True,
            research_eligible=True,
            live_staking_allowed=False if not promotion_allowed else False,
            reason="events_and_stats_available_but_live_staking_disabled",
        )

    if events_available and not stats_available:
        return Level3Classification(
            state=Level3State.EVENTS_ONLY_RESEARCH_READY,
            production_eligible=False,
            research_eligible=True,
            live_staking_allowed=False,
            reason="events_available_without_stats_research_only",
        )

    return Level3Classification(
        state=Level3State.SCORE_ONLY,
        production_eligible=False,
        research_eligible=False,
        live_staking_allowed=False,
        reason="missing_live_events_rejected",
    )
