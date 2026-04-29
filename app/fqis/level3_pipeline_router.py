from __future__ import annotations

from dataclasses import dataclass

from app.fqis.level3_state_classifier import Level3State


@dataclass(frozen=True)
class Level3PipelineRoute:
    pipeline: str
    production_allowed: bool
    research_allowed: bool
    reject: bool
    live_staking_allowed: bool
    reason: str


def route_level3_pipeline(
    *,
    state: str,
    promotion_allowed: bool = False,
) -> Level3PipelineRoute:
    if state == Level3State.REAL_TRADE_READY.value:
        return Level3PipelineRoute(
            pipeline="production",
            production_allowed=True,
            research_allowed=True,
            reject=False,
            live_staking_allowed=False,
            reason="real_trade_ready_but_live_staking_disabled",
        )

    if state == Level3State.EVENTS_ONLY_RESEARCH_READY.value:
        return Level3PipelineRoute(
            pipeline="research",
            production_allowed=False,
            research_allowed=True,
            reject=False,
            live_staking_allowed=False,
            reason="events_only_research_only",
        )

    return Level3PipelineRoute(
        pipeline="reject",
        production_allowed=False,
        research_allowed=False,
        reject=True,
        live_staking_allowed=False,
        reason="score_only_or_unknown_rejected",
    )
