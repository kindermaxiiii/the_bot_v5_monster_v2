from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from app.vnext.board.models import BoardEntry, BoardSnapshot
from app.vnext.execution.models import ExecutionCandidate, ExecutableMarketSelectionResult, MarketOffer
from app.vnext.selection.models import MatchBestCandidate

PublishStatus = Literal["PUBLISH", "DO_NOT_PUBLISH"]


@dataclass(slots=True, frozen=True)
class PublishDecision:
    governed_public_status: Literal["NO_BET", "WATCHLIST", "ELITE"]
    publish_status: PublishStatus
    governance_refusal_summary: tuple[str, ...] = ()
    execution_refusal_summary: tuple[str, ...] = ()
    source: str = "publish_decision.v1"


@dataclass(slots=True, frozen=True)
class PublishableMatchResult:
    fixture_id: int
    match_label: str
    competition_label: str
    governed_public_status: Literal["NO_BET", "WATCHLIST", "ELITE"]
    publish_status: PublishStatus
    best_candidate: MatchBestCandidate | None
    execution_candidate: ExecutionCandidate | None
    selected_offer: MarketOffer | None
    governance_refusal_summary: tuple[str, ...] = ()
    execution_refusal_summary: tuple[str, ...] = ()
    source: str = "publishable_match.v1"


@dataclass(slots=True, frozen=True)
class PipelineSnapshot:
    publish_count: int
    do_not_publish_count: int
    results: tuple[PublishableMatchResult, ...]
    governed_status_counts: dict[str, int] = field(default_factory=dict)
    source_version: str = "pipeline_snapshot.v1"
    notes: tuple[str, ...] = field(default_factory=tuple)
