from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol

from app.vnext.execution.models import MarketOffer
from app.vnext.live.models import LiveSnapshot
from app.vnext.notifier.contracts import NotifierAckRecord, NotifierMode, NotifierSendResult
from app.vnext.publication.models import PublicMatchPayload


SourceType = Literal["live", "snapshot"]
__all__ = [
    "LiveSource",
    "NotifierAckRecord",
    "NotifierMode",
    "NotifierSendResult",
    "RuntimeCounters",
    "RuntimeCycleResult",
    "SourceType",
    "VnextRuntimeConfig",
]


@dataclass(slots=True, frozen=True)
class VnextRuntimeConfig:
    max_active_matches: int = 18
    enable_publication_build: bool = True
    enable_notifier_send: bool = False
    dedupe_cooldown_seconds: int = 180
    source_type: SourceType = "live"
    source_name: str = "mock"


@dataclass(slots=True, frozen=True)
class RuntimeCounters:
    fixture_count_seen: int
    computed_publish_count: int
    deduped_count: int
    notified_count: int
    silent_count: int
    unsent_shadow_count: int = 0
    notifier_attempt_count: int = 0


@dataclass(slots=True, frozen=True)
class RuntimeCycleResult:
    cycle_id: int
    timestamp_utc: datetime
    counters: RuntimeCounters
    payloads: tuple[PublicMatchPayload, ...]
    refusal_summaries: tuple[str, ...] = ()
    fixture_audits: tuple[dict[str, object], ...] = field(default_factory=tuple)
    publication_records: tuple[dict[str, object], ...] = field(default_factory=tuple)
    ops_flags: tuple[str, ...] = field(default_factory=tuple)
    notifier_mode: NotifierMode = "none"
    source: str = "runtime_cycle.vnext.v1"


class LiveSource(Protocol):
    def fetch_live_snapshots(self, max_matches: int) -> tuple[LiveSnapshot, ...]:
        ...

    def fetch_market_offers(self, fixture_id: int) -> tuple[MarketOffer, ...]:
        ...
