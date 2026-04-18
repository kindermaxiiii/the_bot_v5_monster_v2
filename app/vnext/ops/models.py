from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from app.vnext.notifier.contracts import NotifierMode
from app.vnext.publication.models import PublicMatchPayload


DedupOrigin = Literal["deduped_persistent", "deduped_in_memory"]
ArtifactDisposition = Literal["retained", "deduped"]


@dataclass(slots=True, frozen=True)
class DedupRecord:
    key: str
    last_seen_utc: datetime
    source: str = "ops_dedup_record.v1"


@dataclass(slots=True, frozen=True)
class RuntimeFixtureAuditRecord:
    fixture_id: int
    match_label: str
    competition_label: str
    governed_public_status: str
    publish_status: str
    template_key: str | None
    bookmaker_id: int | None
    line: float | None
    odds_decimal: float | None
    governance_refusal_summary: tuple[str, ...] = ()
    execution_refusal_summary: tuple[str, ...] = ()
    candidate_not_selectable_reason: str | None = None
    translated_candidate_count: int | None = None
    selectable_candidate_count: int | None = None
    best_candidate_family: str | None = None
    best_candidate_exists: bool | None = None
    best_candidate_selectable: bool | None = None
    best_candidate_blockers: tuple[str, ...] = ()
    distinct_candidate_blockers_summary: tuple[str, ...] = ()
    execution_candidate_count: int | None = None
    execution_selectable_count: int | None = None
    attempted_template_keys: tuple[str, ...] = ()
    offer_present_template_keys: tuple[str, ...] = ()
    missing_offer_template_keys: tuple[str, ...] = ()
    blocked_execution_reasons_summary: tuple[str, ...] = ()
    final_execution_refusal_reason: str | None = None
    publishability_score: float | None = None
    template_binding_score: float | None = None
    bookmaker_diversity_score: float | None = None
    price_integrity_score: float | None = None
    retrievability_score: float | None = None
    source: str = "runtime_fixture_audit.v1"


@dataclass(slots=True, frozen=True)
class PublishedArtifactRecord:
    cycle_id: int
    timestamp_utc: datetime
    fixture_id: int
    public_status: str
    publish_channel: str
    template_key: str | None
    bookmaker_id: int | None
    bookmaker_name: str | None
    line: float | None
    odds_decimal: float | None
    public_summary: str
    disposition: ArtifactDisposition
    notified: bool
    dedupe_origin: DedupOrigin | None = None
    source: str = "published_artifact.v1"


@dataclass(slots=True, frozen=True)
class RuntimeCycleAuditRecord:
    cycle_id: int
    timestamp_utc: datetime
    fixture_count_seen: int
    pipeline_publish_count: int
    deduped_count: int
    notified_count: int
    silent_count: int
    unsent_shadow_count: int = 0
    notifier_attempt_count: int = 0
    payloads: tuple[PublicMatchPayload, ...] = ()
    refusal_summaries: tuple[str, ...] = ()
    fixture_audits: tuple[RuntimeFixtureAuditRecord, ...] = ()
    publication_records: tuple[PublishedArtifactRecord, ...] = ()
    ops_flags: tuple[str, ...] = ()
    notifier_mode: NotifierMode = "none"
    source: str = "runtime_cycle_audit.v1"
