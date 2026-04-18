from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.vnext.ops.models import (
    DedupRecord,
    PublishedArtifactRecord,
    RuntimeCycleAuditRecord,
    RuntimeFixtureAuditRecord,
)
from app.vnext.ops.store import VnextOpsStore
from app.vnext.publication.models import PublicMatchPayload


def test_ops_store_roundtrip_cycle_and_publication_and_dedup() -> None:
    root = Path("exports") / "vnext" / f"test_ops_store_{uuid4().hex}"
    store = VnextOpsStore(root)
    timestamp = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
    payload = PublicMatchPayload(
        fixture_id=999,
        public_status="WATCHLIST",
        publish_channel="WATCHLIST",
        match_label="Lions vs Falcons",
        competition_label="Premier Test",
        market_label="TEAM_TOTAL",
        line_label="Team Total Away Under Core",
        bookmaker_label="Book 1",
        odds_label="1.87",
        confidence_band="HIGH",
        public_summary="TEAM_TOTAL Team Total Away Under Core @ Book 1 1.87",
    )
    fixture_audit = RuntimeFixtureAuditRecord(
        fixture_id=999,
        match_label="Lions vs Falcons",
        competition_label="Premier Test",
        governed_public_status="WATCHLIST",
        publish_status="PUBLISH",
        template_key="TEAM_TOTAL_AWAY_UNDER_CORE",
        bookmaker_id=1,
        line=1.5,
        odds_decimal=1.87,
    )
    publication = PublishedArtifactRecord(
        cycle_id=1,
        timestamp_utc=timestamp,
        fixture_id=999,
        public_status="WATCHLIST",
        publish_channel="WATCHLIST",
        template_key="TEAM_TOTAL_AWAY_UNDER_CORE",
        bookmaker_id=1,
        bookmaker_name="Book 1",
        line=1.5,
        odds_decimal=1.87,
        public_summary=payload.public_summary,
        disposition="retained",
        notified=False,
    )
    cycle = RuntimeCycleAuditRecord(
        cycle_id=1,
        timestamp_utc=timestamp,
        fixture_count_seen=1,
        pipeline_publish_count=1,
        deduped_count=0,
        notified_count=0,
        silent_count=1,
        unsent_shadow_count=1,
        notifier_attempt_count=0,
        payloads=(payload,),
        refusal_summaries=("elite_thresholds_not_met",),
        fixture_audits=(fixture_audit,),
        publication_records=(publication,),
        notifier_mode="aggregate",
    )
    dedup = DedupRecord(key="k1", last_seen_utc=timestamp)

    store.append_cycle_audit(cycle)
    store.append_publication_records((publication,))
    store.save_dedup_records((dedup,))

    loaded_cycles = store.list_cycle_audits()
    loaded_publications = store.list_publication_records()
    loaded_dedup = store.load_dedup_records()

    assert len(loaded_cycles) == 1
    assert loaded_cycles[0].payloads[0].match_label == "Lions vs Falcons"
    assert loaded_cycles[0].unsent_shadow_count == 1
    assert loaded_cycles[0].publication_records[0].template_key == "TEAM_TOTAL_AWAY_UNDER_CORE"
    assert loaded_cycles[0].notifier_mode == "aggregate"
    assert len(loaded_publications) == 1
    assert loaded_publications[0].template_key == "TEAM_TOTAL_AWAY_UNDER_CORE"
    assert len(loaded_dedup) == 1
    assert loaded_dedup[0].key == "k1"


def test_ops_store_dedup_save_falls_back_when_atomic_replace_fails(monkeypatch) -> None:
    root = Path("exports") / "vnext" / f"test_ops_store_fallback_{uuid4().hex}"
    store = VnextOpsStore(root)
    timestamp = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)

    def fail_replace(_src, _dst) -> None:
        raise PermissionError("simulated_replace_failure")

    monkeypatch.setattr(os, "replace", fail_replace)

    store.save_dedup_records((DedupRecord(key="k1", last_seen_utc=timestamp),))
    loaded = store.load_dedup_records()

    assert len(loaded) == 1
    assert loaded[0].key == "k1"


def test_ops_store_probe_write_access_creates_root() -> None:
    store = VnextOpsStore(Path("exports") / "vnext" / f"test_ops_store_probe_{uuid4().hex}")

    store.probe_write_access()

    assert store.root.exists()
