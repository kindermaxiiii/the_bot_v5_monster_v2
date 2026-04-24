from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.vnext.runtime.exporter import export_cycle_jsonl
from app.vnext.runtime.models import RuntimeCounters, RuntimeCycleResult
from app.vnext.ops.models import RuntimeFixtureAuditRecord, PublishedArtifactRecord
from app.vnext.publication.models import PublicMatchPayload


def test_export_cycle_jsonl_dataclass_audits() -> None:
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
        attempted_template_keys=("TEAM_TOTAL_AWAY_UNDER_CORE",),
        offer_present_template_keys=("TEAM_TOTAL_AWAY_UNDER_CORE",),
        missing_offer_template_keys=(),
        blocked_execution_reasons_summary=("market_unavailable",),
        publishability_score=0.57,
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
    cycle = RuntimeCycleResult(
        cycle_id=1,
        timestamp_utc=timestamp,
        counters=RuntimeCounters(
            fixture_count_seen=1,
            computed_publish_count=1,
            deduped_count=0,
            notified_count=0,
            silent_count=1,
            unsent_shadow_count=1,
            notifier_attempt_count=0,
        ),
        payloads=(payload,),
        refusal_summaries=(),
        fixture_audits=(fixture_audit,),
        publication_records=(publication,),
        notifier_mode="aggregate",
    )

    path = Path(f"exports/vnext/test_runtime_exporter_dataclass_{uuid4().hex}.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    export_cycle_jsonl(path, cycle)

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["fixture_audits"][0]["attempted_template_keys"] == ["TEAM_TOTAL_AWAY_UNDER_CORE"]
    assert data["fixture_audits"][0]["publishability_score"] == 0.57
    assert data["publication_records"][0]["template_key"] == "TEAM_TOTAL_AWAY_UNDER_CORE"