from __future__ import annotations

from datetime import datetime, timezone

from app.vnext.ops.models import PublishedArtifactRecord, RuntimeCycleAuditRecord
from app.vnext.ops.reporter import build_runtime_report
from app.vnext.publication.models import PublicMatchPayload


def test_runtime_report_uses_ops_flags_and_publication_records() -> None:
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
    retained = PublishedArtifactRecord(
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
        notified=True,
    )
    deduped = PublishedArtifactRecord(
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
        disposition="deduped",
        notified=False,
        dedupe_origin="deduped_persistent",
    )
    cycle = RuntimeCycleAuditRecord(
        cycle_id=1,
        timestamp_utc=timestamp,
        fixture_count_seen=1,
        pipeline_publish_count=2,
        deduped_count=1,
        notified_count=0,
        silent_count=2,
        unsent_shadow_count=1,
        notifier_attempt_count=0,
        payloads=(payload,),
        refusal_summaries=("elite_thresholds_not_met",),
        publication_records=(retained, deduped),
        ops_flags=("state_store_unavailable",),
        notifier_mode="explicit_ack",
    )

    report = build_runtime_report((cycle,))

    assert report["unsent_shadow_count"] == 1
    assert report["notifier_attempt_count"] == 0
    assert report["acked_record_count"] == 1
    assert report["notifier_mode_counts"] == [("explicit_ack", 1)]
    assert report["top_ops_flags"] == [("state_store_unavailable", 1)]
    assert report["publication_dispositions"] == [("retained", 1), ("deduped", 1)]
    assert report["dedupe_origin_counts"] == [("deduped_persistent", 1)]
