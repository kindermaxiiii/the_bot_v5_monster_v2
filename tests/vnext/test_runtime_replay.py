from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.vnext.ops.replay import replay_runtime_export
from app.vnext.runtime.exporter import export_cycle_jsonl
from app.vnext.runtime.models import RuntimeCounters, RuntimeCycleResult
from app.vnext.publication.models import PublicMatchPayload


def test_runtime_replay_reconstructs_counts_and_payloads() -> None:
    path = Path("exports") / "vnext" / f"test_runtime_replay_{uuid4().hex}.jsonl"
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
    cycle = RuntimeCycleResult(
        cycle_id=1,
        timestamp_utc=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
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
        refusal_summaries=("elite_thresholds_not_met",),
        fixture_audits=(
            {
                "fixture_id": 999,
                "match_label": "Lions vs Falcons",
                "competition_label": "Premier Test",
                "governed_public_status": "WATCHLIST",
                "publish_status": "PUBLISH",
                "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                "bookmaker_id": 1,
                "line": 1.5,
                "odds_decimal": 1.87,
                "governance_refusal_summary": [],
                "execution_refusal_summary": [],
                "source": "runtime_fixture_audit.v1",
            },
        ),
        publication_records=(
            {
                "cycle_id": 1,
                "timestamp_utc": datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc).isoformat(),
                "fixture_id": 999,
                "public_status": "WATCHLIST",
                "publish_channel": "WATCHLIST",
                "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                "bookmaker_id": 1,
                "bookmaker_name": "Book 1",
                "line": 1.5,
                "odds_decimal": 1.87,
                "public_summary": payload.public_summary,
                "disposition": "retained",
                "notified": False,
                "dedupe_origin": None,
                "source": "published_artifact.v1",
            },
        ),
        ops_flags=("state_store_unavailable",),
        notifier_mode="explicit_ack",
    )
    export_cycle_jsonl(path, cycle)

    replayed = replay_runtime_export(path)

    assert len(replayed) == 1
    assert replayed[0].pipeline_publish_count == 1
    assert replayed[0].unsent_shadow_count == 1
    assert replayed[0].payloads[0].fixture_id == 999
    assert replayed[0].refusal_summaries == ("elite_thresholds_not_met",)
    assert replayed[0].publication_records[0].template_key == "TEAM_TOTAL_AWAY_UNDER_CORE"
    assert replayed[0].ops_flags == ("state_store_unavailable",)
    assert replayed[0].notifier_mode == "explicit_ack"
