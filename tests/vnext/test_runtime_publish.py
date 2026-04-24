from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from app.vnext.ops.publisher import PublishError, publish_and_validate
from app.vnext.runtime.exporter import export_cycle_jsonl
from app.vnext.runtime.models import RuntimeCounters, RuntimeCycleResult
from app.vnext.publication.models import PublicMatchPayload


def _build_good_export(path: Path) -> None:
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
        refusal_summaries=(),
    )
    export_cycle_jsonl(path, cycle)


def test_publish_and_validate_success(tmp_path: Path) -> None:
    export_path = tmp_path / f"export_{uuid4().hex}.jsonl"
    live_path = tmp_path / "live" / "live_bot.jsonl"
    _build_good_export(export_path)

    result = publish_and_validate(export_path, live_path)

    assert live_path.exists()
    assert result["rows_with_missing_audits"] == 0


def test_publish_and_validate_fails_on_missing_audits(tmp_path: Path) -> None:
    export_path = tmp_path / f"export_minimal_{uuid4().hex}.jsonl"
    row = {
        "cycle_id": 1,
        "timestamp_utc": datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc).isoformat(),
        "fixture_count_seen": 1,
        "pipeline_publish_count": 0,
        "fixture_audits": [
            {
                "fixture_id": 999,
                "match_label": "Lions vs Falcons",
                "competition_label": "Premier Test",
                "governed_public_status": "WATCHLIST",
                "publish_status": "PUBLISH",
            }
        ],
    }
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    live_path = tmp_path / "live" / "live_bot.jsonl"

    with pytest.raises(PublishError):
        publish_and_validate(export_path, live_path)

        