from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from app.vnext.ops.store import VnextOpsStore
from app.vnext.runtime.deduper import Deduper
from app.vnext.runtime.demo_data import build_demo_prior_provider, build_demo_snapshot_source
from app.vnext.runtime.models import VnextRuntimeConfig
from app.vnext.runtime.runner import run_vnext_cycle


def test_runner_persists_cycle_and_dedup_state_across_restart() -> None:
    store = VnextOpsStore(Path("exports") / "vnext" / f"test_runner_persistence_{uuid4().hex}")
    source = build_demo_snapshot_source()
    prior_provider = build_demo_prior_provider()
    config = VnextRuntimeConfig(
        max_active_matches=1,
        enable_publication_build=True,
        enable_notifier_send=False,
        dedupe_cooldown_seconds=180,
        source_type="snapshot",
        source_name="demo",
    )

    first_deduper = Deduper(cooldown_seconds=180)
    first_cycle = run_vnext_cycle(
        cycle_id=1,
        config=config,
        source=source,
        prior_result_provider=prior_provider,
        deduper=first_deduper,
        previous_snapshots={},
        ops_store=store,
        now=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
    )

    second_deduper = Deduper(cooldown_seconds=180)
    second_cycle = run_vnext_cycle(
        cycle_id=2,
        config=config,
        source=source,
        prior_result_provider=prior_provider,
        deduper=second_deduper,
        previous_snapshots={},
        ops_store=store,
        now=datetime(2026, 4, 14, 12, 1, tzinfo=timezone.utc),
    )

    stored_cycles = store.list_cycle_audits()
    stored_publications = store.list_publication_records()
    stored_dedup = store.load_dedup_records()

    assert first_cycle.counters.computed_publish_count == 1
    assert second_cycle.counters.computed_publish_count == 1
    assert second_cycle.counters.deduped_count == 1
    assert len(stored_cycles) == 2
    assert stored_publications
    assert stored_dedup
