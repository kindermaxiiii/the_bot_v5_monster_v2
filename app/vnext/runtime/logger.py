from __future__ import annotations

from app.vnext.runtime.models import RuntimeCycleResult


def format_cycle_log(cycle: RuntimeCycleResult) -> str:
    counters = cycle.counters
    base = (
        "vnext_cycle "
        f"id={cycle.cycle_id} "
        f"seen={counters.fixture_count_seen} "
        f"publishable={counters.computed_publish_count} "
        f"deduped={counters.deduped_count} "
        f"shadow_unsent={counters.unsent_shadow_count} "
        f"notify_attempts={counters.notifier_attempt_count} "
        f"sent={counters.notified_count} "
        f"silent={counters.silent_count}"
    )
    if cycle.ops_flags:
        return f"{base} ops_flags={list(cycle.ops_flags)}"
    return base
