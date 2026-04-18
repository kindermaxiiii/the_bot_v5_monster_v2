from __future__ import annotations

from collections import Counter

from app.vnext.ops.models import RuntimeCycleAuditRecord


def build_runtime_report(
    cycles: tuple[RuntimeCycleAuditRecord, ...],
) -> dict[str, object]:
    refusal_counter: Counter[str] = Counter()
    fixture_counter: Counter[int] = Counter()
    ops_flag_counter: Counter[str] = Counter()
    publication_disposition_counter: Counter[str] = Counter()
    dedupe_origin_counter: Counter[str] = Counter()
    notifier_mode_counter: Counter[str] = Counter()

    total_publishable = 0
    total_retained_payloads = 0
    total_deduped = 0
    total_notified = 0
    total_unsent_shadow = 0
    total_notifier_attempts = 0
    total_acked_records = 0

    for cycle in cycles:
        total_publishable += cycle.pipeline_publish_count
        total_retained_payloads += len(cycle.payloads)
        total_deduped += cycle.deduped_count
        total_notified += cycle.notified_count
        total_unsent_shadow += cycle.unsent_shadow_count
        total_notifier_attempts += cycle.notifier_attempt_count
        total_acked_records += sum(1 for record in cycle.publication_records if record.notified)
        refusal_counter.update(cycle.refusal_summaries)
        ops_flag_counter.update(cycle.ops_flags)
        notifier_mode_counter.update((cycle.notifier_mode,))
        fixture_counter.update(payload.fixture_id for payload in cycle.payloads)
        publication_disposition_counter.update(record.disposition for record in cycle.publication_records)
        dedupe_origin_counter.update(
            record.dedupe_origin
            for record in cycle.publication_records
            if record.dedupe_origin is not None
        )

    return {
        "cycle_count": len(cycles),
        "publishable_count": total_publishable,
        "retained_payload_count": total_retained_payloads,
        "deduped_count": total_deduped,
        "notified_count": total_notified,
        "acked_record_count": total_acked_records,
        "unsent_shadow_count": total_unsent_shadow,
        "notifier_attempt_count": total_notifier_attempts,
        "notifier_mode_counts": notifier_mode_counter.most_common(5),
        "top_refusal_summaries": refusal_counter.most_common(5),
        "top_ops_flags": ops_flag_counter.most_common(5),
        "publication_dispositions": publication_disposition_counter.most_common(5),
        "dedupe_origin_counts": dedupe_origin_counter.most_common(5),
        "top_fixtures": fixture_counter.most_common(5),
    }


def format_runtime_report(report: dict[str, object]) -> str:
    return (
        "vnext_runtime_report "
        f"cycles={report.get('cycle_count', 0)} "
        f"publishable={report.get('publishable_count', 0)} "
        f"retained={report.get('retained_payload_count', 0)} "
        f"deduped={report.get('deduped_count', 0)} "
        f"shadow_unsent={report.get('unsent_shadow_count', 0)} "
        f"notify_attempts={report.get('notifier_attempt_count', 0)} "
        f"notified={report.get('notified_count', 0)} "
        f"acked_records={report.get('acked_record_count', 0)} "
        f"notifier_modes={report.get('notifier_mode_counts', [])} "
        f"top_refusals={report.get('top_refusal_summaries', [])} "
        f"top_ops_flags={report.get('top_ops_flags', [])} "
        f"top_fixtures={report.get('top_fixtures', [])}"
    )
