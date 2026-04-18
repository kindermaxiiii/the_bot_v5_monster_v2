from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from app.vnext.board.arbiter import build_board_snapshot
from app.vnext.execution.selector import build_executable_market_selection
from app.vnext.notifier.contracts import NotifierAckRecord, NotifierInput, NotifierMode, send_with_notifier
from app.vnext.ops.models import PublishedArtifactRecord, RuntimeCycleAuditRecord, RuntimeFixtureAuditRecord
from app.vnext.ops.store import VnextOpsStore
from app.vnext.pipeline.builder import build_publishable_pipeline
from app.vnext.pipeline.models import PublishableMatchResult
from app.vnext.posterior.builder import build_scenario_posterior_result
from app.vnext.publication.builder import build_publication_bundles_from_results
from app.vnext.publication.formatter import build_public_payload
from app.vnext.runtime.deduper import Deduper
from app.vnext.live.models import LiveSnapshot
from app.vnext.runtime.models import (
    LiveSource,
    RuntimeCycleResult,
    RuntimeCounters,
    VnextRuntimeConfig,
)
from app.vnext.scenario.models import ScenarioPriorResult
from app.vnext.selection.models import MatchMarketSelectionResult
from app.vnext.selection.match_selector import build_match_market_selection_result
from app.vnext.execution.models import ExecutableMarketSelectionResult, ExecutionCandidate


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _collect_refusals(results) -> tuple[str, ...]:
    refusals = set()
    for result in results:
        refusals.update(result.governance_refusal_summary)
        refusals.update(result.execution_refusal_summary)
    return tuple(sorted(refusals))


def _selection_observation(
    selection_result: MatchMarketSelectionResult | None,
    result: PublishableMatchResult,
) -> dict[str, object]:
    if selection_result is None:
        return {}

    candidates = selection_result.translation_result.candidates
    selectable = tuple(
        candidate
        for candidate in candidates
        if candidate.exists and candidate.is_selectable and candidate.family != "RESULT"
    )
    best_candidate = selection_result.best_candidate.candidate if selection_result.best_candidate is not None else None
    candidate_not_selectable_reason = None
    if "candidate_not_selectable" in result.governance_refusal_summary:
        if selection_result.no_selection_reason == "all_candidates_blocked":
            candidate_not_selectable_reason = "all_candidates_blocked"
        elif best_candidate is not None and not best_candidate.is_selectable:
            candidate_not_selectable_reason = "best_candidate_not_selectable"

    distinct_blockers = tuple(
        sorted(
            {
                blocker.code
                for candidate in candidates
                for blocker in candidate.blockers
            }
        )
    )
    best_candidate_blockers = ()
    best_candidate_exists = None
    best_candidate_selectable = None
    best_candidate_family = None
    if best_candidate is not None:
        best_candidate_blockers = tuple(blocker.code for blocker in best_candidate.blockers)
        best_candidate_exists = best_candidate.exists
        best_candidate_selectable = best_candidate.is_selectable
        best_candidate_family = best_candidate.family

    return {
        "candidate_not_selectable_reason": candidate_not_selectable_reason,
        "translated_candidate_count": len(candidates),
        "selectable_candidate_count": len(selectable),
        "best_candidate_family": best_candidate_family,
        "best_candidate_exists": best_candidate_exists,
        "best_candidate_selectable": best_candidate_selectable,
        "best_candidate_blockers": best_candidate_blockers,
        "distinct_candidate_blockers_summary": distinct_blockers,
    }


def _publishability_probe_candidate(
    execution_result: ExecutableMarketSelectionResult | None,
) -> ExecutionCandidate | None:
    if execution_result is None:
        return None
    if execution_result.execution_candidate is not None:
        return execution_result.execution_candidate

    offer_present = tuple(
        candidate
        for candidate in execution_result.alternatives
        if candidate.offer_exists
    )
    if not offer_present:
        return None
    primary_template = execution_result.template_key
    for candidate in offer_present:
        if candidate.template_key == primary_template:
            return candidate
    return max(
        offer_present,
        key=lambda candidate: (candidate.quality.publishability_score, candidate.selection_score),
    )


def _execution_observation(
    execution_result: ExecutableMarketSelectionResult | None,
    result: PublishableMatchResult,
) -> dict[str, object]:
    if execution_result is None or result.governed_public_status == "NO_BET":
        return {}

    candidates = execution_result.alternatives
    probe_candidate = _publishability_probe_candidate(execution_result)
    final_execution_refusal_reason = execution_result.no_executable_vehicle_reason
    if final_execution_refusal_reason is None and result.execution_refusal_summary:
        final_execution_refusal_reason = result.execution_refusal_summary[0]

    return {
        "execution_candidate_count": len(candidates),
        "execution_selectable_count": sum(1 for candidate in candidates if candidate.is_selectable),
        "attempted_template_keys": tuple(candidate.template_key for candidate in candidates),
        "offer_present_template_keys": tuple(candidate.template_key for candidate in candidates if candidate.offer_exists),
        "missing_offer_template_keys": tuple(candidate.template_key for candidate in candidates if not candidate.offer_exists),
        "blocked_execution_reasons_summary": tuple(
            sorted(
                {
                    blocker.code
                    for candidate in candidates
                    for blocker in candidate.blockers
                }
            )
        ),
        "final_execution_refusal_reason": final_execution_refusal_reason,
        "publishability_score": probe_candidate.quality.publishability_score if probe_candidate is not None else None,
        "template_binding_score": probe_candidate.quality.template_binding_score if probe_candidate is not None else None,
        "bookmaker_diversity_score": probe_candidate.quality.bookmaker_diversity_score if probe_candidate is not None else None,
        "price_integrity_score": probe_candidate.quality.price_integrity_score if probe_candidate is not None else None,
        "retrievability_score": probe_candidate.quality.retrievability_score if probe_candidate is not None else None,
    }


def _build_fixture_audit(
    result: PublishableMatchResult,
    *,
    selection_result: MatchMarketSelectionResult | None,
    execution_result: ExecutableMarketSelectionResult | None,
) -> RuntimeFixtureAuditRecord:
    candidate = result.execution_candidate
    offer = result.selected_offer
    return RuntimeFixtureAuditRecord(
        fixture_id=result.fixture_id,
        match_label=result.match_label,
        competition_label=result.competition_label,
        governed_public_status=result.governed_public_status,
        publish_status=result.publish_status,
        template_key=candidate.template_key if candidate is not None else None,
        bookmaker_id=offer.bookmaker_id if offer is not None else None,
        line=offer.line if offer is not None else None,
        odds_decimal=offer.odds_decimal if offer is not None else None,
        governance_refusal_summary=result.governance_refusal_summary,
        execution_refusal_summary=result.execution_refusal_summary,
        **_selection_observation(selection_result, result),
        **_execution_observation(execution_result, result),
    )


def _build_publication_record(
    *,
    cycle_id: int,
    timestamp: datetime,
    result: PublishableMatchResult,
    disposition: str,
    notified: bool,
    dedupe_origin: str | None = None,
) -> PublishedArtifactRecord:
    payload = build_public_payload(result)
    candidate = result.execution_candidate
    offer = result.selected_offer
    public_status = result.governed_public_status
    publish_channel = payload.publish_channel if payload is not None else public_status
    public_summary = payload.public_summary if payload is not None else result.match_label
    return PublishedArtifactRecord(
        cycle_id=cycle_id,
        timestamp_utc=timestamp,
        fixture_id=result.fixture_id,
        public_status=public_status,
        publish_channel=publish_channel,
        template_key=candidate.template_key if candidate is not None else None,
        bookmaker_id=offer.bookmaker_id if offer is not None else None,
        bookmaker_name=offer.bookmaker_name if offer is not None else None,
        line=offer.line if offer is not None else None,
        odds_decimal=offer.odds_decimal if offer is not None else None,
        public_summary=public_summary,
        disposition=disposition,  # type: ignore[arg-type]
        notified=notified,
        dedupe_origin=dedupe_origin,  # type: ignore[arg-type]
    )


def _build_publication_ack(record: PublishedArtifactRecord) -> NotifierAckRecord:
    return NotifierAckRecord(
        fixture_id=record.fixture_id,
        public_status=record.public_status,
        template_key=record.template_key,
        bookmaker_id=record.bookmaker_id,
        line=record.line,
        odds_decimal=record.odds_decimal,
    )


def _apply_notifier_acks(
    records: list[PublishedArtifactRecord],
    acked_records: tuple[NotifierAckRecord, ...],
) -> list[PublishedArtifactRecord]:
    if not acked_records:
        return records
    acked = set(acked_records)
    updated: list[PublishedArtifactRecord] = []
    for record in records:
        is_acked = record.disposition == "retained" and _build_publication_ack(record) in acked
        updated.append(
            PublishedArtifactRecord(
                cycle_id=record.cycle_id,
                timestamp_utc=record.timestamp_utc,
                fixture_id=record.fixture_id,
                public_status=record.public_status,
                publish_channel=record.publish_channel,
                template_key=record.template_key,
                bookmaker_id=record.bookmaker_id,
                bookmaker_name=record.bookmaker_name,
                line=record.line,
                odds_decimal=record.odds_decimal,
                public_summary=record.public_summary,
                disposition=record.disposition,
                notified=is_acked,
                dedupe_origin=record.dedupe_origin,
                source=record.source,
            )
        )
    return updated


def run_vnext_cycle(
    *,
    cycle_id: int,
    config: VnextRuntimeConfig,
    source: LiveSource,
    prior_result_provider: Callable[[LiveSnapshot], ScenarioPriorResult],
    deduper: Deduper,
    previous_snapshots: dict[int, LiveSnapshot] | None = None,
    notifier: NotifierInput | None = None,
    ops_store: VnextOpsStore | None = None,
    now: datetime | None = None,
) -> RuntimeCycleResult:
    timestamp = now or _now()
    ops_flags: list[str] = []
    if ops_store is not None and not deduper.persistent_state_loaded:
        try:
            deduper.load_records(ops_store.load_dedup_records(), timestamp)
        except Exception:
            ops_flags.append("state_store_unavailable")

    snapshots = source.fetch_live_snapshots(config.max_active_matches)
    selections = []
    executions = []

    for snapshot in snapshots:
        prior_result = prior_result_provider(snapshot)
        previous = None
        if previous_snapshots is not None:
            previous = previous_snapshots.get(snapshot.fixture_id)
            previous_snapshots[snapshot.fixture_id] = snapshot
        posterior = build_scenario_posterior_result(prior_result, snapshot, previous)
        selection = build_match_market_selection_result(posterior)
        offers = source.fetch_market_offers(snapshot.fixture_id)
        execution = build_executable_market_selection(selection, offers)
        selections.append(selection)
        executions.append(execution)

    board_snapshot = build_board_snapshot(tuple(selections))
    pipeline_snapshot = build_publishable_pipeline(board_snapshot, tuple(executions))

    publishable_results = tuple(
        result for result in pipeline_snapshot.results if result.publish_status == "PUBLISH"
    )
    computed_publish_count = len(publishable_results)
    deduped_results = []
    deduped_count = 0
    publication_records: list[PublishedArtifactRecord] = []
    for result in publishable_results:
        duplicate_origin = deduper.duplicate_origin(result, timestamp)
        if duplicate_origin is not None:
            deduped_count += 1
            publication_records.append(
                _build_publication_record(
                    cycle_id=cycle_id,
                    timestamp=timestamp,
                    result=result,
                    disposition="deduped",
                    notified=False,
                    dedupe_origin=duplicate_origin,
                )
            )
            continue
        deduper.mark_seen(result, timestamp)
        deduped_results.append(result)
        publication_records.append(
            _build_publication_record(
                cycle_id=cycle_id,
                timestamp=timestamp,
                result=result,
                disposition="retained",
                notified=False,
            )
        )

    payloads = ()
    bundles = ()
    if config.enable_publication_build and deduped_results:
        bundles = build_publication_bundles_from_results(tuple(deduped_results))
        payloads = tuple(payload for bundle in bundles for payload in bundle.payloads)

    notified_count = 0
    notifier_attempt_count = 0
    notifier_mode: NotifierMode = "none"
    if config.enable_notifier_send and notifier is not None and bundles:
        notifier_send = send_with_notifier(notifier, bundles)
        notifier_attempt_count = notifier_send.attempted_count
        notified_count = notifier_send.notified_count
        notifier_mode = notifier_send.mode
        publication_records = _apply_notifier_acks(publication_records, notifier_send.acked_records)

    unsent_shadow_count = len(payloads) if payloads and not config.enable_notifier_send else 0
    silent_count = computed_publish_count - notified_count
    counters = RuntimeCounters(
        fixture_count_seen=len(snapshots),
        computed_publish_count=computed_publish_count,
        deduped_count=deduped_count,
        notified_count=notified_count,
        silent_count=silent_count,
        unsent_shadow_count=unsent_shadow_count,
        notifier_attempt_count=notifier_attempt_count,
    )
    selection_by_fixture = {
        selection.translation_result.posterior_result.prior_result.fixture_id: selection
        for selection in selections
    }
    execution_by_fixture = {
        execution.fixture_id: execution
        for execution in executions
    }
    fixture_audits = tuple(
        _build_fixture_audit(
            result,
            selection_result=selection_by_fixture.get(result.fixture_id),
            execution_result=execution_by_fixture.get(result.fixture_id),
        )
        for result in pipeline_snapshot.results
    )

    if ops_store is not None:
        try:
            ops_store.save_dedup_records(deduper.snapshot_records(timestamp))
        except Exception:
            ops_flags.append("state_store_unavailable")

    cycle_audit = RuntimeCycleAuditRecord(
        cycle_id=cycle_id,
        timestamp_utc=timestamp,
        fixture_count_seen=counters.fixture_count_seen,
        pipeline_publish_count=counters.computed_publish_count,
        deduped_count=counters.deduped_count,
        notified_count=counters.notified_count,
        silent_count=counters.silent_count,
        unsent_shadow_count=counters.unsent_shadow_count,
        notifier_attempt_count=counters.notifier_attempt_count,
        payloads=tuple(payloads),
        refusal_summaries=_collect_refusals(pipeline_snapshot.results),
        fixture_audits=fixture_audits,
        publication_records=tuple(publication_records),
        ops_flags=tuple(sorted(set(ops_flags))),
        notifier_mode=notifier_mode,
    )
    if ops_store is not None:
        try:
            ops_store.append_cycle_audit(cycle_audit)
            ops_store.append_publication_records(tuple(publication_records))
        except Exception:
            ops_flags.append("audit_store_unavailable")

    return RuntimeCycleResult(
        cycle_id=cycle_id,
        timestamp_utc=timestamp,
        counters=counters,
        payloads=tuple(payloads),
        refusal_summaries=cycle_audit.refusal_summaries,
        fixture_audits=tuple(
            {
                "fixture_id": audit.fixture_id,
                "match_label": audit.match_label,
                "competition_label": audit.competition_label,
                "governed_public_status": audit.governed_public_status,
                "publish_status": audit.publish_status,
                "template_key": audit.template_key,
                "bookmaker_id": audit.bookmaker_id,
                "line": audit.line,
                "odds_decimal": audit.odds_decimal,
                "governance_refusal_summary": list(audit.governance_refusal_summary),
                "execution_refusal_summary": list(audit.execution_refusal_summary),
                "candidate_not_selectable_reason": audit.candidate_not_selectable_reason,
                "translated_candidate_count": audit.translated_candidate_count,
                "selectable_candidate_count": audit.selectable_candidate_count,
                "best_candidate_family": audit.best_candidate_family,
                "best_candidate_exists": audit.best_candidate_exists,
                "best_candidate_selectable": audit.best_candidate_selectable,
                "best_candidate_blockers": list(audit.best_candidate_blockers),
                "distinct_candidate_blockers_summary": list(audit.distinct_candidate_blockers_summary),
                "execution_candidate_count": audit.execution_candidate_count,
                "execution_selectable_count": audit.execution_selectable_count,
                "attempted_template_keys": list(audit.attempted_template_keys),
                "offer_present_template_keys": list(audit.offer_present_template_keys),
                "missing_offer_template_keys": list(audit.missing_offer_template_keys),
                "blocked_execution_reasons_summary": list(audit.blocked_execution_reasons_summary),
                "final_execution_refusal_reason": audit.final_execution_refusal_reason,
                "publishability_score": audit.publishability_score,
                "template_binding_score": audit.template_binding_score,
                "bookmaker_diversity_score": audit.bookmaker_diversity_score,
                "price_integrity_score": audit.price_integrity_score,
                "retrievability_score": audit.retrievability_score,
                "source": audit.source,
            }
            for audit in fixture_audits
        ),
        publication_records=tuple(
            {
                "cycle_id": record.cycle_id,
                "timestamp_utc": record.timestamp_utc.isoformat(),
                "fixture_id": record.fixture_id,
                "public_status": record.public_status,
                "publish_channel": record.publish_channel,
                "template_key": record.template_key,
                "bookmaker_id": record.bookmaker_id,
                "bookmaker_name": record.bookmaker_name,
                "line": record.line,
                "odds_decimal": record.odds_decimal,
                "public_summary": record.public_summary,
                "disposition": record.disposition,
                "notified": record.notified,
                "dedupe_origin": record.dedupe_origin,
                "source": record.source,
            }
            for record in publication_records
        ),
        ops_flags=tuple(sorted(set(ops_flags))),
        notifier_mode=notifier_mode,
    )
