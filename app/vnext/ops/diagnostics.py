from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from app.vnext.ops.inspection import (
    InspectCliError,
    load_cycles_from_export,
    load_latest_run_index,
    load_manifest_document,
    resolve_latest_run_index_path,
)
from app.vnext.ops.models import (
    RuntimeCycleAuditRecord,
    RuntimeFixtureAuditRecord,
)
from app.vnext.ops.reporter import build_runtime_report
from app.vnext.ops.runtime_cli import EXIT_INSPECT_SOURCE_FAILED


VALID_GOVERNED_STATUSES = {"NO_BET", "WATCHLIST", "ELITE"}


@dataclass(slots=True, frozen=True)
class DiagnosticSource:
    mode: str
    latest_path: str
    manifest_path: str
    report_path: str
    export_path: str
    status: str
    source: str
    notifier: str


@dataclass(slots=True, frozen=True)
class DiagnosticCohortFilter:
    label: str = "all"
    only_governed: bool = False
    governed_status: str = ""
    contains_refusal: str = ""

    @property
    def is_active(self) -> bool:
        return self.label != "all"

    def matches(self, audit: RuntimeFixtureAuditRecord) -> bool:
        if self.only_governed and audit.governed_public_status == "NO_BET":
            return False
        if self.governed_status and audit.governed_public_status != self.governed_status:
            return False
        if self.contains_refusal:
            combined_refusals = set(audit.governance_refusal_summary) | set(audit.execution_refusal_summary)
            if self.contains_refusal not in combined_refusals:
                return False
        return True


@dataclass(slots=True, frozen=True)
class TopFixtureSummary:
    fixture_id: int
    match_label: str
    occurrences: int
    last_seen_utc: str
    governed_public_status: str
    publish_status: str
    template_key: str | None
    bookmaker_id: int | None
    line: float | None
    odds_decimal: float | None
    retained_count: int
    deduped_count: int
    watchlist_count: int
    no_bet_count: int
    publish_count: int
    do_not_publish_count: int
    template_hit_count: int
    offer_hit_count: int
    candidate_not_selectable_count: int
    no_best_candidate_count: int
    publishability_low_count: int
    no_offer_found_count: int
    top_transitions: tuple[tuple[str, int], ...]


@dataclass(slots=True, frozen=True)
class FixtureTemporalSummary:
    fixture_id: int
    match_label: str
    matches: int
    first_seen_utc: str
    last_seen_utc: str
    currently_seen: bool
    currently_governed: bool
    currently_watchlist: bool
    last_governed_utc: str
    last_watchlist_utc: str
    cycles_since_last_seen: int
    cycles_since_last_governed: int | None
    cycles_since_last_watchlist: int | None
    current_plateau_active: bool
    behavior: str
    oscillation_count: int
    temporal_transition_counts: tuple[tuple[str, int], ...]
    recent_temporal_steps: tuple[str, ...]
    recent_governed_steps: tuple[str, ...]
    recent_refusal_steps: tuple[str, ...]
    recent_refusals: tuple[str, ...]
    episode_count: int
    no_bet_episode_count: int
    watchlist_episode_count: int
    longest_no_bet_episode: int
    longest_watchlist_episode: int
    tail_episode_status: str
    tail_episode_length: int
    tail_episode_refusals: tuple[str, ...]
    recent_episode_steps: tuple[str, ...]
    no_bet_episode_refusals: tuple[tuple[str, int], ...]
    watchlist_episode_refusals: tuple[tuple[str, int], ...]
    recent_episode_refusals: tuple[tuple[str, int], ...]
    governed_hit_count: int
    watchlist_count: int
    no_bet_count: int
    publish_count: int
    do_not_publish_count: int
    template_hit_count: int
    offer_hit_count: int
    current_watchlist_streak: int
    longest_watchlist_streak: int
    current_governed_streak: int
    longest_governed_streak: int
    tail_governed_status: str
    tail_governed_streak: int


@dataclass(slots=True, frozen=True)
class RecentWindowSummary:
    source_ref: DiagnosticSource
    cohort: DiagnosticCohortFilter
    cycles_read: int
    source_cycles_read: int
    cycles_with_matches: int
    first_cycle_utc: str
    last_cycle_utc: str
    source_first_cycle_utc: str
    source_last_cycle_utc: str
    publishable_count: int
    retained_payload_count: int
    deduped_count: int
    unsent_shadow_count: int
    notifier_attempt_count: int
    notified_count: int
    acked_record_count: int
    fixture_audit_count: int
    source_fixture_audit_count: int
    unique_fixture_count: int
    source_unique_fixture_count: int
    governed_non_no_bet_count: int
    publish_candidate_count: int
    template_attached_count: int
    bookmaker_attached_count: int
    line_attached_count: int
    odds_attached_count: int
    offer_attached_count: int
    retained_record_count: int
    deduped_record_count: int
    no_bet_like_count: int
    governed_but_not_publish_count: int
    publish_deduped_count: int
    publish_retained_count: int
    unique_no_bet_fixture_count: int
    unique_governed_fixture_count: int
    unique_publish_fixture_count: int
    unique_do_not_publish_fixture_count: int
    unique_template_fixture_count: int
    unique_offer_fixture_count: int
    unique_retained_fixture_count: int
    unique_deduped_fixture_count: int
    top_refusals: tuple[tuple[str, int], ...]
    near_publish_refusals: tuple[tuple[str, int], ...]
    transition_counts: tuple[tuple[str, int], ...]
    no_bet_refusals: tuple[tuple[str, int], ...]
    governed_refusals: tuple[tuple[str, int], ...]
    publish_refusals: tuple[tuple[str, int], ...]
    offer_attached_refusals: tuple[tuple[str, int], ...]
    top_ops_flags: tuple[tuple[str, int], ...]
    top_fixtures: tuple[TopFixtureSummary, ...]
    temporal_behavior_counts: tuple[tuple[str, int], ...]
    top_oscillating_fixtures: tuple[FixtureTemporalSummary, ...]
    top_stable_fixtures: tuple[FixtureTemporalSummary, ...]
    top_near_cases: tuple[FixtureTemporalSummary, ...]
    top_watchlist_episode_fixtures: tuple[FixtureTemporalSummary, ...]
    top_alternating_episode_fixtures: tuple[FixtureTemporalSummary, ...]
    top_current_blocked_plateaus: tuple[FixtureTemporalSummary, ...]
    top_historical_blocked_plateaus: tuple[FixtureTemporalSummary, ...]
    top_recently_decayed_fixtures: tuple[FixtureTemporalSummary, ...]


@dataclass(slots=True, frozen=True)
class FixtureRecentSummary:
    fixture_id: int
    window_cycles: int
    matches: int
    match_label: str
    first_seen_utc: str
    last_seen_utc: str
    currently_seen: bool
    currently_governed: bool
    currently_watchlist: bool
    last_governed_utc: str
    last_watchlist_utc: str
    cycles_since_last_seen: int
    cycles_since_last_governed: int | None
    cycles_since_last_watchlist: int | None
    current_plateau_active: bool
    latest_governed_public_status: str
    latest_publish_status: str
    latest_template_key: str | None
    latest_bookmaker_id: int | None
    latest_line: float | None
    latest_odds_decimal: float | None
    recent_timestamps: tuple[str, ...]
    governed_public_statuses: tuple[str, ...]
    publish_statuses: tuple[str, ...]
    governance_refusals: tuple[str, ...]
    execution_refusals: tuple[str, ...]
    watchlist_count: int
    governed_hit_count: int
    no_bet_count: int
    publish_count: int
    do_not_publish_count: int
    template_hit_count: int
    offer_hit_count: int
    transition_counts: tuple[tuple[str, int], ...]
    governance_refusal_counts: tuple[tuple[str, int], ...]
    execution_refusal_counts: tuple[tuple[str, int], ...]
    temporal_behavior: str
    oscillation_count: int
    temporal_transition_counts: tuple[tuple[str, int], ...]
    recent_temporal_steps: tuple[str, ...]
    recent_governed_steps: tuple[str, ...]
    recent_refusal_steps: tuple[str, ...]
    recent_refusals: tuple[str, ...]
    episode_count: int
    no_bet_episode_count: int
    watchlist_episode_count: int
    longest_no_bet_episode: int
    longest_watchlist_episode: int
    tail_episode_status: str
    tail_episode_length: int
    tail_episode_refusals: tuple[str, ...]
    recent_episode_steps: tuple[str, ...]
    no_bet_episode_refusals: tuple[tuple[str, int], ...]
    watchlist_episode_refusals: tuple[tuple[str, int], ...]
    recent_episode_refusals: tuple[tuple[str, int], ...]
    current_watchlist_streak: int
    longest_watchlist_streak: int
    current_governed_streak: int
    longest_governed_streak: int
    tail_governed_status: str
    tail_governed_streak: int
    template_keys: tuple[str, ...]
    bookmaker_ids: tuple[int, ...]
    lines: tuple[float, ...]
    odds_decimals: tuple[float, ...]
    latest_candidate_not_selectable_reason: str | None
    latest_translated_candidate_count: int | None
    latest_selectable_candidate_count: int | None
    latest_best_candidate_family: str | None
    latest_best_candidate_exists: bool | None
    latest_best_candidate_selectable: bool | None
    latest_best_candidate_blockers: tuple[str, ...]
    latest_distinct_candidate_blockers_summary: tuple[str, ...]
    latest_execution_candidate_count: int | None
    latest_execution_selectable_count: int | None
    latest_attempted_template_keys: tuple[str, ...]
    latest_offer_present_template_keys: tuple[str, ...]
    latest_missing_offer_template_keys: tuple[str, ...]
    latest_blocked_execution_reasons_summary: tuple[str, ...]
    latest_final_execution_refusal_reason: str | None
    latest_publishability_score: float | None
    latest_template_binding_score: float | None
    latest_bookmaker_diversity_score: float | None
    latest_price_integrity_score: float | None
    latest_retrievability_score: float | None


def _require_string(payload: dict[str, object], key: str, *, reason: str, path: str) -> str:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise InspectCliError(reason, path, EXIT_INSPECT_SOURCE_FAILED)


def build_cohort_filter(
    *,
    only_governed: bool = False,
    governed_status: str = "",
    contains_refusal: str = "",
) -> DiagnosticCohortFilter:
    normalized_status = governed_status.strip().upper()
    normalized_refusal = contains_refusal.strip()
    if normalized_status and normalized_status not in VALID_GOVERNED_STATUSES:
        raise InspectCliError(
            "filter_invalid",
            f"governed_status:{normalized_status}",
            EXIT_INSPECT_SOURCE_FAILED,
        )
    parts: list[str] = []
    if only_governed:
        parts.append("only_governed")
    if normalized_status:
        parts.append(f"governed_status:{normalized_status}")
    if normalized_refusal:
        parts.append(f"contains_refusal:{normalized_refusal}")
    return DiagnosticCohortFilter(
        label=",".join(parts) if parts else "all",
        only_governed=only_governed,
        governed_status=normalized_status,
        contains_refusal=normalized_refusal,
    )


def _window_cycles(
    cycles: tuple[RuntimeCycleAuditRecord, ...],
    *,
    last_cycles: int,
) -> tuple[RuntimeCycleAuditRecord, ...]:
    if last_cycles <= 0:
        return cycles
    return cycles[-last_cycles:]


def _recent_unique(values):
    seen = set()
    ordered = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def _offer_attached(audit: RuntimeFixtureAuditRecord) -> bool:
    return (
        audit.bookmaker_id is not None
        and audit.line is not None
        and audit.odds_decimal is not None
    )


def _transition_label(audit: RuntimeFixtureAuditRecord) -> str:
    return f"{audit.governed_public_status}->{audit.publish_status}"


def _is_near_publish_candidate(audit: RuntimeFixtureAuditRecord) -> bool:
    return (
        audit.publish_status == "PUBLISH"
        or audit.template_key is not None
        or _offer_attached(audit)
    )


def _display_text(value: str | None) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text or "-"


def _display_number(value: int | float | None) -> str:
    if value is None:
        return "-"
    return str(value)


def _display_bool(value: bool) -> str:
    return "true" if value else "false"


def _display_optional_bool(value: bool | None) -> str:
    if value is None:
        return "-"
    return _display_bool(value)


def _combined_refusals(audit: RuntimeFixtureAuditRecord) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                *audit.governance_refusal_summary,
                *audit.execution_refusal_summary,
            }
        )
    )


def _tail_true_streak(flags: tuple[bool, ...]) -> int:
    streak = 0
    for flag in reversed(flags):
        if not flag:
            break
        streak += 1
    return streak


def _longest_true_streak(flags: tuple[bool, ...]) -> int:
    longest = 0
    current = 0
    for flag in flags:
        if flag:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _tail_status_streak(statuses: tuple[str, ...]) -> tuple[str, int]:
    if not statuses:
        return "-", 0
    tail_status = statuses[-1]
    streak = 0
    for status in reversed(statuses):
        if status != tail_status:
            break
        streak += 1
    return tail_status, streak


def _status_episode_segments(
    history: list[tuple[int, str, RuntimeFixtureAuditRecord]],
) -> list[tuple[str, int, tuple[str, ...]]]:
    if not history:
        return []
    segments: list[tuple[str, int, tuple[str, ...]]] = []
    current_status = history[0][2].governed_public_status
    current_length = 0
    current_refusals: Counter[str] = Counter()
    for _, _, audit in history:
        status = audit.governed_public_status
        if status != current_status:
            segments.append(
                (
                    current_status,
                    current_length,
                    tuple(reason for reason, _ in current_refusals.most_common(5)),
                )
            )
            current_status = status
            current_length = 0
            current_refusals = Counter()
        current_length += 1
        current_refusals.update(_combined_refusals(audit))
    segments.append(
        (
            current_status,
            current_length,
            tuple(reason for reason, _ in current_refusals.most_common(5)),
        )
    )
    return segments


def _fixture_history(
    cycles: tuple[RuntimeCycleAuditRecord, ...],
) -> dict[int, list[tuple[int, str, RuntimeFixtureAuditRecord]]]:
    histories: dict[int, list[tuple[int, str, RuntimeFixtureAuditRecord]]] = {}
    for cycle_index, cycle in enumerate(cycles):
        cycle_utc = cycle.timestamp_utc.isoformat()
        for audit in cycle.fixture_audits:
            histories.setdefault(audit.fixture_id, []).append((cycle_index, cycle_utc, audit))
    return histories


def _classify_fixture_behavior(
    statuses: tuple[str, ...],
    *,
    publish_count: int,
    do_not_publish_count: int,
) -> str:
    unique_statuses = set(statuses)
    changed = any(statuses[index] != statuses[index - 1] for index in range(1, len(statuses)))
    if len(unique_statuses) == 1 and unique_statuses == {"NO_BET"}:
        return "stable_no_bet"
    if len(unique_statuses) == 1 and unique_statuses == {"WATCHLIST"} and publish_count == 0:
        return "stable_watchlist"
    if changed and unique_statuses <= {"NO_BET", "WATCHLIST"}:
        return "oscillating"
    if do_not_publish_count == len(statuses):
        return "mixed_non_publish"
    return "mixed_publish_path"


def _build_fixture_temporal_summary(
    fixture_id: int,
    history: list[tuple[int, str, RuntimeFixtureAuditRecord]],
    *,
    latest_cycle_index: int,
) -> FixtureTemporalSummary:
    cycle_indexes = tuple(cycle_index for cycle_index, _, _ in history)
    timestamps = tuple(timestamp for _, timestamp, _ in history)
    audits = tuple(audit for _, _, audit in history)
    statuses = tuple(audit.governed_public_status for audit in audits)
    governed_flags = tuple(status != "NO_BET" for status in statuses)
    watchlist_flags = tuple(status == "WATCHLIST" for status in statuses)
    publish_count = sum(1 for audit in audits if audit.publish_status == "PUBLISH")
    do_not_publish_count = sum(1 for audit in audits if audit.publish_status == "DO_NOT_PUBLISH")
    recent_governed_steps = tuple(
        f"{timestamp}:{audit.governed_public_status}"
        for _, timestamp, audit in history[-5:]
    )
    recent_refusal_steps = tuple(
        f"{timestamp}:{'|'.join(_combined_refusals(audit)) or '-'}"
        for _, timestamp, audit in history[-5:]
    )
    recent_refusals = _recent_unique(
        reason
        for _, _, audit in reversed(history)
        for reason in _combined_refusals(audit)
    )[:5]
    segments = _status_episode_segments(history)
    no_bet_episode_counter: Counter[str] = Counter()
    watchlist_episode_counter: Counter[str] = Counter()
    recent_episode_counter: Counter[str] = Counter()
    for status, _, refusals in segments:
        if status == "NO_BET":
            no_bet_episode_counter.update(refusals)
        if status == "WATCHLIST":
            watchlist_episode_counter.update(refusals)
    for _, _, refusals in segments[-3:]:
        recent_episode_counter.update(refusals)
    recent_episode_steps = tuple(
        f"{status}x{length}"
        for status, length, _ in segments[-5:]
    )
    tail_episode_status = segments[-1][0]
    tail_episode_length = segments[-1][1]
    tail_episode_refusals = segments[-1][2]
    transition_counter: Counter[str] = Counter()
    recent_steps: list[str] = []
    oscillation_count = 0
    for index in range(1, len(audits)):
        previous = audits[index - 1].governed_public_status
        current = audits[index].governed_public_status
        label = f"{previous}->{current}"
        transition_counter.update((label,))
        if previous != current:
            oscillation_count += 1
        recent_steps.append(f"{timestamps[index]}:{label}")
    last_governed_index = next(
        (cycle_index for cycle_index, _, audit in reversed(history) if audit.governed_public_status != "NO_BET"),
        None,
    )
    last_governed_utc = next(
        (timestamp for _, timestamp, audit in reversed(history) if audit.governed_public_status != "NO_BET"),
        "-",
    )
    last_watchlist_index = next(
        (cycle_index for cycle_index, _, audit in reversed(history) if audit.governed_public_status == "WATCHLIST"),
        None,
    )
    last_watchlist_utc = next(
        (timestamp for _, timestamp, audit in reversed(history) if audit.governed_public_status == "WATCHLIST"),
        "-",
    )
    currently_seen = cycle_indexes[-1] == latest_cycle_index
    currently_governed = audits[-1].governed_public_status != "NO_BET" and currently_seen
    currently_watchlist = audits[-1].governed_public_status == "WATCHLIST" and currently_seen
    cycles_since_last_seen = latest_cycle_index - cycle_indexes[-1]
    cycles_since_last_governed = (
        None if last_governed_index is None else latest_cycle_index - last_governed_index
    )
    cycles_since_last_watchlist = (
        None if last_watchlist_index is None else latest_cycle_index - last_watchlist_index
    )
    current_plateau_active = currently_governed

    return FixtureTemporalSummary(
        fixture_id=fixture_id,
        match_label=audits[-1].match_label,
        matches=len(audits),
        first_seen_utc=timestamps[0],
        last_seen_utc=timestamps[-1],
        currently_seen=currently_seen,
        currently_governed=currently_governed,
        currently_watchlist=currently_watchlist,
        last_governed_utc=last_governed_utc,
        last_watchlist_utc=last_watchlist_utc,
        cycles_since_last_seen=cycles_since_last_seen,
        cycles_since_last_governed=cycles_since_last_governed,
        cycles_since_last_watchlist=cycles_since_last_watchlist,
        current_plateau_active=current_plateau_active,
        behavior=_classify_fixture_behavior(
            statuses,
            publish_count=publish_count,
            do_not_publish_count=do_not_publish_count,
        ),
        oscillation_count=oscillation_count,
        temporal_transition_counts=tuple(transition_counter.most_common(5)),
        recent_temporal_steps=tuple(recent_steps[-5:]),
        recent_governed_steps=recent_governed_steps,
        recent_refusal_steps=recent_refusal_steps,
        recent_refusals=recent_refusals,
        episode_count=len(segments),
        no_bet_episode_count=sum(1 for status, _, _ in segments if status == "NO_BET"),
        watchlist_episode_count=sum(1 for status, _, _ in segments if status == "WATCHLIST"),
        longest_no_bet_episode=max(
            (length for status, length, _ in segments if status == "NO_BET"),
            default=0,
        ),
        longest_watchlist_episode=max(
            (length for status, length, _ in segments if status == "WATCHLIST"),
            default=0,
        ),
        tail_episode_status=tail_episode_status,
        tail_episode_length=tail_episode_length,
        tail_episode_refusals=tail_episode_refusals,
        recent_episode_steps=recent_episode_steps,
        no_bet_episode_refusals=tuple(no_bet_episode_counter.most_common(5)),
        watchlist_episode_refusals=tuple(watchlist_episode_counter.most_common(5)),
        recent_episode_refusals=tuple(recent_episode_counter.most_common(5)),
        governed_hit_count=sum(1 for status in statuses if status != "NO_BET"),
        watchlist_count=sum(1 for audit in audits if audit.governed_public_status == "WATCHLIST"),
        no_bet_count=sum(1 for audit in audits if audit.governed_public_status == "NO_BET"),
        publish_count=publish_count,
        do_not_publish_count=do_not_publish_count,
        template_hit_count=sum(1 for audit in audits if audit.template_key is not None),
        offer_hit_count=sum(1 for audit in audits if _offer_attached(audit)),
        current_watchlist_streak=_tail_true_streak(watchlist_flags),
        longest_watchlist_streak=_longest_true_streak(watchlist_flags),
        current_governed_streak=_tail_true_streak(governed_flags),
        longest_governed_streak=_longest_true_streak(governed_flags),
        tail_governed_status=_tail_status_streak(statuses)[0],
        tail_governed_streak=_tail_status_streak(statuses)[1],
    )


def _resolve_source(
    *,
    manifest_path: Path | None = None,
    export_path: Path | None = None,
) -> DiagnosticSource:
    if manifest_path is not None:
        manifest = load_manifest_document(manifest_path)
        return DiagnosticSource(
            mode="manifest",
            latest_path="",
            manifest_path=str(manifest_path),
            report_path=str(manifest.get("report_path") or ""),
            export_path=_require_string(
                manifest,
                "export_path",
                reason="manifest_invalid",
                path=str(manifest_path),
            ),
            status=_require_string(
                manifest,
                "status",
                reason="manifest_invalid",
                path=str(manifest_path),
            ),
            source=_require_string(
                manifest,
                "source_resolved",
                reason="manifest_invalid",
                path=str(manifest_path),
            ),
            notifier=_require_string(
                manifest,
                "notifier_resolved",
                reason="manifest_invalid",
                path=str(manifest_path),
            ),
        )

    if export_path is not None:
        return DiagnosticSource(
            mode="export",
            latest_path="",
            manifest_path="",
            report_path="",
            export_path=str(export_path),
            status="-",
            source="-",
            notifier="-",
        )

    latest_path = resolve_latest_run_index_path()
    latest = load_latest_run_index(latest_path)
    manifest_path = Path(
        _require_string(
            latest,
            "manifest_path",
            reason="latest_run_invalid",
            path=str(latest_path),
        )
    )
    manifest = load_manifest_document(manifest_path)
    return DiagnosticSource(
        mode="latest",
        latest_path=str(latest_path),
        manifest_path=str(manifest_path),
        report_path=str(manifest.get("report_path") or ""),
        export_path=_require_string(
            manifest,
            "export_path",
            reason="manifest_invalid",
            path=str(manifest_path),
        ),
        status=_require_string(
            manifest,
            "status",
            reason="manifest_invalid",
            path=str(manifest_path),
        ),
        source=_require_string(
            manifest,
            "source_resolved",
            reason="manifest_invalid",
            path=str(manifest_path),
        ),
        notifier=_require_string(
            manifest,
            "notifier_resolved",
            reason="manifest_invalid",
            path=str(manifest_path),
        ),
    )


def _build_top_fixtures(
    cycles: tuple[RuntimeCycleAuditRecord, ...],
) -> tuple[TopFixtureSummary, ...]:
    fixture_occurrences: Counter[int] = Counter()
    last_seen_utc: dict[int, str] = {}
    last_audit: dict[int, RuntimeFixtureAuditRecord] = {}
    retained_by_fixture: Counter[int] = Counter()
    deduped_by_fixture: Counter[int] = Counter()
    watchlist_by_fixture: Counter[int] = Counter()
    no_bet_by_fixture: Counter[int] = Counter()
    publish_by_fixture: Counter[int] = Counter()
    do_not_publish_by_fixture: Counter[int] = Counter()
    template_by_fixture: Counter[int] = Counter()
    offer_by_fixture: Counter[int] = Counter()
    candidate_not_selectable_by_fixture: Counter[int] = Counter()
    no_best_candidate_by_fixture: Counter[int] = Counter()
    publishability_low_by_fixture: Counter[int] = Counter()
    no_offer_found_by_fixture: Counter[int] = Counter()
    transition_by_fixture: dict[int, Counter[str]] = {}

    for cycle in cycles:
        cycle_utc = cycle.timestamp_utc.isoformat()
        for audit in cycle.fixture_audits:
            fixture_occurrences.update((audit.fixture_id,))
            last_seen_utc[audit.fixture_id] = cycle_utc
            last_audit[audit.fixture_id] = audit
            combined_refusals = set(audit.governance_refusal_summary) | set(audit.execution_refusal_summary)
            if audit.governed_public_status == "WATCHLIST":
                watchlist_by_fixture.update((audit.fixture_id,))
            if audit.governed_public_status == "NO_BET":
                no_bet_by_fixture.update((audit.fixture_id,))
            if audit.publish_status == "PUBLISH":
                publish_by_fixture.update((audit.fixture_id,))
            if audit.publish_status == "DO_NOT_PUBLISH":
                do_not_publish_by_fixture.update((audit.fixture_id,))
            if audit.template_key is not None:
                template_by_fixture.update((audit.fixture_id,))
            if _offer_attached(audit):
                offer_by_fixture.update((audit.fixture_id,))
            if "candidate_not_selectable" in combined_refusals:
                candidate_not_selectable_by_fixture.update((audit.fixture_id,))
            if "no_best_candidate" in combined_refusals:
                no_best_candidate_by_fixture.update((audit.fixture_id,))
            if "publishability_low" in combined_refusals:
                publishability_low_by_fixture.update((audit.fixture_id,))
            if "no_offer_found" in combined_refusals:
                no_offer_found_by_fixture.update((audit.fixture_id,))
            transition_by_fixture.setdefault(audit.fixture_id, Counter()).update((_transition_label(audit),))
        for record in cycle.publication_records:
            if record.disposition == "retained":
                retained_by_fixture.update((record.fixture_id,))
            elif record.disposition == "deduped":
                deduped_by_fixture.update((record.fixture_id,))

    summaries: list[TopFixtureSummary] = []
    for fixture_id, count in fixture_occurrences.items():
        audit = last_audit[fixture_id]
        summaries.append(
            TopFixtureSummary(
                fixture_id=fixture_id,
                match_label=audit.match_label,
                occurrences=count,
                last_seen_utc=last_seen_utc[fixture_id],
                governed_public_status=audit.governed_public_status,
                publish_status=audit.publish_status,
                template_key=audit.template_key,
                bookmaker_id=audit.bookmaker_id,
                line=audit.line,
                odds_decimal=audit.odds_decimal,
                retained_count=retained_by_fixture[fixture_id],
                deduped_count=deduped_by_fixture[fixture_id],
                watchlist_count=watchlist_by_fixture[fixture_id],
                no_bet_count=no_bet_by_fixture[fixture_id],
                publish_count=publish_by_fixture[fixture_id],
                do_not_publish_count=do_not_publish_by_fixture[fixture_id],
                template_hit_count=template_by_fixture[fixture_id],
                offer_hit_count=offer_by_fixture[fixture_id],
                candidate_not_selectable_count=candidate_not_selectable_by_fixture[fixture_id],
                no_best_candidate_count=no_best_candidate_by_fixture[fixture_id],
                publishability_low_count=publishability_low_by_fixture[fixture_id],
                no_offer_found_count=no_offer_found_by_fixture[fixture_id],
                top_transitions=tuple(transition_by_fixture.get(fixture_id, Counter()).most_common(3)),
            )
        )

    summaries.sort(
        key=lambda item: (
            int(item.retained_count > 0),
            int(item.deduped_count > 0),
            int(item.publish_status == "PUBLISH"),
            item.publish_count,
            item.template_hit_count,
            item.offer_hit_count,
            item.watchlist_count,
            item.candidate_not_selectable_count + item.no_best_candidate_count + item.publishability_low_count + item.no_offer_found_count,
            item.occurrences,
            item.last_seen_utc,
        ),
        reverse=True,
    )
    return tuple(summaries[:5])


def _build_top_near_cases(
    temporal_summaries: tuple[FixtureTemporalSummary, ...],
) -> tuple[FixtureTemporalSummary, ...]:
    candidates = [
        summary
        for summary in temporal_summaries
        if (
            summary.governed_hit_count > 0
            or summary.template_hit_count > 0
            or summary.offer_hit_count > 0
            or summary.publish_count > 0
            or summary.oscillation_count > 0
        )
    ]
    candidates.sort(
        key=lambda item: (
            item.publish_count,
            item.template_hit_count,
            item.offer_hit_count,
            item.current_governed_streak,
            item.longest_governed_streak,
            item.watchlist_count,
            item.governed_hit_count,
            item.oscillation_count,
            item.last_seen_utc,
        ),
        reverse=True,
    )
    return tuple(candidates[:5])


def _build_top_watchlist_episode_fixtures(
    temporal_summaries: tuple[FixtureTemporalSummary, ...],
) -> tuple[FixtureTemporalSummary, ...]:
    candidates = [
        summary
        for summary in temporal_summaries
        if summary.longest_watchlist_episode > 0
    ]
    candidates.sort(
        key=lambda item: (
            item.longest_watchlist_episode,
            item.watchlist_episode_count,
            item.current_watchlist_streak,
            item.last_seen_utc,
        ),
        reverse=True,
    )
    return tuple(candidates[:3])


def _build_top_alternating_episode_fixtures(
    temporal_summaries: tuple[FixtureTemporalSummary, ...],
) -> tuple[FixtureTemporalSummary, ...]:
    candidates = [
        summary
        for summary in temporal_summaries
        if summary.episode_count > 1 and summary.watchlist_episode_count > 0 and summary.no_bet_episode_count > 0
    ]
    candidates.sort(
        key=lambda item: (
            item.episode_count,
            item.oscillation_count,
            item.watchlist_episode_count,
            item.last_seen_utc,
        ),
        reverse=True,
    )
    return tuple(candidates[:3])


def _build_top_current_blocked_plateaus(
    temporal_summaries: tuple[FixtureTemporalSummary, ...],
) -> tuple[FixtureTemporalSummary, ...]:
    candidates = [
        summary
        for summary in temporal_summaries
        if summary.current_plateau_active and summary.publish_count == 0
    ]
    candidates.sort(
        key=lambda item: (
            item.current_governed_streak,
            item.longest_watchlist_episode,
            item.governed_hit_count,
            item.last_seen_utc,
        ),
        reverse=True,
    )
    return tuple(candidates[:3])


def _build_top_historical_blocked_plateaus(
    temporal_summaries: tuple[FixtureTemporalSummary, ...],
) -> tuple[FixtureTemporalSummary, ...]:
    candidates = [
        summary
        for summary in temporal_summaries
        if not summary.current_plateau_active and summary.governed_hit_count > 0 and summary.publish_count == 0
    ]
    candidates.sort(
        key=lambda item: (
            -item.longest_watchlist_episode,
            item.cycles_since_last_governed or 0,
            -item.governed_hit_count,
            item.last_governed_utc,
        )
    )
    return tuple(candidates[:3])


def _build_top_recently_decayed_fixtures(
    temporal_summaries: tuple[FixtureTemporalSummary, ...],
) -> tuple[FixtureTemporalSummary, ...]:
    candidates = [
        summary
        for summary in temporal_summaries
        if not summary.currently_governed and summary.cycles_since_last_governed is not None
    ]
    candidates.sort(
        key=lambda item: (
            item.cycles_since_last_governed or 0,
            not item.currently_seen,
            -item.longest_watchlist_episode,
            -item.governed_hit_count,
            item.last_seen_utc,
        )
    )
    return tuple(candidates[:3])


def _slice_cycles_for_cohort(
    cycles: tuple[RuntimeCycleAuditRecord, ...],
    cohort: DiagnosticCohortFilter,
) -> tuple[RuntimeCycleAuditRecord, ...]:
    if not cohort.is_active:
        return cycles

    sliced_cycles: list[RuntimeCycleAuditRecord] = []
    for cycle in cycles:
        filtered_audits = tuple(audit for audit in cycle.fixture_audits if cohort.matches(audit))
        fixture_ids = {audit.fixture_id for audit in filtered_audits}
        filtered_payloads = tuple(payload for payload in cycle.payloads if payload.fixture_id in fixture_ids)
        filtered_records = tuple(
            record for record in cycle.publication_records if record.fixture_id in fixture_ids
        )
        refusal_summaries = tuple(
            sorted(
                {
                    reason
                    for audit in filtered_audits
                    for reason in (*audit.governance_refusal_summary, *audit.execution_refusal_summary)
                }
            )
        )
        publishable_count = sum(1 for audit in filtered_audits if audit.publish_status == "PUBLISH")
        deduped_count = sum(1 for record in filtered_records if record.disposition == "deduped")
        notified_count = sum(1 for record in filtered_records if record.notified)
        retained_count = sum(1 for record in filtered_records if record.disposition == "retained")
        sliced_cycles.append(
            RuntimeCycleAuditRecord(
                cycle_id=cycle.cycle_id,
                timestamp_utc=cycle.timestamp_utc,
                fixture_count_seen=len(fixture_ids),
                pipeline_publish_count=publishable_count,
                deduped_count=deduped_count,
                notified_count=notified_count,
                silent_count=max(0, publishable_count - notified_count),
                unsent_shadow_count=max(0, len(filtered_payloads) - notified_count),
                notifier_attempt_count=retained_count,
                payloads=filtered_payloads,
                refusal_summaries=refusal_summaries,
                fixture_audits=filtered_audits,
                publication_records=filtered_records,
                ops_flags=cycle.ops_flags,
                notifier_mode=cycle.notifier_mode,
                source=cycle.source,
            )
        )
    return tuple(sliced_cycles)


def summarize_recent_window(
    *,
    manifest_path: Path | None = None,
    export_path: Path | None = None,
    last_cycles: int = 5,
    cohort: DiagnosticCohortFilter | None = None,
) -> tuple[RecentWindowSummary, tuple[RuntimeCycleAuditRecord, ...]]:
    effective_cohort = cohort or DiagnosticCohortFilter()
    source_ref = _resolve_source(manifest_path=manifest_path, export_path=export_path)
    cycles = load_cycles_from_export(Path(source_ref.export_path))
    source_recent_cycles = _window_cycles(cycles, last_cycles=last_cycles)
    recent_cycles = _slice_cycles_for_cohort(source_recent_cycles, effective_cohort)
    report = build_runtime_report(recent_cycles)

    refusal_counter: Counter[str] = Counter()
    near_publish_refusal_counter: Counter[str] = Counter()
    transition_counter: Counter[str] = Counter()
    no_bet_refusal_counter: Counter[str] = Counter()
    governed_refusal_counter: Counter[str] = Counter()
    publish_refusal_counter: Counter[str] = Counter()
    offer_attached_refusal_counter: Counter[str] = Counter()
    ops_flag_counter: Counter[str] = Counter()
    fixture_audit_count = 0
    unique_fixtures_seen: set[int] = set()
    unique_no_bet_fixtures: set[int] = set()
    unique_governed_fixtures: set[int] = set()
    unique_publish_fixtures: set[int] = set()
    unique_do_not_publish_fixtures: set[int] = set()
    unique_template_fixtures: set[int] = set()
    unique_offer_fixtures: set[int] = set()
    unique_retained_fixtures: set[int] = set()
    unique_deduped_fixtures: set[int] = set()
    governed_non_no_bet_count = 0
    publish_candidate_count = 0
    template_attached_count = 0
    bookmaker_attached_count = 0
    line_attached_count = 0
    odds_attached_count = 0
    offer_attached_count = 0
    no_bet_like_count = 0
    governed_but_not_publish_count = 0
    retained_record_count = 0
    deduped_record_count = 0

    for cycle in recent_cycles:
        refusal_counter.update(cycle.refusal_summaries)
        ops_flag_counter.update(cycle.ops_flags)
        for audit in cycle.fixture_audits:
            fixture_audit_count += 1
            unique_fixtures_seen.add(audit.fixture_id)
            transition_counter.update((_transition_label(audit),))
            if audit.governed_public_status != "NO_BET":
                governed_non_no_bet_count += 1
                unique_governed_fixtures.add(audit.fixture_id)
                governed_refusal_counter.update(audit.governance_refusal_summary)
                governed_refusal_counter.update(audit.execution_refusal_summary)
            else:
                no_bet_like_count += 1
                unique_no_bet_fixtures.add(audit.fixture_id)
                no_bet_refusal_counter.update(audit.governance_refusal_summary)
                no_bet_refusal_counter.update(audit.execution_refusal_summary)
            if audit.publish_status == "PUBLISH":
                publish_candidate_count += 1
                unique_publish_fixtures.add(audit.fixture_id)
                publish_refusal_counter.update(audit.governance_refusal_summary)
                publish_refusal_counter.update(audit.execution_refusal_summary)
            elif audit.governed_public_status != "NO_BET":
                governed_but_not_publish_count += 1
            if audit.publish_status == "DO_NOT_PUBLISH":
                unique_do_not_publish_fixtures.add(audit.fixture_id)
            if audit.template_key is not None:
                template_attached_count += 1
                unique_template_fixtures.add(audit.fixture_id)
            if audit.bookmaker_id is not None:
                bookmaker_attached_count += 1
            if audit.line is not None:
                line_attached_count += 1
            if audit.odds_decimal is not None:
                odds_attached_count += 1
            if _offer_attached(audit):
                offer_attached_count += 1
                unique_offer_fixtures.add(audit.fixture_id)
                offer_attached_refusal_counter.update(audit.governance_refusal_summary)
                offer_attached_refusal_counter.update(audit.execution_refusal_summary)
            if _is_near_publish_candidate(audit):
                near_publish_refusal_counter.update(audit.governance_refusal_summary)
                near_publish_refusal_counter.update(audit.execution_refusal_summary)

        for record in cycle.publication_records:
            if record.disposition == "retained":
                retained_record_count += 1
                unique_retained_fixtures.add(record.fixture_id)
            elif record.disposition == "deduped":
                deduped_record_count += 1
                unique_deduped_fixtures.add(record.fixture_id)

    first_cycle_utc = recent_cycles[0].timestamp_utc.isoformat() if recent_cycles else "-"
    last_cycle_utc = recent_cycles[-1].timestamp_utc.isoformat() if recent_cycles else "-"
    source_first_cycle_utc = source_recent_cycles[0].timestamp_utc.isoformat() if source_recent_cycles else "-"
    source_last_cycle_utc = source_recent_cycles[-1].timestamp_utc.isoformat() if source_recent_cycles else "-"
    source_fixture_audit_count = sum(len(cycle.fixture_audits) for cycle in source_recent_cycles)
    source_unique_fixture_count = len(
        {audit.fixture_id for cycle in source_recent_cycles for audit in cycle.fixture_audits}
    )
    cycles_with_matches = sum(1 for cycle in recent_cycles if cycle.fixture_audits)
    top_fixtures = _build_top_fixtures(recent_cycles)
    latest_cycle_index = max(len(recent_cycles) - 1, 0)
    temporal_summaries = tuple(
        _build_fixture_temporal_summary(
            fixture_id,
            history,
            latest_cycle_index=latest_cycle_index,
        )
        for fixture_id, history in _fixture_history(recent_cycles).items()
    )
    behavior_counter: Counter[str] = Counter(summary.behavior for summary in temporal_summaries)
    top_oscillating_fixtures = tuple(
        sorted(
            (
                summary
                for summary in temporal_summaries
                if summary.behavior == "oscillating"
            ),
            key=lambda item: (item.oscillation_count, item.matches, item.last_seen_utc),
            reverse=True,
        )[:3]
    )
    top_stable_fixtures = tuple(
        sorted(
            (
                summary
                for summary in temporal_summaries
                if summary.behavior in {"stable_no_bet", "stable_watchlist", "mixed_non_publish"}
            ),
            key=lambda item: (item.matches, item.watchlist_count, item.no_bet_count, item.last_seen_utc),
            reverse=True,
        )[:3]
    )
    top_near_cases = _build_top_near_cases(temporal_summaries)
    top_watchlist_episode_fixtures = _build_top_watchlist_episode_fixtures(temporal_summaries)
    top_alternating_episode_fixtures = _build_top_alternating_episode_fixtures(temporal_summaries)
    top_current_blocked_plateaus = _build_top_current_blocked_plateaus(temporal_summaries)
    top_historical_blocked_plateaus = _build_top_historical_blocked_plateaus(temporal_summaries)
    top_recently_decayed_fixtures = _build_top_recently_decayed_fixtures(temporal_summaries)

    return (
        RecentWindowSummary(
            source_ref=source_ref,
            cohort=effective_cohort,
            cycles_read=int(report.get("cycle_count", 0)),
            source_cycles_read=len(source_recent_cycles),
            cycles_with_matches=cycles_with_matches,
            first_cycle_utc=first_cycle_utc,
            last_cycle_utc=last_cycle_utc,
            source_first_cycle_utc=source_first_cycle_utc,
            source_last_cycle_utc=source_last_cycle_utc,
            publishable_count=int(report.get("publishable_count", 0)),
            retained_payload_count=int(report.get("retained_payload_count", 0)),
            deduped_count=int(report.get("deduped_count", 0)),
            unsent_shadow_count=int(report.get("unsent_shadow_count", 0)),
            notifier_attempt_count=int(report.get("notifier_attempt_count", 0)),
            notified_count=int(report.get("notified_count", 0)),
            acked_record_count=int(report.get("acked_record_count", 0)),
            fixture_audit_count=fixture_audit_count,
            source_fixture_audit_count=source_fixture_audit_count,
            unique_fixture_count=len(unique_fixtures_seen),
            source_unique_fixture_count=source_unique_fixture_count,
            governed_non_no_bet_count=governed_non_no_bet_count,
            publish_candidate_count=publish_candidate_count,
            template_attached_count=template_attached_count,
            bookmaker_attached_count=bookmaker_attached_count,
            line_attached_count=line_attached_count,
            odds_attached_count=odds_attached_count,
            offer_attached_count=offer_attached_count,
            retained_record_count=retained_record_count,
            deduped_record_count=deduped_record_count,
            no_bet_like_count=no_bet_like_count,
            governed_but_not_publish_count=governed_but_not_publish_count,
            publish_deduped_count=deduped_record_count,
            publish_retained_count=retained_record_count,
            unique_no_bet_fixture_count=len(unique_no_bet_fixtures),
            unique_governed_fixture_count=len(unique_governed_fixtures),
            unique_publish_fixture_count=len(unique_publish_fixtures),
            unique_do_not_publish_fixture_count=len(unique_do_not_publish_fixtures),
            unique_template_fixture_count=len(unique_template_fixtures),
            unique_offer_fixture_count=len(unique_offer_fixtures),
            unique_retained_fixture_count=len(unique_retained_fixtures),
            unique_deduped_fixture_count=len(unique_deduped_fixtures),
            top_refusals=tuple(refusal_counter.most_common(5)),
            near_publish_refusals=tuple(near_publish_refusal_counter.most_common(5)),
            transition_counts=tuple(transition_counter.most_common(5)),
            no_bet_refusals=tuple(no_bet_refusal_counter.most_common(5)),
            governed_refusals=tuple(governed_refusal_counter.most_common(5)),
            publish_refusals=tuple(publish_refusal_counter.most_common(5)),
            offer_attached_refusals=tuple(offer_attached_refusal_counter.most_common(5)),
            top_ops_flags=tuple(ops_flag_counter.most_common(5)),
            top_fixtures=top_fixtures,
            temporal_behavior_counts=tuple(behavior_counter.most_common(5)),
            top_oscillating_fixtures=top_oscillating_fixtures,
            top_stable_fixtures=top_stable_fixtures,
            top_near_cases=top_near_cases,
            top_watchlist_episode_fixtures=top_watchlist_episode_fixtures,
            top_alternating_episode_fixtures=top_alternating_episode_fixtures,
            top_current_blocked_plateaus=top_current_blocked_plateaus,
            top_historical_blocked_plateaus=top_historical_blocked_plateaus,
            top_recently_decayed_fixtures=top_recently_decayed_fixtures,
        ),
        recent_cycles,
    )


def summarize_fixture_recent(
    *,
    fixture_id: int,
    cycles: tuple[RuntimeCycleAuditRecord, ...],
) -> FixtureRecentSummary:
    matches: list[tuple[int, RuntimeCycleAuditRecord, RuntimeFixtureAuditRecord]] = []
    for cycle_index, cycle in enumerate(cycles):
        for audit in cycle.fixture_audits:
            if audit.fixture_id == fixture_id:
                matches.append((cycle_index, cycle, audit))

    if not matches:
        return FixtureRecentSummary(
            fixture_id=fixture_id,
            window_cycles=len(cycles),
            matches=0,
            match_label="-",
            first_seen_utc="-",
            last_seen_utc="-",
            currently_seen=False,
            currently_governed=False,
            currently_watchlist=False,
            last_governed_utc="-",
            last_watchlist_utc="-",
            cycles_since_last_seen=0,
            cycles_since_last_governed=None,
            cycles_since_last_watchlist=None,
            current_plateau_active=False,
            latest_governed_public_status="-",
            latest_publish_status="-",
            latest_template_key=None,
            latest_bookmaker_id=None,
            latest_line=None,
            latest_odds_decimal=None,
            recent_timestamps=(),
            governed_public_statuses=(),
            publish_statuses=(),
            governance_refusals=(),
            execution_refusals=(),
            watchlist_count=0,
            governed_hit_count=0,
            no_bet_count=0,
            publish_count=0,
            do_not_publish_count=0,
            template_hit_count=0,
            offer_hit_count=0,
            transition_counts=(),
            governance_refusal_counts=(),
            execution_refusal_counts=(),
            temporal_behavior="missing",
            oscillation_count=0,
            temporal_transition_counts=(),
            recent_temporal_steps=(),
            recent_governed_steps=(),
            recent_refusal_steps=(),
            recent_refusals=(),
            episode_count=0,
            no_bet_episode_count=0,
            watchlist_episode_count=0,
            longest_no_bet_episode=0,
            longest_watchlist_episode=0,
            tail_episode_status="-",
            tail_episode_length=0,
            tail_episode_refusals=(),
            recent_episode_steps=(),
            no_bet_episode_refusals=(),
            watchlist_episode_refusals=(),
            recent_episode_refusals=(),
            current_watchlist_streak=0,
            longest_watchlist_streak=0,
            current_governed_streak=0,
            longest_governed_streak=0,
            tail_governed_status="-",
            tail_governed_streak=0,
            template_keys=(),
            bookmaker_ids=(),
            lines=(),
            odds_decimals=(),
            latest_candidate_not_selectable_reason=None,
            latest_translated_candidate_count=None,
            latest_selectable_candidate_count=None,
            latest_best_candidate_family=None,
            latest_best_candidate_exists=None,
            latest_best_candidate_selectable=None,
            latest_best_candidate_blockers=(),
            latest_distinct_candidate_blockers_summary=(),
            latest_execution_candidate_count=None,
            latest_execution_selectable_count=None,
            latest_attempted_template_keys=(),
            latest_offer_present_template_keys=(),
            latest_missing_offer_template_keys=(),
            latest_blocked_execution_reasons_summary=(),
            latest_final_execution_refusal_reason=None,
            latest_publishability_score=None,
            latest_template_binding_score=None,
            latest_bookmaker_diversity_score=None,
            latest_price_integrity_score=None,
            latest_retrievability_score=None,
        )

    recent_matches = list(reversed(matches))
    latest_audit = recent_matches[0][2]
    temporal_history = [
        (cycle_index, cycle.timestamp_utc.isoformat(), audit)
        for cycle_index, cycle, audit in matches
    ]
    temporal_summary = _build_fixture_temporal_summary(
        fixture_id,
        temporal_history,
        latest_cycle_index=max(len(cycles) - 1, 0),
    )
    transition_counter: Counter[str] = Counter()
    governance_refusal_counter: Counter[str] = Counter()
    execution_refusal_counter: Counter[str] = Counter()
    watchlist_count = 0
    no_bet_count = 0
    publish_count = 0
    do_not_publish_count = 0
    template_hit_count = 0
    offer_hit_count = 0
    for _, _, audit in matches:
        transition_counter.update((_transition_label(audit),))
        governance_refusal_counter.update(audit.governance_refusal_summary)
        execution_refusal_counter.update(audit.execution_refusal_summary)
        if audit.governed_public_status == "WATCHLIST":
            watchlist_count += 1
        if audit.governed_public_status == "NO_BET":
            no_bet_count += 1
        if audit.publish_status == "PUBLISH":
            publish_count += 1
        if audit.publish_status == "DO_NOT_PUBLISH":
            do_not_publish_count += 1
        if audit.template_key is not None:
            template_hit_count += 1
        if _offer_attached(audit):
            offer_hit_count += 1
    return FixtureRecentSummary(
        fixture_id=fixture_id,
        window_cycles=len(cycles),
        matches=len(matches),
        match_label=latest_audit.match_label,
        first_seen_utc=matches[0][1].timestamp_utc.isoformat(),
        last_seen_utc=matches[-1][1].timestamp_utc.isoformat(),
        currently_seen=temporal_summary.currently_seen,
        currently_governed=temporal_summary.currently_governed,
        currently_watchlist=temporal_summary.currently_watchlist,
        last_governed_utc=temporal_summary.last_governed_utc,
        last_watchlist_utc=temporal_summary.last_watchlist_utc,
        cycles_since_last_seen=temporal_summary.cycles_since_last_seen,
        cycles_since_last_governed=temporal_summary.cycles_since_last_governed,
        cycles_since_last_watchlist=temporal_summary.cycles_since_last_watchlist,
        current_plateau_active=temporal_summary.current_plateau_active,
        latest_governed_public_status=latest_audit.governed_public_status,
        latest_publish_status=latest_audit.publish_status,
        latest_template_key=latest_audit.template_key,
        latest_bookmaker_id=latest_audit.bookmaker_id,
        latest_line=latest_audit.line,
        latest_odds_decimal=latest_audit.odds_decimal,
        recent_timestamps=tuple(
            cycle.timestamp_utc.isoformat()
            for _, cycle, _ in recent_matches[:3]
        ),
        governed_public_statuses=_recent_unique(
            audit.governed_public_status
            for _, _, audit in recent_matches
        ),
        publish_statuses=_recent_unique(
            audit.publish_status
            for _, _, audit in recent_matches
        ),
        governance_refusals=_recent_unique(
            reason
            for _, _, audit in recent_matches
            for reason in audit.governance_refusal_summary
        ),
        execution_refusals=_recent_unique(
            reason
            for _, _, audit in recent_matches
            for reason in audit.execution_refusal_summary
        ),
        watchlist_count=watchlist_count,
        governed_hit_count=temporal_summary.governed_hit_count,
        no_bet_count=no_bet_count,
        publish_count=publish_count,
        do_not_publish_count=do_not_publish_count,
        template_hit_count=template_hit_count,
        offer_hit_count=offer_hit_count,
        transition_counts=tuple(transition_counter.most_common(5)),
        governance_refusal_counts=tuple(governance_refusal_counter.most_common(5)),
        execution_refusal_counts=tuple(execution_refusal_counter.most_common(5)),
        temporal_behavior=temporal_summary.behavior,
        oscillation_count=temporal_summary.oscillation_count,
        temporal_transition_counts=temporal_summary.temporal_transition_counts,
        recent_temporal_steps=temporal_summary.recent_temporal_steps,
        recent_governed_steps=temporal_summary.recent_governed_steps,
        recent_refusal_steps=temporal_summary.recent_refusal_steps,
        recent_refusals=temporal_summary.recent_refusals,
        episode_count=temporal_summary.episode_count,
        no_bet_episode_count=temporal_summary.no_bet_episode_count,
        watchlist_episode_count=temporal_summary.watchlist_episode_count,
        longest_no_bet_episode=temporal_summary.longest_no_bet_episode,
        longest_watchlist_episode=temporal_summary.longest_watchlist_episode,
        tail_episode_status=temporal_summary.tail_episode_status,
        tail_episode_length=temporal_summary.tail_episode_length,
        tail_episode_refusals=temporal_summary.tail_episode_refusals,
        recent_episode_steps=temporal_summary.recent_episode_steps,
        no_bet_episode_refusals=temporal_summary.no_bet_episode_refusals,
        watchlist_episode_refusals=temporal_summary.watchlist_episode_refusals,
        recent_episode_refusals=temporal_summary.recent_episode_refusals,
        current_watchlist_streak=temporal_summary.current_watchlist_streak,
        longest_watchlist_streak=temporal_summary.longest_watchlist_streak,
        current_governed_streak=temporal_summary.current_governed_streak,
        longest_governed_streak=temporal_summary.longest_governed_streak,
        tail_governed_status=temporal_summary.tail_governed_status,
        tail_governed_streak=temporal_summary.tail_governed_streak,
        template_keys=_recent_unique(
            audit.template_key
            for _, _, audit in recent_matches
            if audit.template_key is not None
        ),
        bookmaker_ids=_recent_unique(
            audit.bookmaker_id
            for _, _, audit in recent_matches
            if audit.bookmaker_id is not None
        ),
        lines=_recent_unique(
            audit.line
            for _, _, audit in recent_matches
            if audit.line is not None
        ),
        odds_decimals=_recent_unique(
            audit.odds_decimal
            for _, _, audit in recent_matches
            if audit.odds_decimal is not None
        ),
        latest_candidate_not_selectable_reason=latest_audit.candidate_not_selectable_reason,
        latest_translated_candidate_count=latest_audit.translated_candidate_count,
        latest_selectable_candidate_count=latest_audit.selectable_candidate_count,
        latest_best_candidate_family=latest_audit.best_candidate_family,
        latest_best_candidate_exists=latest_audit.best_candidate_exists,
        latest_best_candidate_selectable=latest_audit.best_candidate_selectable,
        latest_best_candidate_blockers=latest_audit.best_candidate_blockers,
        latest_distinct_candidate_blockers_summary=latest_audit.distinct_candidate_blockers_summary,
        latest_execution_candidate_count=latest_audit.execution_candidate_count,
        latest_execution_selectable_count=latest_audit.execution_selectable_count,
        latest_attempted_template_keys=latest_audit.attempted_template_keys,
        latest_offer_present_template_keys=latest_audit.offer_present_template_keys,
        latest_missing_offer_template_keys=latest_audit.missing_offer_template_keys,
        latest_blocked_execution_reasons_summary=latest_audit.blocked_execution_reasons_summary,
        latest_final_execution_refusal_reason=latest_audit.final_execution_refusal_reason,
        latest_publishability_score=latest_audit.publishability_score,
        latest_template_binding_score=latest_audit.template_binding_score,
        latest_bookmaker_diversity_score=latest_audit.bookmaker_diversity_score,
        latest_price_integrity_score=latest_audit.price_integrity_score,
        latest_retrievability_score=latest_audit.retrievability_score,
    )


def format_recent_window(
    summary: RecentWindowSummary,
    fixture_summary: FixtureRecentSummary | None = None,
) -> str:
    lines = [
        "vnext_recent_diag "
        f"input={summary.source_ref.mode} "
        f"cohort={summary.cohort.label} "
        f"window_cycles={summary.cycles_read} "
        f"first_cycle_utc={summary.first_cycle_utc} "
        f"last_cycle_utc={summary.last_cycle_utc} "
        f"status={summary.source_ref.status} "
        f"source={summary.source_ref.source} "
        f"notifier={summary.source_ref.notifier}",
        "vnext_recent_diag_scope "
        f"cycles_read_source={summary.source_cycles_read} "
        f"cycles_with_matches_filtered={summary.cycles_with_matches} "
        f"fixture_audits_source={summary.source_fixture_audit_count} "
        f"fixture_audits_filtered={summary.fixture_audit_count} "
        f"fixtures_seen_source={summary.source_unique_fixture_count} "
        f"fixtures_seen_filtered={summary.unique_fixture_count}",
        "vnext_recent_diag_counts "
        f"publishable={summary.publishable_count} "
        f"retained={summary.retained_payload_count} "
        f"deduped={summary.deduped_count} "
        f"shadow_unsent={summary.unsent_shadow_count} "
        f"notify_attempts={summary.notifier_attempt_count} "
        f"notified={summary.notified_count} "
        f"acked_records={summary.acked_record_count}",
        "vnext_recent_diag_chain "
        f"fixture_audits={summary.fixture_audit_count} "
        f"fixtures_seen={summary.unique_fixture_count} "
        f"governed_non_no_bet={summary.governed_non_no_bet_count} "
        f"publish_candidates={summary.publish_candidate_count} "
        f"template_attached={summary.template_attached_count} "
        f"bookmaker_attached={summary.bookmaker_attached_count} "
        f"offer_attached={summary.offer_attached_count} "
        f"retained_records={summary.retained_record_count} "
        f"deduped_records={summary.deduped_record_count}",
        "vnext_recent_diag_cutoffs "
        f"no_bet_like={summary.no_bet_like_count} "
        f"governed_but_not_publish={summary.governed_but_not_publish_count} "
        f"publish_deduped={summary.publish_deduped_count} "
        f"publish_retained={summary.publish_retained_count}",
        "vnext_recent_diag_funnel_fixture "
        f"fixtures_seen={summary.unique_fixture_count} "
        f"no_bet={summary.unique_no_bet_fixture_count} "
        f"governed_non_no_bet={summary.unique_governed_fixture_count} "
        f"publish={summary.unique_publish_fixture_count} "
        f"do_not_publish={summary.unique_do_not_publish_fixture_count} "
        f"template_attached={summary.unique_template_fixture_count} "
        f"offer_attached={summary.unique_offer_fixture_count} "
        f"retained={summary.unique_retained_fixture_count} "
        f"deduped={summary.unique_deduped_fixture_count}",
        "vnext_recent_diag_transitions "
        f"audit_transitions={list(summary.transition_counts)}",
        "vnext_recent_diag_refusal_zones "
        f"no_bet={list(summary.no_bet_refusals)} "
        f"governed={list(summary.governed_refusals)} "
        f"publish={list(summary.publish_refusals)} "
        f"offer_attached={list(summary.offer_attached_refusals)}",
        "vnext_recent_diag_temporal "
        f"behaviors={list(summary.temporal_behavior_counts)}",
        "vnext_recent_diag_near "
        "sort_keys=['publish_hits', 'template_hits', 'offer_hits', "
        "'current_governed_streak', 'longest_governed_streak', "
        "'watchlist_hits', 'governed_hits', 'oscillations', 'last_seen_utc']",
        "vnext_recent_diag_episodes "
        "sort_keys_watchlist=['longest_watchlist_episode', 'watchlist_episodes', "
        "'current_watchlist_streak', 'last_seen_utc'] "
        "sort_keys_alternating=['episodes_total', 'oscillations', "
        "'watchlist_episodes', 'last_seen_utc']",
        "vnext_recent_diag_current "
        "sort_keys_current=['current_governed_streak', 'longest_watchlist_episode', "
        "'governed_hits', 'last_seen_utc'] "
        "sort_keys_historical=['longest_watchlist_episode', 'cycles_since_last_governed', "
        "'governed_hits', 'last_governed_utc'] "
        "sort_keys_recently_decayed=['cycles_since_last_governed', 'currently_seen', "
        "'longest_watchlist_episode', 'governed_hits', 'last_seen_utc']",
        "vnext_recent_diag_top "
        f"refusals={list(summary.top_refusals)} "
        f"near_publish_refusals={list(summary.near_publish_refusals)} "
        f"ops_flags={list(summary.top_ops_flags)}",
        "vnext_recent_diag_paths "
        f"latest={summary.source_ref.latest_path or '-'} "
        f"manifest={summary.source_ref.manifest_path or '-'} "
        f"report={summary.source_ref.report_path or '-'} "
        f"export={summary.source_ref.export_path or '-'}",
    ]
    for fixture in summary.top_fixtures:
        lines.append(
            "vnext_recent_fixture_top "
            f"fixture_id={fixture.fixture_id} "
            f"match_label={fixture.match_label} "
            f"occurrences={fixture.occurrences} "
            f"last_seen_utc={fixture.last_seen_utc} "
            f"governed_public_status={fixture.governed_public_status} "
            f"publish_status={fixture.publish_status} "
            f"template_key={_display_text(fixture.template_key)} "
            f"bookmaker_id={_display_number(fixture.bookmaker_id)} "
            f"line={_display_number(fixture.line)} "
            f"odds_decimal={_display_number(fixture.odds_decimal)} "
            f"retained={fixture.retained_count} "
            f"deduped={fixture.deduped_count} "
            f"watchlist_hits={fixture.watchlist_count} "
            f"publish_hits={fixture.publish_count} "
            f"offer_hits={fixture.offer_hit_count} "
            f"candidate_not_selectable={fixture.candidate_not_selectable_count} "
            f"no_best_candidate={fixture.no_best_candidate_count} "
            f"publishability_low={fixture.publishability_low_count} "
            f"no_offer_found={fixture.no_offer_found_count} "
            f"transitions={list(fixture.top_transitions)}"
        )
    for fixture in summary.top_oscillating_fixtures:
        lines.append(
            "vnext_recent_temporal_top "
            f"kind=oscillating fixture_id={fixture.fixture_id} "
            f"match_label={fixture.match_label} "
            f"matches={fixture.matches} "
            f"behavior={fixture.behavior} "
            f"oscillations={fixture.oscillation_count} "
            f"temporal_transitions={list(fixture.temporal_transition_counts)}"
        )
    for fixture in summary.top_stable_fixtures:
        lines.append(
            "vnext_recent_temporal_top "
            f"kind=stable fixture_id={fixture.fixture_id} "
            f"match_label={fixture.match_label} "
            f"matches={fixture.matches} "
            f"behavior={fixture.behavior} "
            f"oscillations={fixture.oscillation_count} "
            f"temporal_transitions={list(fixture.temporal_transition_counts)}"
        )
    for fixture in summary.top_near_cases:
        lines.append(
            "vnext_recent_near_top "
            f"fixture_id={fixture.fixture_id} "
            f"match_label={fixture.match_label} "
            f"behavior={fixture.behavior} "
            f"watchlist_hits={fixture.watchlist_count} "
            f"governed_hits={fixture.governed_hit_count} "
            f"current_watchlist_streak={fixture.current_watchlist_streak} "
            f"longest_watchlist_streak={fixture.longest_watchlist_streak} "
            f"current_governed_streak={fixture.current_governed_streak} "
            f"longest_governed_streak={fixture.longest_governed_streak} "
            f"template_hits={fixture.template_hit_count} "
            f"offer_hits={fixture.offer_hit_count} "
            f"publish_hits={fixture.publish_count} "
            f"oscillations={fixture.oscillation_count} "
            f"recent_refusals={list(fixture.recent_refusals)} "
            f"last_seen_utc={fixture.last_seen_utc}"
        )
    for fixture in summary.top_watchlist_episode_fixtures:
        lines.append(
            "vnext_recent_episode_top "
            f"kind=watchlist_plateau fixture_id={fixture.fixture_id} "
            f"match_label={fixture.match_label} "
            f"longest_watchlist_episode={fixture.longest_watchlist_episode} "
            f"watchlist_episodes={fixture.watchlist_episode_count} "
            f"tail_episode={fixture.tail_episode_status}x{fixture.tail_episode_length} "
            f"tail_episode_refusals={list(fixture.tail_episode_refusals)} "
            f"last_seen_utc={fixture.last_seen_utc}"
        )
    for fixture in summary.top_alternating_episode_fixtures:
        lines.append(
            "vnext_recent_episode_top "
            f"kind=alternating fixture_id={fixture.fixture_id} "
            f"match_label={fixture.match_label} "
            f"episodes_total={fixture.episode_count} "
            f"watchlist_episodes={fixture.watchlist_episode_count} "
            f"no_bet_episodes={fixture.no_bet_episode_count} "
            f"oscillations={fixture.oscillation_count} "
            f"tail_episode={fixture.tail_episode_status}x{fixture.tail_episode_length} "
            f"last_seen_utc={fixture.last_seen_utc}"
        )
    for fixture in summary.top_current_blocked_plateaus:
        lines.append(
            "vnext_recent_current_top "
            f"kind=current_blocked_plateau fixture_id={fixture.fixture_id} "
            f"match_label={fixture.match_label} "
            f"currently_seen={_display_bool(fixture.currently_seen)} "
            f"currently_governed={_display_bool(fixture.currently_governed)} "
            f"currently_watchlist={_display_bool(fixture.currently_watchlist)} "
            f"current_plateau_active={_display_bool(fixture.current_plateau_active)} "
            f"current_governed_streak={fixture.current_governed_streak} "
            f"longest_watchlist_episode={fixture.longest_watchlist_episode} "
            f"last_governed_utc={fixture.last_governed_utc} "
            f"cycles_since_last_governed={_display_number(fixture.cycles_since_last_governed)}"
        )
    for fixture in summary.top_historical_blocked_plateaus:
        lines.append(
            "vnext_recent_current_top "
            f"kind=historical_blocked_plateau fixture_id={fixture.fixture_id} "
            f"match_label={fixture.match_label} "
            f"currently_seen={_display_bool(fixture.currently_seen)} "
            f"currently_governed={_display_bool(fixture.currently_governed)} "
            f"currently_watchlist={_display_bool(fixture.currently_watchlist)} "
            f"current_plateau_active={_display_bool(fixture.current_plateau_active)} "
            f"longest_watchlist_episode={fixture.longest_watchlist_episode} "
            f"last_governed_utc={fixture.last_governed_utc} "
            f"cycles_since_last_governed={_display_number(fixture.cycles_since_last_governed)}"
        )
    for fixture in summary.top_recently_decayed_fixtures:
        lines.append(
            "vnext_recent_current_top "
            f"kind=recently_decayed fixture_id={fixture.fixture_id} "
            f"match_label={fixture.match_label} "
            f"currently_seen={_display_bool(fixture.currently_seen)} "
            f"currently_governed={_display_bool(fixture.currently_governed)} "
            f"currently_watchlist={_display_bool(fixture.currently_watchlist)} "
            f"current_plateau_active={_display_bool(fixture.current_plateau_active)} "
            f"last_seen_utc={fixture.last_seen_utc} "
            f"last_governed_utc={fixture.last_governed_utc} "
            f"cycles_since_last_seen={fixture.cycles_since_last_seen} "
            f"cycles_since_last_governed={_display_number(fixture.cycles_since_last_governed)}"
        )
    if fixture_summary is None:
        return "\n".join(lines)

    lines.extend(
        (
            "vnext_recent_fixture "
            f"fixture_id={fixture_summary.fixture_id} "
            f"window_cycles={fixture_summary.window_cycles} "
            f"matches={fixture_summary.matches} "
            f"match_label={fixture_summary.match_label} "
            f"first_seen_utc={fixture_summary.first_seen_utc} "
            f"last_seen_utc={fixture_summary.last_seen_utc}",
            "vnext_recent_fixture_latest "
            f"governed_public_status={fixture_summary.latest_governed_public_status} "
            f"publish_status={fixture_summary.latest_publish_status} "
            f"template_key={_display_text(fixture_summary.latest_template_key)} "
            f"bookmaker_id={_display_number(fixture_summary.latest_bookmaker_id)} "
            f"line={_display_number(fixture_summary.latest_line)} "
            f"odds_decimal={_display_number(fixture_summary.latest_odds_decimal)}",
            "vnext_recent_fixture_liveness "
            f"currently_seen={_display_bool(fixture_summary.currently_seen)} "
            f"currently_governed={_display_bool(fixture_summary.currently_governed)} "
            f"currently_watchlist={_display_bool(fixture_summary.currently_watchlist)} "
            f"last_seen_utc={fixture_summary.last_seen_utc} "
            f"last_governed_utc={fixture_summary.last_governed_utc} "
            f"last_watchlist_utc={fixture_summary.last_watchlist_utc} "
            f"cycles_since_last_seen={fixture_summary.cycles_since_last_seen} "
            f"cycles_since_last_governed={_display_number(fixture_summary.cycles_since_last_governed)} "
            f"cycles_since_last_watchlist={_display_number(fixture_summary.cycles_since_last_watchlist)} "
            f"current_plateau_active={_display_bool(fixture_summary.current_plateau_active)}",
            "vnext_recent_fixture_timestamps "
            f"recent_timestamps={list(fixture_summary.recent_timestamps)}",
            "vnext_recent_fixture_status "
            f"governed_public_statuses={list(fixture_summary.governed_public_statuses)} "
            f"publish_statuses={list(fixture_summary.publish_statuses)}",
            "vnext_recent_fixture_funnel "
            f"watchlist={fixture_summary.watchlist_count} "
            f"governed_hits={fixture_summary.governed_hit_count} "
            f"no_bet={fixture_summary.no_bet_count} "
            f"publish={fixture_summary.publish_count} "
            f"do_not_publish={fixture_summary.do_not_publish_count} "
            f"template_hits={fixture_summary.template_hit_count} "
            f"offer_hits={fixture_summary.offer_hit_count}",
            "vnext_recent_fixture_streaks "
            f"current_watchlist_streak={fixture_summary.current_watchlist_streak} "
            f"longest_watchlist_streak={fixture_summary.longest_watchlist_streak} "
            f"current_governed_streak={fixture_summary.current_governed_streak} "
            f"longest_governed_streak={fixture_summary.longest_governed_streak} "
            f"tail_status={fixture_summary.tail_governed_status} "
            f"tail_streak={fixture_summary.tail_governed_streak}",
            "vnext_recent_fixture_episodes "
            f"episodes_total={fixture_summary.episode_count} "
            f"watchlist_episodes={fixture_summary.watchlist_episode_count} "
            f"no_bet_episodes={fixture_summary.no_bet_episode_count} "
            f"longest_watchlist_episode={fixture_summary.longest_watchlist_episode} "
            f"longest_no_bet_episode={fixture_summary.longest_no_bet_episode} "
            f"tail_episode_status={fixture_summary.tail_episode_status} "
            f"tail_episode_length={fixture_summary.tail_episode_length}",
            "vnext_recent_fixture_temporal "
            f"behavior={fixture_summary.temporal_behavior} "
            f"oscillations={fixture_summary.oscillation_count} "
            f"temporal_transitions={list(fixture_summary.temporal_transition_counts)} "
            f"recent_steps={list(fixture_summary.recent_temporal_steps)}",
            "vnext_recent_fixture_recent "
            f"governed_steps={list(fixture_summary.recent_governed_steps)} "
            f"refusal_steps={list(fixture_summary.recent_refusal_steps)} "
            f"recent_refusals={list(fixture_summary.recent_refusals)}",
            "vnext_recent_fixture_episode_recent "
            f"segments={list(fixture_summary.recent_episode_steps)} "
            f"tail_episode_refusals={list(fixture_summary.tail_episode_refusals)} "
            f"no_bet_episode_refusals={list(fixture_summary.no_bet_episode_refusals)} "
            f"watchlist_episode_refusals={list(fixture_summary.watchlist_episode_refusals)} "
            f"recent_episode_refusals={list(fixture_summary.recent_episode_refusals)}",
            "vnext_recent_fixture_transitions "
            f"transitions={list(fixture_summary.transition_counts)} "
            f"governance_counts={list(fixture_summary.governance_refusal_counts)} "
            f"execution_counts={list(fixture_summary.execution_refusal_counts)}",
            "vnext_recent_fixture_refusals "
            f"governance={list(fixture_summary.governance_refusals)} "
            f"execution={list(fixture_summary.execution_refusals)}",
            "vnext_recent_fixture_offer "
            f"template_keys={list(fixture_summary.template_keys)} "
            f"bookmaker_ids={list(fixture_summary.bookmaker_ids)} "
            f"lines={list(fixture_summary.lines)} "
            f"odds={list(fixture_summary.odds_decimals)}",
            "vnext_recent_fixture_selection_obs "
            f"candidate_not_selectable_reason={_display_text(fixture_summary.latest_candidate_not_selectable_reason)} "
            f"translated_candidates={_display_number(fixture_summary.latest_translated_candidate_count)} "
            f"selectable_candidates={_display_number(fixture_summary.latest_selectable_candidate_count)} "
            f"best_candidate_family={_display_text(fixture_summary.latest_best_candidate_family)} "
            f"best_candidate_exists={_display_optional_bool(fixture_summary.latest_best_candidate_exists)} "
            f"best_candidate_selectable={_display_optional_bool(fixture_summary.latest_best_candidate_selectable)} "
            f"best_candidate_blockers={list(fixture_summary.latest_best_candidate_blockers)} "
            f"distinct_blockers={list(fixture_summary.latest_distinct_candidate_blockers_summary)}",
            "vnext_recent_fixture_execution_obs "
            f"final_execution_refusal_reason={_display_text(fixture_summary.latest_final_execution_refusal_reason)} "
            f"execution_candidates={_display_number(fixture_summary.latest_execution_candidate_count)} "
            f"execution_selectable={_display_number(fixture_summary.latest_execution_selectable_count)} "
            f"attempted_templates={list(fixture_summary.latest_attempted_template_keys)} "
            f"offer_present_templates={list(fixture_summary.latest_offer_present_template_keys)} "
            f"missing_offer_templates={list(fixture_summary.latest_missing_offer_template_keys)} "
            f"blocked_execution_reasons={list(fixture_summary.latest_blocked_execution_reasons_summary)} "
            f"publishability_score={_display_number(fixture_summary.latest_publishability_score)} "
            f"template_binding_score={_display_number(fixture_summary.latest_template_binding_score)} "
            f"bookmaker_diversity_score={_display_number(fixture_summary.latest_bookmaker_diversity_score)} "
            f"price_integrity_score={_display_number(fixture_summary.latest_price_integrity_score)} "
            f"retrievability_score={_display_number(fixture_summary.latest_retrievability_score)}",
        )
    )
    return "\n".join(lines)
