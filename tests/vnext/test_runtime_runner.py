from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone, timedelta

from app.clients.discord import DiscordSendResult
from app.vnext.execution.models import (
    ExecutableMarketSelectionResult,
    ExecutionBlocker,
    ExecutionCandidate,
    ExecutionQualityBreakdown,
    MarketOffer,
    MarketOfferGroup,
)
from app.vnext.markets.lines import LINE_TEMPLATES
from app.vnext.markets.lines import line_template
from app.vnext.markets.models import (
    MarketBlocker,
    MarketCandidate,
    MarketSupportBreakdown,
    MarketTranslationResult,
)
from app.vnext.notifier.contracts import ExplicitAckVnextNotifier
from app.vnext.notifier.discord_vnext import DiscordVnextNotifier
from app.vnext.runtime.deduper import Deduper
from app.vnext.runtime.models import NotifierAckRecord, VnextRuntimeConfig
from app.vnext.runtime.runner import run_vnext_cycle
from app.vnext.runtime.source import SnapshotSource
from app.vnext.selection.models import MatchBestCandidate, MatchMarketSelectionResult
from tests.vnext.live_factories import (
    build_reference_posterior_result,
    build_reference_prior_result,
    make_live_snapshot,
)


def build_offer_for_template(template, bookmaker_id: int) -> MarketOffer:
    line = None
    if template.suggested_line_family == "over_1_5_or_2_5":
        line = 2.5
    elif template.suggested_line_family == "under_2_5_or_3_5":
        line = 2.5
    elif template.suggested_line_family == "home_over_0_5_or_1_5":
        line = 0.5
    elif template.suggested_line_family == "away_over_0_5_or_1_5":
        line = 0.5
    elif template.suggested_line_family == "home_under_1_5_or_2_5":
        line = 1.5
    elif template.suggested_line_family == "away_under_1_5_or_2_5":
        line = 1.5
    team_scope = "HOME" if template.direction.startswith("HOME") else "AWAY" if template.direction.startswith("AWAY") else "NONE"
    return MarketOffer(
        bookmaker_id=bookmaker_id,
        bookmaker_name=f"Book {bookmaker_id}",
        market_family=template.family,
        side=template.direction,
        line=line,
        team_scope=team_scope,
        odds_decimal=1.9,
        normalized_market_label=template.family,
        offer_timestamp_utc=datetime.now(timezone.utc),
        freshness_seconds=30,
        raw_source_ref=f"offer:{bookmaker_id}",
    )


def build_offers_for_all_templates() -> tuple[MarketOffer, ...]:
    offers = []
    for template in LINE_TEMPLATES.values():
        for bookmaker_id in (1, 2, 3):
            offers.append(build_offer_for_template(template, bookmaker_id))
    return tuple(offers)


def prior_provider(_snapshot):
    return build_reference_prior_result()


def fixture_aware_prior_provider(snapshot):
    prior_result = deepcopy(build_reference_prior_result())
    prior_result.fixture_id = snapshot.fixture_id
    prior_result.competition_id = snapshot.competition_id
    prior_result.home_team_id = snapshot.home_team_id
    prior_result.away_team_id = snapshot.away_team_id
    prior_result.home_team_name = snapshot.home_team_name
    prior_result.away_team_name = snapshot.away_team_name
    prior_result.competition_name = "Premier Test"
    return prior_result


def _selection_result(
    *,
    fixture_id: int,
    selectable: bool = True,
    no_selection_reason: str | None = None,
    candidate_blockers: tuple[str, ...] = (),
    translated_blockers: tuple[tuple[str, ...], ...] = (),
) -> MatchMarketSelectionResult:
    posterior = build_reference_posterior_result()
    posterior = replace(
        posterior,
        prior_result=replace(posterior.prior_result, fixture_id=fixture_id),
    )
    support_breakdown = MarketSupportBreakdown(
        scenario_support_score=0.7,
        attack_support_score=0.7,
        defensive_support_score=0.6,
        directionality_score=0.65,
        live_support_score=0.6,
        reliability_score=posterior.posterior_reliability.posterior_reliability_score,
        conflict_score=0.1,
        supporting_scenarios=("HOME_CONTROL",),
        supporting_signals=("attack_support",),
    )

    def _candidate(template_key: str, blockers: tuple[str, ...], *, candidate_selectable: bool) -> MarketCandidate:
        return MarketCandidate(
            fixture_id=fixture_id,
            family="OU_FT",
            maturity="APPROVED",
            line_template=line_template(template_key),
            exists=True,
            is_blocked=bool(blockers),
            is_selectable=candidate_selectable,
            support_score=0.72,
            confidence_score=0.68,
            support_breakdown=support_breakdown,
            blockers=tuple(MarketBlocker(tier="STRUCTURAL", code=code) for code in blockers),
        )

    translated_candidates = tuple(
        _candidate("OU_FT_OVER_CORE", blockers, candidate_selectable=False)
        for blockers in translated_blockers
    )
    if translated_candidates:
        translation = MarketTranslationResult(posterior_result=posterior, candidates=translated_candidates)
    else:
        primary_candidate = _candidate(
            "OU_FT_OVER_CORE",
            candidate_blockers,
            candidate_selectable=selectable,
        )
        translation = MarketTranslationResult(posterior_result=posterior, candidates=(primary_candidate,))

    best_candidate = None
    if no_selection_reason is None:
        primary_candidate = translation.candidates[0]
        best_candidate = MatchBestCandidate(candidate=primary_candidate, selection_score=0.71)

    return MatchMarketSelectionResult(
        translation_result=translation,
        best_candidate=best_candidate,
        no_selection_reason=no_selection_reason,
    )


def _execution_result(
    *,
    fixture_id: int,
    template_key: str,
    reason: str,
    offer_exists: bool,
    blockers: tuple[str, ...],
    publishability_score: float | None = None,
    template_binding_score: float | None = None,
    bookmaker_diversity_score: float | None = None,
    price_integrity_score: float | None = None,
    retrievability_score: float | None = None,
) -> ExecutableMarketSelectionResult:
    quality = ExecutionQualityBreakdown(
        offer_exists_score=1.0 if offer_exists else 0.0,
        template_binding_score=template_binding_score if template_binding_score is not None else (1.0 if offer_exists else 0.0),
        market_clarity_score=1.0 if offer_exists else 0.0,
        bookmaker_diversity_score=bookmaker_diversity_score if bookmaker_diversity_score is not None else (0.3333 if offer_exists else 0.0),
        price_integrity_score=price_integrity_score if price_integrity_score is not None else (1.0 if offer_exists else 0.0),
        freshness_score=0.8 if offer_exists else 0.0,
        retrievability_score=retrievability_score if retrievability_score is not None else (0.58 if offer_exists else 0.0),
        publishability_score=publishability_score if publishability_score is not None else (0.57 if offer_exists else 0.0),
    )
    offers = (
        MarketOffer(
            bookmaker_id=7,
            bookmaker_name="Book 7",
            market_family="OU_FT",
            side="OVER",
            line=2.5,
            team_scope="NONE",
            odds_decimal=1.91,
            normalized_market_label="OU_FT",
            offer_timestamp_utc=datetime.now(timezone.utc),
            freshness_seconds=45,
            raw_source_ref="offer:7",
        ),
    ) if offer_exists else ()
    offer_group = MarketOfferGroup(
        template_key=template_key,
        market_family="OU_FT",
        side="OVER",
        team_scope="NONE",
        requested_line_family="over_1_5_or_2_5",
        bound_line=2.5 if offer_exists else None,
        template_binding_status="EXACT" if offer_exists else "NO_BIND",
        offers=offers,
        offer_exists=offer_exists,
    )
    candidate = ExecutionCandidate(
        template_key=template_key,
        market_family="OU_FT",
        template_binding_status=offer_group.template_binding_status,
        offer_group=offer_group,
        selected_offer=offers[0] if offer_exists else None,
        alternatives=offers,
        offer_exists=offer_exists,
        is_blocked=True,
        is_selectable=False,
        selection_score=0.0,
        quality=quality,
        blockers=tuple(ExecutionBlocker(tier="PRODUCT", code=code) for code in blockers),
    )
    return ExecutableMarketSelectionResult(
        fixture_id=fixture_id,
        template_key=template_key,
        execution_candidate=None,
        alternatives=(candidate,),
        offer_chosen=None,
        no_executable_vehicle_reason=reason,
    )


def test_runner_ok_without_notifier() -> None:
    snapshot = make_live_snapshot()
    offers = build_offers_for_all_templates()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={snapshot.fixture_id: offers})
    config = VnextRuntimeConfig(enable_publication_build=True, enable_notifier_send=False)
    deduper = Deduper(cooldown_seconds=180)
    cycle = run_vnext_cycle(
        cycle_id=1,
        config=config,
        source=source,
        prior_result_provider=prior_provider,
        deduper=deduper,
        previous_snapshots={},
        now=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
    )

    assert cycle.counters.fixture_count_seen == 1
    assert cycle.counters.notified_count == 0
    assert cycle.counters.notifier_attempt_count == 0
    assert cycle.notifier_mode == "none"


def test_do_not_publish_never_notifies() -> None:
    snapshot = make_live_snapshot()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={})
    config = VnextRuntimeConfig(enable_publication_build=True, enable_notifier_send=True)
    deduper = Deduper(cooldown_seconds=180)
    cycle = run_vnext_cycle(
        cycle_id=1,
        config=config,
        source=source,
        prior_result_provider=prior_provider,
        deduper=deduper,
        previous_snapshots={},
        now=datetime(2026, 4, 14, 12, 5, tzinfo=timezone.utc),
    )

    assert cycle.counters.computed_publish_count == 0
    assert cycle.counters.notified_count == 0
    assert cycle.counters.unsent_shadow_count == 0
    assert cycle.notifier_mode == "none"


def test_deduped_publish_counts_computed_but_not_notified() -> None:
    snapshot = make_live_snapshot()
    offers = build_offers_for_all_templates()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={snapshot.fixture_id: offers})
    config = VnextRuntimeConfig(enable_publication_build=True, enable_notifier_send=True)
    deduper = Deduper(cooldown_seconds=600)
    now = datetime(2026, 4, 14, 12, 10, tzinfo=timezone.utc)

    def forced_board_snapshot(selections):
        from app.vnext.board.models import BoardEntry, BoardSnapshot

        entries = []
        for rank, selection in enumerate(selections, start=1):
            entries.append(
                BoardEntry(
                    fixture_id=selection.translation_result.posterior_result.prior_result.fixture_id,
                    internal_status="READY",
                    public_status="WATCHLIST",
                    board_score=0.72,
                    rank=rank,
                    selection_result=selection,
                )
            )
        return BoardSnapshot(entries=tuple(entries), elite_count=0, watchlist_count=len(entries))

    import app.vnext.runtime.runner as runner_module
    runner_module.build_board_snapshot = forced_board_snapshot  # type: ignore[assignment]

    first = run_vnext_cycle(
        cycle_id=1,
        config=config,
        source=source,
        prior_result_provider=prior_provider,
        deduper=deduper,
        previous_snapshots={},
        now=now,
    )
    second = run_vnext_cycle(
        cycle_id=2,
        config=config,
        source=source,
        prior_result_provider=prior_provider,
        deduper=deduper,
        previous_snapshots={},
        now=now + timedelta(seconds=60),
    )

    assert first.counters.computed_publish_count > 0
    assert second.counters.deduped_count > 0
    assert second.counters.notified_count == 0


def test_publication_records_do_not_overclaim_notified() -> None:
    snapshot = make_live_snapshot()
    offers = build_offers_for_all_templates()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={snapshot.fixture_id: offers})
    config = VnextRuntimeConfig(enable_publication_build=True, enable_notifier_send=True)
    deduper = Deduper(cooldown_seconds=180)

    def forced_board_snapshot(selections):
        from app.vnext.board.models import BoardEntry, BoardSnapshot

        entries = []
        for rank, selection in enumerate(selections, start=1):
            entries.append(
                BoardEntry(
                    fixture_id=selection.translation_result.posterior_result.prior_result.fixture_id,
                    internal_status="READY",
                    public_status="WATCHLIST",
                    board_score=0.72,
                    rank=rank,
                    selection_result=selection,
                )
            )
        return BoardSnapshot(entries=tuple(entries), elite_count=0, watchlist_count=len(entries))

    def aggregate_notifier(bundles) -> int:
        return len(bundles)

    import app.vnext.runtime.runner as runner_module

    runner_module.build_board_snapshot = forced_board_snapshot  # type: ignore[assignment]

    cycle = run_vnext_cycle(
        cycle_id=1,
        config=config,
        source=source,
        prior_result_provider=prior_provider,
        deduper=deduper,
        previous_snapshots={},
        notifier=aggregate_notifier,
        now=datetime(2026, 4, 14, 12, 15, tzinfo=timezone.utc),
    )

    assert cycle.counters.notified_count > 0
    assert cycle.counters.notifier_attempt_count == 1
    assert cycle.counters.unsent_shadow_count == 0
    assert cycle.notifier_mode == "aggregate"
    assert cycle.publication_records
    assert all(record["notified"] is False for record in cycle.publication_records)


def test_runner_accepts_concrete_discord_vnext_notifier() -> None:
    sent_messages: list[tuple[str, str]] = []
    snapshot = make_live_snapshot()
    offers = build_offers_for_all_templates()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={snapshot.fixture_id: offers})
    config = VnextRuntimeConfig(enable_publication_build=True, enable_notifier_send=True)
    deduper = Deduper(cooldown_seconds=180)

    def forced_board_snapshot(selections):
        from app.vnext.board.models import BoardEntry, BoardSnapshot

        entries = []
        for rank, selection in enumerate(selections, start=1):
            entries.append(
                BoardEntry(
                    fixture_id=selection.translation_result.posterior_result.prior_result.fixture_id,
                    internal_status="READY",
                    public_status="WATCHLIST",
                    board_score=0.72,
                    rank=rank,
                    selection_result=selection,
                )
            )
        return BoardSnapshot(entries=tuple(entries), elite_count=0, watchlist_count=len(entries))

    def fake_sender(webhook_url: str, content: str) -> DiscordSendResult:
        sent_messages.append((webhook_url, content))
        return DiscordSendResult(ok=True, status_code=200)

    import app.vnext.runtime.runner as runner_module

    runner_module.build_board_snapshot = forced_board_snapshot  # type: ignore[assignment]

    cycle = run_vnext_cycle(
        cycle_id=1,
        config=config,
        source=source,
        prior_result_provider=prior_provider,
        deduper=deduper,
        previous_snapshots={},
        notifier=DiscordVnextNotifier(
            webhook_url="https://discord.example/webhook",
            sender=fake_sender,
        ),
        now=datetime(2026, 4, 14, 12, 17, tzinfo=timezone.utc),
    )

    assert cycle.counters.notifier_attempt_count == 1
    assert cycle.counters.notified_count == 1
    assert cycle.notifier_mode == "aggregate"
    assert sent_messages
    assert all(record["notified"] is False for record in cycle.publication_records)


def test_shadow_runtime_counts_unsent_payloads_explicitly() -> None:
    snapshot = make_live_snapshot()
    offers = build_offers_for_all_templates()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={snapshot.fixture_id: offers})
    config = VnextRuntimeConfig(enable_publication_build=True, enable_notifier_send=False)
    deduper = Deduper(cooldown_seconds=180)

    def forced_board_snapshot(selections):
        from app.vnext.board.models import BoardEntry, BoardSnapshot

        entries = []
        for rank, selection in enumerate(selections, start=1):
            entries.append(
                BoardEntry(
                    fixture_id=selection.translation_result.posterior_result.prior_result.fixture_id,
                    internal_status="READY",
                    public_status="WATCHLIST",
                    board_score=0.72,
                    rank=rank,
                    selection_result=selection,
                )
            )
        return BoardSnapshot(entries=tuple(entries), elite_count=0, watchlist_count=len(entries))

    import app.vnext.runtime.runner as runner_module

    runner_module.build_board_snapshot = forced_board_snapshot  # type: ignore[assignment]

    cycle = run_vnext_cycle(
        cycle_id=1,
        config=config,
        source=source,
        prior_result_provider=prior_provider,
        deduper=deduper,
        previous_snapshots={},
        now=datetime(2026, 4, 14, 12, 20, tzinfo=timezone.utc),
    )

    assert cycle.counters.computed_publish_count > 0
    assert cycle.counters.unsent_shadow_count == len(cycle.payloads)
    assert cycle.counters.notifier_attempt_count == 0
    assert cycle.notifier_mode == "none"


def test_explicit_notifier_ack_marks_only_acked_records() -> None:
    snapshots = (
        make_live_snapshot(fixture_id=999, home_team_id=1, away_team_id=2, home_team_name="Lions", away_team_name="Falcons"),
        make_live_snapshot(
            fixture_id=1001,
            home_team_id=11,
            away_team_id=12,
            home_team_name="Wolves",
            away_team_name="Bears",
        ),
    )
    offers = build_offers_for_all_templates()
    source = SnapshotSource(
        snapshots=snapshots,
        offers_by_fixture={snapshot.fixture_id: offers for snapshot in snapshots},
    )
    config = VnextRuntimeConfig(enable_publication_build=True, enable_notifier_send=True)
    deduper = Deduper(cooldown_seconds=180)

    def forced_board_snapshot(selections):
        from app.vnext.board.models import BoardEntry, BoardSnapshot

        entries = []
        for rank, selection in enumerate(selections, start=1):
            entries.append(
                BoardEntry(
                    fixture_id=selection.translation_result.posterior_result.prior_result.fixture_id,
                    internal_status="READY",
                    public_status="WATCHLIST",
                    board_score=0.72,
                    rank=rank,
                    selection_result=selection,
                )
            )
        return BoardSnapshot(entries=tuple(entries), elite_count=0, watchlist_count=len(entries))

    explicit_ack_notifier = ExplicitAckVnextNotifier(
        acked_records=(
            NotifierAckRecord(
                fixture_id=999,
                public_status="WATCHLIST",
                template_key="TEAM_TOTAL_AWAY_UNDER_CORE",
                bookmaker_id=1,
                line=1.5,
                odds_decimal=1.9,
            ),
        ),
        attempted_count=1,
        notified_count=1,
    )

    import app.vnext.runtime.runner as runner_module

    runner_module.build_board_snapshot = forced_board_snapshot  # type: ignore[assignment]

    cycle = run_vnext_cycle(
        cycle_id=1,
        config=config,
        source=source,
        prior_result_provider=fixture_aware_prior_provider,
        deduper=deduper,
        previous_snapshots={},
        notifier=explicit_ack_notifier,
        now=datetime(2026, 4, 14, 12, 25, tzinfo=timezone.utc),
    )

    assert cycle.counters.computed_publish_count == 2
    assert cycle.counters.notified_count == 1
    assert cycle.notifier_mode == "explicit_ack"
    acked = [record for record in cycle.publication_records if record["notified"] is True]
    unacked = [record for record in cycle.publication_records if record["notified"] is False]
    assert len(acked) == 1
    assert acked[0]["fixture_id"] == 999
    assert len(unacked) == 1
    assert unacked[0]["fixture_id"] == 1001


def test_runtime_audit_exports_candidate_not_selectable_all_candidates_blocked() -> None:
    snapshot = make_live_snapshot()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={snapshot.fixture_id: ()})
    config = VnextRuntimeConfig(enable_publication_build=False, enable_notifier_send=False)
    deduper = Deduper(cooldown_seconds=180)
    selection = _selection_result(
        fixture_id=snapshot.fixture_id,
        no_selection_reason="all_candidates_blocked",
        translated_blockers=(("state_conflict",), ("low_live_snapshot_quality",)),
    )

    import app.vnext.runtime.runner as runner_module
    from app.vnext.board.arbiter import build_board_snapshot as actual_build_board_snapshot

    original_selector = runner_module.build_match_market_selection_result
    original_board = runner_module.build_board_snapshot
    try:
        runner_module.build_match_market_selection_result = lambda posterior: selection  # type: ignore[assignment]
        runner_module.build_board_snapshot = actual_build_board_snapshot  # type: ignore[assignment]
        cycle = run_vnext_cycle(
            cycle_id=1,
            config=config,
            source=source,
            prior_result_provider=fixture_aware_prior_provider,
            deduper=deduper,
            previous_snapshots={},
            now=datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc),
        )
    finally:
        runner_module.build_match_market_selection_result = original_selector  # type: ignore[assignment]
        runner_module.build_board_snapshot = original_board  # type: ignore[assignment]

    audit = cycle.fixture_audits[0]
    assert audit["governed_public_status"] == "NO_BET"
    assert "candidate_not_selectable" in audit["governance_refusal_summary"]
    assert audit["candidate_not_selectable_reason"] == "all_candidates_blocked"
    assert audit["translated_candidate_count"] == 2
    assert audit["selectable_candidate_count"] == 0
    assert audit["best_candidate_family"] is None
    assert audit["best_candidate_exists"] is None
    assert audit["best_candidate_selectable"] is None
    assert audit["best_candidate_blockers"] == []
    assert audit["distinct_candidate_blockers_summary"] == ["low_live_snapshot_quality", "state_conflict"]


def test_runtime_audit_exports_candidate_not_selectable_best_candidate_not_selectable() -> None:
    snapshot = make_live_snapshot()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={snapshot.fixture_id: ()})
    config = VnextRuntimeConfig(enable_publication_build=False, enable_notifier_send=False)
    deduper = Deduper(cooldown_seconds=180)
    selection = _selection_result(
        fixture_id=snapshot.fixture_id,
        selectable=False,
        candidate_blockers=("weak_directionality", "low_posterior_reliability"),
    )

    import app.vnext.runtime.runner as runner_module
    from app.vnext.board.arbiter import build_board_snapshot as actual_build_board_snapshot

    original_selector = runner_module.build_match_market_selection_result
    original_board = runner_module.build_board_snapshot
    try:
        runner_module.build_match_market_selection_result = lambda posterior: selection  # type: ignore[assignment]
        runner_module.build_board_snapshot = actual_build_board_snapshot  # type: ignore[assignment]
        cycle = run_vnext_cycle(
            cycle_id=1,
            config=config,
            source=source,
            prior_result_provider=fixture_aware_prior_provider,
            deduper=deduper,
            previous_snapshots={},
            now=datetime(2026, 4, 18, 12, 5, tzinfo=timezone.utc),
        )
    finally:
        runner_module.build_match_market_selection_result = original_selector  # type: ignore[assignment]
        runner_module.build_board_snapshot = original_board  # type: ignore[assignment]

    audit = cycle.fixture_audits[0]
    assert audit["governed_public_status"] == "NO_BET"
    assert "candidate_not_selectable" in audit["governance_refusal_summary"]
    assert audit["candidate_not_selectable_reason"] == "best_candidate_not_selectable"
    assert audit["translated_candidate_count"] == 1
    assert audit["selectable_candidate_count"] == 0
    assert audit["best_candidate_family"] == "OU_FT"
    assert audit["best_candidate_exists"] is True
    assert audit["best_candidate_selectable"] is False
    assert audit["best_candidate_blockers"] == ["weak_directionality", "low_posterior_reliability"]
    assert audit["distinct_candidate_blockers_summary"] == ["low_posterior_reliability", "weak_directionality"]


def test_runtime_audit_exports_publishability_low_execution_observation() -> None:
    snapshot = make_live_snapshot()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={snapshot.fixture_id: ()})
    config = VnextRuntimeConfig(enable_publication_build=False, enable_notifier_send=False)
    deduper = Deduper(cooldown_seconds=180)
    selection = _selection_result(fixture_id=snapshot.fixture_id)
    execution = _execution_result(
        fixture_id=snapshot.fixture_id,
        template_key="OU_FT_OVER_CORE",
        reason="publishability_low",
        offer_exists=True,
        blockers=("publishability_low", "retrievability_low"),
        publishability_score=0.57,
        template_binding_score=1.0,
        bookmaker_diversity_score=0.3333,
        price_integrity_score=1.0,
        retrievability_score=0.58,
    )

    def forced_board_snapshot(selections):
        from app.vnext.board.models import BoardEntry, BoardSnapshot

        return BoardSnapshot(
            entries=(
                BoardEntry(
                    fixture_id=snapshot.fixture_id,
                    internal_status="READY",
                    public_status="WATCHLIST",
                    board_score=0.72,
                    rank=1,
                    selection_result=selections[0],
                ),
            ),
            elite_count=0,
            watchlist_count=1,
        )

    import app.vnext.runtime.runner as runner_module

    original_selector = runner_module.build_match_market_selection_result
    original_execution = runner_module.build_executable_market_selection
    original_board = runner_module.build_board_snapshot
    try:
        runner_module.build_match_market_selection_result = lambda posterior: selection  # type: ignore[assignment]
        runner_module.build_executable_market_selection = lambda selection_result, offers: execution  # type: ignore[assignment]
        runner_module.build_board_snapshot = forced_board_snapshot  # type: ignore[assignment]
        cycle = run_vnext_cycle(
            cycle_id=1,
            config=config,
            source=source,
            prior_result_provider=fixture_aware_prior_provider,
            deduper=deduper,
            previous_snapshots={},
            now=datetime(2026, 4, 18, 12, 10, tzinfo=timezone.utc),
        )
    finally:
        runner_module.build_match_market_selection_result = original_selector  # type: ignore[assignment]
        runner_module.build_executable_market_selection = original_execution  # type: ignore[assignment]
        runner_module.build_board_snapshot = original_board  # type: ignore[assignment]

    audit = cycle.fixture_audits[0]
    assert audit["governed_public_status"] == "WATCHLIST"
    assert audit["publish_status"] == "DO_NOT_PUBLISH"
    assert audit["template_key"] is None
    assert audit["bookmaker_id"] is None
    assert audit["line"] is None
    assert audit["odds_decimal"] is None
    assert audit["final_execution_refusal_reason"] == "publishability_low"
    assert audit["execution_candidate_count"] == 1
    assert audit["execution_selectable_count"] == 0
    assert audit["attempted_template_keys"] == ["OU_FT_OVER_CORE"]
    assert audit["offer_present_template_keys"] == ["OU_FT_OVER_CORE"]
    assert audit["missing_offer_template_keys"] == []
    assert audit["blocked_execution_reasons_summary"] == ["publishability_low", "retrievability_low"]
    assert audit["publishability_score"] == 0.57
    assert audit["template_binding_score"] == 1.0
    assert audit["bookmaker_diversity_score"] == 0.3333
    assert audit["price_integrity_score"] == 1.0
    assert audit["retrievability_score"] == 0.58


def test_runtime_audit_exports_no_offer_found_execution_observation_without_final_offer_fields() -> None:
    snapshot = make_live_snapshot()
    source = SnapshotSource(snapshots=(snapshot,), offers_by_fixture={snapshot.fixture_id: ()})
    config = VnextRuntimeConfig(enable_publication_build=False, enable_notifier_send=False)
    deduper = Deduper(cooldown_seconds=180)
    selection = _selection_result(fixture_id=snapshot.fixture_id)
    execution = _execution_result(
        fixture_id=snapshot.fixture_id,
        template_key="OU_FT_OVER_CORE",
        reason="no_offer_found",
        offer_exists=False,
        blockers=("no_offer_found", "market_unavailable"),
    )

    def forced_board_snapshot(selections):
        from app.vnext.board.models import BoardEntry, BoardSnapshot

        return BoardSnapshot(
            entries=(
                BoardEntry(
                    fixture_id=snapshot.fixture_id,
                    internal_status="READY",
                    public_status="WATCHLIST",
                    board_score=0.72,
                    rank=1,
                    selection_result=selections[0],
                ),
            ),
            elite_count=0,
            watchlist_count=1,
        )

    import app.vnext.runtime.runner as runner_module

    original_selector = runner_module.build_match_market_selection_result
    original_execution = runner_module.build_executable_market_selection
    original_board = runner_module.build_board_snapshot
    try:
        runner_module.build_match_market_selection_result = lambda posterior: selection  # type: ignore[assignment]
        runner_module.build_executable_market_selection = lambda selection_result, offers: execution  # type: ignore[assignment]
        runner_module.build_board_snapshot = forced_board_snapshot  # type: ignore[assignment]
        cycle = run_vnext_cycle(
            cycle_id=1,
            config=config,
            source=source,
            prior_result_provider=fixture_aware_prior_provider,
            deduper=deduper,
            previous_snapshots={},
            now=datetime(2026, 4, 18, 12, 15, tzinfo=timezone.utc),
        )
    finally:
        runner_module.build_match_market_selection_result = original_selector  # type: ignore[assignment]
        runner_module.build_executable_market_selection = original_execution  # type: ignore[assignment]
        runner_module.build_board_snapshot = original_board  # type: ignore[assignment]

    audit = cycle.fixture_audits[0]
    assert audit["governed_public_status"] == "WATCHLIST"
    assert audit["publish_status"] == "DO_NOT_PUBLISH"
    assert audit["template_key"] is None
    assert audit["bookmaker_id"] is None
    assert audit["line"] is None
    assert audit["odds_decimal"] is None
    assert audit["final_execution_refusal_reason"] == "no_offer_found"
    assert audit["execution_candidate_count"] == 1
    assert audit["execution_selectable_count"] == 0
    assert audit["attempted_template_keys"] == ["OU_FT_OVER_CORE"]
    assert audit["offer_present_template_keys"] == []
    assert audit["missing_offer_template_keys"] == ["OU_FT_OVER_CORE"]
    assert audit["blocked_execution_reasons_summary"] == ["market_unavailable", "no_offer_found"]
    assert audit["publishability_score"] is None
    assert audit["template_binding_score"] is None
    assert audit["bookmaker_diversity_score"] is None
    assert audit["price_integrity_score"] is None
    assert audit["retrievability_score"] is None
