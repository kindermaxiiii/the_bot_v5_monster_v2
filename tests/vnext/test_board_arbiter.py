from __future__ import annotations

from dataclasses import replace

from app.vnext.board.arbiter import build_board_snapshot
from app.vnext.markets.lines import line_template
from app.vnext.markets.models import MarketCandidate, MarketSupportBreakdown, MarketTranslationResult
from app.vnext.selection.models import MatchBestCandidate, MatchMarketSelectionResult
from tests.vnext.live_factories import build_reference_posterior_result


def _selection(
    *,
    fixture_id: int,
    support: float,
    confidence: float,
    directionality: float,
) -> MatchMarketSelectionResult:
    posterior = build_reference_posterior_result()
    posterior = replace(posterior, prior_result=replace(posterior.prior_result, fixture_id=fixture_id))
    support_breakdown = MarketSupportBreakdown(
        scenario_support_score=0.7,
        attack_support_score=0.7,
        defensive_support_score=0.6,
        directionality_score=directionality,
        live_support_score=0.6,
        reliability_score=posterior.posterior_reliability.posterior_reliability_score,
        conflict_score=0.1,
        supporting_scenarios=("HOME_CONTROL",),
        supporting_signals=("attack_support",),
    )
    candidate = MarketCandidate(
        fixture_id=fixture_id,
        family="OU_FT",
        maturity="APPROVED",
        line_template=line_template("OU_FT_OVER_CORE"),
        exists=True,
        is_blocked=False,
        is_selectable=True,
        support_score=support,
        confidence_score=confidence,
        support_breakdown=support_breakdown,
        blockers=(),
    )
    translation = MarketTranslationResult(posterior_result=posterior, candidates=(candidate,))
    return MatchMarketSelectionResult(
        translation_result=translation,
        best_candidate=MatchBestCandidate(candidate=candidate, selection_score=round((support + confidence + directionality) / 3.0, 4)),
        no_selection_reason=None,
    )


def test_elite_cap_and_watchlist_cap() -> None:
    selections = (
        _selection(fixture_id=1, support=0.82, confidence=0.74, directionality=0.70),
        _selection(fixture_id=2, support=0.76, confidence=0.70, directionality=0.68),
        _selection(fixture_id=3, support=0.74, confidence=0.68, directionality=0.66),
        _selection(fixture_id=4, support=0.72, confidence=0.66, directionality=0.64),
        _selection(fixture_id=5, support=0.70, confidence=0.64, directionality=0.62),
    )
    snapshot = build_board_snapshot(selections)

    assert snapshot.elite_count <= 1
    assert snapshot.watchlist_count <= 3
    assert sum(1 for entry in snapshot.entries if entry.public_status == "ELITE") <= 1
    assert sum(1 for entry in snapshot.entries if entry.public_status == "WATCHLIST") <= 3


def test_board_refusals_are_separate_from_match_refusals() -> None:
    selections = (
        _selection(fixture_id=10, support=0.82, confidence=0.74, directionality=0.70),
        _selection(fixture_id=11, support=0.70, confidence=0.60, directionality=0.60),
        _selection(fixture_id=12, support=0.68, confidence=0.58, directionality=0.58),
        _selection(fixture_id=13, support=0.66, confidence=0.56, directionality=0.56),
    )
    snapshot = build_board_snapshot(selections)

    for entry in snapshot.entries:
        if entry.match_refusals:
            assert all(refusal in {"support_too_weak", "confidence_too_weak", "directionality_too_weak", "posterior_too_weak"} for refusal in entry.match_refusals)
        if entry.board_refusals:
            assert all(refusal in {"board_not_dominant", "elite_thresholds_not_met", "watchlist_capacity_reached", "elite_capacity_reached", "better_match_already_promoted"} for refusal in entry.board_refusals)
