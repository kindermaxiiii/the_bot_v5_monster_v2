from __future__ import annotations

from dataclasses import replace

from app.vnext.governance.promoter import evaluate_match_level
from app.vnext.markets.lines import line_template
from app.vnext.markets.models import MarketCandidate, MarketSupportBreakdown, MarketTranslationResult
from app.vnext.selection.models import MatchBestCandidate, MatchMarketSelectionResult
from tests.vnext.live_factories import build_reference_posterior_result


def _selection_with_candidate(*, support: float, confidence: float, directionality: float, selectable: bool = True):
    posterior = build_reference_posterior_result()
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
        fixture_id=posterior.prior_result.fixture_id,
        family="OU_FT",
        maturity="APPROVED",
        line_template=line_template("OU_FT_OVER_CORE"),
        exists=True,
        is_blocked=not selectable,
        is_selectable=selectable,
        support_score=support,
        confidence_score=confidence,
        support_breakdown=support_breakdown,
        blockers=(),
    )
    translation = MarketTranslationResult(posterior_result=posterior, candidates=(candidate,))
    return MatchMarketSelectionResult(
        translation_result=translation,
        best_candidate=MatchBestCandidate(candidate=candidate, selection_score=0.7),
        no_selection_reason=None,
    )


def test_match_progression_tracking_to_armed_ready() -> None:
    result_tracking = MatchMarketSelectionResult(
        translation_result=MarketTranslationResult(
            posterior_result=build_reference_posterior_result(),
            candidates=(),
        ),
        best_candidate=None,
        no_selection_reason="no_family_with_directional_support",
    )
    decision_tracking = evaluate_match_level(result_tracking)
    assert decision_tracking.internal_status == "TRACKING"

    result_armed = _selection_with_candidate(support=0.50, confidence=0.50, directionality=0.50, selectable=True)
    decision_armed = evaluate_match_level(result_armed)
    assert decision_armed.internal_status == "ARMED"
    assert "support_too_weak" in decision_armed.match_refusals

    result_ready = _selection_with_candidate(support=0.72, confidence=0.68, directionality=0.65, selectable=True)
    decision_ready = evaluate_match_level(result_ready)
    assert decision_ready.internal_status == "READY"


def test_candidate_not_selectable_is_armed() -> None:
    result = _selection_with_candidate(support=0.72, confidence=0.68, directionality=0.65, selectable=False)
    decision = evaluate_match_level(result)
    assert decision.internal_status == "ARMED"
    assert "candidate_not_selectable" in decision.match_refusals
