from __future__ import annotations

from app.vnext.markets.models import MarketCandidate
from app.vnext.markets.translators import translate_market_candidates
from app.vnext.posterior.models import ScenarioPosteriorResult
from app.vnext.selection.models import MatchBestCandidate, MatchMarketSelectionResult


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _selection_score(candidate: MarketCandidate) -> float:
    return _clip(
        (candidate.support_score * 0.58)
        + (candidate.confidence_score * 0.24)
        + (candidate.support_breakdown.directionality_score * 0.18)
    )


def build_match_market_selection_result(
    posterior_result: ScenarioPosteriorResult,
) -> MatchMarketSelectionResult:
    translation_result = translate_market_candidates(posterior_result)
    candidates = translation_result.candidates
    if not candidates:
        return MatchMarketSelectionResult(
            translation_result=translation_result,
            best_candidate=None,
            no_selection_reason="no_family_with_directional_support",
        )

    directional_candidates = [
        candidate
        for candidate in candidates
        if candidate.support_score >= 0.60 and candidate.support_breakdown.directionality_score >= 0.60
    ]
    if not directional_candidates:
        return MatchMarketSelectionResult(
            translation_result=translation_result,
            best_candidate=None,
            no_selection_reason="no_family_with_directional_support",
        )

    selectable = [
        candidate
        for candidate in candidates
        if candidate.exists and candidate.is_selectable and candidate.family != "RESULT"
    ]
    if not selectable:
        if posterior_result.posterior_reliability.posterior_reliability_score < 0.58:
            reason = "posterior_too_weak"
        else:
            reason = "all_candidates_blocked"
        return MatchMarketSelectionResult(
            translation_result=translation_result,
            best_candidate=None,
            no_selection_reason=reason,
        )

    ranked = sorted(
        selectable,
        key=lambda candidate: (
            _selection_score(candidate),
            candidate.support_score,
            candidate.confidence_score,
            candidate.support_breakdown.directionality_score,
        ),
        reverse=True,
    )
    best = ranked[0]
    return MatchMarketSelectionResult(
        translation_result=translation_result,
        best_candidate=MatchBestCandidate(
            candidate=best,
            selection_score=round(_selection_score(best), 4),
            rationale=(
                best.family,
                best.line_template.key,
                *best.support_breakdown.supporting_scenarios[:2],
            ),
        ),
        no_selection_reason=None,
    )
