from __future__ import annotations

from app.vnext.selection.models import MatchMarketSelectionResult


def board_score(selection_result: MatchMarketSelectionResult) -> float:
    if selection_result.best_candidate is None:
        return 0.0
    candidate = selection_result.best_candidate.candidate
    posterior = selection_result.translation_result.posterior_result
    return round(
        (candidate.support_score * 0.50)
        + (candidate.confidence_score * 0.25)
        + (candidate.support_breakdown.directionality_score * 0.15)
        + (posterior.posterior_reliability.posterior_reliability_score * 0.10),
        4,
    )
