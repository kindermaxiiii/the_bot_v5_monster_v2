from __future__ import annotations

from app.vnext.governance.models import GovernanceThresholds, PromotionDecision
from app.vnext.governance.rules import DEFAULT_THRESHOLDS
from app.vnext.selection.models import MatchMarketSelectionResult


def evaluate_match_level(
    selection_result: MatchMarketSelectionResult,
    *,
    thresholds: GovernanceThresholds = DEFAULT_THRESHOLDS,
) -> PromotionDecision:
    match_refusals: list[str] = []
    if selection_result.best_candidate is None:
        if selection_result.no_selection_reason:
            if selection_result.no_selection_reason == "posterior_too_weak":
                match_refusals.append("posterior_too_weak")
            elif selection_result.no_selection_reason == "all_candidates_blocked":
                match_refusals.append("candidate_not_selectable")
            else:
                match_refusals.append("no_best_candidate")
        else:
            match_refusals.append("no_best_candidate")
        return PromotionDecision(
            internal_status="TRACKING",
            public_status="NO_BET",
            match_refusals=tuple(match_refusals),
        )

    candidate = selection_result.best_candidate.candidate
    if not candidate.is_selectable:
        match_refusals.append("candidate_not_selectable")
        return PromotionDecision(
            internal_status="ARMED",
            public_status="NO_BET",
            match_refusals=tuple(match_refusals),
        )

    if candidate.family not in {"OU_FT", "BTTS", "TEAM_TOTAL"}:
        match_refusals.append("family_not_allowed")
        return PromotionDecision(
            internal_status="ARMED",
            public_status="NO_BET",
            match_refusals=tuple(match_refusals),
        )

    posterior = selection_result.translation_result.posterior_result
    if posterior.posterior_reliability.posterior_reliability_score < thresholds.ready_min_posterior_reliability:
        match_refusals.append("posterior_too_weak")
    if candidate.support_breakdown.directionality_score < thresholds.ready_min_directionality:
        match_refusals.append("directionality_too_weak")
    if candidate.support_score < thresholds.ready_min_support:
        match_refusals.append("support_too_weak")
    if candidate.confidence_score < thresholds.ready_min_confidence:
        match_refusals.append("confidence_too_weak")

    if match_refusals:
        return PromotionDecision(
            internal_status="ARMED",
            public_status="NO_BET",
            match_refusals=tuple(match_refusals),
        )

    return PromotionDecision(
        internal_status="READY",
        public_status="NO_BET",
        match_refusals=tuple(match_refusals),
    )
