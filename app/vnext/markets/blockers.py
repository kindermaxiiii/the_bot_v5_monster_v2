from __future__ import annotations

from app.vnext.markets.models import MarketBlocker, MarketCandidate
from app.vnext.posterior.models import ScenarioPosteriorResult


def _state_conflict(candidate: MarketCandidate, posterior_result: ScenarioPosteriorResult) -> bool:
    state = posterior_result.live_context.state
    line_key = candidate.line_template.key
    if state.time_band != "LATE":
        return False
    if line_key == "OU_FT_OVER_CORE":
        return abs(state.score_diff) >= 2 and candidate.support_breakdown.attack_support_score < 0.70
    if line_key == "OU_FT_UNDER_CORE":
        return abs(state.score_diff) == 0 and candidate.support_breakdown.defensive_support_score < 0.60
    if line_key == "BTTS_YES_CORE":
        return abs(state.score_diff) >= 2 and candidate.support_breakdown.attack_support_score < 0.68
    if line_key == "BTTS_NO_CORE":
        return state.leading_side == "DRAW" and candidate.support_breakdown.defensive_support_score < 0.62
    if line_key == "TEAM_TOTAL_HOME_OVER_CORE":
        return state.leading_side == "AWAY" and candidate.support_breakdown.directionality_score < 0.70
    if line_key == "TEAM_TOTAL_AWAY_OVER_CORE":
        return state.leading_side == "HOME" and candidate.support_breakdown.directionality_score < 0.70
    if line_key == "TEAM_TOTAL_HOME_UNDER_CORE":
        return state.leading_side == "HOME" and candidate.support_breakdown.defensive_support_score < 0.72
    if line_key == "TEAM_TOTAL_AWAY_UNDER_CORE":
        return state.leading_side == "AWAY" and candidate.support_breakdown.defensive_support_score < 0.72
    return False


def evaluate_candidate_blockers(
    candidate: MarketCandidate,
    posterior_result: ScenarioPosteriorResult,
) -> tuple[MarketBlocker, ...]:
    blockers: list[MarketBlocker] = []
    support = candidate.support_breakdown
    reliability = posterior_result.posterior_reliability

    if candidate.maturity == "PROBATION":
        blockers.append(MarketBlocker(tier="HARD", code="family_on_probation"))
    if candidate.maturity == "LAB_ONLY":
        blockers.append(MarketBlocker(tier="HARD", code="family_lab_only"))
    if support.conflict_score >= 0.52:
        blockers.append(MarketBlocker(tier="HARD", code="scenario_conflict"))
    if reliability.posterior_reliability_score < 0.55:
        blockers.append(MarketBlocker(tier="HARD", code="posterior_not_stable_enough"))

    if candidate.line_template.direction in {"OVER", "YES", "HOME_OVER", "AWAY_OVER"}:
        if support.attack_support_score < 0.56:
            blockers.append(MarketBlocker(tier="STRUCTURAL", code="insufficient_attack_support"))
    if candidate.line_template.direction in {"UNDER", "NO", "HOME_UNDER", "AWAY_UNDER"}:
        if support.defensive_support_score < 0.56:
            blockers.append(MarketBlocker(tier="STRUCTURAL", code="insufficient_defensive_support"))
    if support.directionality_score < 0.55:
        blockers.append(MarketBlocker(tier="STRUCTURAL", code="weak_directionality"))
    if _state_conflict(candidate, posterior_result):
        blockers.append(MarketBlocker(tier="STRUCTURAL", code="state_conflict"))

    if reliability.posterior_reliability_score < 0.66:
        blockers.append(MarketBlocker(tier="CONFIDENCE", code="low_posterior_reliability"))
    if reliability.live_snapshot_quality_score < 0.74:
        blockers.append(MarketBlocker(tier="CONFIDENCE", code="low_live_snapshot_quality"))

    return tuple(blockers)
