from __future__ import annotations

from app.vnext.markets.models import MarketBlocker, MarketCandidate
from app.vnext.posterior.models import ScenarioPosteriorResult

DEFENSIVE_RELIEF_KEYS = {
    "OU_FT_UNDER_CORE",
    "BTTS_NO_CORE",
    "TEAM_TOTAL_HOME_UNDER_CORE",
    "TEAM_TOTAL_AWAY_UNDER_CORE",
}


def _is_defensive_relief_candidate(candidate: MarketCandidate) -> bool:
    return candidate.line_template.key in DEFENSIVE_RELIEF_KEYS


def _is_late_state(posterior_result: ScenarioPosteriorResult) -> bool:
    return posterior_result.live_context.state.time_band == "LATE"


def _is_late_draw_state(posterior_result: ScenarioPosteriorResult) -> bool:
    state = posterior_result.live_context.state
    return state.time_band == "LATE" and state.score_diff == 0


def _posterior_stability_hard_floor(
    candidate: MarketCandidate,
    posterior_result: ScenarioPosteriorResult,
) -> float:
    if _is_defensive_relief_candidate(candidate):
        if _is_late_draw_state(posterior_result):
            return 0.53
        if _is_late_state(posterior_result):
            return 0.51
        return 0.48
    return 0.52


def _defensive_support_floor(
    candidate: MarketCandidate,
    posterior_result: ScenarioPosteriorResult,
) -> float:
    if not _is_defensive_relief_candidate(candidate):
        return 0.56
    if _is_late_draw_state(posterior_result):
        return 0.56
    if _is_late_state(posterior_result):
        return 0.54
    return 0.50


def _directionality_floor(
    candidate: MarketCandidate,
    posterior_result: ScenarioPosteriorResult,
) -> float:
    if not _is_defensive_relief_candidate(candidate):
        return 0.55
    if _is_late_draw_state(posterior_result):
        return 0.55
    if _is_late_state(posterior_result):
        return 0.53
    return 0.50


def _posterior_reliability_confidence_floor(
    candidate: MarketCandidate,
    posterior_result: ScenarioPosteriorResult,
) -> float:
    if _is_defensive_relief_candidate(candidate):
        if _is_late_draw_state(posterior_result):
            return 0.60
        if _is_late_state(posterior_result):
            return 0.58
        return 0.55
    return 0.60


def _live_snapshot_quality_floor(
    candidate: MarketCandidate,
    posterior_result: ScenarioPosteriorResult,
) -> float:
    if _is_defensive_relief_candidate(candidate):
        if _is_late_draw_state(posterior_result):
            return 0.70
        if _is_late_state(posterior_result):
            return 0.68
        return 0.64
    return 0.68


def _state_conflict(candidate: MarketCandidate, posterior_result: ScenarioPosteriorResult) -> bool:
    state = posterior_result.live_context.state
    line_key = candidate.line_template.key
    support = candidate.support_breakdown

    if state.time_band != "LATE":
        return False

    if line_key == "OU_FT_OVER_CORE":
        return abs(state.score_diff) >= 2 and support.attack_support_score < 0.70

    if line_key == "OU_FT_UNDER_CORE":
        if abs(state.score_diff) == 0:
            return (
                support.defensive_support_score < 0.62
                or support.directionality_score < 0.56
            )
        return False

    if line_key == "BTTS_YES_CORE":
        return abs(state.score_diff) >= 2 and support.attack_support_score < 0.68

    if line_key == "BTTS_NO_CORE":
        if state.leading_side == "DRAW":
            return support.defensive_support_score < 0.64
        return False

    if line_key == "TEAM_TOTAL_HOME_OVER_CORE":
        return state.leading_side == "AWAY" and support.directionality_score < 0.70

    if line_key == "TEAM_TOTAL_AWAY_OVER_CORE":
        return state.leading_side == "HOME" and support.directionality_score < 0.70

    if line_key == "TEAM_TOTAL_HOME_UNDER_CORE":
        if state.leading_side == "HOME":
            return support.defensive_support_score < 0.72
        if state.leading_side == "DRAW":
            return (
                support.defensive_support_score < 0.66
                or support.directionality_score < 0.60
            )
        return False

    if line_key == "TEAM_TOTAL_AWAY_UNDER_CORE":
        if state.leading_side == "AWAY":
            return support.defensive_support_score < 0.72
        if state.leading_side == "DRAW":
            return (
                support.defensive_support_score < 0.66
                or support.directionality_score < 0.60
            )
        return False

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
    if reliability.posterior_reliability_score < _posterior_stability_hard_floor(candidate, posterior_result):
        blockers.append(MarketBlocker(tier="HARD", code="posterior_not_stable_enough"))

    if candidate.line_template.direction in {"OVER", "YES", "HOME_OVER", "AWAY_OVER"}:
        if support.attack_support_score < 0.56:
            blockers.append(MarketBlocker(tier="STRUCTURAL", code="insufficient_attack_support"))

    if candidate.line_template.direction in {"UNDER", "NO", "HOME_UNDER", "AWAY_UNDER"}:
        if support.defensive_support_score < _defensive_support_floor(candidate, posterior_result):
            blockers.append(MarketBlocker(tier="STRUCTURAL", code="insufficient_defensive_support"))

    if support.directionality_score < _directionality_floor(candidate, posterior_result):
        blockers.append(MarketBlocker(tier="STRUCTURAL", code="weak_directionality"))
    if _state_conflict(candidate, posterior_result):
        blockers.append(MarketBlocker(tier="STRUCTURAL", code="state_conflict"))

    if reliability.posterior_reliability_score < _posterior_reliability_confidence_floor(candidate, posterior_result):
        blockers.append(MarketBlocker(tier="CONFIDENCE", code="low_posterior_reliability"))
    if reliability.live_snapshot_quality_score < _live_snapshot_quality_floor(candidate, posterior_result):
        blockers.append(MarketBlocker(tier="CONFIDENCE", code="low_live_snapshot_quality"))

    return tuple(blockers)
    