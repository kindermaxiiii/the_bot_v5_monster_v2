from __future__ import annotations

from statistics import mean

from app.vnext.data.normalized_models import quality_flag_from_score, worst_quality_flag
from app.vnext.live.models import LiveContextPack
from app.vnext.posterior.models import (
    PosteriorReliabilityBreakdown,
    ScenarioPosteriorCandidate,
    ScenarioPosteriorResult,
)
from app.vnext.posterior.modifiers import build_modifier
from app.vnext.scenario.models import ScenarioPriorResult


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _event_strength(context: LiveContextPack) -> float:
    events = context.break_events
    strength = 0.0
    if events.goal_scored:
        strength = max(strength, 0.45)
    if events.equalizer_event:
        strength = max(strength, 0.70)
    if events.red_card_occurred:
        strength = max(strength, 0.90)
    if events.lead_change_event or events.two_goal_gap_event:
        strength = 1.0
    return strength


def _phase_factor(time_band: str) -> float:
    if time_band == "EARLY":
        return 0.85
    if time_band == "MID":
        return 1.0
    return 1.12


def _status_shift(status: str, *, event_strength: float) -> float:
    scale = 0.015 + (0.015 * event_strength)
    if status == "CONFIRME":
        return scale
    if status == "RUPTURE":
        return -(scale * 1.5)
    if status == "CONTRARIE":
        return -scale
    return 0.0


def _reliability_tailwind(
    context: LiveContextPack,
    *,
    live_snapshot_quality_score: float,
    event_clarity_score: float,
    state_coherence_score: float,
) -> float:
    bonus = 0.0

    if live_snapshot_quality_score >= 0.70:
        bonus += 0.02
    if state_coherence_score >= 0.78:
        bonus += 0.02

    # Late, coherent, non-chaotic states should not be over-penalized
    # just because no big break event happened recently.
    if (
        context.state.time_band == "LATE"
        and state_coherence_score >= 0.78
        and event_clarity_score <= 0.35
    ):
        bonus += 0.02

    return min(bonus, 0.05)


def build_posterior_reliability(
    prior_result: ScenarioPriorResult,
    context: LiveContextPack,
) -> PosteriorReliabilityBreakdown:
    prior_reliability_score = prior_result.prior_reliability.prior_reliability_score
    live_snapshot_quality_score = context.current_snapshot.live_snapshot_quality_score
    event_clarity_score = context.break_events.event_clarity_score
    state_coherence_score = context.state.state_coherence_score

    tailwind = _reliability_tailwind(
        context,
        live_snapshot_quality_score=live_snapshot_quality_score,
        event_clarity_score=event_clarity_score,
        state_coherence_score=state_coherence_score,
    )

    posterior_reliability_score = _clip(
        (prior_reliability_score * 0.42)
        + (live_snapshot_quality_score * 0.30)
        + (event_clarity_score * 0.08)
        + (state_coherence_score * 0.20)
        + tailwind
    )

    return PosteriorReliabilityBreakdown(
        prior_reliability_score=round(prior_reliability_score, 4),
        live_snapshot_quality_score=round(live_snapshot_quality_score, 4),
        event_clarity_score=round(event_clarity_score, 4),
        state_coherence_score=round(state_coherence_score, 4),
        posterior_reliability_score=round(posterior_reliability_score, 4),
    )


def update_scenario_posterior(
    prior_result: ScenarioPriorResult,
    context: LiveContextPack,
) -> ScenarioPosteriorResult:
    posterior_reliability = build_posterior_reliability(prior_result, context)
    event_strength = _event_strength(context)
    phase_factor = _phase_factor(context.state.time_band)
    alignment_weight = (0.12 + (0.08 * event_strength)) * phase_factor
    alignment_weight *= 0.55 + (posterior_reliability.posterior_reliability_score * 0.45)
    break_weight = 0.48 * event_strength * context.break_events.event_clarity_score
    posterior_candidates: list[ScenarioPosteriorCandidate] = []
    for prior_candidate in prior_result.scenarios:
        modifier = build_modifier(key=prior_candidate.key, context=context)
        posterior_score = _clip(
            prior_candidate.score
            + (modifier.live_alignment_score * alignment_weight)
            + (modifier.break_event_impact * break_weight)
            + _status_shift(modifier.status, event_strength=event_strength)
        )
        posterior_candidates.append(
            ScenarioPosteriorCandidate(
                key=prior_candidate.key,
                label=prior_candidate.label,
                prior_score=prior_candidate.score,
                posterior_score=round(posterior_score, 4),
                delta_score=round(posterior_score - prior_candidate.score, 4),
                modifier=modifier,
                explanation=", ".join(modifier.rationale),
            )
        )

    ranked = tuple(sorted(posterior_candidates, key=lambda candidate: candidate.posterior_score, reverse=True))
    top = ranked[0]
    data_quality_score = mean(
        [
            prior_result.prior_reliability.data_quality_score,
            context.current_snapshot.live_snapshot_quality_score,
            context.state.state_coherence_score,
        ]
    )
    return ScenarioPosteriorResult(
        prior_result=prior_result,
        live_context=context,
        posterior_reliability=posterior_reliability,
        scenarios=ranked,
        top_prior_scenario_key=prior_result.top_scenario.key,
        top_posterior_scenario=top,
        top_changed=top.key != prior_result.top_scenario.key,
        data_quality_flag=worst_quality_flag(
            prior_result.data_quality_flag,
            quality_flag_from_score(data_quality_score),
            context.current_snapshot.data_quality_flag,
        ),
        notes=(),
    )
    