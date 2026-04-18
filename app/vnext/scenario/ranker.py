from __future__ import annotations

from app.vnext.scenario.catalog import SCENARIO_CATALOG
from app.vnext.scenario.models import (
    HistoricalSubScores,
    ScenarioCandidate,
    ScenarioDefinition,
    ScenarioScoreBreakdown,
)


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _round(value: float) -> float:
    return round(value, 4)


def _style_to_signal(value: float) -> float:
    return _clip((value * 2.0) - 1.0, low=-1.0, high=1.0)


def _subscore_value(subscores: HistoricalSubScores, key: str) -> float:
    value = float(getattr(subscores, key))
    if key in {"balance_score"}:
        return _style_to_signal(value)
    return value


def _explain_name(key: str) -> str:
    return key


def evaluate_scenario(
    definition: ScenarioDefinition,
    subscores: HistoricalSubScores,
) -> ScenarioCandidate:
    structural_contributions: dict[str, float] = {}
    aligned_structural_signals: dict[str, float] = {}
    for key, weight in definition.structural_weights.items():
        signal = _subscore_value(subscores, key)
        structural_contributions[key] = signal * weight
        aligned_structural_signals[key] = signal if weight >= 0 else -signal

    style_contributions: dict[str, float] = {}
    for key, weight in definition.style_weights.items():
        style_contributions[key] = _style_to_signal(_subscore_value(subscores, key)) * weight

    matchup_score = _clip(subscores.matchup_nudge * definition.matchup_weight, low=-0.04, high=0.04)
    structural_score = sum(structural_contributions.values())
    raw_style_score = sum(style_contributions.values())
    style_cap = max(0.04, abs(structural_score) * 0.45)
    style_score = min(raw_style_score, style_cap) if raw_style_score > 0 else raw_style_score
    style_capped = raw_style_score > style_score + 1e-9

    positive_supports = [
        key
        for key, aligned_signal in aligned_structural_signals.items()
        if aligned_signal >= 0.05
    ]
    structural_support_count = len(positive_supports)
    support_shortfall = max(0, definition.minimum_structural_supports - structural_support_count)
    convergence_bonus = min(structural_support_count, 4) * 0.03
    if support_shortfall:
        convergence_bonus -= support_shortfall * 0.09

    raw_total = structural_score + style_score + matchup_score + convergence_bonus
    score = _clip(0.5 + (raw_total / 1.2))
    top_supporting = tuple(
        key
        for key, _ in sorted(
            (
                (key, contribution)
                for key, contribution in structural_contributions.items()
                if contribution > 0 and aligned_structural_signals[key] > 0
            ),
            key=lambda item: item[1],
            reverse=True,
        )[:4]
    )
    explanation = ", ".join(_explain_name(name) for name in top_supporting) or "no_convergent_support"
    return ScenarioCandidate(
        key=definition.key,
        label=definition.label,
        score=_round(score),
        breakdown=ScenarioScoreBreakdown(
            structural_score=_round(structural_score),
            style_score=_round(style_score),
            matchup_score=_round(matchup_score),
            convergence_bonus=_round(convergence_bonus),
            structural_support_count=structural_support_count,
            support_shortfall=support_shortfall,
            style_capped=style_capped,
            top_supporting_subscores=top_supporting,
        ),
        supporting_subscores=top_supporting,
        explanation=explanation,
    )


def rank_scenarios(
    subscores: HistoricalSubScores,
    *,
    catalog: tuple[ScenarioDefinition, ...] = SCENARIO_CATALOG,
) -> tuple[ScenarioCandidate, ...]:
    scenarios = tuple(evaluate_scenario(definition, subscores) for definition in catalog)
    return tuple(sorted(scenarios, key=lambda candidate: candidate.score, reverse=True))
