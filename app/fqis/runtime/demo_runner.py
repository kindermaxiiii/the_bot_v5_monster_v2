from __future__ import annotations

from dataclasses import dataclass

from app.fqis.contracts.core import BookOffer, ExecutableBet, StatisticalThesis
from app.fqis.contracts.enums import ThesisKey
from app.fqis.engine import GovernedOutcome, run_governed_thesis_pipeline
from app.fqis.thesis.builder import build_statistical_theses
from app.fqis.thesis.features import SimpleMatchFeatures


@dataclass(slots=True, frozen=True)
class ThesisRunResult:
    thesis: StatisticalThesis
    outcome: GovernedOutcome


@dataclass(slots=True, frozen=True)
class DemoCycleResult:
    theses: tuple[StatisticalThesis, ...]
    thesis_results: tuple[ThesisRunResult, ...]
    best_accepted_bet: ExecutableBet | None


def run_demo_cycle(
    features: SimpleMatchFeatures,
    offers: tuple[BookOffer, ...],
    *,
    p_real_by_thesis: dict[ThesisKey, dict[str, float]],
    min_strength: float,
    min_confidence: float,
    min_edge: float,
    min_ev: float,
    min_odds: float,
    max_odds: float,
    technical_min_edge: float = 0.0,
    technical_min_ev: float = -1.0,
) -> DemoCycleResult:
    theses = build_statistical_theses(features)

    thesis_results: list[ThesisRunResult] = []

    for thesis in theses:
        p_real_by_intent_key = p_real_by_thesis.get(thesis.thesis_key, {})
        outcome = run_governed_thesis_pipeline(
            thesis,
            offers,
            p_real_by_intent_key=p_real_by_intent_key,
            min_strength=min_strength,
            min_confidence=min_confidence,
            min_edge=min_edge,
            min_ev=min_ev,
            min_odds=min_odds,
            max_odds=max_odds,
            technical_min_edge=technical_min_edge,
            technical_min_ev=technical_min_ev,
        )
        thesis_results.append(ThesisRunResult(thesis=thesis, outcome=outcome))

    accepted_bets = [
        result.outcome.accepted_bet
        for result in thesis_results
        if result.outcome.accepted_bet is not None
    ]

    best_accepted_bet = _select_best_accepted_bet(tuple(accepted_bets))

    return DemoCycleResult(
        theses=theses,
        thesis_results=tuple(thesis_results),
        best_accepted_bet=best_accepted_bet,
    )


def _select_best_accepted_bet(bets: tuple[ExecutableBet | None, ...]) -> ExecutableBet | None:
    filtered = [bet for bet in bets if bet is not None]
    if not filtered:
        return None

    return max(
        filtered,
        key=lambda bet: (
            bet.score_final,
            bet.ev,
            bet.edge,
            bet.odds_decimal,
        ),
    )

    