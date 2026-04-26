from __future__ import annotations

import math

import pytest

from app.fqis.contracts.core import BookOffer, StatisticalThesis
from app.fqis.contracts.enums import (
    MarketFamily,
    MarketSide,
    Period,
    TeamRole,
    ThesisKey,
)
from app.fqis.probability.hybrid import (
    HybridProbabilityConfig,
    blend_model_and_market_probability,
    build_hybrid_probability_result,
    run_hybrid_model_governed_thesis_pipeline,
    run_hybrid_model_thesis_pipeline,
)
from app.fqis.probability.live_goal_model import LiveGoalFeatures


def test_blend_model_and_market_probability_uses_normalized_weights() -> None:
    probability = blend_model_and_market_probability(
        p_model=0.64,
        p_market_no_vig=0.56,
        config=HybridProbabilityConfig(model_weight=0.70, market_weight=0.30),
    )

    assert math.isclose(probability, 0.616, rel_tol=1e-12)


def test_hybrid_probability_result_uses_market_when_available() -> None:
    result = build_hybrid_probability_result(
        {"BTTS|NO|NONE|NA": 0.64},
        {"BTTS|NO|NONE|NA": 0.56},
        config=HybridProbabilityConfig(model_weight=0.70, market_weight=0.30),
    )

    probability = result.probabilities[0]

    assert result.hybrid_count == 1
    assert result.model_only_count == 0
    assert probability.source == "hybrid"
    assert probability.has_market_prior
    assert math.isclose(probability.p_hybrid, 0.616, rel_tol=1e-12)
    assert result.p_real_by_intent_key["BTTS|NO|NONE|NA"] == probability.p_hybrid


def test_hybrid_probability_result_falls_back_to_model_when_market_missing() -> None:
    result = build_hybrid_probability_result(
        {
            "BTTS|NO|NONE|NA": 0.64,
            "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.92,
        },
        {"BTTS|NO|NONE|NA": 0.56},
    )

    by_key = {probability.intent_key: probability for probability in result.probabilities}

    assert result.hybrid_count == 1
    assert result.model_only_count == 1
    assert by_key["TEAM_TOTAL_AWAY|UNDER|AWAY|1.5"].source == "model_only"
    assert by_key["TEAM_TOTAL_AWAY|UNDER|AWAY|1.5"].p_hybrid == 0.92
    assert by_key["TEAM_TOTAL_AWAY|UNDER|AWAY|1.5"].p_market_no_vig is None


def test_hybrid_probability_config_rejects_invalid_weights() -> None:
    with pytest.raises(ValueError):
        HybridProbabilityConfig(model_weight=-0.1, market_weight=1.1)

    with pytest.raises(ValueError):
        HybridProbabilityConfig(model_weight=0.0, market_weight=0.0)


def test_hybrid_model_thesis_pipeline_produces_bet_with_hybrid_probabilities() -> None:
    thesis = _low_away_thesis()
    features = _low_away_features()
    offers = _offers_with_market_pairs()

    outcome = run_hybrid_model_thesis_pipeline(
        thesis,
        offers,
        features=features,
        hybrid_config=HybridProbabilityConfig(model_weight=0.70, market_weight=0.30),
        min_edge=0.01,
        min_ev=0.0,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert outcome.bridge_result.has_probabilities
    assert outcome.market_prior_by_intent_key
    assert outcome.hybrid_result.hybrid_count >= 1
    assert outcome.hybrid_result.p_real_by_intent_key
    assert outcome.pipeline_outcome.best_bet is not None
    assert outcome.pipeline_outcome.best_bet.p_real in set(
        outcome.hybrid_result.p_real_by_intent_key.values()
    )


def test_hybrid_model_governed_pipeline_accepts_bet() -> None:
    thesis = _low_away_thesis()
    features = _low_away_features()
    offers = _offers_with_market_pairs()

    outcome = run_hybrid_model_governed_thesis_pipeline(
        thesis,
        offers,
        features=features,
        hybrid_config=HybridProbabilityConfig(model_weight=0.70, market_weight=0.30),
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.01,
        min_ev=0.0,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert outcome.bridge_result.has_probabilities
    assert outcome.market_prior_by_intent_key
    assert outcome.hybrid_result.hybrid_count >= 1
    assert outcome.governed_outcome.technical_best_bet is not None
    assert outcome.governed_outcome.accepted_bet is not None


def test_hybrid_pipeline_falls_back_to_model_if_market_prior_incomplete() -> None:
    thesis = _low_away_thesis()
    features = _low_away_features()
    offers = _offers_without_market_pairs()

    outcome = run_hybrid_model_thesis_pipeline(
        thesis,
        offers,
        features=features,
        min_edge=0.01,
        min_ev=0.0,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert outcome.bridge_result.has_probabilities
    assert outcome.market_prior_by_intent_key == {}
    assert outcome.hybrid_result.hybrid_count == 0
    assert outcome.hybrid_result.model_only_count >= 1
    assert outcome.pipeline_outcome.best_bet is not None


def _low_away_thesis() -> StatisticalThesis:
    return StatisticalThesis(
        event_id=2901,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.84,
        confidence=0.80,
    )


def _low_away_features() -> LiveGoalFeatures:
    return LiveGoalFeatures(
        event_id=2901,
        minute=58,
        home_score=1,
        away_score=0,
        home_xg_live=0.95,
        away_xg_live=0.18,
        home_shots_total=8,
        away_shots_total=3,
        home_shots_on_target=4,
        away_shots_on_target=1,
        home_corners=4,
        away_corners=1,
    )


def _offers_with_market_pairs() -> tuple[BookOffer, ...]:
    return (
        _team_total_away_offer(MarketSide.UNDER, 1.5, 1.92),
        _team_total_away_offer(MarketSide.OVER, 1.5, 1.92),
        _btts_offer(MarketSide.NO, 1.75),
        _btts_offer(MarketSide.YES, 2.05),
    )


def _offers_without_market_pairs() -> tuple[BookOffer, ...]:
    return (
        _team_total_away_offer(MarketSide.UNDER, 1.5, 1.92),
        _btts_offer(MarketSide.NO, 1.75),
    )


def _team_total_away_offer(
    side: MarketSide,
    line: float,
    odds_decimal: float,
) -> BookOffer:
    return BookOffer(
        event_id=2901,
        bookmaker_id=1,
        bookmaker_name="BookA",
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=side,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=line,
        odds_decimal=odds_decimal,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=8,
    )


def _btts_offer(side: MarketSide, odds_decimal: float) -> BookOffer:
    return BookOffer(
        event_id=2901,
        bookmaker_id=2,
        bookmaker_name="BookB",
        family=MarketFamily.BTTS,
        side=side,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line=None,
        odds_decimal=odds_decimal,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=9,
    )

    