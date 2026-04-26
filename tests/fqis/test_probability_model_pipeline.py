from __future__ import annotations

import pytest

from app.fqis.contracts.core import BookOffer, StatisticalThesis
from app.fqis.contracts.enums import (
    MarketFamily,
    MarketSide,
    Period,
    TeamRole,
    ThesisKey,
)
from app.fqis.probability.live_goal_model import LiveGoalFeatures
from app.fqis.probability.model_pipeline import (
    run_external_probability_thesis_pipeline,
    run_model_generated_governed_thesis_pipeline,
    run_model_generated_thesis_pipeline,
    run_probability_thesis_pipeline,
)


def test_model_generated_thesis_pipeline_produces_executable_bet() -> None:
    thesis = _low_away_thesis()
    offers = _low_away_offers()
    features = _low_away_features()

    outcome = run_model_generated_thesis_pipeline(
        thesis,
        offers,
        features=features,
        min_edge=0.01,
        min_ev=0.0,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert outcome.p_real_source == "model"
    assert outcome.bridge_result is not None
    assert outcome.bridge_result.has_probabilities
    assert "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5" in outcome.p_real_by_intent_key
    assert "BTTS|NO|NONE|NA" in outcome.p_real_by_intent_key
    assert outcome.pipeline_outcome.best_bet is not None
    assert outcome.pipeline_outcome.best_bet.p_real > 0.70


def test_external_probability_pipeline_keeps_existing_behavior() -> None:
    thesis = _low_away_thesis()
    offers = _low_away_offers()

    outcome = run_external_probability_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key={
            "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.62,
            "BTTS|NO|NONE|NA": 0.59,
        },
        min_edge=0.01,
        min_ev=0.0,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert outcome.p_real_source == "external"
    assert outcome.bridge_result is None
    assert outcome.pipeline_outcome.best_bet is not None
    assert outcome.pipeline_outcome.best_bet.p_real in {0.62, 0.59}


def test_probability_pipeline_requires_features_in_model_mode() -> None:
    with pytest.raises(ValueError):
        run_probability_thesis_pipeline(
            _low_away_thesis(),
            _low_away_offers(),
            p_real_source="model",
            features=None,
        )


def test_probability_pipeline_requires_p_real_in_external_mode() -> None:
    with pytest.raises(ValueError):
        run_probability_thesis_pipeline(
            _low_away_thesis(),
            _low_away_offers(),
            p_real_source="external",
            p_real_by_intent_key=None,
        )


def test_model_generated_governed_pipeline_accepts_bet() -> None:
    thesis = _low_away_thesis()
    offers = _low_away_offers()
    features = _low_away_features()

    outcome = run_model_generated_governed_thesis_pipeline(
        thesis,
        offers,
        features=features,
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.01,
        min_ev=0.0,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert outcome.p_real_source == "model"
    assert outcome.bridge_result is not None
    assert outcome.governed_outcome.technical_best_bet is not None
    assert outcome.governed_outcome.accepted_bet is not None


def _low_away_thesis() -> StatisticalThesis:
    return StatisticalThesis(
        event_id=2501,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.84,
        confidence=0.80,
    )


def _low_away_features() -> LiveGoalFeatures:
    return LiveGoalFeatures(
        event_id=2501,
        minute=58,
        home_score=1,
        away_score=0,
        home_xg_live=0.95,
        away_xg_live=0.18,
        home_shots_on_target=4,
        away_shots_on_target=1,
    )


def _low_away_offers() -> tuple[BookOffer, ...]:
    return (
        BookOffer(
            event_id=2501,
            bookmaker_id=1,
            bookmaker_name="BookA",
            family=MarketFamily.TEAM_TOTAL_AWAY,
            side=MarketSide.UNDER,
            period=Period.FT,
            team_role=TeamRole.AWAY,
            line=1.5,
            odds_decimal=1.92,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=8,
        ),
        BookOffer(
            event_id=2501,
            bookmaker_id=2,
            bookmaker_name="BookB",
            family=MarketFamily.BTTS,
            side=MarketSide.NO,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=None,
            odds_decimal=1.75,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=9,
        ),
    )

    