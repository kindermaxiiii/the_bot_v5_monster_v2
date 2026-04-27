from __future__ import annotations

import math

from app.fqis.contracts.core import StatisticalThesis
from app.fqis.contracts.enums import ThesisKey
from app.fqis.probability.intent_bridge import (
    build_p_real_by_intent_key,
    build_probability_intent_bridge_result,
)
from app.fqis.probability.live_goal_model import LiveGoalFeatures


def test_probability_intent_bridge_builds_p_real_for_low_away_hazard() -> None:
    thesis = StatisticalThesis(
        event_id=2401,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.84,
        confidence=0.80,
    )
    features = LiveGoalFeatures(
        event_id=2401,
        minute=58,
        home_score=1,
        away_score=0,
        home_xg_live=0.95,
        away_xg_live=0.18,
        home_shots_on_target=4,
        away_shots_on_target=1,
    )

    result = build_probability_intent_bridge_result((thesis,), features)

    assert result.has_probabilities
    assert result.unsupported_count == 0
    assert "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5" in result.p_real_by_intent_key
    assert "BTTS|NO|NONE|NA" in result.p_real_by_intent_key
    assert result.p_real_by_intent_key["TEAM_TOTAL_AWAY|UNDER|AWAY|1.5"] > 0.90
    assert result.p_real_by_intent_key["BTTS|NO|NONE|NA"] > 0.70
    assert math.isclose(result.distribution.total_probability, 1.0, rel_tol=1e-12)


def test_probability_intent_bridge_builds_p_real_for_low_home_hazard_without_xg() -> None:
    thesis = StatisticalThesis(
        event_id=2402,
        thesis_key=ThesisKey.LOW_HOME_SCORING_HAZARD,
        strength=0.82,
        confidence=0.78,
    )
    features = LiveGoalFeatures(
        event_id=2402,
        minute=54,
        home_score=0,
        away_score=1,
        home_shots_total=2,
        away_shots_total=9,
        home_shots_on_target=0,
        away_shots_on_target=4,
        home_corners=1,
        away_corners=5,
    )

    result = build_probability_intent_bridge_result((thesis,), features)

    assert result.has_probabilities
    assert result.unsupported_count == 0
    assert "TEAM_TOTAL_HOME|UNDER|HOME|1.5" in result.p_real_by_intent_key
    assert "BTTS|NO|NONE|NA" in result.p_real_by_intent_key
    assert result.p_real_by_intent_key["TEAM_TOTAL_HOME|UNDER|HOME|1.5"] > 0.80


def test_build_p_real_by_intent_key_supports_multiple_theses() -> None:
    theses = (
        StatisticalThesis(
            event_id=2403,
            thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
            strength=0.84,
            confidence=0.80,
        ),
        StatisticalThesis(
            event_id=2403,
            thesis_key=ThesisKey.CAGEY_GAME,
            strength=0.72,
            confidence=0.70,
        ),
    )
    features = LiveGoalFeatures(
        event_id=2403,
        minute=62,
        home_score=1,
        away_score=0,
        home_xg_live=0.70,
        away_xg_live=0.12,
        home_shots_on_target=3,
        away_shots_on_target=0,
    )

    p_real = build_p_real_by_intent_key(theses, features)

    assert "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5" in p_real
    assert "BTTS|NO|NONE|NA" in p_real
    assert "MATCH_TOTAL|UNDER|NONE|2.5" in p_real
    assert all(0.0 <= value <= 1.0 for value in p_real.values())


def test_probability_intent_bridge_is_deterministic() -> None:
    thesis = StatisticalThesis(
        event_id=2404,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.84,
        confidence=0.80,
    )
    features = LiveGoalFeatures(
        event_id=2404,
        minute=58,
        home_score=1,
        away_score=0,
        home_xg_live=0.95,
        away_xg_live=0.18,
        home_shots_on_target=4,
        away_shots_on_target=1,
    )

    first = build_p_real_by_intent_key((thesis,), features)
    second = build_p_real_by_intent_key((thesis,), features)

    assert first == second


def test_probability_intent_bridge_empty_theses_returns_empty_probabilities() -> None:
    features = LiveGoalFeatures(
        event_id=2405,
        minute=58,
        home_score=1,
        away_score=0,
        home_xg_live=0.95,
        away_xg_live=0.18,
    )

    result = build_probability_intent_bridge_result((), features)

    assert not result.has_probabilities
    assert result.p_real_by_intent_key == {}
    assert result.market_probabilities == ()
    assert result.unsupported_intents == ()

    