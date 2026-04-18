from __future__ import annotations

import json

from app.v2.contracts import (
    BoardBestVehicle,
    MarketProjectionV2,
    MatchBestVehicle,
    MatchIntelligenceSnapshot,
    MatchPrioritySnapshot,
    ProbabilityState,
)


def test_contract_instantiation_and_simple_serialization() -> None:
    intelligence = MatchIntelligenceSnapshot(
        fixture_id=101,
        minute=62,
        score="1-0",
        home_goals=1,
        away_goals=0,
        fixture_priority_score=7.4,
        regime_label="OPEN_EXCHANGE",
        regime_confidence=0.71,
        pressure_home=0.66,
        pressure_away=0.49,
        threat_home=0.59,
        threat_away=0.41,
        openness=0.63,
        slowdown=0.34,
        chaos=0.47,
        remaining_goal_expectancy=1.18,
        score_state_fragility=0.51,
        feed_quality=0.74,
        market_quality=0.68,
        diagnostics={"quote_count": 6},
    )
    probability = ProbabilityState(
        fixture_id=101,
        minute=62,
        score="1-0",
        home_goals=1,
        away_goals=0,
        lambda_home_remaining=0.66,
        lambda_away_remaining=0.52,
        lambda_total_remaining=1.18,
        ft_score_grid={"1-0": 0.31, "2-0": 0.19},
        remaining_added_goal_probs={0: 0.31, 1: 0.42, 2: 0.19, 3: 0.08},
        final_total_goal_probs={1: 0.31, 2: 0.42, 3: 0.19, 4: 0.08},
        home_goal_probs={1: 0.47, 2: 0.35, 3: 0.18},
        away_goal_probs={0: 0.56, 1: 0.31, 2: 0.13},
        uncertainty_score=0.24,
        diagnostics={"distribution_cap": 6},
    )
    projection = MarketProjectionV2(
        market_key="OU_FT",
        side="UNDER",
        line=2.5,
        bookmaker="bet365",
        odds_decimal=1.85,
        raw_probability=0.73,
        calibrated_probability=0.71,
        market_no_vig_probability=0.54,
        edge=0.17,
        expected_value=0.31,
        executable=True,
        price_state="VIVANT",
        payload={},
        reasons=["shared_probability_core"],
        vetoes=[],
        favorable_resolution_distance=0.0,
        adverse_resolution_distance=2.0,
        resolution_pressure=0.59,
        state_fragility_score=0.34,
        late_fragility_score=0.28,
        early_fragility_score=0.18,
        score_state_budget=1,
        market_findability_score=0.72,
        publishability_score=0.78,
        reasons_of_refusal=["single_book_market"],
        market_gate_state="MARKET_ELIGIBLE",
        thesis_gate_state="PUBLISHABLE",
    )
    match_best = MatchBestVehicle(
        fixture_id=101,
        best_projection=projection,
        dominance_score=0.14,
        candidate_count=2,
        diagnostics={"best_score": 1.2},
    )
    priority = MatchPrioritySnapshot(
        fixture_id=101,
        q_match=7.8,
        q_stats=7.2,
        q_odds=7.4,
        q_live=6.8,
        q_competition=7.6,
        q_noise=2.5,
        priority_tier="ELITE_CANDIDATE",
        match_gate_state="MATCH_ELIGIBLE",
        diagnostics={"tier_reasons": ["structural_quality_supports_elite"]},
    )
    board_best = BoardBestVehicle(
        best_projection=projection,
        match_rankings=[match_best],
        board_dominance_score=0.14,
        top_bet_eligible=False,
        board_gate_state="NO_BET",
        shadow_alert_tier="NONE",
        elite_shadow_eligible=False,
        watchlist_shadow_eligible=False,
        diagnostics={"best_fixture_id": 101},
    )

    serialized = json.dumps(
        {
            "intelligence": intelligence.to_dict(),
            "probability": probability.to_dict(),
            "priority": priority.to_dict(),
            "match_best": match_best.to_dict(),
            "board_best": board_best.to_dict(),
        },
        ensure_ascii=True,
    )

    assert '"fixture_id": 101' in serialized
    assert '"market_key": "OU_FT"' in serialized
    assert '"bookmaker": "bet365"' in serialized
    assert '"market_findability_score": 0.72' in serialized
    assert '"match_gate_state": "MATCH_ELIGIBLE"' in serialized
    assert board_best.to_dict()["top_bet_eligible"] is False
    assert projection.to_public_dict()["market_key"] == "OU_FT"
    assert "market_findability_score" not in projection.to_public_dict()
    assert projection.to_debug_dict()["market_findability_score"] == 0.72
    assert priority.to_public_dict()["priority_tier"] == "ELITE_CANDIDATE"
    assert "match_gate_state" not in priority.to_public_dict()
    assert board_best.to_public_dict(shadow_alert_tier="NO_BET")["shadow_alert_tier"] == "NO_BET"
