from __future__ import annotations

import json

from app.core.match_state import MarketQuote, MatchState, TeamLiveStats
from app.v2.intelligence.match_intelligence_layer import MatchIntelligenceLayer
from app.v2.markets.btts_translator import BTTSTranslator
from app.v2.probability.unified_probability_core import UnifiedProbabilityCore
from app.v2.runtime.runtime_cycle_v2 import RuntimeCycleV2


def _state_with_btts_quotes(
    *,
    home_goals: int = 1,
    away_goals: int = 0,
    include_btts_pair: bool = True,
    include_ou_pair: bool = False,
) -> MatchState:
    quotes: list[MarketQuote] = []

    if include_btts_pair:
        quotes.extend(
            [
                MarketQuote(
                    market_key="BTTS",
                    scope="FT",
                    side="YES",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=2.20,
                    raw={},
                ),
                MarketQuote(
                    market_key="BTTS",
                    scope="FT",
                    side="NO",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=1.72,
                    raw={},
                ),
            ]
        )

    if include_ou_pair:
        quotes.extend(
            [
                MarketQuote(
                    market_key="OU_FT",
                    scope="FT",
                    side="OVER",
                    line=2.5,
                    bookmaker="bet365",
                    odds_decimal=3.00,
                    raw={},
                ),
                MarketQuote(
                    market_key="OU_FT",
                    scope="FT",
                    side="UNDER",
                    line=2.5,
                    bookmaker="bet365",
                    odds_decimal=1.40,
                    raw={},
                ),
            ]
        )

    return MatchState(
        fixture_id=5050,
        competition_id=61,
        competition_name="Ligue 1",
        country_name="France",
        minute=72,
        phase="2H",
        status="2H",
        home_goals=home_goals,
        away_goals=away_goals,
        feed_quality_score=0.80,
        market_quality_score=0.74,
        home=TeamLiveStats(
            team_id=1,
            name="Home",
            shots_total=13,
            shots_on_target=5,
            shots_inside_box=8,
            corners=6,
            possession=57.0,
            dangerous_attacks=34,
            attacks=78,
        ),
        away=TeamLiveStats(
            team_id=2,
            name="Away",
            shots_total=5,
            shots_on_target=1,
            shots_inside_box=2,
            corners=2,
            possession=43.0,
            dangerous_attacks=15,
            attacks=47,
        ),
        quotes=quotes,
    )


def _build_context(state: MatchState):
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)
    return intelligence, probability


def test_btts_yes_and_no_are_coherent_on_live_same_book_pair() -> None:
    state = _state_with_btts_quotes()
    intelligence, probability = _build_context(state)
    projections = BTTSTranslator().translate(state, intelligence, probability)

    yes_projection = next(item for item in projections if item.side == "YES")
    no_projection = next(item for item in projections if item.side == "NO")

    assert yes_projection.market_key == "BTTS"
    assert no_projection.market_key == "BTTS"
    assert yes_projection.executable is True
    assert no_projection.executable is True
    assert abs((yes_projection.raw_probability + no_projection.raw_probability) - 1.0) < 1e-9


def test_btts_yes_already_won_at_score_is_vetoed_and_not_executable() -> None:
    state = _state_with_btts_quotes(home_goals=1, away_goals=1)
    intelligence, probability = _build_context(state)
    projections = BTTSTranslator().translate(state, intelligence, probability)

    yes_projection = next(item for item in projections if item.side == "YES")
    no_projection = next(item for item in projections if item.side == "NO")

    assert "btts_yes_already_won_at_score" in yes_projection.vetoes
    assert "btts_no_already_lost_at_score" in no_projection.vetoes
    assert yes_projection.executable is False
    assert no_projection.executable is False


def test_btts_no_lives_when_only_one_team_has_scored() -> None:
    state = _state_with_btts_quotes(home_goals=1, away_goals=0)
    intelligence, probability = _build_context(state)
    projections = BTTSTranslator().translate(state, intelligence, probability)

    no_projection = next(item for item in projections if item.side == "NO")

    assert no_projection.executable is True
    assert no_projection.payload["silent_team"] == "AWAY"
    assert no_projection.payload["teams_already_both_scored"] is False
    assert no_projection.payload["btts_state_resolved"] is False
    assert no_projection.score_state_budget == 1
    assert no_projection.favorable_resolution_distance == 0.0
    assert no_projection.adverse_resolution_distance == 1.0
    assert 0.0 <= no_projection.resolution_pressure


def test_btts_no_budget_differs_between_zero_zero_and_one_zero() -> None:
    zero_zero_state = _state_with_btts_quotes(home_goals=0, away_goals=0)
    one_zero_state = _state_with_btts_quotes(home_goals=1, away_goals=0)

    zero_zero_intelligence, zero_zero_probability = _build_context(zero_zero_state)
    one_zero_intelligence, one_zero_probability = _build_context(one_zero_state)

    zero_zero_projection = next(
        item
        for item in BTTSTranslator().translate(zero_zero_state, zero_zero_intelligence, zero_zero_probability)
        if item.side == "NO"
    )
    one_zero_projection = next(
        item
        for item in BTTSTranslator().translate(one_zero_state, one_zero_intelligence, one_zero_probability)
        if item.side == "NO"
    )

    assert zero_zero_projection.score_state_budget == 2
    assert one_zero_projection.score_state_budget == 1
    assert zero_zero_projection.score_state_budget != one_zero_projection.score_state_budget


def test_single_sided_btts_quote_is_not_executable() -> None:
    state = _state_with_btts_quotes(include_btts_pair=False)
    state.quotes = [
        MarketQuote(
            market_key="BTTS",
            scope="FT",
            side="YES",
            line=None,
            bookmaker="bet365",
            odds_decimal=2.20,
            raw={},
        )
    ]
    intelligence, probability = _build_context(state)
    projections = BTTSTranslator().translate(state, intelligence, probability)

    assert len(projections) == 1
    assert projections[0].price_state == "DEGRADE_MAIS_VIVANT"
    assert projections[0].executable is False
    assert "pair_not_fully_live_same_book" in projections[0].vetoes


def test_no_crash_without_btts_quotes() -> None:
    state = _state_with_btts_quotes(include_btts_pair=False, include_ou_pair=True)
    intelligence, probability = _build_context(state)

    projections = BTTSTranslator().translate(state, intelligence, probability)
    assert projections == []


def test_runtime_shadow_can_select_btts_as_best_vehicle() -> None:
    state = _state_with_btts_quotes(home_goals=1, away_goals=0, include_btts_pair=True, include_ou_pair=True)
    state.minute = 79
    state.quotes = [
        MarketQuote(
            market_key="BTTS",
            scope="FT",
            side="YES",
            line=None,
            bookmaker="bet365",
            odds_decimal=2.55,
            raw={},
        ),
        MarketQuote(
            market_key="BTTS",
            scope="FT",
            side="NO",
            line=None,
            bookmaker="bet365",
            odds_decimal=2.30,
            raw={},
        ),
        MarketQuote(
            market_key="OU_FT",
            scope="FT",
            side="OVER",
            line=2.5,
            bookmaker="bet365",
            odds_decimal=3.00,
            raw={},
        ),
        MarketQuote(
            market_key="OU_FT",
            scope="FT",
            side="UNDER",
            line=2.5,
            bookmaker="bet365",
            odds_decimal=1.40,
            raw={},
        ),
    ]

    captured_exports: list[dict[str, object]] = []
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase2_test.jsonl")
    runtime._write_export = captured_exports.append
    payload = runtime.run_states([state])

    best_projection = payload["match_results"][0]["match_best"]["best_projection"]
    assert best_projection["market_key"] == "BTTS"
    assert best_projection["side"] == "NO"

    assert len(captured_exports) == 1
    exported_line = json.loads(json.dumps(captured_exports[0]))
    assert exported_line["best_projection"]["market_key"] == "BTTS"
    assert exported_line["best_projection"]["side"] == "NO"
