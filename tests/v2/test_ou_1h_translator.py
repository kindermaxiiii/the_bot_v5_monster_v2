from __future__ import annotations

import json

from app.core.match_state import MarketQuote, MatchState, TeamLiveStats
from app.v2.intelligence.match_intelligence_layer import MatchIntelligenceLayer
from app.v2.markets.ou_1h_translator import OU1HTranslator
from app.v2.probability.unified_probability_core import UnifiedProbabilityCore
from app.v2.runtime.runtime_cycle_v2 import RuntimeCycleV2


def _state_with_ou_1h_quotes(
    *,
    minute: int = 26,
    phase: str = "1H",
    status: str = "1H",
    home_goals: int = 0,
    away_goals: int = 0,
    include_pair: bool = True,
    include_other_markets: bool = False,
) -> MatchState:
    quotes: list[MarketQuote] = []

    if include_pair:
        quotes.extend(
            [
                MarketQuote(
                    market_key="OU_1H",
                    scope="1H",
                    side="OVER",
                    line=0.5,
                    bookmaker="bet365",
                    odds_decimal=2.35,
                    raw={},
                ),
                MarketQuote(
                    market_key="OU_1H",
                    scope="1H",
                    side="UNDER",
                    line=0.5,
                    bookmaker="bet365",
                    odds_decimal=1.58,
                    raw={},
                ),
            ]
        )

    if include_other_markets:
        quotes.extend(
            [
                MarketQuote(
                    market_key="OU_FT",
                    scope="FT",
                    side="OVER",
                    line=2.5,
                    bookmaker="bet365",
                    odds_decimal=1.75,
                    raw={},
                ),
                MarketQuote(
                    market_key="BTTS",
                    scope="FT",
                    side="YES",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=1.85,
                    raw={},
                ),
                MarketQuote(
                    market_key="TEAM_TOTAL",
                    scope="FT",
                    side="HOME_OVER",
                    line=1.5,
                    bookmaker="bet365",
                    odds_decimal=1.90,
                    raw={},
                ),
                MarketQuote(
                    market_key="RESULT",
                    scope="FT",
                    side="HOME",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=2.20,
                    raw={},
                ),
            ]
        )

    return MatchState(
        fixture_id=8080,
        competition_id=140,
        competition_name="La Liga",
        country_name="Spain",
        minute=minute,
        phase=phase,
        status=status,
        home_goals=home_goals,
        away_goals=away_goals,
        feed_quality_score=0.81,
        market_quality_score=0.74,
        home=TeamLiveStats(
            team_id=1,
            name="Home",
            shots_total=8,
            shots_on_target=3,
            shots_inside_box=5,
            corners=4,
            possession=56.0,
            dangerous_attacks=22,
            attacks=50,
        ),
        away=TeamLiveStats(
            team_id=2,
            name="Away",
            shots_total=6,
            shots_on_target=2,
            shots_inside_box=3,
            corners=2,
            possession=44.0,
            dangerous_attacks=17,
            attacks=42,
        ),
        quotes=quotes,
    )


def _build_context(state: MatchState):
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)
    return intelligence, probability


def test_ou_1h_probabilities_sum_to_one() -> None:
    state = _state_with_ou_1h_quotes()
    intelligence, probability = _build_context(state)
    projections = OU1HTranslator().translate(state, intelligence, probability)

    over_projection = next(item for item in projections if item.side == "OVER" and item.line == 0.5)
    under_projection = next(item for item in projections if item.side == "UNDER" and item.line == 0.5)

    assert abs((over_projection.raw_probability + under_projection.raw_probability) - 1.0) < 1e-9


def test_ou_1h_returns_no_market_in_second_half() -> None:
    state = _state_with_ou_1h_quotes(minute=58, phase="2H", status="2H")
    intelligence, probability = _build_context(state)

    projections = OU1HTranslator().translate(state, intelligence, probability)
    assert projections == []


def test_ou_1h_single_sided_pair_is_not_executable() -> None:
    state = _state_with_ou_1h_quotes(include_pair=False)
    state.quotes = [
        MarketQuote(
            market_key="OU_1H",
            scope="1H",
            side="OVER",
            line=0.5,
            bookmaker="bet365",
            odds_decimal=2.35,
            raw={},
        )
    ]
    intelligence, probability = _build_context(state)
    projections = OU1HTranslator().translate(state, intelligence, probability)

    assert len(projections) == 1
    assert projections[0].executable is False
    assert "pair_not_fully_live_same_book" in projections[0].vetoes


def test_ou_1h_over_already_won_has_explicit_veto() -> None:
    state = _state_with_ou_1h_quotes(minute=31, home_goals=1, away_goals=0)
    state.quotes = [
        MarketQuote(
            market_key="OU_1H",
            scope="1H",
            side="OVER",
            line=0.5,
            bookmaker="bet365",
            odds_decimal=1.15,
            raw={},
        ),
        MarketQuote(
            market_key="OU_1H",
            scope="1H",
            side="UNDER",
            line=0.5,
            bookmaker="bet365",
            odds_decimal=5.50,
            raw={},
        ),
    ]
    intelligence, probability = _build_context(state)
    projections = OU1HTranslator().translate(state, intelligence, probability)

    over_projection = next(item for item in projections if item.side == "OVER")
    assert "over_already_won_at_score" in over_projection.vetoes
    assert over_projection.executable is False


def test_ou_1h_under_already_lost_has_explicit_veto() -> None:
    state = _state_with_ou_1h_quotes(minute=31, home_goals=1, away_goals=0)
    state.quotes = [
        MarketQuote(
            market_key="OU_1H",
            scope="1H",
            side="OVER",
            line=0.5,
            bookmaker="bet365",
            odds_decimal=1.15,
            raw={},
        ),
        MarketQuote(
            market_key="OU_1H",
            scope="1H",
            side="UNDER",
            line=0.5,
            bookmaker="bet365",
            odds_decimal=5.50,
            raw={},
        ),
    ]
    intelligence, probability = _build_context(state)
    projections = OU1HTranslator().translate(state, intelligence, probability)

    under_projection = next(item for item in projections if item.side == "UNDER")
    assert "under_already_lost_at_score" in under_projection.vetoes
    assert under_projection.executable is False


def test_runtime_shadow_can_select_ou_1h_as_best_vehicle() -> None:
    state = _state_with_ou_1h_quotes(include_pair=True, include_other_markets=True)
    state.quotes[0].odds_decimal = 3.00
    state.quotes[1].odds_decimal = 1.35

    captured_exports: list[dict[str, object]] = []
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase5_test.jsonl")
    runtime._write_export = captured_exports.append
    payload = runtime.run_states([state])

    best_projection = payload["match_results"][0]["match_best"]["best_projection"]
    assert best_projection["market_key"] == "OU_1H"
    assert best_projection["side"] == "OVER"

    assert len(captured_exports) == 1
    exported_line = json.loads(json.dumps(captured_exports[0]))
    assert exported_line["best_projection"]["market_key"] == "OU_1H"
    assert exported_line["best_projection"]["side"] == "OVER"
