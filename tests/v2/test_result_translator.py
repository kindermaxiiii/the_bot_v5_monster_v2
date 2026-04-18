from __future__ import annotations

import json

from app.core.match_state import MarketQuote, MatchState, TeamLiveStats
from app.v2.intelligence.match_intelligence_layer import MatchIntelligenceLayer
from app.v2.markets.result_translator import ResultTranslator
from app.v2.probability.unified_probability_core import UnifiedProbabilityCore
from app.v2.runtime.runtime_cycle_v2 import RuntimeCycleV2


def _state_with_result_quotes(
    *,
    home_goals: int = 0,
    away_goals: int = 0,
    include_result_triplet: bool = True,
    include_other_markets: bool = False,
) -> MatchState:
    quotes: list[MarketQuote] = []

    if include_result_triplet:
        quotes.extend(
            [
                MarketQuote(
                    market_key="RESULT",
                    scope="FT",
                    side="HOME",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=2.55,
                    raw={},
                ),
                MarketQuote(
                    market_key="RESULT",
                    scope="FT",
                    side="DRAW",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=2.90,
                    raw={},
                ),
                MarketQuote(
                    market_key="RESULT",
                    scope="FT",
                    side="AWAY",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=5.60,
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
                    odds_decimal=1.55,
                    raw={},
                ),
                MarketQuote(
                    market_key="OU_FT",
                    scope="FT",
                    side="UNDER",
                    line=2.5,
                    bookmaker="bet365",
                    odds_decimal=2.55,
                    raw={},
                ),
                MarketQuote(
                    market_key="BTTS",
                    scope="FT",
                    side="YES",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=1.70,
                    raw={},
                ),
                MarketQuote(
                    market_key="BTTS",
                    scope="FT",
                    side="NO",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=2.20,
                    raw={},
                ),
                MarketQuote(
                    market_key="TEAM_TOTAL",
                    scope="FT",
                    side="HOME_OVER",
                    line=1.5,
                    bookmaker="bet365",
                    odds_decimal=1.65,
                    raw={},
                ),
                MarketQuote(
                    market_key="TEAM_TOTAL",
                    scope="FT",
                    side="HOME_UNDER",
                    line=1.5,
                    bookmaker="bet365",
                    odds_decimal=2.15,
                    raw={},
                ),
            ]
        )

    return MatchState(
        fixture_id=7070,
        competition_id=135,
        competition_name="Bundesliga",
        country_name="Germany",
        minute=74,
        phase="2H",
        status="2H",
        home_goals=home_goals,
        away_goals=away_goals,
        feed_quality_score=0.82,
        market_quality_score=0.76,
        home=TeamLiveStats(
            team_id=1,
            name="Home",
            shots_total=14,
            shots_on_target=6,
            shots_inside_box=9,
            corners=7,
            possession=59.0,
            dangerous_attacks=36,
            attacks=84,
        ),
        away=TeamLiveStats(
            team_id=2,
            name="Away",
            shots_total=5,
            shots_on_target=1,
            shots_inside_box=2,
            corners=2,
            possession=41.0,
            dangerous_attacks=13,
            attacks=44,
        ),
        quotes=quotes,
    )


def _build_context(state: MatchState):
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)
    return intelligence, probability


def test_result_probabilities_sum_to_one() -> None:
    state = _state_with_result_quotes(home_goals=0, away_goals=0)
    intelligence, probability = _build_context(state)
    projections = ResultTranslator().translate(state, intelligence, probability)

    home_projection = next(item for item in projections if item.side == "HOME")
    draw_projection = next(item for item in projections if item.side == "DRAW")
    away_projection = next(item for item in projections if item.side == "AWAY")

    assert abs((home_projection.raw_probability + draw_projection.raw_probability + away_projection.raw_probability) - 1.0) < 1e-9


def test_result_draw_already_won_at_tied_score_is_vetoed() -> None:
    state = _state_with_result_quotes(home_goals=1, away_goals=1)
    intelligence, probability = _build_context(state)
    projections = ResultTranslator().translate(state, intelligence, probability)

    draw_projection = next(item for item in projections if item.side == "DRAW")

    assert "result_draw_already_won_at_score" in draw_projection.vetoes
    assert draw_projection.executable is False


def test_result_distances_are_coherent_when_home_is_already_leading() -> None:
    state = _state_with_result_quotes(home_goals=1, away_goals=0)
    intelligence, probability = _build_context(state)
    projections = ResultTranslator().translate(state, intelligence, probability)

    home_projection = next(item for item in projections if item.side == "HOME")
    away_projection = next(item for item in projections if item.side == "AWAY")

    assert home_projection.favorable_resolution_distance == 0.0
    assert home_projection.adverse_resolution_distance == 1.0
    assert away_projection.favorable_resolution_distance == 2.0
    assert home_projection.favorable_resolution_distance < away_projection.favorable_resolution_distance


def test_result_triplet_incomplete_is_not_executable() -> None:
    state = _state_with_result_quotes(include_result_triplet=False)
    state.quotes = [
        MarketQuote(
            market_key="RESULT",
            scope="FT",
            side="HOME",
            line=None,
            bookmaker="bet365",
            odds_decimal=2.55,
            raw={},
        ),
        MarketQuote(
            market_key="RESULT",
            scope="FT",
            side="DRAW",
            line=None,
            bookmaker="bet365",
            odds_decimal=2.90,
            raw={},
        ),
    ]
    intelligence, probability = _build_context(state)
    projections = ResultTranslator().translate(state, intelligence, probability)

    assert len(projections) == 2
    assert all(projection.executable is False for projection in projections)
    assert all("pair_or_triplet_not_fully_live_same_book" in projection.vetoes for projection in projections)


def test_no_crash_without_result_quotes() -> None:
    state = _state_with_result_quotes(include_result_triplet=False, include_other_markets=True)
    intelligence, probability = _build_context(state)

    projections = ResultTranslator().translate(state, intelligence, probability)
    assert projections == []


def test_runtime_shadow_can_select_result_as_best_vehicle() -> None:
    state = _state_with_result_quotes(home_goals=0, away_goals=0, include_result_triplet=True, include_other_markets=False)
    state.quotes = [
        MarketQuote(
            market_key="RESULT",
            scope="FT",
            side="HOME",
            line=None,
            bookmaker="bet365",
            odds_decimal=3.40,
            raw={},
        ),
        MarketQuote(
            market_key="RESULT",
            scope="FT",
            side="DRAW",
            line=None,
            bookmaker="bet365",
            odds_decimal=2.40,
            raw={},
        ),
        MarketQuote(
            market_key="RESULT",
            scope="FT",
            side="AWAY",
            line=None,
            bookmaker="bet365",
            odds_decimal=1.30,
            raw={},
        ),
    ]
    state.quotes.extend(
        [
            MarketQuote(
                market_key="OU_FT",
                scope="FT",
                side="OVER",
                line=2.5,
                bookmaker="bet365",
                odds_decimal=1.55,
                raw={},
            ),
            MarketQuote(
                market_key="BTTS",
                scope="FT",
                side="YES",
                line=None,
                bookmaker="bet365",
                odds_decimal=1.70,
                raw={},
            ),
            MarketQuote(
                market_key="TEAM_TOTAL",
                scope="FT",
                side="HOME_OVER",
                line=1.5,
                bookmaker="bet365",
                odds_decimal=1.65,
                raw={},
            ),
        ]
    )

    captured_exports: list[dict[str, object]] = []
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase4_test.jsonl")
    runtime._write_export = captured_exports.append
    payload = runtime.run_states([state])

    best_projection = payload["match_results"][0]["match_best"]["best_projection"]
    assert best_projection["market_key"] == "RESULT"
    assert best_projection["side"] == "HOME"

    assert len(captured_exports) == 1
    exported_line = json.loads(json.dumps(captured_exports[0]))
    assert exported_line["best_projection"]["market_key"] == "RESULT"
    assert exported_line["best_projection"]["side"] == "HOME"
