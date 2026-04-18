from __future__ import annotations

import json

from app.core.match_state import MarketQuote, MatchState, TeamLiveStats
from app.v2.intelligence.match_intelligence_layer import MatchIntelligenceLayer
from app.v2.markets.team_total_translator import TeamTotalTranslator
from app.v2.probability.unified_probability_core import UnifiedProbabilityCore
from app.v2.runtime.runtime_cycle_v2 import RuntimeCycleV2


def _state_with_team_total_quotes(
    *,
    home_goals: int = 1,
    away_goals: int = 0,
    include_team_total_pair: bool = True,
    include_btts_pair: bool = False,
    include_ou_pair: bool = False,
) -> MatchState:
    quotes: list[MarketQuote] = []

    if include_team_total_pair:
        quotes.extend(
            [
                MarketQuote(
                    market_key="TEAM_TOTAL",
                    scope="FT",
                    side="AWAY_OVER",
                    line=0.5,
                    bookmaker="bet365",
                    odds_decimal=1.60,
                    raw={},
                ),
                MarketQuote(
                    market_key="TEAM_TOTAL",
                    scope="FT",
                    side="AWAY_UNDER",
                    line=0.5,
                    bookmaker="bet365",
                    odds_decimal=2.35,
                    raw={},
                ),
            ]
        )

    if include_btts_pair:
        quotes.extend(
            [
                MarketQuote(
                    market_key="BTTS",
                    scope="FT",
                    side="YES",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=3.30,
                    raw={},
                ),
                MarketQuote(
                    market_key="BTTS",
                    scope="FT",
                    side="NO",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=1.35,
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
                    odds_decimal=3.10,
                    raw={},
                ),
                MarketQuote(
                    market_key="OU_FT",
                    scope="FT",
                    side="UNDER",
                    line=2.5,
                    bookmaker="bet365",
                    odds_decimal=1.45,
                    raw={},
                ),
            ]
        )

    return MatchState(
        fixture_id=6060,
        competition_id=78,
        competition_name="Serie A",
        country_name="Italy",
        minute=76,
        phase="2H",
        status="2H",
        home_goals=home_goals,
        away_goals=away_goals,
        feed_quality_score=0.81,
        market_quality_score=0.75,
        home=TeamLiveStats(
            team_id=1,
            name="Home",
            shots_total=15,
            shots_on_target=6,
            shots_inside_box=9,
            corners=7,
            possession=58.0,
            dangerous_attacks=37,
            attacks=82,
        ),
        away=TeamLiveStats(
            team_id=2,
            name="Away",
            shots_total=4,
            shots_on_target=1,
            shots_inside_box=1,
            corners=1,
            possession=42.0,
            dangerous_attacks=12,
            attacks=41,
        ),
        quotes=quotes,
    )


def _build_context(state: MatchState):
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)
    return intelligence, probability


def test_team_total_over_and_under_are_coherent_on_live_pair() -> None:
    state = _state_with_team_total_quotes()
    intelligence, probability = _build_context(state)
    projections = TeamTotalTranslator().translate(state, intelligence, probability)

    away_over = next(item for item in projections if item.side == "AWAY_OVER")
    away_under = next(item for item in projections if item.side == "AWAY_UNDER")

    assert away_over.market_key == "TEAM_TOTAL"
    assert away_under.market_key == "TEAM_TOTAL"
    assert away_over.executable is True
    assert away_under.executable is True
    assert abs((away_over.raw_probability + away_under.raw_probability) - 1.0) < 1e-9


def test_team_total_over_already_won_at_score_has_explicit_veto() -> None:
    state = _state_with_team_total_quotes(home_goals=2, away_goals=0, include_team_total_pair=False)
    state.quotes = [
        MarketQuote(
            market_key="TEAM_TOTAL",
            scope="FT",
            side="HOME_OVER",
            line=1.5,
            bookmaker="bet365",
            odds_decimal=1.25,
            raw={},
        ),
        MarketQuote(
            market_key="TEAM_TOTAL",
            scope="FT",
            side="HOME_UNDER",
            line=1.5,
            bookmaker="bet365",
            odds_decimal=4.50,
            raw={},
        ),
    ]
    intelligence, probability = _build_context(state)
    projections = TeamTotalTranslator().translate(state, intelligence, probability)

    home_over = next(item for item in projections if item.side == "HOME_OVER")

    assert "team_total_over_already_won_at_score" in home_over.vetoes
    assert home_over.executable is False


def test_single_sided_team_total_quote_is_not_executable() -> None:
    state = _state_with_team_total_quotes(include_team_total_pair=False)
    state.quotes = [
        MarketQuote(
            market_key="TEAM_TOTAL",
            scope="FT",
            side="AWAY_UNDER",
            line=0.5,
            bookmaker="bet365",
            odds_decimal=2.35,
            raw={},
        )
    ]
    intelligence, probability = _build_context(state)
    projections = TeamTotalTranslator().translate(state, intelligence, probability)

    assert len(projections) == 1
    assert projections[0].executable is False
    assert "pair_not_fully_live_same_book" in projections[0].vetoes


def test_no_crash_without_team_total_quotes() -> None:
    state = _state_with_team_total_quotes(include_team_total_pair=False, include_btts_pair=True, include_ou_pair=True)
    intelligence, probability = _build_context(state)

    projections = TeamTotalTranslator().translate(state, intelligence, probability)
    assert projections == []


def test_runtime_shadow_can_select_team_total_as_best_vehicle() -> None:
    state = _state_with_team_total_quotes(include_team_total_pair=True, include_btts_pair=True, include_ou_pair=True)
    state.minute = 81

    captured_exports: list[dict[str, object]] = []
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase3_test.jsonl")
    runtime._write_export = captured_exports.append
    payload = runtime.run_states([state])

    best_projection = payload["match_results"][0]["match_best"]["best_projection"]
    assert best_projection["market_key"] == "TEAM_TOTAL"
    assert best_projection["side"] == "AWAY_UNDER"

    assert len(captured_exports) == 1
    exported_line = json.loads(json.dumps(captured_exports[0]))
    assert exported_line["best_projection"]["market_key"] == "TEAM_TOTAL"
    assert exported_line["best_projection"]["side"] == "AWAY_UNDER"
