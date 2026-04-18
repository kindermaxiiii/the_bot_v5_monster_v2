from __future__ import annotations

from app.core.match_state import MarketQuote, MatchState, TeamLiveStats
from app.v2.intelligence.match_intelligence_layer import MatchIntelligenceLayer
from app.v2.markets.ou_ft_translator import OUFTTranslator
from app.v2.probability.unified_probability_core import UnifiedProbabilityCore


def _state_with_quotes() -> MatchState:
    return MatchState(
        fixture_id=888,
        competition_id=140,
        competition_name="La Liga",
        country_name="Spain",
        minute=58,
        phase="2H",
        status="2H",
        home_goals=1,
        away_goals=0,
        feed_quality_score=0.76,
        market_quality_score=0.71,
        home=TeamLiveStats(
            team_id=10,
            name="Home",
            shots_total=10,
            shots_on_target=4,
            shots_inside_box=6,
            corners=5,
            possession=55.0,
            dangerous_attacks=29,
            attacks=68,
        ),
        away=TeamLiveStats(
            team_id=20,
            name="Away",
            shots_total=6,
            shots_on_target=2,
            shots_inside_box=2,
            corners=2,
            possession=45.0,
            dangerous_attacks=17,
            attacks=51,
        ),
        quotes=[
            MarketQuote(
                market_key="OU_FT",
                scope="FT",
                side="OVER",
                line=2.5,
                bookmaker="bet365",
                odds_decimal=2.02,
                raw={},
            ),
            MarketQuote(
                market_key="OU_FT",
                scope="FT",
                side="UNDER",
                line=2.5,
                bookmaker="bet365",
                odds_decimal=1.82,
                raw={},
            ),
            MarketQuote(
                market_key="OU_FT",
                scope="FT",
                side="OVER",
                line=1.5,
                bookmaker="bet365",
                odds_decimal=1.45,
                raw={},
            ),
            MarketQuote(
                market_key="OU_FT",
                scope="FT",
                side="UNDER",
                line=1.5,
                bookmaker="bet365",
                odds_decimal=2.65,
                raw={},
            ),
        ],
    )


def _build_context(state: MatchState):
    intelligence = MatchIntelligenceLayer().build(state)
    probability = UnifiedProbabilityCore().build(intelligence)
    return intelligence, probability


def test_under_budget_discrete_is_coherent() -> None:
    state = _state_with_quotes()
    intelligence, probability = _build_context(state)
    projections = OUFTTranslator().translate(state, intelligence, probability)

    under_25 = next(item for item in projections if item.side == "UNDER" and item.line == 2.5)
    assert under_25.score_state_budget == 1
    assert under_25.payload["goal_budget_under"] == 1
    assert under_25.adverse_resolution_distance == 2.0


def test_over_goals_needed_is_coherent() -> None:
    state = _state_with_quotes()
    intelligence, probability = _build_context(state)
    projections = OUFTTranslator().translate(state, intelligence, probability)

    over_25 = next(item for item in projections if item.side == "OVER" and item.line == 2.5)
    assert over_25.score_state_budget == 2
    assert over_25.payload["goals_needed_for_over"] == 2
    assert over_25.favorable_resolution_distance == 2.0


def test_output_is_homogeneous() -> None:
    state = _state_with_quotes()
    intelligence, probability = _build_context(state)
    projections = OUFTTranslator().translate(state, intelligence, probability)

    assert projections
    for projection in projections:
        assert projection.market_key == "OU_FT"
        assert projection.side in {"OVER", "UNDER"}
        assert projection.bookmaker == "bet365"
        assert projection.odds_decimal is not None
        assert projection.payload["score"] == "1-0"
        assert isinstance(projection.reasons, list)
        assert isinstance(projection.vetoes, list)


def test_no_crash_without_quotes() -> None:
    state = _state_with_quotes()
    state.quotes = []
    intelligence, probability = _build_context(state)

    projections = OUFTTranslator().translate(state, intelligence, probability)
    assert projections == []


def test_single_sided_ou_quote_is_not_executable() -> None:
    state = _state_with_quotes()
    state.quotes = [
        MarketQuote(
            market_key="OU_FT",
            scope="FT",
            side="OVER",
            line=2.5,
            bookmaker="bet365",
            odds_decimal=2.02,
            raw={},
        )
    ]
    intelligence, probability = _build_context(state)
    projections = OUFTTranslator().translate(state, intelligence, probability)

    assert len(projections) == 1
    assert projections[0].price_state == "DEGRADE_MAIS_VIVANT"
    assert projections[0].executable is False
    assert "pair_not_fully_live_same_book" in projections[0].vetoes


def test_same_book_live_pair_is_executable() -> None:
    state = _state_with_quotes()
    intelligence, probability = _build_context(state)
    projections = OUFTTranslator().translate(state, intelligence, probability)

    under_25 = next(item for item in projections if item.side == "UNDER" and item.line == 2.5)
    over_25 = next(item for item in projections if item.side == "OVER" and item.line == 2.5)

    assert under_25.executable is True
    assert over_25.executable is True
    assert under_25.price_state == "VIVANT"
    assert over_25.price_state == "VIVANT"


def test_executable_flag_is_false_when_quote_is_not_live() -> None:
    state = _state_with_quotes()
    state.quotes.append(
        MarketQuote(
            market_key="OU_FT",
            scope="FT",
            side="UNDER",
            line=0.5,
            bookmaker="slowbook",
            odds_decimal=1.95,
            raw={"is_stopped": True},
        )
    )
    intelligence, probability = _build_context(state)
    projections = OUFTTranslator().translate(state, intelligence, probability)

    under_25 = next(item for item in projections if item.side == "UNDER" and item.line == 2.5)
    dead_under_05 = next(item for item in projections if item.side == "UNDER" and item.line == 0.5)

    assert under_25.executable is True
    assert dead_under_05.executable is False
    assert "quote_not_live" in dead_under_05.vetoes


def test_over_already_won_at_score_has_explicit_veto() -> None:
    state = _state_with_quotes()
    state.home_goals = 2
    state.away_goals = 1
    state.quotes = [
        MarketQuote(
            market_key="OU_FT",
            scope="FT",
            side="OVER",
            line=2.5,
            bookmaker="bet365",
            odds_decimal=1.20,
            raw={},
        ),
        MarketQuote(
            market_key="OU_FT",
            scope="FT",
            side="UNDER",
            line=2.5,
            bookmaker="bet365",
            odds_decimal=4.80,
            raw={},
        ),
    ]
    intelligence, probability = _build_context(state)
    projections = OUFTTranslator().translate(state, intelligence, probability)

    over_25 = next(item for item in projections if item.side == "OVER" and item.line == 2.5)

    assert "over_already_won_at_score" in over_25.vetoes
    assert over_25.executable is False
