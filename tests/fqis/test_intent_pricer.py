import math

from app.fqis.contracts.core import BookOffer, MarketIntent, StatisticalThesis
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey
from app.fqis.pricing.intent_pricer import price_intent, to_executable_bet


def test_price_intent_computes_probabilities_edge_and_ev() -> None:
    thesis = StatisticalThesis(
        event_id=301,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.82,
        confidence=0.78,
    )
    intent = MarketIntent(
        event_id=301,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
    )
    offer = BookOffer(
        event_id=301,
        bookmaker_id=7,
        bookmaker_name="Betify",
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
        odds_decimal=1.91,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=8,
    )

    priced = price_intent(thesis, intent, offer, p_real=0.61)

    assert math.isclose(priced.p_implied, 1 / 1.91, rel_tol=1e-9)
    assert priced.edge > 0.0
    assert priced.ev > 0.0
    assert priced.score_final > 0.0


def test_to_executable_bet_preserves_priced_values() -> None:
    thesis = StatisticalThesis(
        event_id=302,
        thesis_key=ThesisKey.OPEN_GAME,
        strength=0.76,
        confidence=0.74,
    )
    intent = MarketIntent(
        event_id=302,
        thesis_key=ThesisKey.OPEN_GAME,
        family=MarketFamily.MATCH_TOTAL,
        side=MarketSide.OVER,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line=2.5,
    )
    offer = BookOffer(
        event_id=302,
        bookmaker_id=9,
        bookmaker_name="Betano",
        family=MarketFamily.MATCH_TOTAL,
        side=MarketSide.OVER,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line=2.5,
        odds_decimal=2.05,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=12,
    )

    priced = price_intent(thesis, intent, offer, p_real=0.56)
    bet = to_executable_bet(priced)

    assert bet.event_id == 302
    assert bet.odds_decimal == 2.05
    assert bet.ev == priced.ev