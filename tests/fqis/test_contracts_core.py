from app.fqis.contracts.core import BookOffer, ExecutableBet, MarketIntent, StatisticalThesis
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey


def test_statistical_thesis_instantiation() -> None:
    thesis = StatisticalThesis(
        event_id=123,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.72,
        confidence=0.81,
        rationale=("away threat low",),
        features={"away_xg_live": 0.31},
    )
    assert thesis.event_id == 123
    assert thesis.thesis_key == ThesisKey.LOW_AWAY_SCORING_HAZARD
    assert thesis.features["away_xg_live"] == 0.31


def test_market_intent_instantiation() -> None:
    intent = MarketIntent(
        event_id=123,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
        rationale=("best vehicle candidate",),
    )
    assert intent.family == MarketFamily.TEAM_TOTAL_AWAY
    assert intent.line == 1.5


def test_book_offer_instantiation() -> None:
    offer = BookOffer(
        event_id=123,
        bookmaker_id=7,
        bookmaker_name="Betify",
        family=MarketFamily.BTTS,
        side=MarketSide.NO,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line=None,
        odds_decimal=1.91,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=8,
    )
    assert offer.bookmaker_name == "Betify"
    assert offer.odds_decimal == 1.91


def test_executable_bet_instantiation() -> None:
    bet = ExecutableBet(
        event_id=123,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
        bookmaker_id=7,
        bookmaker_name="Betify",
        odds_decimal=1.91,
        p_real=0.61,
        p_implied=0.52356,
        edge=0.08644,
        ev=0.1651,
        score_stat=0.81,
        score_exec=0.74,
        score_final=0.79,
        rationale=("priced above implied",),
    )
    assert bet.edge > 0.0
    assert bet.ev > 0.0