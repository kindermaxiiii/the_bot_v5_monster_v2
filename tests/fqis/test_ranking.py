from app.fqis.contracts.core import BookOffer, MarketIntent, StatisticalThesis
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey
from app.fqis.pricing.intent_pricer import price_intent
from app.fqis.pricing.ranking import rank_priced_intents, select_best_priced_intent


def _build_priced(ev_real: float, odds: float, p_real: float):
    thesis = StatisticalThesis(
        event_id=401,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.80,
        confidence=0.77,
    )
    intent = MarketIntent(
        event_id=401,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
    )
    offer = BookOffer(
        event_id=401,
        bookmaker_id=1,
        bookmaker_name="Betify",
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
        odds_decimal=odds,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=10,
    )
    return price_intent(thesis, intent, offer, p_real=p_real)


def test_rank_priced_intents_orders_best_first() -> None:
    a = _build_priced(ev_real=0.0, odds=1.80, p_real=0.54)
    b = _build_priced(ev_real=0.0, odds=2.00, p_real=0.58)

    ranked = rank_priced_intents((a, b))

    assert ranked[0].score_final >= ranked[1].score_final


def test_select_best_priced_intent_returns_none_on_empty() -> None:
    assert select_best_priced_intent(()) is None