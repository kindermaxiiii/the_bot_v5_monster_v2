from app.fqis.binding.binder import bind_offers_to_intent, select_best_bound_offer
from app.fqis.contracts.core import BookOffer, MarketIntent
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey


def test_bind_offers_to_intent_keeps_only_exactly_compatible_offers() -> None:
    intent = MarketIntent(
        event_id=501,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
    )

    exact = BookOffer(
        event_id=501,
        bookmaker_id=1,
        bookmaker_name="Betify",
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
        odds_decimal=1.91,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=9,
    )
    wrong_side = BookOffer(
        event_id=501,
        bookmaker_id=2,
        bookmaker_name="Betano",
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.OVER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
        odds_decimal=1.80,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=7,
    )
    wrong_line = BookOffer(
        event_id=501,
        bookmaker_id=3,
        bookmaker_name="Pinnacle",
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=0.5,
        odds_decimal=1.55,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=6,
    )

    result = bind_offers_to_intent(intent, (exact, wrong_side, wrong_line))

    assert result.matched_offers == (exact,)
    assert len(result.rejected_offers) == 2
    assert {reason for _, reason in result.rejected_offers} == {"side_mismatch", "line_mismatch"}


def test_select_best_bound_offer_prefers_best_odds_then_freshness() -> None:
    intent = MarketIntent(
        event_id=502,
        thesis_key=ThesisKey.OPEN_GAME,
        family=MarketFamily.MATCH_TOTAL,
        side=MarketSide.OVER,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line=2.5,
    )

    offer_a = BookOffer(
        event_id=502,
        bookmaker_id=1,
        bookmaker_name="A",
        family=MarketFamily.MATCH_TOTAL,
        side=MarketSide.OVER,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line=2.5,
        odds_decimal=1.95,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=15,
    )
    offer_b = BookOffer(
        event_id=502,
        bookmaker_id=2,
        bookmaker_name="B",
        family=MarketFamily.MATCH_TOTAL,
        side=MarketSide.OVER,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line=2.5,
        odds_decimal=2.00,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=20,
    )

    best = select_best_bound_offer(intent, (offer_a, offer_b))

    assert best is offer_b

    