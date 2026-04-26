from app.fqis.audit.rejection_codes import RejectionCode
from app.fqis.contracts.core import BookOffer, StatisticalThesis
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey
from app.fqis.pipeline import run_thesis_pipeline


def test_pipeline_returns_best_bet_when_multiple_vehicles_exist() -> None:
    thesis = StatisticalThesis(
        event_id=601,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.84,
        confidence=0.80,
    )

    offers = (
        BookOffer(
            event_id=601,
            bookmaker_id=1,
            bookmaker_name="BookA",
            family=MarketFamily.TEAM_TOTAL_AWAY,
            side=MarketSide.UNDER,
            period=Period.FT,
            team_role=TeamRole.AWAY,
            line=1.5,
            odds_decimal=1.95,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=8,
        ),
        BookOffer(
            event_id=601,
            bookmaker_id=2,
            bookmaker_name="BookB",
            family=MarketFamily.BTTS,
            side=MarketSide.NO,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=None,
            odds_decimal=1.72,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=9,
        ),
    )

    p_real_by_intent_key = {
        "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.62,
        "BTTS|NO|NONE|NA": 0.60,
    }

    outcome = run_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key=p_real_by_intent_key,
    )

    assert outcome.best_bet is not None
    assert outcome.best_bet.edge > 0
    assert outcome.best_bet.ev > 0


def test_pipeline_rejects_when_price_too_low() -> None:
    thesis = StatisticalThesis(
        event_id=602,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.84,
        confidence=0.80,
    )

    offers = (
        BookOffer(
            event_id=602,
            bookmaker_id=1,
            bookmaker_name="BookA",
            family=MarketFamily.TEAM_TOTAL_AWAY,
            side=MarketSide.UNDER,
            period=Period.FT,
            team_role=TeamRole.AWAY,
            line=1.5,
            odds_decimal=1.40,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=8,
        ),
    )

    p_real_by_intent_key = {
        "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.70,
        "BTTS|NO|NONE|NA": 0.58,
    }

    outcome = run_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key=p_real_by_intent_key,
    )

    assert outcome.best_bet is None
    assert any(rej.code == RejectionCode.PRICE_TOO_LOW for rej in outcome.rejections)


def test_pipeline_rejects_when_no_compatible_offer_exists() -> None:
    thesis = StatisticalThesis(
        event_id=603,
        thesis_key=ThesisKey.OPEN_GAME,
        strength=0.78,
        confidence=0.73,
    )

    offers = (
        BookOffer(
            event_id=603,
            bookmaker_id=1,
            bookmaker_name="BookA",
            family=MarketFamily.MATCH_TOTAL,
            side=MarketSide.UNDER,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=2.5,
            odds_decimal=1.95,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=8,
        ),
    )

    p_real_by_intent_key = {
        "MATCH_TOTAL|OVER|NONE|2.5": 0.58,
        "BTTS|YES|NONE|NA": 0.56,
    }

    outcome = run_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key=p_real_by_intent_key,
    )

    assert outcome.best_bet is None
    assert any(rej.code == RejectionCode.OFFER_NOT_FOUND for rej in outcome.rejections)

    