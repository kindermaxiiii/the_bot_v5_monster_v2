from app.fqis.contracts.core import BookOffer, StatisticalThesis
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey
from app.fqis.engine import run_governed_thesis_pipeline


def test_governed_pipeline_accepts_valid_bet() -> None:
    thesis = StatisticalThesis(
        event_id=801,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.84,
        confidence=0.80,
    )

    offers = (
        BookOffer(
            event_id=801,
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
            event_id=801,
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

    outcome = run_governed_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key={
            "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.62,
            "BTTS|NO|NONE|NA": 0.60,
        },
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.02,
        min_ev=0.01,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert outcome.technical_best_bet is not None
    assert outcome.accepted_bet is not None
    assert outcome.accepted_bet.edge > 0
    assert outcome.accepted_bet.ev > 0


def test_governed_pipeline_keeps_technical_bet_but_rejects_it_on_risk() -> None:
    thesis = StatisticalThesis(
        event_id=802,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.84,
        confidence=0.80,
    )

    offers = (
        BookOffer(
            event_id=802,
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
    )

    outcome = run_governed_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key={
            "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.54,
            "BTTS|NO|NONE|NA": 0.53,
        },
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.06,
        min_ev=0.05,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert outcome.technical_best_bet is not None
    assert outcome.accepted_bet is None
    assert len(outcome.risk_rejections) >= 1


def test_governed_pipeline_returns_no_bet_when_pipeline_finds_nothing() -> None:
    thesis = StatisticalThesis(
        event_id=803,
        thesis_key=ThesisKey.OPEN_GAME,
        strength=0.79,
        confidence=0.76,
    )

    offers = (
        BookOffer(
            event_id=803,
            bookmaker_id=1,
            bookmaker_name="BookA",
            family=MarketFamily.MATCH_TOTAL,
            side=MarketSide.UNDER,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=2.5,
            odds_decimal=1.90,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=8,
        ),
    )

    outcome = run_governed_thesis_pipeline(
        thesis,
        offers,
        p_real_by_intent_key={
            "MATCH_TOTAL|OVER|NONE|2.5": 0.58,
            "BTTS|YES|NONE|NA": 0.56,
        },
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.02,
        min_ev=0.01,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert outcome.technical_best_bet is None
    assert outcome.accepted_bet is None
    assert len(outcome.pipeline_rejections) >= 1