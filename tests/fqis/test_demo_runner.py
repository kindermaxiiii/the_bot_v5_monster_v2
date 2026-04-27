from app.fqis.contracts.core import BookOffer
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey
from app.fqis.runtime.demo_runner import run_demo_cycle
from app.fqis.thesis.features import SimpleMatchFeatures


def test_run_demo_cycle_returns_best_accepted_bet() -> None:
    features = SimpleMatchFeatures(
        event_id=901,
        home_xg_live=0.95,
        away_xg_live=0.18,
        home_shots_on_target=4,
        away_shots_on_target=1,
        minute=58,
        home_score=1,
        away_score=0,
    )

    offers = (
        BookOffer(
            event_id=901,
            bookmaker_id=1,
            bookmaker_name="BookA",
            family=MarketFamily.TEAM_TOTAL_AWAY,
            side=MarketSide.UNDER,
            period=Period.FT,
            team_role=TeamRole.AWAY,
            line=1.5,
            odds_decimal=1.92,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=8,
        ),
        BookOffer(
            event_id=901,
            bookmaker_id=2,
            bookmaker_name="BookB",
            family=MarketFamily.BTTS,
            side=MarketSide.NO,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=None,
            odds_decimal=1.75,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=9,
        ),
    )

    result = run_demo_cycle(
        features,
        offers,
        p_real_by_thesis={
            ThesisKey.LOW_AWAY_SCORING_HAZARD: {
                "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.62,
                "BTTS|NO|NONE|NA": 0.59,
            },
            ThesisKey.CAGEY_GAME: {
                "MATCH_TOTAL|UNDER|NONE|2.5": 0.57,
            },
        },
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.02,
        min_ev=0.01,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert len(result.theses) >= 1
    assert len(result.thesis_results) >= 1
    assert result.best_accepted_bet is not None
    assert result.best_accepted_bet.edge > 0.0


def test_run_demo_cycle_returns_none_when_no_bet_is_accepted() -> None:
    features = SimpleMatchFeatures(
        event_id=902,
        home_xg_live=1.10,
        away_xg_live=0.95,
        home_shots_on_target=5,
        away_shots_on_target=4,
        minute=64,
        home_score=1,
        away_score=1,
    )

    offers = (
        BookOffer(
            event_id=902,
            bookmaker_id=1,
            bookmaker_name="BookA",
            family=MarketFamily.MATCH_TOTAL,
            side=MarketSide.OVER,
            period=Period.FT,
            team_role=TeamRole.NONE,
            line=2.5,
            odds_decimal=1.55,
            source_timestamp_utc="2026-04-26T00:00:00+00:00",
            freshness_seconds=8,
        ),
    )

    result = run_demo_cycle(
        features,
        offers,
        p_real_by_thesis={
            ThesisKey.OPEN_GAME: {
                "MATCH_TOTAL|OVER|NONE|2.5": 0.53,
                "BTTS|YES|NONE|NA": 0.52,
            },
        },
        min_strength=0.80,
        min_confidence=0.80,
        min_edge=0.05,
        min_ev=0.05,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert result.best_accepted_bet is None