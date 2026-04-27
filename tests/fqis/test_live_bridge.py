from app.fqis.contracts.enums import ThesisKey
from app.fqis.live.bridge import run_live_bridge_cycle


def test_run_live_bridge_cycle_returns_best_accepted_bet() -> None:
    live_match_row = {
        "event_id": 1101,
        "home_xg_live": 0.95,
        "away_xg_live": 0.18,
        "home_shots_on_target": 4,
        "away_shots_on_target": 1,
        "minute": 58,
        "home_score": 1,
        "away_score": 0,
    }

    live_offer_rows = (
        {
            "event_id": 1101,
            "bookmaker_id": 1,
            "bookmaker_name": "BookA",
            "family": "TEAM_TOTAL_AWAY",
            "side": "UNDER",
            "period": "FT",
            "team_role": "AWAY",
            "line": 1.5,
            "odds_decimal": 1.92,
            "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
            "freshness_seconds": 8,
        },
        {
            "event_id": 1101,
            "bookmaker_id": 2,
            "bookmaker_name": "BookB",
            "family": "BTTS",
            "side": "NO",
            "period": "FT",
            "team_role": "NONE",
            "line": None,
            "odds_decimal": 1.75,
            "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
            "freshness_seconds": 9,
        },
    )

    result = run_live_bridge_cycle(
        live_match_row,
        live_offer_rows,
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

    assert result.best_accepted_bet is not None
    assert result.best_accepted_bet.edge > 0.0


def test_run_live_bridge_cycle_returns_none_when_no_valid_offer_exists() -> None:
    live_match_row = {
        "event_id": 1102,
        "home_xg_live": 1.10,
        "away_xg_live": 0.95,
        "home_shots_on_target": 5,
        "away_shots_on_target": 4,
        "minute": 64,
        "home_score": 1,
        "away_score": 1,
    }

    live_offer_rows = (
        {
            "event_id": 1102,
            "bookmaker_id": 1,
            "bookmaker_name": "BookA",
            "family": "MATCH_TOTAL",
            "side": "UNDER",
            "period": "FT",
            "team_role": "NONE",
            "line": 2.5,
            "odds_decimal": 1.90,
            "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
            "freshness_seconds": 8,
        },
    )

    result = run_live_bridge_cycle(
        live_match_row,
        live_offer_rows,
        p_real_by_thesis={
            ThesisKey.OPEN_GAME: {
                "MATCH_TOTAL|OVER|NONE|2.5": 0.58,
                "BTTS|YES|NONE|NA": 0.56,
            },
        },
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.02,
        min_ev=0.01,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert result.best_accepted_bet is None

    