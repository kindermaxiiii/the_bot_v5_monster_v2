from app.fqis.contracts.enums import MarketFamily, MarketSide, TeamRole
from app.fqis.live.adapters import adapt_live_match_to_features, adapt_live_offers_to_book_offers


def test_adapt_live_match_to_features() -> None:
    row = {
        "event_id": 1001,
        "home_xg_live": 0.93,
        "away_xg_live": 0.21,
        "home_shots_on_target": 4,
        "away_shots_on_target": 1,
        "minute": 57,
        "home_score": 1,
        "away_score": 0,
    }

    features = adapt_live_match_to_features(row)

    assert features.event_id == 1001
    assert features.home_xg_live == 0.93
    assert features.away_score == 0


def test_adapt_live_offers_to_book_offers() -> None:
    rows = (
        {
            "event_id": 1001,
            "bookmaker_id": 1,
            "bookmaker_name": "BookA",
            "family": "TEAM_TOTAL_AWAY",
            "side": "UNDER",
            "period": "FT",
            "team_role": "AWAY",
            "line": 1.5,
            "odds_decimal": 1.91,
            "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
            "freshness_seconds": 7,
        },
    )

    offers = adapt_live_offers_to_book_offers(rows)

    assert len(offers) == 1
    assert offers[0].family == MarketFamily.TEAM_TOTAL_AWAY
    assert offers[0].side == MarketSide.UNDER
    assert offers[0].team_role == TeamRole.AWAY
    assert offers[0].line == 1.5

    