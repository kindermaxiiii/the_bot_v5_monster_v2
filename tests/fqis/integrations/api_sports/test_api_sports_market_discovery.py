from app.fqis.integrations.api_sports.market_discovery import (
    ApiSportsMarketSource,
    FqisMarketFamily,
    MarketMappingStatus,
    build_market_discovery_report,
    classify_market_bet,
    discover_all_markets,
    extract_market_items,
)
from app.fqis.integrations.api_sports.schemas import ApiSportsResponse


class FakeMarketClient:
    def __init__(self):
        self.calls = []

    def odds_bets(self, *, search=None):
        self.calls.append(("odds_bets", search))
        return _response(
            "odds/bets",
            [
                {"id": 1, "name": "Match Winner"},
                {"id": 5, "name": "Goals Over/Under"},
                {"id": 8, "name": "Both Teams To Score"},
            ],
        )

    def live_odds_bets(self, *, search=None):
        self.calls.append(("odds/live/bets", search))
        return _response(
            "odds/live/bets",
            [
                {"id": 1, "name": "Goals Over/Under"},
                {"id": 2, "name": "Corner Over/Under"},
            ],
        )


def test_classifies_priority_markets():
    cases = [
        ("Goals Over/Under", FqisMarketFamily.TOTALS_FULL_TIME, True),
        ("1st Half Goals Over/Under", FqisMarketFamily.TOTALS_HALF_TIME, True),
        ("Both Teams To Score", FqisMarketFamily.BTTS, False),
        ("Team Goals Over/Under", FqisMarketFamily.TEAM_TOTALS, True),
        ("Match Winner", FqisMarketFamily.MATCH_WINNER, False),
    ]

    for name, family, requires_line in cases:
        candidate = classify_market_bet(
            source=ApiSportsMarketSource.PRE_MATCH,
            provider_market_id=1,
            provider_name=name,
        )

        assert candidate.status is MarketMappingStatus.MAPPED
        assert candidate.fqis_family is family
        assert candidate.requires_line is requires_line


def test_ignores_false_positive_corner_total():
    candidate = classify_market_bet(
        source=ApiSportsMarketSource.PRE_MATCH,
        provider_market_id=99,
        provider_name="Corner Over/Under",
    )

    assert candidate.status is MarketMappingStatus.IGNORED
    assert candidate.fqis_family is None
    assert "excluded_corners_market" in candidate.reasons


def test_same_numeric_id_keeps_prematch_and_live_keys_separate():
    prematch = classify_market_bet(
        source=ApiSportsMarketSource.PRE_MATCH,
        provider_market_id=1,
        provider_name="Match Winner",
    )
    live = classify_market_bet(
        source=ApiSportsMarketSource.LIVE,
        provider_market_id=1,
        provider_name="Goals Over/Under",
    )

    assert prematch.provider_key == "api_sports:pre_match:1"
    assert live.provider_key == "api_sports:live:1"
    assert prematch.provider_key != live.provider_key


def test_discover_all_markets_calls_separate_reference_endpoints():
    client = FakeMarketClient()

    candidates = discover_all_markets(client, search="Over", include_unmapped=True)

    assert client.calls == [("odds_bets", "Over"), ("odds/live/bets", "Over")]
    assert len(candidates) == 5
    assert any(candidate.source is ApiSportsMarketSource.PRE_MATCH for candidate in candidates)
    assert any(candidate.source is ApiSportsMarketSource.LIVE for candidate in candidates)


def test_build_market_discovery_report_counts_statuses():
    candidates = discover_all_markets(FakeMarketClient(), include_unmapped=True)

    report = build_market_discovery_report(candidates)

    assert report["status"] == "COMPLETED"
    assert report["mode"] == "shadow_only_market_discovery"
    assert report["summary"] == {"total": 5, "mapped": 4, "review": 0, "ignored": 1}


def test_extract_market_items_ignores_invalid_rows():
    response = _response(
        "odds/bets",
        [
            {"id": "5", "name": "Goals Over/Under"},
            {"id": None, "name": "Broken"},
            {"id": 9, "name": None},
            "not-a-dict",
        ],
    )

    assert extract_market_items(response) == [{"id": 5, "name": "Goals Over/Under"}]


def _response(endpoint, rows):
    return ApiSportsResponse.from_payload(
        {
            "get": endpoint,
            "parameters": {},
            "errors": [],
            "results": len(rows),
            "paging": {"current": 1, "total": 1},
            "response": rows,
        }
    )