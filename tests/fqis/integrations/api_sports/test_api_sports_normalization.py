import json

from app.fqis.integrations.api_sports.normalization import (
    ApiSportsNormalizer,
    FqisNormalizedWriter,
    FqisOddsSelection,
    infer_line,
    infer_period,
    infer_selection,
    normalize_fixture,
    normalize_odds_response,
)


def test_normalize_fixture_basic():
    fixture = normalize_fixture(
        {
            "fixture": {"id": 123, "date": "2026-04-27T19:00:00+00:00", "status": {"short": "NS", "long": "Not Started", "elapsed": None}},
            "league": {"id": 61, "name": "Ligue 1", "country": "France", "season": 2026},
            "teams": {
                "home": {"id": 1, "name": "Home FC"},
                "away": {"id": 2, "name": "Away FC"},
            },
        }
    )

    assert fixture is not None
    assert fixture.provider_fixture_id == "123"
    assert fixture.fixture_key == "api_sports:fixture:123"
    assert fixture.league_name == "Ligue 1"
    assert fixture.home_team_name == "Home FC"
    assert fixture.away_team_name == "Away FC"


def test_normalize_fixture_missing_id_returns_none():
    assert normalize_fixture({"fixture": {}, "teams": {}}) is None


def test_normalize_pre_match_odds_offer():
    offers = normalize_odds_response(
        [
            {
                "fixture": {"id": 123},
                "update": "2026-04-27T12:00:00+00:00",
                "bookmakers": [
                    {
                        "id": 8,
                        "name": "Book",
                        "bets": [
                            {
                                "id": 5,
                                "name": "Goals Over/Under",
                                "values": [
                                    {"value": "Over 2.5", "odd": "1.91"},
                                    {"value": "Under 2.5", "odd": "1.95"},
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
        source="pre_match",
    )

    assert len(offers) == 2
    assert offers[0].fixture_key == "api_sports:fixture:123"
    assert offers[0].provider_market_key.startswith("api_sports:pre_match:")
    assert offers[0].fqis_market_family == "totals_full_time"
    assert offers[0].selection == "over"
    assert offers[0].line == 2.5
    assert offers[0].decimal_odds == 1.91


def test_live_and_pre_match_market_keys_are_distinct():
    pre = normalize_odds_response(
        [{"fixture": {"id": 1}, "bookmakers": [{"id": 1, "name": "B", "bets": [{"id": 5, "name": "Goals Over/Under", "values": [{"value": "Over 2.5", "odd": "1.9"}]}]}]}],
        source="pre_match",
    )[0]
    live = normalize_odds_response(
        [{"fixture": {"id": 1}, "bookmakers": [{"id": 1, "name": "B", "bets": [{"id": 5, "name": "Goals Over/Under", "values": [{"value": "Over 2.5", "odd": "1.9"}]}]}]}],
        source="live",
    )[0]

    assert pre.provider_market_key != live.provider_market_key
    assert pre.provider_market_key == "api_sports:pre_match:5"
    assert live.provider_market_key == "api_sports:live:5"


def test_invalid_odds_are_rejected():
    offer = normalize_odds_response(
        [{"fixture": {"id": 1}, "bookmakers": [{"id": 1, "name": "B", "bets": [{"id": 5, "name": "Goals Over/Under", "values": [{"value": "Over 2.5", "odd": "1.0"}]}]}]}],
        source="pre_match",
    )[0]

    assert offer.normalization_status == "REJECTED"
    assert "invalid_decimal_odds" in offer.warnings


def test_unknown_market_goes_to_review_or_rejected():
    offer = normalize_odds_response(
        [{"fixture": {"id": 1}, "bookmakers": [{"id": 1, "name": "B", "bets": [{"id": 9999, "name": "Alien Market", "values": [{"value": "Something", "odd": "2.0"}]}]}]}],
        source="pre_match",
    )[0]

    assert offer.mapping_status in {"REVIEW", "IGNORED"}
    assert offer.normalization_status in {"REVIEW", "REJECTED"}


def test_infer_helpers():
    assert infer_selection("Over 2.5", market_name="Goals Over/Under") == FqisOddsSelection.OVER
    assert infer_selection("No", market_name="Both Teams Score") == FqisOddsSelection.NO
    assert infer_line("Under 3.5") == 3.5
    assert infer_period("First Half Goals Over/Under") == "first_half"


def test_normalizer_fixture_payload():
    batch = ApiSportsNormalizer().normalize_payload(
        {
            "get": "fixtures",
            "response": [
                {
                    "fixture": {"id": 123, "date": "2026-04-27T19:00:00+00:00", "status": {"short": "NS"}},
                    "league": {"id": 61, "name": "Ligue 1"},
                    "teams": {"home": {"id": 1, "name": "A"}, "away": {"id": 2, "name": "B"}},
                }
            ],
        },
        source="fixtures",
        run_id="run_1",
        snapshot_id="snap_1",
    )

    assert batch.summary["fixtures"] == 1
    assert batch.summary["odds_offers"] == 0


def test_writer_roundtrip(tmp_path):
    batch = ApiSportsNormalizer().normalize_payload(
        {"get": "fixtures", "response": []},
        source="fixtures",
        run_id="run_1",
        snapshot_id="snap_1",
    )

    path = FqisNormalizedWriter(tmp_path).write(batch)
    data = json.loads(path.read_text(encoding="utf-8"))

    assert data["provider"] == "api_sports_api_football"
    assert data["run_id"] == "run_1"
    assert path.exists()
