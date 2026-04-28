
import json

from app.fqis.integrations.api_sports.live_market_snapshot import (
    ApiSportsLiveMarketSnapshotConfig,
    build_api_sports_live_market_snapshot,
    render_api_sports_live_market_snapshot_markdown,
    write_api_sports_live_market_snapshot,
)


def _offers():
    return [
        {
            "match": "Team A vs Team B",
            "market": "1X2",
            "selection": "Home",
            "bookmaker": "Book A",
            "odds": 2.00,
            "fixture_id": 100,
            "kickoff_utc": "2026-04-28T19:00:00+00:00",
        },
        {
            "match": "Team A vs Team B",
            "market": "1X2",
            "selection": "Home",
            "bookmaker": "Book B",
            "odds": 2.10,
            "fixture_id": 100,
            "kickoff_utc": "2026-04-28T19:00:00+00:00",
        },
        {
            "match": "Team A vs Team B",
            "market": "Total Goals",
            "selection": "Over 2.5",
            "bookmaker": "Book A",
            "odds": 1.95,
            "fixture_id": 100,
        },
    ]


def test_live_market_snapshot_aggregates_best_odds():
    result = build_api_sports_live_market_snapshot(candidates=_offers())

    assert result.status == "READY"
    assert result.mode == "LIVE_MARKET_SNAPSHOT"
    assert result.real_staking_enabled is False

    home = result.rows[0]

    assert home.match == "Team A vs Team B"
    assert home.market == "1X2"
    assert home.selection == "Home"
    assert home.best_bookmaker == "Book B"
    assert home.best_odds == 2.10
    assert home.average_odds == 2.05
    assert home.bookmakers_count == 2
    assert home.offers_count == 2
    assert home.status == "OBSERVATION_ONLY"


def test_live_market_snapshot_can_require_min_bookmakers():
    result = build_api_sports_live_market_snapshot(
        candidates=_offers(),
        config=ApiSportsLiveMarketSnapshotConfig(min_bookmakers=2),
    )

    assert len(result.rows) == 1
    assert result.rows[0].selection == "Home"


def test_write_live_market_snapshot_creates_json_and_markdown(tmp_path):
    source = tmp_path / "paper_candidates.json"
    output = tmp_path / "live_market_snapshot.json"
    markdown = tmp_path / "live_market_snapshot.md"

    source.write_text(json.dumps({"candidates": _offers()}), encoding="utf-8")

    result = write_api_sports_live_market_snapshot(
        source_path=source,
        output_path=output,
        markdown_path=markdown,
    )

    assert result.status == "READY"
    assert output.exists()
    assert markdown.exists()

    payload = json.loads(output.read_text(encoding="utf-8"))

    assert payload["status"] == "READY"
    assert payload["mode"] == "LIVE_MARKET_SNAPSHOT"
    assert payload["rows"][0]["best_bookmaker"] == "Book B"
    assert "Best Odds View" in markdown.read_text(encoding="utf-8")


def test_render_live_market_snapshot_markdown_contains_observation_warning():
    result = build_api_sports_live_market_snapshot(candidates=_offers())
    markdown = render_api_sports_live_market_snapshot_markdown(result)

    assert "OBSERVATION ONLY" in markdown
    assert "Team A vs Team B" in markdown
    assert "Book B" in markdown
