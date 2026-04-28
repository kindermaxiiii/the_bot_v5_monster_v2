import json

from app.fqis.integrations.api_sports.live_odds_coverage_diagnostics import (
    ApiSportsLiveOddsCoverageDiagnosticsConfig,
    build_api_sports_live_odds_coverage_diagnostics,
    render_api_sports_live_odds_coverage_diagnostics_markdown,
    sample_live_odds_coverage_payloads,
)


def test_live_odds_coverage_sample_metrics():
    fixtures_payload, odds_payload, candidates_payload = sample_live_odds_coverage_payloads()

    result = build_api_sports_live_odds_coverage_diagnostics(
        fixtures_payload=fixtures_payload,
        odds_payload=odds_payload,
        candidates_payload=candidates_payload,
        config=ApiSportsLiveOddsCoverageDiagnosticsConfig(min_odds=1.25, max_odds=8.0),
    )

    assert result.status == "READY"
    assert result.mode == "LIVE_ODDS_COVERAGE_DIAGNOSTICS"
    assert result.real_staking_enabled is False
    assert result.metrics["live_fixtures_total"] == 2
    assert result.metrics["live_odds_fixtures_total"] == 2
    assert result.metrics["matched_fixture_odds_total"] == 2
    assert result.metrics["blocked_fixtures_total"] == 1
    assert result.metrics["markets_total"] == 3
    assert result.metrics["supported_markets_total"] == 2
    assert result.metrics["unsupported_markets_total"] == 1
    assert result.metrics["values_total"] == 7
    assert result.metrics["suspended_values_total"] == 4
    assert result.metrics["invalid_odds_total"] == 1
    assert result.metrics["below_min_odds_total"] == 1
    assert result.metrics["supported_tradable_values_total"] == 3
    assert result.metrics["candidates_total"] == 3


def test_live_odds_coverage_detects_no_matched_fixture_odds():
    fixtures_payload = {
        "fixtures": [
            {"fixture_id": 111, "match": "A vs B", "live": True},
        ]
    }
    odds_payload = {
        "response": [
            {
                "fixture": {"id": 222},
                "status": {"blocked": True, "finished": False},
                "odds": [],
            }
        ]
    }

    result = build_api_sports_live_odds_coverage_diagnostics(
        fixtures_payload=fixtures_payload,
        odds_payload=odds_payload,
        candidates_payload={"candidates": []},
    )

    assert result.metrics["matched_fixture_odds_total"] == 0
    assert result.metrics["live_fixtures_without_live_odds_total"] == 1
    assert "NO_MATCHED_LIVE_ODDS_FOR_INPLAY_FIXTURES" in result.warnings
    assert "NO_CANDIDATES_FOUND" in result.warnings


def test_live_odds_coverage_reads_paths_and_renders_markdown(tmp_path):
    fixtures_payload, odds_payload, candidates_payload = sample_live_odds_coverage_payloads()

    fixtures = tmp_path / "inplay_fixtures.json"
    odds = tmp_path / "odds_live_raw.json"
    candidates = tmp_path / "paper_candidates.json"

    fixtures.write_text(json.dumps(fixtures_payload), encoding="utf-8")
    odds.write_text(json.dumps(odds_payload), encoding="utf-8")
    candidates.write_text(json.dumps(candidates_payload), encoding="utf-8")

    result = build_api_sports_live_odds_coverage_diagnostics(
        fixtures_path=fixtures,
        odds_path=odds,
        candidates_path=candidates,
    )

    markdown = render_api_sports_live_odds_coverage_diagnostics_markdown(result)

    assert result.metrics["candidates_total"] == 3
    assert "Coverage Metrics" in markdown
    assert "OBSERVATION ONLY" in markdown
