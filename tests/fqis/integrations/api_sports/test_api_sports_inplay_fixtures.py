import json

from app.fqis.integrations.api_sports.inplay_fixtures import (
    ApiSportsInplayFixturesConfig,
    build_api_sports_inplay_fixtures_from_payload,
    sample_inplay_fixtures_payload,
    write_api_sports_inplay_fixtures,
)


def test_inplay_fixtures_sample_extracts_only_live_fixture():
    result = build_api_sports_inplay_fixtures_from_payload(
        sample_inplay_fixtures_payload(),
        config=ApiSportsInplayFixturesConfig(max_fixtures=10),
    )

    assert result.status == "READY"
    assert result.mode == "INPLAY_FIXTURES"
    assert result.real_staking_enabled is False
    assert result.ready is True
    assert len(result.fixtures) == 1
    assert len(result.rejected) == 1

    fixture = result.fixtures[0]

    assert fixture.fixture_id == 9001
    assert fixture.match == "Sample Home vs Sample Away"
    assert fixture.elapsed == 37
    assert fixture.status_short == "1H"
    assert fixture.score_home == 1
    assert fixture.score_away == 0
    assert fixture.live is True


def test_inplay_fixtures_empty_payload_is_ready_with_warning():
    result = build_api_sports_inplay_fixtures_from_payload(
        {"response": []},
        config=ApiSportsInplayFixturesConfig(max_fixtures=10),
    )

    assert result.status == "READY"
    assert result.ready is True
    assert len(result.fixtures) == 0
    assert "NO_INPLAY_FIXTURES_FOUND" in result.warnings


def test_write_inplay_fixtures_sample_creates_file(tmp_path):
    output = tmp_path / "inplay_fixtures.json"

    result = write_api_sports_inplay_fixtures(
        output_path=output,
        sample=True,
        config=ApiSportsInplayFixturesConfig(max_fixtures=10),
    )

    assert result.status == "READY"
    assert output.exists()

    payload = json.loads(output.read_text(encoding="utf-8"))

    assert payload["status"] == "READY"
    assert payload["mode"] == "INPLAY_FIXTURES"
    assert payload["fixtures"][0]["fixture_id"] == 9001
    assert payload["fixtures"][0]["live"] is True


def test_inplay_fixtures_respects_max_fixtures():
    payload = sample_inplay_fixtures_payload()
    payload["response"] = [payload["response"][0], payload["response"][0]]

    result = build_api_sports_inplay_fixtures_from_payload(
        payload,
        config=ApiSportsInplayFixturesConfig(max_fixtures=1),
    )

    assert len(result.fixtures) == 1
