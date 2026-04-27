import json

import pytest

from app.fqis.integrations.api_sports.replay import (
    ApiSportsReplayError,
    replay_normalized_snapshot,
)


def _normalized_payload():
    return {
        "metadata": {
            "provider": "api_sports_api_football",
            "source": "pre_match",
            "snapshot_id": "snap_123",
            "run_id": "run_123",
        },
        "fixtures": [
            {
                "provider": "api_sports_api_football",
                "provider_fixture_id": "1",
                "fixture_key": "api_sports:fixture:1",
            },
            {
                "provider": "api_sports_api_football",
                "provider_fixture_id": "2",
                "fixture_key": "api_sports:fixture:2",
            },
        ],
        "odds_offers": [
            {
                "provider": "api_sports_api_football",
                "source": "pre_match",
                "fixture_key": "api_sports:fixture:1",
                "provider_bookmaker_id": "8",
                "provider_market_key": "api_sports:pre_match:5",
                "normalization_status": "VALID",
                "warnings": [],
            },
            {
                "provider": "api_sports_api_football",
                "source": "pre_match",
                "fixture_key": "api_sports:fixture:1",
                "provider_bookmaker_id": "8",
                "provider_market_key": "api_sports:pre_match:5",
                "normalization_status": "REVIEW",
                "warnings": ["unknown_selection"],
            },
            {
                "provider": "api_sports_api_football",
                "source": "pre_match",
                "fixture_key": "api_sports:fixture:2",
                "provider_bookmaker_id": "9",
                "provider_market_key": "api_sports:pre_match:1",
                "normalization_status": "REJECTED",
                "warnings": ["invalid_decimal_odds"],
            },
        ],
    }


def test_replay_normalized_snapshot_counts_and_manifest(tmp_path):
    input_path = tmp_path / "normalized.json"
    output_dir = tmp_path / "audit"
    input_path.write_text(json.dumps(_normalized_payload()), encoding="utf-8")

    manifest = replay_normalized_snapshot(input_path, output_dir=output_dir)

    assert manifest.status == "COMPLETED"
    assert manifest.mode == "shadow_only_snapshot_replay"
    assert manifest.provider == "api_sports_api_football"
    assert manifest.source == "pre_match"
    assert manifest.snapshot_id == "snap_123"
    assert manifest.run_id == "run_123"
    assert manifest.counts.fixtures_total == 2
    assert manifest.counts.offers_total == 3
    assert manifest.counts.offers_valid == 1
    assert manifest.counts.offers_review == 1
    assert manifest.counts.offers_rejected == 1
    assert manifest.counts.markets_total == 2
    assert manifest.counts.bookmakers_total == 2
    assert manifest.counts.fixtures_with_offers == 2
    assert "unknown_selection" in manifest.warnings
    assert "invalid_decimal_odds" in manifest.warnings

    output_path = output_dir / f"{manifest.replay_id}.json"
    assert output_path.exists()
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["status"] == "COMPLETED"
    assert written["counts"]["offers_total"] == 3


def test_replay_no_write_does_not_create_manifest(tmp_path):
    input_path = tmp_path / "normalized.json"
    output_dir = tmp_path / "audit"
    input_path.write_text(json.dumps(_normalized_payload()), encoding="utf-8")

    manifest = replay_normalized_snapshot(input_path, output_dir=output_dir, write_manifest=False)

    assert manifest.output_path is None
    assert not output_dir.exists()


def test_replay_missing_input_raises(tmp_path):
    with pytest.raises(ApiSportsReplayError, match="Input path does not exist"):
        replay_normalized_snapshot(tmp_path / "missing.json")


def test_replay_invalid_json_raises(tmp_path):
    input_path = tmp_path / "broken.json"
    input_path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(ApiSportsReplayError, match="not valid JSON"):
        replay_normalized_snapshot(input_path)


def test_replay_warns_when_empty_snapshot(tmp_path):
    input_path = tmp_path / "empty.json"
    input_path.write_text(json.dumps({"metadata": {"provider": "api_sports_api_football"}}), encoding="utf-8")

    manifest = replay_normalized_snapshot(input_path, output_dir=tmp_path / "audit", write_manifest=False)

    assert manifest.counts.fixtures_total == 0
    assert manifest.counts.offers_total == 0
    assert "no_fixtures_in_normalized_snapshot" in manifest.warnings
    assert "no_odds_offers_in_normalized_snapshot" in manifest.warnings
