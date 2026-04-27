import json

import pytest

from app.fqis.integrations.api_sports.quality_gates import (
    ApiSportsQualityGateConfig,
    ApiSportsQualityGateError,
    ApiSportsQualityStatus,
    assert_snapshot_ready,
    evaluate_snapshot_quality,
    evaluate_snapshot_quality_file,
)


def _ready_payload():
    return {
        "fixtures": [
            {
                "fixture_key": "api_sports:fixture:1",
                "provider_fixture_id": "1",
            }
        ],
        "odds_offers": [
            {
                "fixture_key": "api_sports:fixture:1",
                "provider_fixture_id": "1",
                "provider_bookmaker_id": "8",
                "bookmaker_name": "Book",
                "provider_market_id": "5",
                "provider_market_key": "api_sports:pre_match:5",
                "mapping_status": "MAPPED",
                "normalization_status": "OK",
                "selection": "OVER",
                "line": 2.5,
                "decimal_odds": 1.91,
            }
        ],
    }


def test_quality_gate_passes_ready_payload():
    report = evaluate_snapshot_quality(_ready_payload())

    assert report.status is ApiSportsQualityStatus.PASS
    assert report.ready is True
    assert report.counts["fixtures_total"] == 1
    assert report.counts["offers_ready"] == 1
    assert report.issues == ()


def test_quality_gate_blocks_empty_payload():
    report = evaluate_snapshot_quality({"fixtures": [], "odds_offers": []})

    assert report.status is ApiSportsQualityStatus.BLOCKED
    assert report.ready is False
    assert {issue.code for issue in report.issues} >= {
        "MIN_FIXTURES_NOT_MET",
        "MIN_OFFERS_NOT_MET",
    }


def test_quality_gate_blocks_invalid_odds():
    payload = _ready_payload()
    payload["odds_offers"][0]["decimal_odds"] = 1.0

    report = evaluate_snapshot_quality(payload)

    assert report.status is ApiSportsQualityStatus.BLOCKED
    assert "INVALID_ODDS_RATIO_TOO_HIGH" in {issue.code for issue in report.issues}


def test_quality_gate_blocks_missing_fixture_key():
    payload = _ready_payload()
    payload["odds_offers"][0]["fixture_key"] = None

    report = evaluate_snapshot_quality(payload)

    assert report.status is ApiSportsQualityStatus.BLOCKED
    assert "MISSING_FIXTURE_KEY_RATIO_TOO_HIGH" in {issue.code for issue in report.issues}


def test_quality_gate_warns_on_high_review_ratio_when_not_blocked():
    payload = _ready_payload()
    payload["odds_offers"].append(
        {
            "fixture_key": "api_sports:fixture:1",
            "provider_bookmaker_id": "9",
            "bookmaker_name": "Book 2",
            "provider_market_id": "999",
            "provider_market_key": "api_sports:pre_match:999",
            "mapping_status": "REVIEW",
            "normalization_status": "REVIEW",
            "selection": "UNKNOWN",
            "line": None,
            "decimal_odds": 2.0,
        }
    )

    config = ApiSportsQualityGateConfig(
        min_fixtures=1,
        min_offers=1,
        min_mapped_offer_ratio=0.10,
        max_review_offer_ratio=0.25,
        max_rejected_offer_ratio=1.0,
        max_invalid_odds_ratio=0.0,
        max_missing_fixture_key_ratio=0.0,
        max_missing_market_key_ratio=0.0,
        max_duplicate_offer_key_ratio=0.0,
    )

    report = evaluate_snapshot_quality(payload, config=config)

    assert report.status is ApiSportsQualityStatus.WARN
    assert report.ready is True
    assert "REVIEW_OFFER_RATIO_HIGH" in {issue.code for issue in report.issues}


def test_quality_gate_detects_duplicate_offer_keys():
    payload = _ready_payload()
    payload["odds_offers"].append(dict(payload["odds_offers"][0]))

    report = evaluate_snapshot_quality(payload)

    assert report.status is ApiSportsQualityStatus.BLOCKED
    assert report.counts["duplicate_offer_keys"] == 1
    assert "DUPLICATE_OFFER_KEY_RATIO_TOO_HIGH" in {issue.code for issue in report.issues}


def test_quality_gate_file_roundtrip(tmp_path):
    path = tmp_path / "normalized.json"
    path.write_text(json.dumps(_ready_payload()), encoding="utf-8")

    report = evaluate_snapshot_quality_file(path)

    assert report.status is ApiSportsQualityStatus.PASS
    assert report.source_path == str(path)
    assert report.payload_sha256 is not None


def test_assert_snapshot_ready_raises_on_blocked_payload():
    with pytest.raises(ApiSportsQualityGateError):
        assert_snapshot_ready({"fixtures": [], "odds_offers": []})