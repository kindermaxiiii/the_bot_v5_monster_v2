import json

from app.fqis.integrations.api_sports.inplay_live_odds_candidates import (
    ApiSportsInplayLiveOddsConfig,
    build_api_sports_inplay_live_odds_candidates_from_payload,
    sample_inplay_live_odds_payload,
    write_api_sports_inplay_live_odds_candidates,
)


def test_inplay_live_odds_sample_extracts_core_candidates():
    result = build_api_sports_inplay_live_odds_candidates_from_payload(
        sample_inplay_live_odds_payload(),
        config=ApiSportsInplayLiveOddsConfig(max_candidates=20),
    )

    assert result.status == "READY"
    assert result.mode == "INPLAY_LIVE_ODDS_CANDIDATES"
    assert result.real_staking_enabled is False
    assert len(result.candidates) == 7

    selections = {(item.market, item.selection) for item in result.candidates}

    assert ("1X2", "Home") in selections
    assert ("1X2", "Draw") in selections
    assert ("1X2", "Away") in selections
    assert ("Total Goals", "Over 2.5") in selections
    assert ("Total Goals", "Under 2.5") in selections
    assert ("Both Teams To Score", "Yes") in selections
    assert ("Both Teams To Score", "No") in selections


def test_inplay_live_odds_rejects_non_main_and_unsupported_lines():
    result = build_api_sports_inplay_live_odds_candidates_from_payload(
        sample_inplay_live_odds_payload(),
        config=ApiSportsInplayLiveOddsConfig(max_candidates=20),
    )

    rejected_reasons = {item["reason"] for item in result.rejected}

    assert "non-main live odds value" in rejected_reasons
    assert "unsupported in-play live odds market selection" in rejected_reasons


def test_write_inplay_live_odds_sample_creates_file(tmp_path):
    output = tmp_path / "paper_candidates.json"

    result = write_api_sports_inplay_live_odds_candidates(
        output_path=output,
        sample=True,
        config=ApiSportsInplayLiveOddsConfig(max_candidates=5),
    )

    assert result.status == "READY"
    assert output.exists()

    payload = json.loads(output.read_text(encoding="utf-8"))

    assert payload["status"] == "READY"
    assert payload["mode"] == "INPLAY_LIVE_ODDS_CANDIDATES"
    assert len(payload["candidates"]) == 5


def test_inplay_live_odds_empty_payload_ready_with_warning():
    result = build_api_sports_inplay_live_odds_candidates_from_payload(
        {"response": []},
        config=ApiSportsInplayLiveOddsConfig(max_candidates=10),
    )

    assert result.status == "READY"
    assert len(result.candidates) == 0
    assert "NO_INPLAY_LIVE_ODDS_FOUND" in result.warnings
