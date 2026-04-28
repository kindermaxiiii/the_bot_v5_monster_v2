
import json

from app.fqis.integrations.api_sports.live_paper_candidates import (
    ApiSportsLivePaperConfig,
    build_api_sports_live_paper_candidates_from_payloads,
    sample_fixture_map,
    sample_odds_payload,
    write_api_sports_live_paper_candidates,
)


def test_live_paper_candidates_sample_extracts_candidates():
    result = build_api_sports_live_paper_candidates_from_payloads(
        odds_payloads=[sample_odds_payload()],
        fixtures=sample_fixture_map(),
        config=ApiSportsLivePaperConfig(date="2026-04-28", max_candidates=4),
    )

    assert result.status == "READY"
    assert result.mode == "LIVE_PAPER_CANDIDATES"
    assert result.real_staking_enabled is False
    assert len(result.candidates) == 4
    assert result.candidates[0].match == "Sample Home vs Sample Away"
    assert result.candidates[0].source == "api-sports-live-paper"
    assert "NO_MODEL_EDGE_VALIDATION" in result.warnings


def test_live_paper_candidates_uses_discounted_implied_probability():
    result = build_api_sports_live_paper_candidates_from_payloads(
        odds_payloads=[sample_odds_payload()],
        fixtures=sample_fixture_map(),
        config=ApiSportsLivePaperConfig(date="2026-04-28", max_candidates=1),
    )

    candidate = result.candidates[0]
    assert candidate.model_probability < round(1.0 / candidate.odds, 6)
    assert "No model edge validation" in candidate.reason


def test_write_live_paper_candidates_sample_creates_file(tmp_path):
    output_path = tmp_path / "paper_candidates.json"

    result = write_api_sports_live_paper_candidates(
        output_path=output_path,
        sample=True,
        config=ApiSportsLivePaperConfig(date="2026-04-28", max_candidates=3),
    )

    assert result.status == "READY"
    assert output_path.exists()

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert payload["mode"] == "LIVE_PAPER_CANDIDATES"
    assert payload["real_staking_enabled"] is False
    assert len(payload["candidates"]) == 3
