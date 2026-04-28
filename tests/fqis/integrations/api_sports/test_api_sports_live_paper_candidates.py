
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


def test_live_paper_candidates_keeps_only_core_market_selections():
    payload = {
        "response": [
            {
                "fixture": {"id": 1001, "date": "2026-04-28T19:00:00+00:00"},
                "bookmakers": [
                    {
                        "name": "StrictBook",
                        "bets": [
                            {
                                "name": "Match Winner",
                                "values": [
                                    {"value": "Home", "odd": "2.00"},
                                    {"value": "Home/Draw", "odd": "1.30"},
                                ],
                            },
                            {
                                "name": "Goals Over/Under",
                                "values": [
                                    {"value": "Over 1.5", "odd": "1.35"},
                                    {"value": "Over 2.5", "odd": "2.05"},
                                    {"value": "Under 2.5", "odd": "1.80"},
                                ],
                            },
                            {
                                "name": "Both Teams Score",
                                "values": [
                                    {"value": "Yes", "odd": "1.90"},
                                    {"value": "No", "odd": "1.95"},
                                    {"value": "Draw/Yes", "odd": "4.50"},
                                    {"value": "Home/No", "odd": "4.33"},
                                ],
                            },
                            {
                                "name": "Result/Both Teams Score",
                                "values": [
                                    {"value": "Yes", "odd": "2.10"},
                                ],
                            },
                        ],
                    }
                ],
            }
        ]
    }

    result = build_api_sports_live_paper_candidates_from_payloads(
        odds_payloads=[payload],
        fixtures=sample_fixture_map(),
        config=ApiSportsLivePaperConfig(date="2026-04-28", max_candidates=20),
    )

    selections = {(item.market, item.selection) for item in result.candidates}

    assert selections == {
        ("1X2", "Home"),
        ("Total Goals", "Over 2.5"),
        ("Total Goals", "Under 2.5"),
        ("Both Teams To Score", "Yes"),
        ("Both Teams To Score", "No"),
    }

    rejected_selections = {item["selection"] for item in result.rejected}

    assert "Over 1.5" in rejected_selections
    assert "Home/Draw" in rejected_selections
    assert "Draw/Yes" in rejected_selections
    assert "Home/No" in rejected_selections

