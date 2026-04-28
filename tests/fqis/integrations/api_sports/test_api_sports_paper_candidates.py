
import json

from app.fqis.integrations.api_sports.paper_candidates import (
    ApiSportsPaperCandidateConfig,
    build_api_sports_paper_candidates,
    write_api_sports_paper_candidates,
)


def _inputs():
    return [
        {
            "match": "Arsenal vs Everton",
            "market": "Total Goals",
            "selection": "Over 2.5",
            "odds": 1.92,
            "model_probability": 0.568,
        },
        {
            "home_team": "Lyon",
            "away_team": "Nantes",
            "market": "Draw No Bet",
            "selection": "Lyon DNB",
            "odds": 1.89,
            "model_probability": 53.5,
        },
    ]


def test_paper_candidates_sample_is_ready():
    result = build_api_sports_paper_candidates(sample=True)

    assert result.status == "READY"
    assert result.mode == "PAPER_CANDIDATES"
    assert result.real_staking_enabled is False
    assert len(result.candidates) >= 3
    assert result.errors == ()


def test_paper_candidates_accepts_inline_inputs():
    result = build_api_sports_paper_candidates(candidates=_inputs())

    assert result.status == "READY"
    assert len(result.candidates) == 2
    assert result.candidates[1].match == "Lyon vs Nantes"
    assert result.candidates[1].model_probability == 0.535


def test_paper_candidates_rejects_invalid_candidate_without_blocking():
    result = build_api_sports_paper_candidates(
        candidates=[
            {
                "match": "Bad Match",
                "market": "1X2",
                "selection": "Home",
                "odds": 1.8,
            }
        ]
    )

    assert result.status == "READY"
    assert result.candidates == ()
    assert len(result.rejected) == 1
    assert "model_probability" in result.rejected[0].reason


def test_write_paper_candidates_creates_output_file(tmp_path):
    output_path = tmp_path / "paper_candidates.json"

    result = write_api_sports_paper_candidates(
        candidates=_inputs(),
        output_path=output_path,
    )

    assert result.status == "READY"
    assert output_path.exists()

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert payload["mode"] == "PAPER_CANDIDATES"
    assert payload["real_staking_enabled"] is False
    assert len(payload["candidates"]) == 2


def test_paper_candidates_loads_utf8_sig_source(tmp_path):
    source_path = tmp_path / "source.json"
    source_path.write_text(
        "\ufeff" + json.dumps({"candidates": _inputs()}),
        encoding="utf-8",
    )

    result = build_api_sports_paper_candidates(source_path=source_path)

    assert result.status == "READY"
    assert len(result.candidates) == 2


def test_paper_candidates_respects_max_candidates():
    result = build_api_sports_paper_candidates(
        sample=True,
        config=ApiSportsPaperCandidateConfig(max_candidates=2),
    )

    assert result.status == "READY"
    assert len(result.candidates) == 2
