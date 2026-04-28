
import json

from scripts.fqis_api_sports_paper_preview import main


def test_paper_preview_script_sample_writes_output(tmp_path, capsys):
    output_path = tmp_path / "paper_preview.json"

    code = main(["--sample", "--output", str(output_path), "--require-ready"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "READY"
    assert payload["mode"] == "PAPER_ONLY"
    assert payload["real_staking_enabled"] is False
    assert output_path.exists()
    assert len(payload["bets"]) >= 1


def test_paper_preview_script_candidates_file(tmp_path, capsys):
    candidates_path = tmp_path / "candidates.json"
    candidates_path.write_text(
        json.dumps(
            [
                {
                    "match": "Team A vs Team B",
                    "market": "Total Goals",
                    "selection": "Over 2.5",
                    "odds": 1.92,
                    "model_probability": 0.568,
                }
            ]
        ),
        encoding="utf-8",
    )

    code = main(["--candidates", str(candidates_path), "--require-ready"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "READY"
    assert len(payload["bets"]) == 1


def test_paper_preview_script_missing_candidates_returns_failed(tmp_path, capsys):
    code = main(["--candidates", str(tmp_path / "missing.json")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"
