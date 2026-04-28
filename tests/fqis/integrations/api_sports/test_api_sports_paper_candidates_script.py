
import json

from scripts.fqis_api_sports_paper_candidates import main


def test_paper_candidates_script_sample_writes_output(tmp_path, capsys):
    output_path = tmp_path / "paper_candidates.json"

    code = main(["--sample", "--output", str(output_path), "--require-ready"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "READY"
    assert payload["mode"] == "PAPER_CANDIDATES"
    assert output_path.exists()
    assert len(payload["candidates"]) >= 3


def test_paper_candidates_script_input_file(tmp_path, capsys):
    source_path = tmp_path / "source.json"
    source_path.write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "match": "Arsenal vs Everton",
                        "market": "Total Goals",
                        "selection": "Over 2.5",
                        "odds": 1.92,
                        "model_probability": 0.568,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    code = main(["--input", str(source_path), "--require-ready"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "READY"
    assert len(payload["candidates"]) == 1


def test_paper_candidates_script_missing_input_returns_failed(tmp_path, capsys):
    code = main(["--input", str(tmp_path / "missing.json")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"
