
import json

from scripts.fqis_api_sports_live_paper_candidates import main


def test_live_paper_candidates_script_sample_writes_output(tmp_path, capsys):
    output_path = tmp_path / "paper_candidates.json"

    code = main(["--sample", "--output", str(output_path), "--require-ready", "--max-candidates", "3"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "READY"
    assert payload["mode"] == "LIVE_PAPER_CANDIDATES"
    assert output_path.exists()
    assert len(payload["candidates"]) == 3


def test_live_paper_candidates_script_missing_key_returns_failed(tmp_path, capsys, monkeypatch):
    for name in [
        "APISPORTS_API_KEY",
        "APISPORTS_KEY",
        "API_SPORTS_KEY",
        "API_FOOTBALL_KEY",
        "RAPIDAPI_KEY",
    ]:
        monkeypatch.delenv(name, raising=False)

    monkeypatch.chdir(tmp_path)

    code = main(["--output", str(tmp_path / "paper_candidates.json")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"
