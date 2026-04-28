import json

from scripts.fqis_api_sports_inplay_live_odds_candidates import main


def test_inplay_live_odds_script_sample_writes_output(tmp_path, capsys):
    output = tmp_path / "paper_candidates.json"

    code = main(["--sample", "--output", str(output), "--require-ready", "--max-candidates", "5"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "READY"
    assert payload["mode"] == "INPLAY_LIVE_ODDS_CANDIDATES"
    assert output.exists()
    assert len(payload["candidates"]) == 5


def test_inplay_live_odds_script_missing_key_returns_failed(tmp_path, capsys, monkeypatch):
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
