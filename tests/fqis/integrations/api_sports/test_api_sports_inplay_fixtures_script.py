import json

from scripts.fqis_api_sports_inplay_fixtures import main


def test_inplay_fixtures_script_sample_writes_output(tmp_path, capsys):
    output = tmp_path / "inplay_fixtures.json"

    code = main(["--sample", "--output", str(output), "--require-ready"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "READY"
    assert payload["mode"] == "INPLAY_FIXTURES"
    assert payload["fixtures"][0]["fixture_id"] == 9001
    assert output.exists()


def test_inplay_fixtures_script_missing_key_returns_failed(tmp_path, capsys, monkeypatch):
    for name in [
        "APISPORTS_API_KEY",
        "APISPORTS_KEY",
        "API_SPORTS_KEY",
        "API_FOOTBALL_KEY",
        "RAPIDAPI_KEY",
    ]:
        monkeypatch.delenv(name, raising=False)

    monkeypatch.chdir(tmp_path)

    code = main(["--output", str(tmp_path / "inplay_fixtures.json")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"
