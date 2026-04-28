import json

from scripts.fqis_api_sports_live_odds_coverage_diagnostics import main


def test_live_odds_coverage_diagnostics_script_sample_writes_output(tmp_path, capsys):
    output = tmp_path / "live_odds_coverage.json"
    markdown = tmp_path / "live_odds_coverage.md"

    code = main(
        [
            "--sample",
            "--output",
            str(output),
            "--markdown",
            str(markdown),
            "--require-ready",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out.split("\n# FQIS API-Sports Live Odds Coverage Diagnostics")[0])

    assert code == 0
    assert payload["status"] == "READY"
    assert payload["mode"] == "LIVE_ODDS_COVERAGE_DIAGNOSTICS"
    assert payload["metrics"]["live_fixtures_total"] == 2
    assert output.exists()
    assert markdown.exists()


def test_live_odds_coverage_diagnostics_script_missing_key_fails(tmp_path, capsys, monkeypatch):
    for name in [
        "APISPORTS_API_KEY",
        "APISPORTS_KEY",
        "API_SPORTS_KEY",
        "API_FOOTBALL_KEY",
        "RAPIDAPI_KEY",
    ]:
        monkeypatch.delenv(name, raising=False)

    monkeypatch.chdir(tmp_path)

    code = main(["--output", str(tmp_path / "coverage.json")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"
