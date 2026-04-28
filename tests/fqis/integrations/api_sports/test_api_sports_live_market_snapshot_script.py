
import json

from scripts.fqis_api_sports_live_market_snapshot import main


def test_live_market_snapshot_script_writes_outputs(tmp_path, capsys):
    source = tmp_path / "paper_candidates.json"
    output = tmp_path / "live_market_snapshot.json"
    markdown = tmp_path / "live_market_snapshot.md"

    source.write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "match": "Team A vs Team B",
                        "market": "1X2",
                        "selection": "Home",
                        "bookmaker": "Book A",
                        "odds": 2.0,
                    },
                    {
                        "match": "Team A vs Team B",
                        "market": "1X2",
                        "selection": "Home",
                        "bookmaker": "Book B",
                        "odds": 2.1,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    code = main(
        [
            "--input",
            str(source),
            "--output",
            str(output),
            "--markdown",
            str(markdown),
            "--require-ready",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out.split("\n# FQIS API-Sports Live Market Snapshot")[0])

    assert code == 0
    assert payload["status"] == "READY"
    assert payload["rows"][0]["best_bookmaker"] == "Book B"
    assert output.exists()
    assert markdown.exists()


def test_live_market_snapshot_script_missing_input_fails(tmp_path, capsys):
    code = main(
        [
            "--input",
            str(tmp_path / "missing.json"),
            "--output",
            str(tmp_path / "snapshot.json"),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"
