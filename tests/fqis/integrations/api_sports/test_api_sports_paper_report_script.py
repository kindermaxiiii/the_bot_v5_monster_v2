
import json

from scripts.fqis_api_sports_paper_report import main


def _preview(status="READY"):
    return {
        "status": status,
        "mode": "PAPER_ONLY",
        "real_staking_enabled": False,
        "max_stake_units": 0.05,
        "generated_at_utc": "2026-04-28T00:00:00+00:00",
        "bets": [
            {
                "match": "Arsenal vs Everton",
                "market": "Total Goals",
                "selection": "Over 2.5",
                "odds": 1.92,
                "model_probability": 0.568,
                "fair_odds": 1.7606,
                "edge_pct": 9.06,
                "stake_units": 0.05,
                "decision": "PAPER_BET",
                "reason": "Value paper.",
                "warnings": ["PAPER_ONLY"],
            }
        ],
        "watchlist": [],
        "rejected": [],
        "errors": [],
    }


def test_paper_report_script_writes_markdown(tmp_path, capsys):
    preview_path = tmp_path / "paper_preview.json"
    output_path = tmp_path / "paper_report.md"
    preview_path.write_text(json.dumps(_preview()), encoding="utf-8")

    code = main(
        [
            "--preview",
            str(preview_path),
            "--output",
            str(output_path),
            "--require-ready",
        ]
    )

    captured = capsys.readouterr()

    assert code == 0
    assert output_path.exists()
    assert "Arsenal vs Everton" in captured.out
    assert "PAPER ONLY" in output_path.read_text(encoding="utf-8")


def test_paper_report_script_returns_non_zero_when_blocked(tmp_path, capsys):
    preview_path = tmp_path / "paper_preview.json"
    preview_path.write_text(json.dumps(_preview(status="BLOCKED")), encoding="utf-8")

    code = main(["--preview", str(preview_path), "--require-ready"])

    captured = capsys.readouterr()

    assert code == 1
    assert "PAPER_PREVIEW_NOT_READY" in captured.out


def test_paper_report_script_missing_preview_returns_failed(tmp_path, capsys):
    code = main(["--preview", str(tmp_path / "missing.json")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"
