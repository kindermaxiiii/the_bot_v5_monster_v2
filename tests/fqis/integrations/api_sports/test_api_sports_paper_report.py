
import json

from app.fqis.integrations.api_sports.paper_report import (
    build_api_sports_paper_report,
    write_api_sports_paper_report,
)


def _preview(status="READY"):
    return {
        "status": status,
        "mode": "PAPER_ONLY",
        "real_staking_enabled": False,
        "max_stake_units": 0.05,
        "generated_at_utc": "2026-04-28T00:00:00+00:00",
        "bets": [
            {
                "match": "Real Sociedad vs Valencia",
                "market": "Both Teams To Score",
                "selection": "BTTS Yes",
                "odds": 2.05,
                "model_probability": 0.545,
                "fair_odds": 1.8349,
                "edge_pct": 11.73,
                "stake_units": 0.05,
                "decision": "PAPER_BET",
                "reason": "Test paper: value theorique elevee.",
                "warnings": ["PAPER_ONLY", "NO_REAL_MONEY_VALIDATION", "MICRO_STAKE_MAX"],
            }
        ],
        "watchlist": [
            {
                "match": "Lyon vs Nantes",
                "market": "Draw No Bet",
                "selection": "Lyon DNB",
                "odds": 1.89,
                "model_probability": 0.535,
                "fair_odds": 1.8692,
                "edge_pct": 1.11,
                "stake_units": 0.0,
                "decision": "WATCHLIST",
                "reason": "Signal faible.",
                "warnings": ["PAPER_ONLY", "NO_REAL_MONEY_VALIDATION"],
            }
        ],
        "rejected": [
            {
                "match": "Inter vs Torino",
                "market": "1X2",
                "selection": "Inter Win",
                "odds": 1.42,
                "model_probability": 0.68,
                "fair_odds": 1.4706,
                "edge_pct": -3.44,
                "stake_units": 0.0,
                "decision": "REJECTED",
                "reason": "Pas de value.",
                "warnings": ["PAPER_ONLY", "NO_REAL_MONEY_VALIDATION"],
            }
        ],
        "errors": [],
    }


def test_paper_report_builds_markdown_from_preview_payload():
    report = build_api_sports_paper_report(preview=_preview())

    assert report.status == "READY"
    assert report.mode == "PAPER_REPORT"
    assert report.real_staking_enabled is False
    assert report.bets_total == 1
    assert report.watchlist_total == 1
    assert report.rejected_total == 1
    assert "Real Sociedad vs Valencia" in report.markdown
    assert "PAPER ONLY" in report.markdown
    assert "No real money" in report.markdown


def test_paper_report_blocks_when_preview_not_ready():
    report = build_api_sports_paper_report(preview=_preview(status="BLOCKED"))

    assert report.status == "BLOCKED"
    assert "PAPER_PREVIEW_NOT_READY" in report.errors
    assert "## Errors" in report.markdown


def test_write_paper_report_creates_markdown_file(tmp_path):
    output_path = tmp_path / "paper_report.md"

    report = write_api_sports_paper_report(
        preview=_preview(),
        output_path=output_path,
    )

    assert report.status == "READY"
    assert report.report_path == str(output_path)
    assert output_path.exists()

    markdown = output_path.read_text(encoding="utf-8")
    assert "# FQIS API-Sports Paper Report" in markdown
    assert "## Paper Bets" in markdown
    assert "## Watchlist" in markdown
    assert "## Rejected" in markdown


def test_paper_report_loads_utf8_sig_preview_file(tmp_path):
    preview_path = tmp_path / "paper_preview.json"
    preview_path.write_text("\ufeff" + json.dumps(_preview()), encoding="utf-8")

    report = build_api_sports_paper_report(preview_path=preview_path)

    assert report.status == "READY"
    assert report.preview_path == str(preview_path)
    assert "BTTS Yes" in report.markdown
