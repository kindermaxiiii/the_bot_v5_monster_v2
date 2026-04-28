import json

from scripts.fqis_api_sports_inplay_orchestrator import main


def test_inplay_orchestrator_sample_writes_outputs(tmp_path):
    output_dir = tmp_path / "api_sports"

    code = main(
        [
            "--sample",
            "--output-dir",
            str(output_dir),
            "--require-ready",
            "--max-candidates",
            "20",
        ]
    )

    assert code == 0

    summary_path = output_dir / "inplay_orchestrator_summary.json"
    markdown_path = output_dir / "inplay_orchestrator_summary.md"

    assert summary_path.exists()
    assert markdown_path.exists()
    assert (output_dir / "inplay_fixtures.json").exists()
    assert (output_dir / "paper_candidates.json").exists()
    assert (output_dir / "live_market_snapshot.json").exists()
    assert (output_dir / "live_market_snapshot.md").exists()
    assert (output_dir / "live_odds_coverage_diagnostics.json").exists()
    assert (output_dir / "live_odds_coverage_diagnostics.md").exists()

    payload = json.loads(summary_path.read_text(encoding="utf-8"))

    assert payload["status"] == "READY"
    assert payload["ready"] is True
    assert payload["mode"] == "INPLAY_ORCHESTRATOR"
    assert payload["real_staking_enabled"] is False
    assert payload["summary"]["steps_total"] == 4
    assert payload["summary"]["failed_steps_total"] == 0
    assert payload["metrics"]["fixtures_total"] >= 1
    assert payload["metrics"]["candidates_total"] >= 1
    assert payload["metrics"]["snapshot_rows_total"] >= 1
    assert "OBSERVATION_ONLY" in payload["warnings"]
