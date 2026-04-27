import json
import subprocess
import sys
from pathlib import Path


def test_replay_script_missing_input_fails_safely():
    result = subprocess.run(
        [
            sys.executable,
            "scripts/fqis_api_sports_replay.py",
            "--input",
            "data/normalized/api_sports/missing.json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "FAILED"
    assert "Input path does not exist" in payload["reason"]


def test_replay_script_outputs_manifest_without_write(tmp_path):
    input_path = tmp_path / "normalized.json"
    input_path.write_text(
        json.dumps(
            {
                "metadata": {"provider": "api_sports_api_football", "source": "pre_match"},
                "fixtures": [{"fixture_key": "api_sports:fixture:1"}],
                "odds_offers": [
                    {
                        "fixture_key": "api_sports:fixture:1",
                        "provider_market_key": "api_sports:pre_match:5",
                        "provider_bookmaker_id": "8",
                        "normalization_status": "VALID",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/fqis_api_sports_replay.py",
            "--input",
            str(input_path),
            "--no-write",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "COMPLETED"
    assert payload["mode"] == "shadow_only_snapshot_replay"
    assert payload["output_path"] is None
    assert payload["counts"]["fixtures_total"] == 1
    assert payload["counts"]["offers_valid"] == 1
