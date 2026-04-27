
import json

from scripts.fqis_api_sports_pipeline import main


def test_pipeline_script_dry_run_outputs_manifest(tmp_path, capsys):
    code = main(
        [
            "--normalized-input",
            str(tmp_path / "missing.json"),
            "--output-dir",
            str(tmp_path / "pipeline"),
            "--run-id",
            "script-dry",
            "--dry-run",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "DRY_RUN"
    assert payload["run_id"] == "script-dry"


def test_pipeline_script_missing_input_returns_failure(tmp_path, capsys):
    code = main(
        [
            "--normalized-input",
            str(tmp_path / "missing.json"),
            "--output-dir",
            str(tmp_path / "pipeline"),
            "--run-id",
            "script-missing",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == "FAILED"
    assert payload["errors"]
