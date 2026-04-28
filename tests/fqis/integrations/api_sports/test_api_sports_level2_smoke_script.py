
import json
from pathlib import Path

from scripts.fqis_api_sports_level2_smoke import main


def test_level2_smoke_runner_passes_with_require_ready(tmp_path, capsys):
    root = tmp_path / "smoke"

    code = main(["--root", str(root), "--require-ready"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "READY"
    assert payload["release_ready"] is True
    assert payload["components"]["audit_bundle"]["ready"] is True
    assert payload["components"]["operator_report"]["status"] == "PASS"
    assert payload["components"]["release_gate"]["status"] == "PASS"
    assert payload["components"]["release_manifest"]["status"] == "READY"
    assert payload["components"]["release_pack"]["status"] == "READY"

    assert Path(payload["paths"]["run_ledger"]).exists()
    assert Path(payload["paths"]["audit_bundle"]).exists()
    assert Path(payload["paths"]["audit_index"]).exists()
    assert Path(payload["paths"]["operator_report"]).exists()
    assert Path(payload["paths"]["release_gate"]).exists()
    assert Path(payload["paths"]["release_manifest"]).exists()
    assert Path(payload["paths"]["release_pack"]).exists()


def test_level2_smoke_runner_can_be_re_run_on_same_root(tmp_path, capsys):
    root = tmp_path / "smoke"

    first_code = main(["--root", str(root), "--require-ready"])
    first_payload = json.loads(capsys.readouterr().out)

    second_code = main(["--root", str(root), "--require-ready"])
    second_payload = json.loads(capsys.readouterr().out)

    assert first_code == 0
    assert second_code == 0
    assert first_payload["release_ready"] is True
    assert second_payload["release_ready"] is True
    assert second_payload["components"]["release_pack"]["status"] == "READY"
