
import json

import pytest

from app.fqis.integrations.api_sports.release_gate import (
    ApiSportsReleaseGateConfig,
    ApiSportsReleaseGateError,
    assert_api_sports_release_ready,
    evaluate_api_sports_release_gate,
    write_api_sports_release_gate,
)


def _ledger_entry(run_id, *, status="COMPLETED", ready=True):
    return {
        "run_id": run_id,
        "status": status,
        "ready": ready,
        "ledger_key": f"{run_id}:sha",
        "manifest_path": f"{run_id}/pipeline_manifest.json",
        "manifest_sha256": f"{run_id}-sha",
        "run_dir": f"data/pipeline/api_sports/{run_id}",
        "normalized_input": "data/normalized/api_sports/sample.json",
        "payload_sha256": "payload-sha",
        "started_at_utc": "2026-04-27T10:00:00+00:00",
        "completed_at_utc": "2026-04-27T10:01:00+00:00",
        "steps_total": 2,
        "steps_completed": 2 if ready else 1,
        "steps_failed": 0 if ready else 1,
        "errors_total": 0 if ready else 1,
        "quality_status": "PASS" if ready else "BLOCKED",
        "quality_ready": ready,
        "quality_issues_total": 0 if ready else 1,
    }


def _bundle(run_id, *, ready=True, created_at="2026-04-27T10:00:00+00:00"):
    return {
        "status": "BUILT",
        "run_id": run_id,
        "ready": ready,
        "created_at_utc": created_at,
        "output_path": f"{run_id}_audit_bundle.json",
        "manifest_path": f"{run_id}/pipeline_manifest.json",
        "files": [{"role": "pipeline_manifest", "path": "manifest.json", "exists": True}],
        "ledger_entry": {"run_id": run_id},
        "manifest": {"status": "COMPLETED" if ready else "FAILED"},
        "quality_report": {"status": "PASS" if ready else "BLOCKED"},
        "errors": [] if ready else ["PIPELINE_STATUS_FAILED"],
    }


def _write_ledger(path, entries):
    path.write_text(
        "\n".join(json.dumps(entry) for entry in entries) + "\n",
        encoding="utf-8",
    )


def test_release_gate_passes_when_operator_stack_is_ready(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    decision = evaluate_api_sports_release_gate(ledger_path=ledger_path, bundle_dir=bundle_dir)

    assert decision.status == "PASS"
    assert decision.release_ready is True
    assert decision.latest_ready_run_id == "run-1"
    assert decision.latest_ready_audit_bundle_run_id == "run-1"
    assert decision.errors == ()


def test_release_gate_blocks_missing_inputs(tmp_path):
    decision = evaluate_api_sports_release_gate(
        ledger_path=tmp_path / "missing_ledger.jsonl",
        bundle_dir=tmp_path / "missing_bundles",
    )

    assert decision.status == "BLOCKED"
    assert decision.release_ready is False
    assert decision.errors


def test_release_gate_blocks_warnings_by_default(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1"), _ledger_entry("run-2")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    decision = evaluate_api_sports_release_gate(ledger_path=ledger_path, bundle_dir=bundle_dir)

    assert decision.status == "BLOCKED"
    assert decision.release_ready is False
    assert any(check.status == "WARN" for check in decision.checks)


def test_release_gate_can_allow_warnings(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1"), _ledger_entry("run-2")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    decision = evaluate_api_sports_release_gate(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        config=ApiSportsReleaseGateConfig(allow_warnings=True),
    )

    assert decision.status == "WARN"
    assert decision.release_ready is True


def test_release_gate_can_relax_audit_bundle_requirement(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    _write_ledger(ledger_path, [_ledger_entry("run-1")])

    decision = evaluate_api_sports_release_gate(
        ledger_path=ledger_path,
        bundle_dir=tmp_path / "missing_bundles",
        config=ApiSportsReleaseGateConfig(
            allow_warnings=True,
            require_audit_bundle=False,
        ),
    )

    assert decision.status == "WARN"
    assert decision.release_ready is True


def test_write_release_gate_creates_output_file(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    output_path = tmp_path / "release_gate.json"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    decision = write_api_sports_release_gate(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        output_path=output_path,
    )

    assert decision.status == "PASS"
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"


def test_assert_release_ready_raises_when_blocked(tmp_path):
    with pytest.raises(ApiSportsReleaseGateError):
        assert_api_sports_release_ready(
            ledger_path=tmp_path / "missing_ledger.jsonl",
            bundle_dir=tmp_path / "missing_bundles",
        )
