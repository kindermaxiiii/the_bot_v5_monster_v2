
import json

from app.fqis.integrations.api_sports.operator_report import (
    build_api_sports_operator_report,
    write_api_sports_operator_report,
)


def _ledger_entry(run_id, *, status="COMPLETED", ready=True):
    return {
        "run_id": run_id,
        "status": status,
        "ready": ready,
        "ledger_key": f"{run_id}:sha",
        "manifest_path": f"data/pipeline/api_sports/{run_id}/pipeline_manifest.json",
        "manifest_sha256": f"{run_id}-sha",
        "run_dir": f"data/pipeline/api_sports/{run_id}",
        "normalized_input": "data/normalized/api_sports/sample.json",
        "payload_sha256": "payload-sha",
        "started_at_utc": "2026-04-27T10:00:00+00:00",
        "completed_at_utc": "2026-04-27T10:01:00+00:00",
        "steps_total": 2,
        "steps_completed": 2 if status == "COMPLETED" else 1,
        "steps_failed": 0 if status == "COMPLETED" else 1,
        "errors_total": 0 if status == "COMPLETED" else 1,
        "quality_status": "PASS" if ready else "BLOCKED",
        "quality_ready": ready,
        "quality_issues_total": 0 if ready else 1,
    }


def _bundle(run_id, *, ready=True, created_at="2026-04-27T10:00:00+00:00", quality_status="PASS"):
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
        "quality_report": {"status": quality_status},
        "errors": [] if ready else ["PIPELINE_STATUS_FAILED"],
    }


def _write_ledger(path, entries):
    path.write_text(
        "\n".join(json.dumps(entry) for entry in entries) + "\n",
        encoding="utf-8",
    )


def test_operator_report_passes_when_ready_run_and_bundle_match(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    report = build_api_sports_operator_report(ledger_path=ledger_path, bundle_dir=bundle_dir)

    assert report.status == "PASS"
    assert report.ready is True
    assert report.counts["runs_total"] == 1
    assert report.counts["audit_bundles_ready"] == 1
    assert report.errors == ()


def test_operator_report_blocks_without_runs(tmp_path):
    report = build_api_sports_operator_report(
        ledger_path=tmp_path / "missing_ledger.jsonl",
        bundle_dir=tmp_path / "missing_bundles",
    )

    assert report.status == "BLOCKED"
    assert report.ready is False
    assert report.counts["operator_blockers_total"] >= 1
    assert any(check.name == "run_ledger_has_entries" for check in report.checks)


def test_operator_report_warns_without_bundle_when_bundle_not_required(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    _write_ledger(ledger_path, [_ledger_entry("run-1")])

    report = build_api_sports_operator_report(
        ledger_path=ledger_path,
        bundle_dir=tmp_path / "missing_bundles",
        require_audit_bundle=False,
    )

    assert report.status == "WARN"
    assert report.ready is True
    assert report.counts["operator_warnings_total"] >= 1
    assert report.counts["operator_blockers_total"] == 0


def test_operator_report_blocks_failed_latest_run(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1", status="FAILED", ready=False)])
    (bundle_dir / "run-1_audit_bundle.json").write_text(
        json.dumps(_bundle("run-1", ready=False, quality_status="BLOCKED")),
        encoding="utf-8",
    )

    report = build_api_sports_operator_report(ledger_path=ledger_path, bundle_dir=bundle_dir)

    assert report.status == "BLOCKED"
    assert report.ready is False
    assert any(check.name == "latest_run_status" and check.status == "BLOCKED" for check in report.checks)


def test_operator_report_warns_when_latest_ready_run_and_bundle_do_not_match(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1"), _ledger_entry("run-2")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    report = build_api_sports_operator_report(ledger_path=ledger_path, bundle_dir=bundle_dir)

    assert report.status == "WARN"
    assert report.ready is True
    assert any(check.name == "latest_ready_run_has_matching_bundle" for check in report.checks)


def test_write_operator_report_creates_output_file(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    output_path = tmp_path / "operator_report.json"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    report = write_api_sports_operator_report(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        output_path=output_path,
    )

    assert report.status == "PASS"
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
