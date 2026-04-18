from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.vnext.ops.runtime_cli import (
    EXIT_INSPECT_SOURCE_FAILED,
    EXIT_LATEST_RUN_MISSING,
    EXIT_PATH_UNREADABLE,
    EXIT_PREFLIGHT_FAILED,
    EXIT_SUCCESS,
)
from app.vnext.publication.models import PublicMatchPayload
from app.vnext.runtime.exporter import export_cycle_jsonl
from app.vnext.runtime.models import RuntimeCounters, RuntimeCycleResult


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _clean_env() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.pop("VNEXT_LATEST_RUN_PATH", None)
    return env


def _case_root(name: str) -> Path:
    root = Path("exports") / "pytest_runtime_inspection" / f"{name}_{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _build_runtime_export(path: Path) -> None:
    payload = PublicMatchPayload(
        fixture_id=999,
        public_status="WATCHLIST",
        publish_channel="WATCHLIST",
        match_label="Lions vs Falcons",
        competition_label="Premier Test",
        market_label="TEAM_TOTAL",
        line_label="Team Total Away Under Core",
        bookmaker_label="Book 1",
        odds_label="1.87",
        confidence_band="HIGH",
        public_summary="TEAM_TOTAL Team Total Away Under Core @ Book 1 1.87",
    )
    export_cycle_jsonl(
        path,
        RuntimeCycleResult(
            cycle_id=1,
            timestamp_utc=datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc),
            counters=RuntimeCounters(
                fixture_count_seen=1,
                computed_publish_count=1,
                deduped_count=0,
                notified_count=0,
                silent_count=1,
                unsent_shadow_count=1,
                notifier_attempt_count=0,
            ),
            payloads=(payload,),
            notifier_mode="none",
        ),
    )


def _latest_run_path(case_root: Path) -> Path:
    return case_root / "latest_run.json"


def test_shadow_script_writes_latest_run_index_and_keeps_it_coherent() -> None:
    case_root = _case_root("latest_success")
    export_path = case_root / "shadow.jsonl"
    manifest_path = export_path.with_suffix(".manifest.json")
    latest_path = _latest_run_path(case_root)
    env = _clean_env()
    env["VNEXT_LATEST_RUN_PATH"] = str(latest_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_vnext_shadow.py",
            "--source",
            "demo",
            "--cycles",
            "1",
            "--notifier",
            "none",
            "--export-path",
            str(export_path),
        ],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert completed.returncode == EXIT_SUCCESS
    assert latest["manifest_path"] == str(manifest_path)
    assert latest["export_path"] == str(export_path)
    assert latest["report_path"] is None
    assert latest["status"] == manifest["status"] == "success"
    assert latest["source"] == manifest["source_resolved"] == "demo"
    assert latest["notifier"] == manifest["notifier_resolved"] == "none"
    assert latest["timestamp_utc"] == manifest["finished_at_utc"]


def test_shadow_script_updates_latest_run_index_for_refused_run() -> None:
    case_root = _case_root("latest_refused")
    export_path = case_root / "shadow_live.jsonl"
    manifest_path = export_path.with_suffix(".manifest.json")
    latest_path = _latest_run_path(case_root)
    env = _clean_env()
    env["API_FOOTBALL_KEY"] = ""
    env["VNEXT_LATEST_RUN_PATH"] = str(latest_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_vnext_shadow.py",
            "--source",
            "live",
            "--cycles",
            "1",
            "--notifier",
            "none",
            "--export-path",
            str(export_path),
        ],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    latest = json.loads(latest_path.read_text(encoding="utf-8"))

    assert completed.returncode == EXIT_PREFLIGHT_FAILED
    assert latest["manifest_path"] == str(manifest_path)
    assert latest["status"] == "preflight_failed"
    assert latest["source"] == "api_football"
    assert latest["notifier"] == "none"


def test_inspect_script_defaults_to_latest_run() -> None:
    case_root = _case_root("inspect_latest")
    export_path = case_root / "shadow.jsonl"
    latest_path = _latest_run_path(case_root)
    env = _clean_env()
    env["VNEXT_LATEST_RUN_PATH"] = str(latest_path)

    run_completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_vnext_shadow.py",
            "--source",
            "demo",
            "--cycles",
            "1",
            "--notifier",
            "none",
            "--export-path",
            str(export_path),
        ],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    inspect_completed = subprocess.run(
        [sys.executable, "scripts/inspect_vnext_run.py"],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert run_completed.returncode == EXIT_SUCCESS
    assert inspect_completed.returncode == EXIT_SUCCESS
    assert "vnext_run_inspect " in inspect_completed.stdout
    assert "status=success source=demo notifier=none cycles_requested=1 cycles_executed=1 preflight=ready" in inspect_completed.stdout
    assert "vnext_run_inspect_counts publishable=1 retained=1 deduped=0 shadow_unsent=1 notify_attempts=0 notified=0 acked_records=0" in inspect_completed.stdout
    assert f"latest={latest_path}" in inspect_completed.stdout


def test_inspect_script_reads_explicit_manifest() -> None:
    case_root = _case_root("inspect_manifest")
    export_path = case_root / "shadow.jsonl"
    manifest_path = export_path.with_suffix(".manifest.json")

    run_completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_vnext_shadow.py",
            "--source",
            "demo",
            "--cycles",
            "1",
            "--notifier",
            "none",
            "--export-path",
            str(export_path),
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    inspect_completed = subprocess.run(
        [sys.executable, "scripts/inspect_vnext_run.py", "--manifest", str(manifest_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert run_completed.returncode == EXIT_SUCCESS
    assert inspect_completed.returncode == EXIT_SUCCESS
    assert f"manifest={manifest_path}" in inspect_completed.stdout
    assert "latest=-" in inspect_completed.stdout


def test_inspect_script_reads_explicit_export() -> None:
    case_root = _case_root("inspect_export")
    export_path = case_root / "runtime.jsonl"
    _build_runtime_export(export_path)

    completed = subprocess.run(
        [sys.executable, "scripts/inspect_vnext_run.py", "--export", str(export_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert "status=- source=- notifier=- cycles_requested=- cycles_executed=1 preflight=-" in completed.stdout
    assert "publishable=1 retained=1 deduped=0 shadow_unsent=1 notify_attempts=0 notified=0 acked_records=0" in completed.stdout
    assert f"export={export_path}" in completed.stdout


def test_inspect_script_fails_cleanly_when_latest_run_is_missing() -> None:
    case_root = _case_root("inspect_latest_missing")
    latest_path = _latest_run_path(case_root)
    env = _clean_env()
    env["VNEXT_LATEST_RUN_PATH"] = str(latest_path)

    completed = subprocess.run(
        [sys.executable, "scripts/inspect_vnext_run.py"],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_LATEST_RUN_MISSING
    assert f"vnext_inspect_error reason=latest_run_missing path={latest_path}" in completed.stderr


def test_inspect_script_fails_cleanly_when_manifest_is_missing() -> None:
    missing_manifest = _case_root("inspect_manifest_missing") / "missing.manifest.json"

    completed = subprocess.run(
        [sys.executable, "scripts/inspect_vnext_run.py", "--manifest", str(missing_manifest)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_INSPECT_SOURCE_FAILED
    assert f"vnext_inspect_error reason=manifest_missing path={missing_manifest}" in completed.stderr


def test_inspect_script_fails_cleanly_when_manifest_is_invalid() -> None:
    manifest_path = _case_root("inspect_manifest_invalid") / "invalid.manifest.json"
    manifest_path.write_text("{invalid-json\n", encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, "scripts/inspect_vnext_run.py", "--manifest", str(manifest_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_INSPECT_SOURCE_FAILED
    assert f"vnext_inspect_error reason=manifest_invalid path={manifest_path}" in completed.stderr


def test_inspect_script_fails_cleanly_when_export_is_missing() -> None:
    missing_export = _case_root("inspect_export_missing") / "missing.jsonl"

    completed = subprocess.run(
        [sys.executable, "scripts/inspect_vnext_run.py", "--export", str(missing_export)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_INSPECT_SOURCE_FAILED
    assert f"vnext_inspect_error reason=export_missing path={missing_export}" in completed.stderr


def test_inspect_script_fails_cleanly_when_export_is_invalid() -> None:
    export_path = _case_root("inspect_export_invalid") / "invalid.jsonl"
    export_path.write_text("{invalid-json\n", encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, "scripts/inspect_vnext_run.py", "--export", str(export_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_INSPECT_SOURCE_FAILED
    assert f"vnext_inspect_error reason=export_invalid path={export_path}" in completed.stderr


def test_inspect_script_fails_cleanly_when_path_is_unreadable() -> None:
    unreadable_path = _case_root("inspect_unreadable")

    completed = subprocess.run(
        [sys.executable, "scripts/inspect_vnext_run.py", "--manifest", str(unreadable_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_PATH_UNREADABLE
    assert f"vnext_inspect_error reason=path_unreadable path={unreadable_path}" in completed.stderr
