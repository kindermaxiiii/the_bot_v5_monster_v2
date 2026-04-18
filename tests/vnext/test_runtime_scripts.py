from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.vnext.ops.runtime_cli import (
    EXIT_PATH_UNWRITABLE,
    EXIT_PREFLIGHT_FAILED,
    EXIT_REPLAY_SOURCE_FAILED,
    EXIT_SUCCESS,
    EXIT_SUCCESS_DEGRADED,
    derive_run_manifest_path,
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
            timestamp_utc=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
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


def _case_root(name: str) -> Path:
    root = Path("exports") / "pytest_runtime_scripts" / f"{name}_{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _latest_run_path(case_root: Path) -> Path:
    return case_root / "latest_run.json"


def test_replay_script_runs_directly_from_repo_root_without_pythonpath() -> None:
    case_root = _case_root("replay_ok")
    export_path = case_root / "runtime.jsonl"
    report_path = case_root / "runtime_report.json"
    _build_runtime_export(export_path)

    completed = subprocess.run(
        [sys.executable, "scripts/replay_vnext_runtime.py", str(export_path), "--report", str(report_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert "vnext_runtime_report" in completed.stdout
    assert json.loads(report_path.read_text(encoding="utf-8"))["cycle_count"] == 1


def test_replay_script_missing_source_fails_cleanly() -> None:
    missing_path = _case_root("replay_missing") / "missing.jsonl"

    completed = subprocess.run(
        [sys.executable, "scripts/replay_vnext_runtime.py", str(missing_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_REPLAY_SOURCE_FAILED
    assert completed.stdout == ""
    assert f"vnext_replay_error reason=replay_source_missing path={missing_path}" in completed.stderr


def test_replay_script_invalid_source_fails_cleanly() -> None:
    invalid_path = _case_root("replay_invalid") / "invalid.jsonl"
    invalid_path.write_text("{invalid-json\n", encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, "scripts/replay_vnext_runtime.py", str(invalid_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_REPLAY_SOURCE_FAILED
    assert completed.stdout == ""
    assert f"vnext_replay_error reason=replay_source_invalid path={invalid_path}" in completed.stderr


def test_replay_script_report_path_directory_fails_cleanly() -> None:
    case_root = _case_root("replay_report_path")
    export_path = case_root / "runtime.jsonl"
    _build_runtime_export(export_path)

    completed = subprocess.run(
        [sys.executable, "scripts/replay_vnext_runtime.py", str(export_path), "--report", str(case_root)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_PATH_UNWRITABLE
    assert f"vnext_replay_error reason=path_unwritable path={case_root}" in completed.stderr


def test_shadow_script_prints_preflight_and_start_summary() -> None:
    case_root = _case_root("shadow_preflight")
    export_path = case_root / "shadow.jsonl"
    env = _clean_env()
    env["VNEXT_LATEST_RUN_PATH"] = str(_latest_run_path(case_root))

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

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_preflight "
        f"status=ready source_requested=demo source_resolved=demo notifier_requested=none "
        f"notifier_resolved=none persist_state=false export_path={export_path} report_path=- ops_store=-"
    ) in completed.stdout
    assert (
        "vnext_run_start "
        f"source=demo notifier=none persist_state=false export_path={export_path} report_path=-"
    ) in completed.stdout
    assert "vnext_run_complete status=success cycles_requested=1 cycles_executed=1 ops_flags=[]" in completed.stdout


def test_shadow_script_writes_manifest_with_operational_fields() -> None:
    case_root = _case_root("shadow_manifest")
    export_path = case_root / "shadow.jsonl"
    manifest_path = derive_run_manifest_path(export_path)
    report_path = case_root / "shadow_report.json"
    env = _clean_env()
    env["VNEXT_LATEST_RUN_PATH"] = str(_latest_run_path(case_root))

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
            "--report",
            str(report_path),
        ],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert completed.returncode == EXIT_SUCCESS
    assert manifest["status"] == "success"
    assert manifest["source_requested"] == "demo"
    assert manifest["source_resolved"] == "demo"
    assert manifest["notifier_requested"] == "none"
    assert manifest["notifier_resolved"] == "none"
    assert manifest["persist_state"] is False
    assert manifest["export_path"] == str(export_path)
    assert manifest["report_path"] == str(report_path)
    assert manifest["cycles_requested"] == 1
    assert manifest["cycles_executed"] == 1
    assert manifest["ops_flags"] == []


def test_shadow_script_discord_without_webhook_falls_back_cleanly() -> None:
    case_root = _case_root("shadow_discord_fallback")
    export_path = case_root / "shadow.jsonl"
    manifest_path = derive_run_manifest_path(export_path)
    env = _clean_env()
    env["DISCORD_WEBHOOK_REAL"] = ""
    env["DISCORD_WEBHOOK_DOC"] = ""
    env["DISCORD_WEBHOOK_LOGS"] = ""
    env["VNEXT_LATEST_RUN_PATH"] = str(_latest_run_path(case_root))

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_vnext_shadow.py",
            "--source",
            "demo",
            "--cycles",
            "1",
            "--notifier",
            "discord",
            "--export-path",
            str(export_path),
        ],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert completed.returncode == EXIT_SUCCESS_DEGRADED
    assert "vnext_preflight_warning reason=discord_webhook_missing" in completed.stderr
    assert (
        "vnext_preflight "
        f"status=degraded source_requested=demo source_resolved=demo notifier_requested=discord "
        f"notifier_resolved=none persist_state=false export_path={export_path} report_path=- ops_store=-"
    ) in completed.stdout
    assert (
        "vnext_run_start "
        f"source=demo notifier=none persist_state=false export_path={export_path} report_path=-"
    ) in completed.stdout
    assert "shadow_unsent=1" in completed.stdout
    assert "notify_attempts=0" in completed.stdout
    assert "vnext_run_complete status=success_degraded cycles_requested=1 cycles_executed=1 ops_flags=[]" in completed.stdout
    assert manifest["status"] == "success_degraded"
    assert manifest["preflight_warnings"] == ["discord_webhook_missing"]
    assert export_path.exists()


def test_shadow_script_live_without_api_key_refuses_cleanly() -> None:
    case_root = _case_root("shadow_live_refused")
    export_path = case_root / "shadow_live.jsonl"
    manifest_path = derive_run_manifest_path(export_path)
    env = _clean_env()
    env["API_FOOTBALL_KEY"] = ""
    env["VNEXT_LATEST_RUN_PATH"] = str(_latest_run_path(case_root))

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

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert completed.returncode == EXIT_PREFLIGHT_FAILED
    assert "vnext_preflight_error reason=live_api_key_missing" in completed.stderr
    assert (
        "vnext_preflight "
        f"status=refused source_requested=live source_resolved=api_football notifier_requested=none "
        f"notifier_resolved=none persist_state=false export_path={export_path} report_path=- ops_store=-"
    ) in completed.stdout
    assert "vnext_run_start" not in completed.stdout
    assert manifest["status"] == "preflight_failed"
    assert manifest["preflight_errors"] == ["live_api_key_missing"]
    assert manifest["cycles_executed"] == 0


def test_shadow_script_directory_export_path_fails_cleanly() -> None:
    case_root = _case_root("shadow_export_dir")
    env = _clean_env()
    env["VNEXT_LATEST_RUN_PATH"] = str(_latest_run_path(case_root))

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
            str(case_root),
        ],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_PATH_UNWRITABLE
    assert f"vnext_preflight_error reason=export_path_unwritable path={case_root}" in completed.stderr
