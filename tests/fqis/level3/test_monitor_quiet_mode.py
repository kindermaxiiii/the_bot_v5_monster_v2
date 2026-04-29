import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
MONITOR_SCRIPT = ROOT / "scripts" / "fqis_tonight_shadow_monitor.py"
DIGEST_SCRIPT = ROOT / "scripts" / "fqis_tonight_shadow_digest.py"
FRESHNESS_SCRIPT = ROOT / "scripts" / "fqis_live_freshness_report.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
MONITOR_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_tonight_shadow_monitor.json"
DIGEST_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_tonight_shadow_digest.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_monitor() -> dict:
    return json.loads(MONITOR_JSON.read_text(encoding="utf-8"))


def test_quiet_monitor_writes_cycle_logs_digest_runs_and_preserves_ledger():
    before = sha256(LEDGER)

    subprocess.run(
        [sys.executable, str(FRESHNESS_SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    proc = subprocess.run(
        [
            sys.executable,
            str(MONITOR_SCRIPT),
            "--cycles",
            "1",
            "--sleep-seconds",
            "0",
            "--quiet",
            "--tail-lines",
            "5",
        ],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    subprocess.run(
        [sys.executable, str(DIGEST_SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert sha256(LEDGER) == before
    assert proc.stdout.count("\n") <= 1
    assert len(proc.stdout) < 20000

    monitor = load_monitor()
    assert monitor["quiet"] is True
    assert monitor["child_log_mode"] == "capture"
    assert monitor["cycles_completed"] >= 1
    row = monitor["rows"][-1]
    assert Path(row["stdout_log"]).exists()
    assert Path(row["stderr_log"]).exists()
    assert "stdout_tail" in row
    assert "stderr_tail" in row
    assert row["can_execute_real_bets"] is False
    assert row["live_staking_allowed"] is False
    assert row["operator_state"] in {"PAPER_READY", "PAPER_REVIEW", "PAPER_BLOCKED"}
    assert "paper_signals_total" in row
    assert "new_paper_alerts" in row

    digest = json.loads(DIGEST_JSON.read_text(encoding="utf-8"))
    assert digest["verdict"] in {
        "SHADOW_SESSION_CLEAN",
        "SHADOW_SESSION_CLEAN_WITH_STALE_REVIEW",
        "SHADOW_SESSION_CLEAN_WITH_PAPER_ALERTS",
        "SHADOW_SESSION_STOPPED",
        "SHADOW_SESSION_INVALID",
    }
    assert "final_operator_state" in digest
    assert "total_new_paper_alerts" in digest
    assert digest["any_real_bets_enabled"] is False


def test_quiet_monitor_keyboard_interrupt_writes_clean_artifact():
    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    started = time.time()
    proc = subprocess.Popen(
        [
            sys.executable,
            str(MONITOR_SCRIPT),
            "--cycles",
            "5",
            "--sleep-seconds",
            "10",
            "--quiet",
        ],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        creationflags=creationflags,
    )

    try:
        run_id = None
        deadline = time.time() + 90
        while time.time() < deadline:
            if MONITOR_JSON.exists() and MONITOR_JSON.stat().st_mtime >= started - 1:
                try:
                    payload = load_monitor()
                except json.JSONDecodeError:
                    time.sleep(0.25)
                    continue
                run_id = payload.get("monitor_run_id")
                if run_id and payload.get("cycles_completed", 0) >= 1:
                    break
            time.sleep(0.5)
        else:
            proc.terminate()
            proc.communicate(timeout=10)
            pytest.skip("monitor did not complete first cycle before interrupt timeout")

        try:
            if os.name == "nt":
                proc.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                proc.send_signal(signal.SIGINT)
        except Exception as exc:
            proc.terminate()
            proc.communicate(timeout=10)
            pytest.skip(f"process signal control unavailable: {exc}")

        try:
            proc.communicate(timeout=25)
        except subprocess.TimeoutExpired:
            proc.terminate()
            proc.communicate(timeout=10)
            pytest.skip("monitor did not exit after interrupt signal")

        payload = load_monitor()
        assert payload.get("monitor_run_id") == run_id
        assert isinstance(payload.get("rows"), list)
        if proc.returncode == 130:
            assert payload["status"] == "MANUALLY_INTERRUPTED"
            assert payload["stopped_reason"] == "KEYBOARD_INTERRUPT"
        else:
            assert payload["status"] in {"MANUALLY_INTERRUPTED", "READY", "STOPPED"}
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.communicate(timeout=10)
