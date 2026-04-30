import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_tonight_operator_launch.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"

SAFETY_FLAGS = [
    "can_execute_real_bets",
    "can_enable_live_staking",
    "can_mutate_ledger",
    "live_staking_allowed",
    "promotion_allowed",
    "live_execution_enabled",
    "discord_send_performed",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def good_paths(tmp_path: Path) -> dict[str, Path]:
    preview = tmp_path / "latest_discord_alert_preview.html"
    preview.write_text("<html>preview</html>", encoding="utf-8")
    return {
        "full_cycle": write_json(tmp_path / "latest_full_cycle_report.json", {"status": "READY"}),
        "discord_renderer": write_json(
            tmp_path / "latest_discord_alert_renderer.json",
            {
                "status": "READY",
                "elite_alerts_count": 1,
                "model_logs_count": 2,
                "preview_html": str(preview),
                "elite_alerts": [{"risk_notes": ["clean paper alert"]}],
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "live_staking_allowed": False,
                "promotion_allowed": False,
            },
        ),
        "final_dashboard": write_json(
            tmp_path / "latest_final_operator_readiness_dashboard.json",
            {
                "status": "BLOCKED",
                "readiness_level": "BLOCKED",
                "hard_blockers": ["PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH"],
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "live_staking_allowed": False,
                "promotion_allowed": False,
                "live_execution_enabled": False,
            },
        ),
        "operator_shadow_console": write_json(
            tmp_path / "latest_operator_shadow_console.json",
            {
                "status": "READY",
                "operator_state": "PAPER_READY",
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "promotion_allowed": False,
            },
        ),
        "signal_settlement": write_json(
            tmp_path / "latest_signal_settlement_report.json",
            {
                "status": "READY",
                "settled_signals": 42,
                "paper_roi": 0.08,
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "promotion_allowed": False,
            },
        ),
        "calibration": write_json(
            tmp_path / "latest_calibration_report.json",
            {
                "status": "READY",
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "promotion_allowed": False,
            },
        ),
        "promotion_policy": write_json(
            tmp_path / "latest_promotion_policy_report.json",
            {
                "status": "READY",
                "final_verdict": "NO_PROMOTION_KEEP_RESEARCH",
                "promotion_allowed": False,
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "live_staking_allowed": False,
            },
        ),
        "preview_html": preview,
    }


def cli_args(paths: dict[str, Path], output_json: Path, output_md: Path) -> list[str]:
    return [
        "--skip-run",
        "--full-cycle-path",
        str(paths["full_cycle"]),
        "--discord-renderer-path",
        str(paths["discord_renderer"]),
        "--final-dashboard-path",
        str(paths["final_dashboard"]),
        "--operator-shadow-console-path",
        str(paths["operator_shadow_console"]),
        "--signal-settlement-path",
        str(paths["signal_settlement"]),
        "--calibration-path",
        str(paths["calibration"]),
        "--promotion-policy-path",
        str(paths["promotion_policy"]),
        "--preview-html",
        str(paths["preview_html"]),
        "--output-json",
        str(output_json),
        "--output-md",
        str(output_md),
    ]


def run_with_paths(tmp_path: Path, paths: dict[str, Path]) -> tuple[dict[str, Any], str, dict[str, Any]]:
    output_json = tmp_path / "latest_tonight_operator_launch.json"
    output_md = tmp_path / "latest_tonight_operator_launch.md"
    result = run_script(*cli_args(paths, output_json, output_md))
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    markdown = output_md.read_text(encoding="utf-8")
    stdout_payload = json.loads(result.stdout)
    return payload, markdown, stdout_payload


def test_tonight_operator_launch_compiles_runs_safe_outputs_and_preserves_ledger(tmp_path: Path):
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    payload, markdown, stdout_payload = run_with_paths(tmp_path, good_paths(tmp_path))

    assert sha256(LEDGER) == before
    assert payload["paper_session_verdict"] == "READY_FOR_PAPER_SESSION"
    assert stdout_payload["paper_session_verdict"] == "READY_FOR_PAPER_SESSION"
    assert payload["preview_html_exists"] is True
    assert "latest_discord_alert_preview.html" in payload["preview_html"]
    assert "latest_discord_alert_preview.html" in markdown
    for flag in SAFETY_FLAGS:
        assert payload[flag] is False
        assert payload["safety"][flag] is False


def test_tonight_operator_launch_accepts_final_readiness_blocked_by_no_promotion_only(tmp_path: Path):
    payload, markdown, _ = run_with_paths(tmp_path, good_paths(tmp_path))

    assert payload["final_readiness_status"] == "BLOCKED"
    assert payload["final_readiness_hard_blockers"] == ["PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH"]
    assert payload["promotion_policy_verdict"] == "NO_PROMOTION_KEEP_RESEARCH"
    assert payload["paper_session_verdict"] == "READY_FOR_PAPER_SESSION"
    assert "PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH_LIVE_ONLY" in payload["warnings"]
    assert "python scripts/fqis_tonight_operator_launch.py" in markdown
    assert "start data\\pipeline\\api_sports\\orchestrator\\latest_discord_alert_preview.html" in markdown
    assert "notepad data\\pipeline\\api_sports\\orchestrator\\latest_tonight_operator_launch.md" in markdown


def test_tonight_operator_launch_blocks_unsafe_true_input_flag(tmp_path: Path):
    paths = good_paths(tmp_path)
    full_cycle = json.loads(paths["full_cycle"].read_text(encoding="utf-8"))
    full_cycle["can_execute_real_bets"] = True
    paths["full_cycle"].write_text(json.dumps(full_cycle), encoding="utf-8")

    payload, _, _ = run_with_paths(tmp_path, paths)

    assert payload["paper_session_verdict"] == "BLOCKED_FOR_PAPER_SESSION"
    assert "UNSAFE_TRUE_FLAG_IN_INPUT" in payload["blockers"]
    assert any("full_cycle:can_execute_real_bets" in item for item in payload["unsafe_flag_paths"])
    for flag in SAFETY_FLAGS:
        assert payload[flag] is False


def test_tonight_operator_launch_allows_review_empty_evidence_but_warns(tmp_path: Path):
    paths = good_paths(tmp_path)
    signal = json.loads(paths["signal_settlement"].read_text(encoding="utf-8"))
    signal["status"] = "EMPTY"
    paths["signal_settlement"].write_text(json.dumps(signal), encoding="utf-8")
    calibration = json.loads(paths["calibration"].read_text(encoding="utf-8"))
    calibration["status"] = "REVIEW"
    paths["calibration"].write_text(json.dumps(calibration), encoding="utf-8")

    payload, _, _ = run_with_paths(tmp_path, paths)

    assert payload["paper_session_verdict"] == "PAPER_SESSION_REVIEW"
    assert "SIGNAL_SETTLEMENT_EMPTY" in payload["warnings"]
    assert "CALIBRATION_REVIEW" in payload["warnings"]
    for flag in SAFETY_FLAGS:
        assert payload[flag] is False
