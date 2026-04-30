import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_final_operator_readiness_dashboard.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_final_operator_readiness_dashboard.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_final_operator_readiness_dashboard.md"

SAFETY_FLAGS = [
    "can_execute_real_bets",
    "can_enable_live_staking",
    "can_mutate_ledger",
    "live_staking_allowed",
    "promotion_allowed",
    "live_execution_enabled",
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


def base_reports(tmp_path: Path, *, final_verdict: str = "PAPER_ELITE_CANDIDATE_REVIEW", sample: int = 60) -> dict[str, Path]:
    settled = max(0, min(sample, sample - 2))
    return {
        "full_cycle": write_json(
            tmp_path / "latest_full_cycle_report.json",
            {
                "status": "READY",
                "reports": {
                    "paper_signal_export": {"paper_signals_total": sample},
                    "paper_alert_ranker": {"top_ranked_alert_count": 3},
                },
            },
        ),
        "operator_shadow_console": write_json(
            tmp_path / "latest_operator_shadow_console.json",
            {
                "status": "READY",
                "operator_state": "PAPER_READY",
                "next_action": "CONTINUE_PAPER_VALIDATION",
                "total_paper_signals": sample,
                "top_ranked_alert_count": 3,
            },
        ),
        "signal_settlement": write_json(
            tmp_path / "latest_signal_settlement_report.json",
            {
                "status": "READY",
                "total_signals": sample,
                "settled_signals": settled,
                "unsettled_signals": sample - settled,
                "win_count": 30,
                "loss_count": 20,
                "push_count": 0,
                "unknown_count": 0,
                "paper_pnl_total": 4.2,
                "paper_roi": 0.07,
                "warning_flags": [],
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "live_staking_allowed": False,
                "promotion_allowed": False,
            },
        ),
        "calibration": write_json(
            tmp_path / "latest_calibration_report.json",
            {
                "status": "READY",
                "total_rows": sample,
                "eligible_settled_rows": settled,
                "brier_score": 0.21,
                "log_loss": 0.63,
                "warning_flags": [],
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "live_staking_allowed": False,
                "promotion_allowed": False,
            },
        ),
        "proxy_clv": write_json(
            tmp_path / "latest_clv_tracker_report.json",
            {
                "status": "READY",
                "total_records": sample,
                "eligible_records": sample,
                "favorable_move_count": 36,
                "unfavorable_move_count": 18,
                "flat_move_count": 6,
                "favorable_move_rate": 0.60,
                "warning_flags": [],
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "live_staking_allowed": False,
                "promotion_allowed": False,
            },
        ),
        "promotion_policy": write_json(
            tmp_path / "latest_promotion_policy_report.json",
            {
                "status": "READY",
                "final_verdict": final_verdict,
                "promotion_allowed": False,
                "promotion_allowed_count": 0,
                "paper_elite_candidate_count": 1 if final_verdict == "PAPER_ELITE_CANDIDATE_REVIEW" else 0,
                "warning_flags": [],
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "live_staking_allowed": False,
            },
        ),
        "discord_preview": write_json(
            tmp_path / "latest_discord_alert_renderer.json",
            {
                "status": "READY",
                "elite_alerts_count": 1,
                "model_logs_count": 0,
                "no_send_count": 0,
                "discord_send_performed": False,
                "read": {"purpose": "PRESENTATION_ONLY"},
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "live_staking_allowed": False,
                "promotion_allowed": False,
            },
        ),
        "live_freshness": write_json(
            tmp_path / "latest_live_freshness_report.json",
            {"status": "READY", "freshness_flags": []},
        ),
        "go_no_go": write_json(
            tmp_path / "latest_go_no_go_report.json",
            {
                "status": "READY",
                "go_no_go_state": "NO_GO_DRY_RUN_ONLY",
                "promotion_allowed": False,
                "live_staking_allowed": False,
                "simulation_only": True,
            },
        ),
    }


def cli_args(paths: dict[str, Path], output_json: Path, output_md: Path) -> list[str]:
    return [
        "--full-cycle-path",
        str(paths["full_cycle"]),
        "--operator-shadow-console-path",
        str(paths["operator_shadow_console"]),
        "--signal-settlement-path",
        str(paths["signal_settlement"]),
        "--calibration-path",
        str(paths["calibration"]),
        "--proxy-clv-path",
        str(paths["proxy_clv"]),
        "--promotion-policy-path",
        str(paths["promotion_policy"]),
        "--discord-preview-path",
        str(paths["discord_preview"]),
        "--live-freshness-path",
        str(paths["live_freshness"]),
        "--go-no-go-path",
        str(paths["go_no_go"]),
        "--output-json",
        str(output_json),
        "--output-md",
        str(output_md),
    ]


def run_with_reports(tmp_path: Path, **kwargs: Any) -> dict[str, Any]:
    paths = base_reports(tmp_path, **kwargs)
    output_json = tmp_path / "dashboard.json"
    output_md = tmp_path / "dashboard.md"
    run_script(*cli_args(paths, output_json, output_md))
    assert output_json.exists()
    assert output_md.exists()
    return json.loads(output_json.read_text(encoding="utf-8"))


def test_final_operator_readiness_dashboard_compiles_runs_outputs_safe_report_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    result = run_script()

    assert sha256(LEDGER) == before
    assert OUT_JSON.exists()
    assert OUT_MD.exists()

    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["mode"] == "FQIS_FINAL_OPERATOR_READINESS_DASHBOARD"
    assert payload["readiness_level"] in {
        "BLOCKED",
        "RESEARCH_READY",
        "PAPER_VALIDATION_READY",
        "PROMOTION_REVIEW_CANDIDATE",
        "LIVE_READY_FORBIDDEN",
    }
    assert 0 <= payload["readiness_score_0_100"] <= 100
    for flag in SAFETY_FLAGS:
        assert payload[flag] is False
        assert payload["safety"][flag] is False
        assert payload["system_state"][flag] is False

    assert payload["promotion_summary"]["promotion_allowed"] is False
    assert '"live_execution_enabled": true' not in json.dumps(payload).lower()
    assert "LIVE execution enabled" not in OUT_MD.read_text(encoding="utf-8")
    stdout_payload = json.loads(result.stdout)
    assert stdout_payload["live_execution_enabled"] is False


def test_final_operator_readiness_no_promotion_verdict_lists_clear_blocker(tmp_path: Path):
    payload = run_with_reports(tmp_path, final_verdict="NO_PROMOTION_KEEP_RESEARCH")

    assert payload["promotion_allowed"] is False
    assert payload["promotion_summary"]["final_verdict"] == "NO_PROMOTION_KEEP_RESEARCH"
    assert "PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH" in payload["hard_blockers"]
    assert payload["readiness_level"] == "BLOCKED"


def test_final_operator_readiness_small_signal_sample_lists_insufficient_evidence_blocker(tmp_path: Path):
    payload = run_with_reports(tmp_path, sample=3)
    blockers = " ".join([*payload["hard_blockers"], *payload["soft_blockers"]]).lower()

    assert "sample" in blockers or "insufficient" in blockers
    assert "INSUFFICIENT_SIGNAL_SAMPLE_SIZE:3<30" in payload["soft_blockers"]
    assert payload["promotion_allowed"] is False
    assert payload["live_execution_enabled"] is False


def test_final_operator_readiness_rejects_unsafe_true_input_flags_without_enabling_execution(tmp_path: Path):
    paths = base_reports(tmp_path)
    promotion = json.loads(paths["promotion_policy"].read_text(encoding="utf-8"))
    promotion["promotion_allowed"] = True
    paths["promotion_policy"].write_text(json.dumps(promotion), encoding="utf-8")

    output_json = tmp_path / "dashboard.json"
    output_md = tmp_path / "dashboard.md"
    run_script(*cli_args(paths, output_json, output_md))
    payload = json.loads(output_json.read_text(encoding="utf-8"))

    assert any("UNSAFE_TRUE_FLAG_IN_INPUT:promotion_policy" in item for item in payload["hard_blockers"])
    assert "PROMOTION_POLICY_UNSAFE_PROMOTION_ALLOWED_TRUE" in payload["hard_blockers"]
    for flag in SAFETY_FLAGS:
        assert payload[flag] is False
        assert payload["safety"][flag] is False
    assert payload["readiness_level"] == "BLOCKED"
