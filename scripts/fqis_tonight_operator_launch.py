from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
RESEARCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"
VENV_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
CHILD_PYTHON = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

FULL_CYCLE_JSON = ORCH_DIR / "latest_full_cycle_report.json"
DISCORD_RENDERER_JSON = ORCH_DIR / "latest_discord_alert_renderer.json"
FINAL_DASHBOARD_JSON = ORCH_DIR / "latest_final_operator_readiness_dashboard.json"
OPERATOR_CONSOLE_JSON = ORCH_DIR / "latest_operator_shadow_console.json"
SIGNAL_SETTLEMENT_JSON = RESEARCH_DIR / "latest_signal_settlement_report.json"
CALIBRATION_JSON = RESEARCH_DIR / "latest_calibration_report.json"
PROMOTION_POLICY_JSON = ORCH_DIR / "latest_promotion_policy_report.json"
PREVIEW_HTML = ORCH_DIR / "latest_discord_alert_preview.html"
OUT_JSON = ORCH_DIR / "latest_tonight_operator_launch.json"
OUT_MD = ORCH_DIR / "latest_tonight_operator_launch.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
    "live_execution_enabled": False,
    "discord_send_performed": False,
    "paper_only": True,
}

UNSAFE_FLAG_NAMES = {
    "can_execute_real_bets",
    "can_enable_live_staking",
    "can_mutate_ledger",
    "live_staking_allowed",
    "promotion_allowed",
    "live_execution_enabled",
}

ACCEPTED_EVIDENCE_STATUSES = {"READY", "REVIEW", "EMPTY"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            return {"error": "JSON_ROOT_NOT_OBJECT", "path": str(path)}
        return payload
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def safe_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").replace("\n", " ").replace("|", "/").strip()
    return text or default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", ".").strip()))
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def truthy(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def unsafe_true_flags(payload: Any, prefix: str = "") -> list[str]:
    hits: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if key in UNSAFE_FLAG_NAMES and truthy(value):
                hits.append(path)
            hits.extend(unsafe_true_flags(value, path))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            hits.extend(unsafe_true_flags(value, f"{prefix}[{index}]"))
    return hits


def run_step(label: str, cmd: list[str], *, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {"label": label, "cmd": cmd, "ok": True, "returncode": 0, "skipped": True}

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return {
        "label": label,
        "cmd": cmd,
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout_tail": (proc.stdout or "")[-2000:],
        "stderr_tail": (proc.stderr or "")[-2000:],
    }


def hard_blockers_except_live_promotion(dashboard: dict[str, Any]) -> list[str]:
    blockers = [str(item) for item in dashboard.get("hard_blockers") or []]
    return [item for item in blockers if item != "PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH"]


def contains_quarantine(value: Any) -> bool:
    if isinstance(value, dict):
        return any(contains_quarantine(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_quarantine(item) for item in value)
    text = str(value or "").upper()
    return "QUARANTINE" in text or "KILL_OR_QUARANTINE_BUCKET" in text


def quarantined_elite_count(renderer: dict[str, Any]) -> int:
    elite = renderer.get("elite_alerts") or []
    if not isinstance(elite, list):
        return 0
    return sum(1 for item in elite if contains_quarantine(item))


def input_status(name: str, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "path": str(path),
        "status": payload.get("status", "MISSING" if payload.get("missing") else "UNKNOWN"),
        "missing": payload.get("missing") is True,
        "error": payload.get("error"),
        "generated_at_utc": payload.get("generated_at_utc"),
    }


def build_report(
    *,
    paths: dict[str, Path],
    steps: list[dict[str, Any]],
) -> dict[str, Any]:
    reports = {name: read_json(path) for name, path in paths.items() if name != "preview_html"}
    preview_html = paths["preview_html"]

    full_cycle = reports["full_cycle"]
    renderer = reports["discord_renderer"]
    dashboard = reports["final_dashboard"]
    operator = reports["operator_shadow_console"]
    signal = reports["signal_settlement"]
    calibration = reports["calibration"]
    promotion = reports["promotion_policy"]

    unsafe_hits: list[str] = []
    for name, payload in reports.items():
        unsafe_hits.extend(f"{name}:{hit}" for hit in unsafe_true_flags(payload))

    blockers: list[str] = []
    warnings: list[str] = []

    for step in steps:
        if step.get("ok") is not True:
            blockers.append(f"STEP_FAILED:{step.get('label')}:{step.get('returncode')}")

    for name, payload in reports.items():
        if payload.get("missing"):
            blockers.append(f"MISSING_INPUT:{name}")
        if payload.get("error"):
            blockers.append(f"JSON_READ_ERROR:{name}")

    if unsafe_hits:
        blockers.append("UNSAFE_TRUE_FLAG_IN_INPUT")
    if full_cycle.get("status") != "READY":
        blockers.append("FULL_CYCLE_NOT_READY")
    if renderer.get("status") != "READY":
        blockers.append("DISCORD_RENDERER_NOT_READY")
    if operator.get("status") != "READY":
        blockers.append("OPERATOR_CONSOLE_NOT_READY")
    if operator.get("operator_state") != "PAPER_READY":
        blockers.append("OPERATOR_STATE_NOT_PAPER_READY")
    if not preview_html.exists():
        blockers.append("DISCORD_PREVIEW_HTML_MISSING")

    quarantined_elite = quarantined_elite_count(renderer)
    if quarantined_elite > 0:
        blockers.append("QUARANTINED_ELITE_ALERTS_PRESENT")

    if signal.get("status") not in ACCEPTED_EVIDENCE_STATUSES:
        blockers.append("SIGNAL_SETTLEMENT_STATUS_NOT_ACCEPTED")
    elif signal.get("status") in {"REVIEW", "EMPTY"}:
        warnings.append(f"SIGNAL_SETTLEMENT_{signal.get('status')}")

    if calibration.get("status") not in ACCEPTED_EVIDENCE_STATUSES:
        blockers.append("CALIBRATION_STATUS_NOT_ACCEPTED")
    elif calibration.get("status") in {"REVIEW", "EMPTY"}:
        warnings.append(f"CALIBRATION_{calibration.get('status')}")

    dashboard_extra_blockers = hard_blockers_except_live_promotion(dashboard)
    if dashboard.get("status") == "BLOCKED" and not dashboard_extra_blockers:
        warnings.append("FINAL_READINESS_BLOCKED_ONLY_BY_PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH")
    elif dashboard_extra_blockers:
        blockers.append("FINAL_READINESS_HAS_NON_PAPER_BLOCKERS")

    promotion_verdict = promotion.get("final_verdict")
    if promotion_verdict == "NO_PROMOTION_KEEP_RESEARCH":
        warnings.append("PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH_LIVE_ONLY")
    elif promotion_verdict not in {"PAPER_ELITE_CANDIDATE_REVIEW", "NO_PROMOTION_KEEP_RESEARCH"}:
        warnings.append("PROMOTION_POLICY_VERDICT_REVIEW")

    if blockers:
        verdict = "BLOCKED_FOR_PAPER_SESSION"
        status = "BLOCKED"
    elif warnings:
        verdict = "PAPER_SESSION_REVIEW"
        status = "REVIEW"
    else:
        verdict = "READY_FOR_PAPER_SESSION"
        status = "READY"

    if (
        verdict == "PAPER_SESSION_REVIEW"
        and set(warnings) <= {
            "FINAL_READINESS_BLOCKED_ONLY_BY_PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH",
            "PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH_LIVE_ONLY",
        }
    ):
        verdict = "READY_FOR_PAPER_SESSION"
        status = "READY"

    return {
        "mode": "FQIS_TONIGHT_OPERATOR_LAUNCH",
        "status": status,
        "paper_session_verdict": verdict,
        "generated_at_utc": utc_now(),
        "steps": steps,
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "unsafe_flag_paths": sorted(set(unsafe_hits)),
        "input_report_statuses": {
            name: input_status(name, paths[name], reports[name])
            for name in reports
        },
        "full_cycle_status": full_cycle.get("status"),
        "operator_status": operator.get("status"),
        "operator_state": operator.get("operator_state"),
        "discord_renderer_status": renderer.get("status"),
        "elite_alerts_count": safe_int(renderer.get("elite_alerts_count")),
        "model_logs_count": safe_int(renderer.get("model_logs_count")),
        "quarantined_elite_alerts_count": quarantined_elite,
        "settled_signals": safe_int(signal.get("settled_signals")),
        "signal_settlement_status": signal.get("status"),
        "paper_roi": safe_float(signal.get("paper_roi")),
        "calibration_status": calibration.get("status"),
        "promotion_policy_status": promotion.get("status"),
        "promotion_policy_verdict": promotion_verdict,
        "final_readiness_status": dashboard.get("status"),
        "final_readiness_level": dashboard.get("readiness_level"),
        "final_readiness_hard_blockers": dashboard.get("hard_blockers") or [],
        "preview_html": str(preview_html),
        "preview_html_exists": preview_html.exists(),
        "dashboard_md": str(OUT_MD),
        "safety": dict(SAFETY_BLOCK),
        **SAFETY_BLOCK,
    }


def write_json_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def write_markdown_report(report: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# FQIS Tonight Operator Launch",
        "",
        "PAPER ONLY | NO REAL BET | NO STAKE | NO BOOKMAKER EXECUTION | NO DISCORD WEBHOOK SEND",
        "",
        "## Verdict Paper Session",
        "",
        f"- Verdict: **{report.get('paper_session_verdict')}**",
        f"- Status: **{report.get('status')}**",
        f"- Generated at UTC: `{report.get('generated_at_utc')}`",
        f"- Operator state: **{report.get('operator_state')}**",
        "",
        "## Commandes à Lancer",
        "",
        "```powershell",
        "python scripts/fqis_tonight_operator_launch.py",
        "start data\\pipeline\\api_sports\\orchestrator\\latest_discord_alert_preview.html",
        "notepad data\\pipeline\\api_sports\\orchestrator\\latest_tonight_operator_launch.md",
        "```",
        "",
        "## Fichiers à Ouvrir",
        "",
        f"- Discord preview HTML: `{report.get('preview_html')}`",
        f"- Launch runbook: `{output_path}`",
        f"- Final readiness dashboard: `{ORCH_DIR / 'latest_final_operator_readiness_dashboard.md'}`",
        f"- Full cycle report: `{ORCH_DIR / 'latest_full_cycle_report.md'}`",
        "",
        "## Statuts",
        "",
        "- READY_FOR_PAPER_SESSION: utilisable ce soir en observation paper-only.",
        "- PAPER_SESSION_REVIEW: utilisable seulement après lecture des warnings.",
        "- BLOCKED_FOR_PAPER_SESSION: ne pas utiliser avant correction du blocker.",
        "",
        "## PAPER VALUE",
        "",
        "- Un PAPER VALUE est un signal de recherche affiché pour observation, journalisation et revue opérateur.",
        "- Ce n'est pas une instruction de pari, pas une mise réelle, pas une promotion live-money.",
        "",
        "## Ce Qu'il Ne Faut Surtout Pas Faire",
        "",
        "- Ne pas activer live staking.",
        "- Ne pas exécuter de pari bookmaker.",
        "- Ne pas envoyer de webhook Discord réel.",
        "- Ne pas modifier les seuils de pricing, modèles, staking ou ledger.",
        "",
        "## Checklist Opérateur",
        "",
        "1. Lancer `python scripts/fqis_tonight_operator_launch.py`.",
        "2. Vérifier que le verdict est READY_FOR_PAPER_SESSION ou PAPER_SESSION_REVIEW assumé.",
        "3. Ouvrir `latest_discord_alert_preview.html`.",
        "4. Lire les blockers et warnings de ce runbook.",
        "5. Confirmer `operator_state = PAPER_READY`.",
        "6. Vérifier que tous les flags live/staking/mutation sont `False`.",
        "7. Lire les PAPER VALUE comme observations paper-only.",
        "8. Ignorer toute idée de stake réel ou exécution bookmaker.",
        "9. Garder le preview local; ne pas envoyer de webhook réel.",
        "10. Stopper la session si une stop condition apparaît.",
        "",
        "## Stop Conditions",
        "",
        "- unsafe flag true",
        "- full cycle non READY",
        "- Discord preview absent",
        "- erreur lecture JSON",
        "- trop d'alertes quarantined en elite",
        "- operator_state absent ou non PAPER_READY",
        "",
        "## Snapshot",
        "",
        f"- Full cycle status: **{report.get('full_cycle_status')}**",
        f"- Discord renderer status: **{report.get('discord_renderer_status')}**",
        f"- Elite alerts / model logs: **{report.get('elite_alerts_count')} / {report.get('model_logs_count')}**",
        f"- Signal settlement status: **{report.get('signal_settlement_status')}**",
        f"- Settled signals: **{report.get('settled_signals')}**",
        f"- Paper ROI: **{report.get('paper_roi')}**",
        f"- Calibration status: **{report.get('calibration_status')}**",
        f"- Promotion verdict: **{report.get('promotion_policy_verdict')}**",
        f"- Final readiness: **{report.get('final_readiness_status')} / {report.get('final_readiness_level')}**",
        "",
        "## Safety Flags",
        "",
        f"- can_execute_real_bets: **{report.get('can_execute_real_bets')}**",
        f"- can_enable_live_staking: **{report.get('can_enable_live_staking')}**",
        f"- can_mutate_ledger: **{report.get('can_mutate_ledger')}**",
        f"- live_staking_allowed: **{report.get('live_staking_allowed')}**",
        f"- promotion_allowed: **{report.get('promotion_allowed')}**",
        f"- live_execution_enabled: **{report.get('live_execution_enabled')}**",
        f"- discord_send_performed: **{report.get('discord_send_performed')}**",
        "",
        "## Blockers",
        "",
    ]
    blockers = report.get("blockers") or []
    lines.extend(f"- {safe_text(item)}" for item in blockers) if blockers else lines.append("- NONE")
    lines += ["", "## Warnings", ""]
    warnings = report.get("warnings") or []
    lines.extend(f"- {safe_text(item)}" for item in warnings) if warnings else lines.append("- NONE")
    lines += ["", "## Unsafe Flag Paths", ""]
    unsafe = report.get("unsafe_flag_paths") or []
    lines.extend(f"- {safe_text(item)}" for item in unsafe) if unsafe else lines.append("- NONE")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def short_console_payload(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": report["status"],
        "paper_session_verdict": report["paper_session_verdict"],
        "full_cycle_status": report.get("full_cycle_status"),
        "operator_state": report.get("operator_state"),
        "elite_alerts_count": report.get("elite_alerts_count"),
        "model_logs_count": report.get("model_logs_count"),
        "settled_signals": report.get("settled_signals"),
        "paper_roi": report.get("paper_roi"),
        "preview_html": report.get("preview_html"),
        "dashboard_md": report.get("dashboard_md"),
        "can_execute_real_bets": False,
        "can_enable_live_staking": False,
        "can_mutate_ledger": False,
        "live_execution_enabled": False,
        "promotion_allowed": False,
    }


def parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch and summarize tonight's FQIS paper-only operator session.")
    parser.add_argument("--skip-run", action="store_true", help="Read existing artifacts without running child scripts.")
    parser.add_argument("--skip-full-cycle", action="store_true", help="Internal full-cycle mode: do not recurse into full cycle.")
    parser.add_argument("--full-cycle-path", default=str(FULL_CYCLE_JSON))
    parser.add_argument("--discord-renderer-path", default=str(DISCORD_RENDERER_JSON))
    parser.add_argument("--final-dashboard-path", default=str(FINAL_DASHBOARD_JSON))
    parser.add_argument("--operator-shadow-console-path", default=str(OPERATOR_CONSOLE_JSON))
    parser.add_argument("--signal-settlement-path", default=str(SIGNAL_SETTLEMENT_JSON))
    parser.add_argument("--calibration-path", default=str(CALIBRATION_JSON))
    parser.add_argument("--promotion-policy-path", default=str(PROMOTION_POLICY_JSON))
    parser.add_argument("--preview-html", default=str(PREVIEW_HTML))
    parser.add_argument("--output-json", default=str(OUT_JSON))
    parser.add_argument("--output-md", default=str(OUT_MD))
    return parser


def main() -> int:
    args = parser().parse_args()
    paths = {
        "full_cycle": Path(args.full_cycle_path),
        "discord_renderer": Path(args.discord_renderer_path),
        "final_dashboard": Path(args.final_dashboard_path),
        "operator_shadow_console": Path(args.operator_shadow_console_path),
        "signal_settlement": Path(args.signal_settlement_path),
        "calibration": Path(args.calibration_path),
        "promotion_policy": Path(args.promotion_policy_path),
        "preview_html": Path(args.preview_html),
    }

    run_enabled = not args.skip_run
    steps = [
        run_step(
            "01_full_cycle",
            [CHILD_PYTHON, str(ROOT / "scripts" / "fqis_run_full_audit_cycle.py")],
            enabled=run_enabled and not args.skip_full_cycle,
        ),
        run_step(
            "02_discord_alert_renderer",
            [CHILD_PYTHON, str(ROOT / "scripts" / "fqis_discord_alert_renderer.py")],
            enabled=run_enabled,
        ),
        run_step(
            "03_final_operator_readiness_dashboard",
            [CHILD_PYTHON, str(ROOT / "scripts" / "fqis_final_operator_readiness_dashboard.py")],
            enabled=run_enabled,
        ),
    ]

    report = build_report(paths=paths, steps=steps)
    write_json_report(report, Path(args.output_json))
    write_markdown_report(report, Path(args.output_md))
    print(json.dumps(short_console_payload(report), indent=2, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
