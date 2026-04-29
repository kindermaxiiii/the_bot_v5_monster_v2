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
LATEST_MD = ORCH_DIR / "latest_full_cycle_report.md"
LATEST_JSON = ORCH_DIR / "latest_full_cycle_report.json"

REPORT_PATHS = {
    "live_decisions": ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json",
    "operator": ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_operator_report.json",
    "research_candidates": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_candidates.json",
    "research_settlement": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_settlement.json",
    "clv_horizon": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_clv_horizon_audit.json",
    "research_performance": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_performance_report.json",
    "provider_coverage": ROOT / "data" / "pipeline" / "api_sports" / "provider_coverage" / "latest_provider_coverage_report.json",
    "daily_audit": ROOT / "data" / "pipeline" / "api_sports" / "audit" / "latest_daily_audit_report.json",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def run_step(label: str, cmd: list[str], run_dir: Path) -> dict[str, Any]:
    stdout_path = run_dir / f"{label}.stdout.log"
    stderr_path = run_dir / f"{label}.stderr.log"

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    stdout_path.write_text(proc.stdout or "", encoding="utf-8")
    stderr_path.write_text(proc.stderr or "", encoding="utf-8")

    return {
        "label": label,
        "returncode": proc.returncode,
        "ok": proc.returncode == 0,
        "cmd": cmd,
        "stdout_log": str(stdout_path),
        "stderr_log": str(stderr_path),
    }


def pct(x: Any) -> str:
    try:
        return f"{float(x):.2%}"
    except Exception:
        return "0.00%"


def write_master_report(payload: dict[str, Any]) -> None:
    reports = payload["reports"]
    steps = payload["steps"]

    live = reports.get("live_decisions", {})
    live_summary = live.get("summary") or {}

    daily = reports.get("daily_audit", {})
    verdict = daily.get("verdict") or {}

    perf = reports.get("research_performance", {})
    perf_summary = perf.get("summary") or {}

    provider = reports.get("provider_coverage", {})
    provider_summary = provider.get("summary") or {}

    clv = reports.get("clv_horizon", {})
    horizons = ((clv.get("summary") or {}).get("horizons") or {})

    settlement = reports.get("research_settlement", {})
    settlement_summary = settlement.get("summary") or {}

    candidates = reports.get("research_candidates", {})
    candidates_summary = candidates.get("summary") or {}

    lines = [
        "# FQIS Full Cycle Report",
        "",
        "## Status",
        "",
        f"- Generated at UTC: `{payload['generated_at_utc']}`",
        f"- Overall status: **{payload['status']}**",
        f"- Run dir: `{payload['run_dir']}`",
        "",
        "## Final Verdict",
        "",
        f"- Verdict: **{verdict.get('final_verdict', 'UNKNOWN')}**",
        f"- Promotion allowed: **{verdict.get('promotion_allowed', False)}**",
        "",
        "## Flags",
        "",
    ]

    flags = verdict.get("flags") or []
    if not flags:
        lines.append("- No flags found.")
    else:
        for flag in flags:
            lines.append(f"- {flag}")

    lines += [
        "",
        "## Production / Bridge",
        "",
        f"- Groups total: **{live_summary.get('groups_total', 0)}**",
        f"- Groups priced: **{live_summary.get('groups_priced', 0)}**",
        f"- Groups skipped no Level 3: **{live_summary.get('groups_skipped_no_level3', 0)}**",
        f"- Decisions total: **{live_summary.get('decisions_total', 0)}**",
        f"- Official decisions: **{live_summary.get('official_decisions', 0)}**",
        f"- Watchlist decisions: **{live_summary.get('watchlist_decisions', 0)}**",
        f"- Blocked decisions: **{live_summary.get('blocked_decisions', 0)}**",
        f"- Level 3 state ready: **{live_summary.get('level3_state_ready', 0)}**",
        f"- Level 3 trade ready: **{live_summary.get('level3_trade_ready', 0)}**",
        f"- Level 3 stats available: **{live_summary.get('level3_stats_available', 0)}**",
        f"- Level 3 events available: **{live_summary.get('level3_events_available', 0)}**",
        "",
        "## Research Pipeline",
        "",
        f"- Candidates this cycle: **{candidates_summary.get('candidates_this_cycle', 0)}**",
        f"- Strict events+stats candidates: **{candidates_summary.get('strict_events_plus_stats', 0)}**",
        f"- Events-only research candidates: **{candidates_summary.get('events_only_research', 0)}**",
        f"- New snapshots appended: **{candidates_summary.get('new_snapshots_appended', 0)}**",
        f"- Settled rows: **{settlement_summary.get('settled', 0)}**",
        f"- Research PnL: **{settlement_summary.get('pnl_unit_total', 0)}u**",
        f"- Research ROI: **{settlement_summary.get('roi_unit', 0)}**",
        "",
        "## Research Performance",
        "",
        "| Level | Rows | Settled | PnL | ROI | Avg CLV |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for level in ["snapshot", "signal", "match"]:
        m = (perf_summary.get(level) or {})
        avg_clv = m.get("avg_clv_decimal")
        avg_clv_text = "" if avg_clv is None else str(avg_clv)
        lines.append(
            f"| {level.upper()} | {m.get('rows', 0)} | {m.get('settled', 0)} | {m.get('pnl_unit', 0)} | {m.get('roi_unit', 0)} | {avg_clv_text} |"
        )

    lines += [
        "",
        "## Fixed-Horizon CLV",
        "",
        "| Horizon | Tracked | Positive | Negative | Avg CLV | Positive rate |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for h in ["1m", "5m", "15m", "near_close"]:
        m = horizons.get(h) or {}
        lines.append(
            f"| {h} | {m.get('tracked', 0)} | {m.get('positive', 0)} | {m.get('negative', 0)} | {pct(m.get('avg', 0))} | {pct(m.get('positive_rate', 0))} |"
        )

    lines += [
        "",
        "## Historical Provider Coverage",
        "",
        f"- Fixtures total: **{provider_summary.get('fixtures_total', 0)}**",
        f"- Events coverage rate: **{pct(provider_summary.get('events_coverage_rate', 0))}**",
        f"- Statistics coverage rate: **{pct(provider_summary.get('statistics_coverage_rate', 0))}**",
        f"- Events + stats rate: **{pct(provider_summary.get('events_plus_stats_rate', 0))}**",
        "",
        "## Step Execution",
        "",
        "| Step | Status | Return code |",
        "|---|---|---:|",
    ]

    for step in steps:
        status = "OK" if step["ok"] else "FAILED"
        lines.append(f"| {step['label']} | {status} | {step['returncode']} |")

    lines += [
        "",
        "## Institutional Read",
        "",
        "This full cycle is valid only if every step is OK. Promotion remains forbidden unless the daily audit explicitly returns promotion_allowed = true.",
    ]

    LATEST_MD.parent.mkdir(parents=True, exist_ok=True)
    LATEST_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")



def inject_research_screening_diagnostics_into_latest_full_cycle_report() -> None:
    import json
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]

    report_path = (
        root
        / "data"
        / "pipeline"
        / "api_sports"
        / "orchestrator"
        / "latest_full_cycle_report.md"
    )

    audit_path = (
        root
        / "data"
        / "pipeline"
        / "api_sports"
        / "audit"
        / "latest_daily_audit_report.json"
    )

    if not report_path.exists() or not audit_path.exists():
        return

    try:
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
    except Exception:
        return

    diag = audit.get("research_diagnostics") or {}
    verdict = audit.get("verdict") or {}
    key_metrics = verdict.get("key_metrics") or {}

    def val(name: str, default=0):
        return diag.get(name, key_metrics.get(name, default))

    def fnum(x, default: float = 0.0) -> float:
        try:
            if x is None or x == "":
                return default
            return float(str(x).replace(",", ".").strip())
        except Exception:
            return default

    def pct(x) -> str:
        return f"{fnum(x) * 100:.2f}%"

    section = "\n".join([
        "## Research Screening Diagnostics",
        "",
        f"- Decisions screened: **{val('decisions_screened')}**",
        f"- Candidates accepted: **{val('candidates_this_cycle')}**",
        f"- Research acceptance rate: **{pct(val('research_acceptance_rate'))}**",
        f"- Strict events+stats candidates: **{val('strict_events_plus_stats')}**",
        f"- Events-only research candidates: **{val('events_only_research')}**",
        f"- New snapshots appended: **{val('new_snapshots_appended')}**",
        f"- Rejected by timing policy: **{val('timing_policy_rejected')}** = **{pct(val('timing_rejection_rate'))}**",
        f"- Rejected by data tier: **{val('data_tier_rejected')}** = **{pct(val('data_tier_rejection_rate'))}**",
        f"- Rejected by non-positive edge/EV: **{val('non_positive_edge_or_ev_rejected')}** = **{pct(val('non_positive_edge_or_ev_rejection_rate'))}**",
        f"- Rejected by final status: **{val('final_status_rejected')}**",
        f"- Rejected by negative-value veto: **{val('negative_value_veto_rejected')}**",
        "",
    ])

    report = report_path.read_text(encoding="utf-8")

    report = re.sub(
        r"\n## Research Screening Diagnostics\n.*?(?=\n## )",
        "\n",
        report,
        flags=re.DOTALL,
    )

    marker = "\n## Research Performance"
    if marker in report:
        report = report.replace(marker, "\n" + section + marker, 1)
    else:
        report = report.rstrip() + "\n\n" + section + "\n"

    report_path.write_text(report, encoding="utf-8")



def inject_fixture_level_research_into_latest_full_cycle_report() -> None:
    import json
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]

    report_path = (
        root
        / "data"
        / "pipeline"
        / "api_sports"
        / "orchestrator"
        / "latest_full_cycle_report.md"
    )

    fixture_path = (
        root
        / "data"
        / "pipeline"
        / "api_sports"
        / "research_ledger"
        / "latest_fixture_level_research_report.json"
    )

    if not report_path.exists() or not fixture_path.exists():
        return

    try:
        fixture_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    except Exception:
        return

    snapshot = fixture_payload.get("snapshot") or {}
    fixture = fixture_payload.get("fixture") or {}
    side = fixture_payload.get("side_distribution") or {}
    concentration = fixture_payload.get("concentration") or []

    section_lines = [
        "## Fixture-Level Research Read",
        "",
        "> Official conservative read: one economic thesis per fixture. Snapshot PnL remains diagnostic only.",
        "",
        f"- Ledger rows: **{fixture_payload.get('ledger_rows', 0)}**",
        f"- Snapshot settled: **{snapshot.get('settled', 0)}**",
        f"- Snapshot PnL: **{snapshot.get('pnl_unit', 0)}u**",
        f"- Fixture-level settled: **{fixture.get('settled', 0)}**",
        f"- Fixture-level PnL: **{fixture.get('pnl_unit', 0)}u**",
        f"- Fixture-level ROI: **{fixture.get('roi_unit', 0)}**",
        f"- Fixture-level wins/losses/pushes: **{fixture.get('wins', 0)} / {fixture.get('losses', 0)} / {fixture.get('pushes', 0)}**",
        f"- UNDER share: **{float(side.get('under_share', 0)) * 100:.2f}%**",
        f"- OVER share: **{float(side.get('over_share', 0)) * 100:.2f}%**",
        "",
        "| Match | Snapshot rows | Snapshot PnL | Fixture PnL | First selection | First odds | First result |",
        "|---|---:|---:|---:|---|---:|---|",
    ]

    for row in concentration[:10]:
        section_lines.append(
            "| {match} | {snapshot_rows} | {snapshot_pnl} | {fixture_pnl} | {first_selection} | {first_odds} | {first_result} |".format(
                match=str(row.get("match", "")).replace("|", "/"),
                snapshot_rows=row.get("snapshot_rows", 0),
                snapshot_pnl=row.get("snapshot_pnl", 0),
                fixture_pnl=row.get("fixture_pnl", 0),
                first_selection=row.get("first_selection", ""),
                first_odds=row.get("first_odds", ""),
                first_result=row.get("first_result", ""),
            )
        )

    section_lines.append("")

    section = "\n".join(section_lines)

    report = report_path.read_text(encoding="utf-8")

    report = re.sub(
        r"\n## Fixture-Level Research Read\n.*?(?=\n## )",
        "\n",
        report,
        flags=re.DOTALL,
    )

    marker = "\n## Fixed-Horizon CLV"
    if marker in report:
        report = report.replace(marker, "\n" + section + marker, 1)
    else:
        report = report.rstrip() + "\n\n" + section + "\n"

    report_path.write_text(report, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-bridge", action="store_true")
    parser.add_argument("--discord", action="store_true")
    parser.add_argument("--min-edge", type=float, default=0.025)
    parser.add_argument("--min-ev", type=float, default=0.020)
    parser.add_argument("--max-alerts", type=int, default=5)
    parser.add_argument("--level3-max-fixtures", type=int, default=15)
    parser.add_argument("--level3-cache-ttl-seconds", type=int, default=120)
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
    run_dir = ORCH_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    steps: list[dict[str, Any]] = []

    if not args.skip_bridge:
        bridge_cmd = [
            sys.executable,
            str(ROOT / "scripts" / "fqis_api_sports_live_decision_bridge.py"),
            "--output-dir",
            "data/pipeline/api_sports/decision_bridge_live",
            "--use-level3-state",
            "--level3-output-dir",
            "data/pipeline/api_sports/level3_live_state",
            "--level3-max-fixtures",
            str(args.level3_max_fixtures),
            "--level3-cache-ttl-seconds",
            str(args.level3_cache_ttl_seconds),
            "--min-edge",
            str(args.min_edge),
            "--min-ev",
            str(args.min_ev),
            "--max-alerts",
            str(args.max_alerts),
        ]

        if args.discord:
            bridge_cmd.append("--discord")

        steps.append(run_step("01_decision_bridge", bridge_cmd, run_dir))

    scripts = [
        ("02_research_ledger", "fqis_research_ledger.py"),
        ("03_research_settlement", "fqis_research_settlement.py"),
        ("04_clv_tracker", "fqis_clv_tracker.py"),
        ("05_clv_horizon_audit", "fqis_clv_horizon_audit.py"),
        ("06_research_performance", "fqis_research_performance_report.py"),
        ("07_operator_report", "fqis_operator_decision_report.py"),
        ("08_provider_coverage", "fqis_provider_coverage_report.py"),
        ("09_daily_audit", "fqis_daily_audit_report.py"),
    ]

    for label, script in scripts:
        steps.append(run_step(label, [sys.executable, str(ROOT / "scripts" / script)], run_dir))

    reports = {name: read_json(path) for name, path in REPORT_PATHS.items()}

    status = "READY" if all(s["ok"] for s in steps) else "PARTIAL_FAILURE"

    payload = {
        "mode": "FQIS_FULL_CYCLE_ORCHESTRATOR",
        "status": status,
        "generated_at_utc": utc_now(),
        "run_dir": str(run_dir),
        "steps": steps,
        "reports": reports,
        "latest_md": str(LATEST_MD),
        "latest_json": str(LATEST_JSON),
    }

    LATEST_JSON.parent.mkdir(parents=True, exist_ok=True)
    LATEST_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    write_master_report(payload)

    verdict = ((reports.get("daily_audit") or {}).get("verdict") or {})

    print(json.dumps({
        "status": status,
        "final_verdict": verdict.get("final_verdict"),
        "promotion_allowed": verdict.get("promotion_allowed"),
        "run_dir": str(run_dir),
        "latest_md": str(LATEST_MD),
        "latest_json": str(LATEST_JSON),
    }, indent=2, ensure_ascii=False))

    return 0 if status == "READY" else 2


if __name__ == "__main__":
    exit_code = main()

    # Fixture-level report is conservative anti-duplication research accounting.
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    fixture_script = root / "scripts" / "fqis_fixture_level_research_report.py"
    if fixture_script.exists():
        subprocess.run([sys.executable, str(fixture_script)], check=False)

    inject_research_screening_diagnostics_into_latest_full_cycle_report()
    inject_fixture_level_research_into_latest_full_cycle_report()
    raise SystemExit(exit_code)

