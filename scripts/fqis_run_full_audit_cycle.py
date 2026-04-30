from __future__ import annotations

import argparse
import hashlib
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
RESEARCH_CANDIDATES_LEDGER = (
    ROOT
    / "data"
    / "pipeline"
    / "api_sports"
    / "research_ledger"
    / "research_candidates_ledger.csv"
)
VENV_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
CHILD_PYTHON = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

REPORT_PATHS = {
    "live_decisions": ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json",
    "operator": ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_operator_report.json",
    "research_candidates": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_candidates.json",
    "research_settlement": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_settlement.json",
    "clv_horizon": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_clv_horizon_audit.json",
    "research_performance": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_performance_report.json",
    "provider_coverage": ROOT / "data" / "pipeline" / "api_sports" / "provider_coverage" / "latest_provider_coverage_report.json",
    "level3_stats_coverage_diagnostic": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_level3_stats_coverage_diagnostic.json",
    "daily_audit": ROOT / "data" / "pipeline" / "api_sports" / "audit" / "latest_daily_audit_report.json",
    "final_pipeline_audit": ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_final_pipeline_audit.json",
    "bucket_alpha_audit": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_alpha_audit.json",
    "bucket_policy_audit": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_policy_audit.json",
    "bucket_quarantine_dry_run": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_quarantine_dry_run.json",
    "post_quarantine_pnl_simulation": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_post_quarantine_pnl_simulation.json",
    "go_no_go": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_go_no_go_report.json",
    "shadow_readiness": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_shadow_readiness_report.json",
    "live_freshness": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_live_freshness_report.json",
    "live_opportunity_scanner": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_live_opportunity_scanner.json",
    "paper_signal_export": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_signal_export.json",
    "paper_alert_dedupe": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_alert_dedupe.json",
    "paper_alert_ranker": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_alert_ranker.json",
    "operator_paper_decision_sheet": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_paper_decision_sheet.json",
    "discord_paper_payload": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_discord_paper_payload.json",
    "operator_shadow_console": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_shadow_console.json",
    "shadow_session_quality": ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_shadow_session_quality_report.json",
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


def capture_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "content": b"",
            "sha256_before": "",
        }

    content = path.read_bytes()
    return {
        "path": str(path),
        "exists": True,
        "content": content,
        "sha256_before": hashlib.sha256(content).hexdigest(),
    }


def restore_file(snapshot: dict[str, Any]) -> dict[str, Any]:
    path = Path(str(snapshot["path"]))

    if snapshot["exists"]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(snapshot["content"])
        content_after = path.read_bytes()
        sha256_after = hashlib.sha256(content_after).hexdigest()
    else:
        if path.exists():
            path.unlink()
        sha256_after = ""

    return {
        "path": str(path),
        "exists_before": bool(snapshot["exists"]),
        "sha256_before": str(snapshot["sha256_before"]),
        "sha256_after": sha256_after,
        "preserved": str(snapshot["sha256_before"]) == sha256_after,
    }


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


def read_reports(exclude: set[str] | None = None) -> dict[str, Any]:
    excluded = exclude or set()
    return {
        name: read_json(path)
        for name, path in REPORT_PATHS.items()
        if name not in excluded
    }


def cycle_status(steps: list[dict[str, Any]], ledger_restore: dict[str, Any]) -> str:
    return "READY" if all(s["ok"] for s in steps) and ledger_restore["preserved"] else "PARTIAL_FAILURE"


def build_payload(
    *,
    status: str,
    generated_at_utc: str,
    run_dir: Path,
    steps: list[dict[str, Any]],
    reports: dict[str, Any],
    ledger_restore: dict[str, Any],
) -> dict[str, Any]:
    return {
        "mode": "FQIS_FULL_CYCLE_ORCHESTRATOR",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "run_dir": str(run_dir),
        "steps": steps,
        "reports": reports,
        "invariants": {
            "research_candidates_ledger_preserved": ledger_restore["preserved"],
            "research_candidates_ledger": ledger_restore,
            "live_staking_enabled": False,
            "simulation_only": True,
        },
        "latest_md": str(LATEST_MD),
        "latest_json": str(LATEST_JSON),
    }


def write_latest_payload(payload: dict[str, Any]) -> None:
    LATEST_JSON.parent.mkdir(parents=True, exist_ok=True)
    LATEST_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


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
    stats_diag = reports.get("level3_stats_coverage_diagnostic", {})
    stats_diag_summary = stats_diag.get("summary") or {}

    clv = reports.get("clv_horizon", {})
    horizons = ((clv.get("summary") or {}).get("horizons") or {})

    settlement = reports.get("research_settlement", {})
    settlement_summary = settlement.get("summary") or {}

    candidates = reports.get("research_candidates", {})
    candidates_summary = candidates.get("summary") or {}

    final_pipeline = reports.get("final_pipeline_audit", {})
    final_pipeline_counts = final_pipeline.get("final_pipeline_counts") or {}
    gate_state_counts = final_pipeline.get("level3_gate_state_counts") or {}

    bucket_policy = reports.get("bucket_policy_audit", {})
    bucket_policies = bucket_policy.get("buckets") or {}

    bucket_quarantine = reports.get("bucket_quarantine_dry_run", {})

    post_quarantine = reports.get("post_quarantine_pnl_simulation", {})
    pq_base = post_quarantine.get("baseline") or {}
    pq_post = post_quarantine.get("post_quarantine") or {}
    pq_removed = post_quarantine.get("removed_by_quarantine") or {}
    pq_delta = post_quarantine.get("delta") or {}
    go_no_go = reports.get("go_no_go", {})
    shadow = reports.get("shadow_readiness", {})
    shadow_base = shadow.get("baseline") or {}
    shadow_post = shadow.get("post_quarantine") or {}
    freshness = reports.get("live_freshness", {})
    freshness_flags = freshness.get("freshness_flags") or []
    scanner = reports.get("live_opportunity_scanner", {})
    paper_export = reports.get("paper_signal_export", {})
    paper_dedupe = reports.get("paper_alert_dedupe", {})
    paper_ranker = reports.get("paper_alert_ranker", {})
    decision_sheet = reports.get("operator_paper_decision_sheet", {})
    discord_payload = reports.get("discord_paper_payload", {})
    operator_console = reports.get("operator_shadow_console", {})
    shadow_session_quality = reports.get("shadow_session_quality", {})

    lines = [
        "# FQIS Full Cycle Report",
        "",
        "## Status",
        "",
        f"- Generated at UTC: `{payload['generated_at_utc']}`",
        f"- Overall status: **{payload['status']}**",
        f"- Run dir: `{payload['run_dir']}`",
        f"- Research candidates ledger preserved: **{payload.get('invariants', {}).get('research_candidates_ledger_preserved', False)}**",
        "",
        "## Go / No-Go Verdict",
        "",
        f"- State: **{go_no_go.get('go_no_go_state', 'UNKNOWN')}**",
        f"- Promotion allowed: **{go_no_go.get('promotion_allowed', False)}**",
        f"- Live staking allowed: **{go_no_go.get('live_staking_allowed', False)}**",
        f"- Simulation only: **{go_no_go.get('simulation_only', True)}**",
        "",
        "### Reasons",
        "",
    ]

    go_no_go_reasons = go_no_go.get("reasons") or []
    if not go_no_go_reasons:
        lines.append("- No go/no-go reasons found.")
    else:
        for reason in go_no_go_reasons:
            lines.append(f"- {reason}")

    lines += [
        "",
        "## Shadow Readiness",
        "",
        f"- State: **{shadow.get('shadow_state', 'UNKNOWN')}**",
        f"- Status: **{shadow.get('status', 'UNKNOWN')}**",
        f"- Can publish Discord paper-only: **{shadow.get('can_publish_to_discord_paper_only', False)}**",
        f"- Can execute real bets: **{shadow.get('can_execute_real_bets', False)}**",
        f"- Can mutate ledger: **{shadow.get('can_mutate_ledger', False)}**",
        f"- Can enable live staking: **{shadow.get('can_enable_live_staking', False)}**",
        f"- Baseline PnL / ROI: **{shadow_base.get('pnl', 0)}u / {shadow_base.get('roi', 0)}**",
        f"- Post-quarantine PnL / ROI: **{shadow_post.get('pnl', 0)}u / {shadow_post.get('roi', 0)}**",
        "",
        "### Shadow Reasons",
        "",
    ]

    shadow_reasons = shadow.get("reasons") or []
    if not shadow_reasons:
        lines.append("- No shadow readiness reasons found.")
    else:
        for reason in shadow_reasons:
            lines.append(f"- {reason}")

    lines += [
        "",
        "## Live Freshness",
        "",
        f"- Status: **{freshness.get('status', 'UNKNOWN')}**",
        f"- Decisions total: **{freshness.get('decisions_total', 0)}**",
        f"- Candidates this cycle: **{freshness.get('candidates_this_cycle', 0)}**",
        f"- New snapshots appended: **{freshness.get('new_snapshots_appended', 0)}**",
        f"- Ledger rows total: **{freshness.get('ledger_rows_total', 0)}**",
        f"- Freshness flags: **{', '.join(str(flag) for flag in freshness_flags) if freshness_flags else 'NONE'}**",
        f"- Historical static review: **{', '.join(str(flag) for flag in freshness.get('historical_metric_static_review') or []) or 'NONE'}**",
        "",
        "## Live Opportunity Scanner",
        "",
        f"- Status: **{scanner.get('status', 'UNKNOWN')}**",
        f"- Operator read: **{scanner.get('operator_read', 'UNKNOWN_REVIEW')}**",
        f"- Live fixtures seen: **{scanner.get('live_fixtures_seen', 0)}**",
        f"- Groups total / priced: **{scanner.get('groups_total', 0)} / {scanner.get('groups_priced', 0)}**",
        f"- Decisions total: **{scanner.get('decisions_total', 0)}**",
        f"- Official / watchlist / blocked: **{scanner.get('official_decisions', 0)} / {scanner.get('watchlist_decisions', 0)} / {scanner.get('blocked_decisions', 0)}**",
        f"- Candidates this cycle: **{scanner.get('candidates_this_cycle', 0)}**",
        f"- New snapshots appended: **{scanner.get('new_snapshots_appended', 0)}**",
        f"- Level 3 state/trade ready: **{scanner.get('level3_state_ready_count', 0)} / {scanner.get('level3_trade_ready_count', 0)}**",
        f"- Level 3 events/stats available: **{scanner.get('level3_events_available_count', 0)} / {scanner.get('level3_stats_available_count', 0)}**",
        f"- Score-only decisions: **{scanner.get('score_only_decisions', 0)}**",
        f"- Rejected non-positive edge/EV: **{scanner.get('rejected_by_non_positive_edge_ev', 0)}**",
        f"- Rejected timing/data/final/negative-veto: **{scanner.get('rejected_by_timing_policy', 0)} / {scanner.get('rejected_by_data_tier', 0)} / {scanner.get('rejected_by_final_status', 0)} / {scanner.get('rejected_by_negative_value_veto', 0)}**",
        f"- Safety flags false: **{scanner.get('can_execute_real_bets', False) is False and scanner.get('can_enable_live_staking', False) is False and scanner.get('can_mutate_ledger', False) is False and scanner.get('live_staking_allowed', False) is False and scanner.get('promotion_allowed', False) is False}**",
        "",
        "## Level 3 Stats Coverage Diagnostic",
        "",
        f"- Status: **{stats_diag.get('status', 'UNKNOWN')}**",
        f"- Fixtures seen: **{stats_diag_summary.get('fixtures_seen', 0)}**",
        f"- Events available: **{stats_diag_summary.get('events_available', 0)}**",
        f"- Raw stats available: **{stats_diag_summary.get('raw_stats_available', 0)}**",
        f"- Parsed stats available: **{stats_diag_summary.get('parsed_stats_available', 0)}**",
        f"- Events-only no stats: **{stats_diag_summary.get('events_only_no_stats', 0)}**",
        f"- Stats parser empty: **{stats_diag_summary.get('stats_parser_empty', 0)}**",
        f"- Stats endpoint missing: **{stats_diag_summary.get('stats_endpoint_missing', 0)}**",
        "",
        "## Paper Signal Export",
        "",
        f"- Status: **{paper_export.get('status', 'UNKNOWN')}**",
        f"- Total paper signals: **{paper_export.get('paper_signals_total', paper_export.get('total_decisions', 0))}**",
        f"- Paper production sim-only: **{paper_export.get('paper_production_sim_only_count', 0)}**",
        f"- Paper research watch: **{paper_export.get('paper_research_watch_count', 0)}**",
        f"- Paper rejected: **{paper_export.get('paper_rejected_count', 0)}**",
        f"- Can execute real bets: **{(paper_export.get('safety') or {}).get('can_execute_real_bets', False)}**",
        "",
        "## Paper Alert Dedupe",
        "",
        f"- Status: **{paper_dedupe.get('status', 'UNKNOWN')}**",
        f"- New paper alerts: **{paper_dedupe.get('new_alerts', 0)}**",
        f"- Repeated paper alerts: **{paper_dedupe.get('repeated_alerts', 0)}**",
        f"- Suppressed repeats: **{paper_dedupe.get('suppressed_repeats', 0)}**",
        f"- State size: **{paper_dedupe.get('state_size', 0)}**",
        "",
        "## Paper Alert Ranker",
        "",
        f"- Status: **{paper_ranker.get('status', 'UNKNOWN')}**",
        f"- Ranked alert count: **{paper_ranker.get('ranked_alert_count', 0)}**",
        f"- Top ranked alert count: **{paper_ranker.get('top_ranked_alert_count', 0)}**",
        f"- New ranked alerts: **{paper_ranker.get('new_ranked_alert_count', 0)}**",
        f"- Repeated ranked alerts: **{paper_ranker.get('repeated_ranked_alert_count', 0)}**",
        f"- Paper only: **{paper_ranker.get('paper_only', True)}**",
        f"- Can execute real bets: **{paper_ranker.get('can_execute_real_bets', False)}**",
        f"- Can enable live staking: **{paper_ranker.get('can_enable_live_staking', False)}**",
        f"- Can mutate ledger: **{paper_ranker.get('can_mutate_ledger', False)}**",
        "",
        "## Operator Paper Decision Sheet",
        "",
        f"- Status: **{decision_sheet.get('status', 'UNKNOWN')}**",
        f"- Ranked alert count: **{decision_sheet.get('ranked_alert_count', 0)}**",
        f"- Top ranked alert count: **{decision_sheet.get('top_ranked_alert_count', 0)}**",
        f"- New paper alerts: **{decision_sheet.get('new_paper_alerts', 0)}**",
        f"- Repeated paper alerts: **{decision_sheet.get('repeated_paper_alerts', 0)}**",
        f"- Paper only: **{decision_sheet.get('paper_only', True)}**",
        f"- Can execute real bets: **{decision_sheet.get('can_execute_real_bets', False)}**",
        f"- Can enable live staking: **{decision_sheet.get('can_enable_live_staking', False)}**",
        f"- Can mutate ledger: **{decision_sheet.get('can_mutate_ledger', False)}**",
        "",
        "## Discord Paper Payload",
        "",
        f"- Status: **{discord_payload.get('status', 'UNKNOWN')}**",
        f"- Sendable: **{discord_payload.get('sendable', False)}**",
        f"- Send reason: **{discord_payload.get('send_reason', 'UNKNOWN')}**",
        f"- Discord send performed: **{discord_payload.get('discord_send_performed', False)}**",
        "",
        "## Operator Shadow Console",
        "",
        f"- Status: **{operator_console.get('status', 'UNKNOWN')}**",
        f"- Operator state: **{operator_console.get('operator_state', 'UNKNOWN')}**",
        f"- Next action: **{operator_console.get('next_action', 'UNKNOWN')}**",
        f"- Total paper signals: **{operator_console.get('total_paper_signals', 0)}**",
        f"- Top ranked alerts: **{operator_console.get('top_ranked_alert_count', 0)}**",
        f"- New paper alerts: **{operator_console.get('new_paper_alerts', 0)}**",
        "",
        "## Shadow Session Quality",
        "",
        f"- Status: **{shadow_session_quality.get('status', 'NO_MONITOR_SESSION_AVAILABLE')}**",
        f"- Quality state: **{shadow_session_quality.get('quality_state', shadow_session_quality.get('status', 'NO_MONITOR_SESSION_AVAILABLE'))}**",
        f"- Cycles completed: **{shadow_session_quality.get('cycles_completed', 0)}**",
        f"- Ready cycles: **{shadow_session_quality.get('ready_cycles', 0)}**",
        f"- Raw new paper alerts: **{shadow_session_quality.get('total_raw_new_paper_alerts', 0)}**",
        f"- Canonical new alerts: **{shadow_session_quality.get('total_canonical_new_alerts', 0)}**",
        f"- Material updates: **{shadow_session_quality.get('total_material_updates', 0)}**",
        f"- Alert noise ratio: **{shadow_session_quality.get('alert_noise_ratio', 0)}**",
        f"- Recommended next action: **{shadow_session_quality.get('recommended_next_action', 'RUN_SHADOW_MONITOR')}**",
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
        "## Level 3 Final Pipeline Audit",
        "",
        f"- Production routed: **{final_pipeline_counts.get('production', 0)}**",
        f"- Research routed: **{final_pipeline_counts.get('research', 0)}**",
        f"- Rejected routed: **{final_pipeline_counts.get('reject', 0)}**",
        f"- REAL_TRADE_READY decisions: **{gate_state_counts.get('REAL_TRADE_READY', 0)}**",
        f"- EVENTS_ONLY_RESEARCH_READY decisions: **{gate_state_counts.get('EVENTS_ONLY_RESEARCH_READY', 0)}**",
        f"- SCORE_ONLY decisions: **{gate_state_counts.get('SCORE_ONLY', 0)}**",
        f"- Live staking true count: **{final_pipeline.get('live_staking_allowed_true_count', 0)}**",
        f"- Live staking invariant disabled: **{final_pipeline.get('invariant_live_staking_disabled', False)}**",
        "",
        "## Bucket Alpha Policy",
        "",
        "| Bucket | Action | Settled | ROI | PnL | Win rate |",
        "|---|---|---:|---:|---:|---:|",
        *[
            f"| {bucket} | {m.get('action')} | {m.get('settled')} | {m.get('roi')} | {m.get('pnl')} | {m.get('win_rate')} |"
            for bucket, m in sorted(bucket_policies.items())
        ],
        "",
        "## Bucket Quarantine Dry Run",
        "",
        f"- Mode: **{bucket_quarantine.get('mode', 'UNKNOWN')}**",
        f"- Rows total: **{bucket_quarantine.get('rows_total', 0)}**",
        f"- Would keep: **{bucket_quarantine.get('would_keep', 0)}**",
        f"- Would quarantine: **{bucket_quarantine.get('would_quarantine', 0)}**",
        f"- Would quarantine rate: **{pct(bucket_quarantine.get('would_quarantine_rate', 0))}**",
        "",
        "## Post-Quarantine PnL Simulation",
        "",
        f"- Mode: **{post_quarantine.get('mode', 'UNKNOWN')}**",
        f"- Baseline PnL / ROI: **{pq_base.get('pnl', 0)}u / {pq_base.get('roi', 0)}**",
        f"- Post-quarantine PnL / ROI: **{pq_post.get('pnl', 0)}u / {pq_post.get('roi', 0)}**",
        f"- Removed PnL / ROI: **{pq_removed.get('pnl', 0)}u / {pq_removed.get('roi', 0)}**",
        f"- PnL improvement: **{pq_delta.get('pnl_improvement', 0)}u**",
        f"- ROI improvement: **{pq_delta.get('roi_improvement', 0)}**",
        "- Scope: **simulation-only / dry-run-only / no ledger mutation**",
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

    def pct_text(x) -> str:
        return f"{fnum(x) * 100:.2f}%"

    section = "\n".join([
        "## Research Screening Diagnostics",
        "",
        f"- Decisions screened: **{val('decisions_screened')}**",
        f"- Candidates accepted: **{val('candidates_this_cycle')}**",
        f"- Research acceptance rate: **{pct_text(val('research_acceptance_rate'))}**",
        f"- Strict events+stats candidates: **{val('strict_events_plus_stats')}**",
        f"- Events-only research candidates: **{val('events_only_research')}**",
        f"- New snapshots appended: **{val('new_snapshots_appended')}**",
        f"- Rejected by timing policy: **{val('timing_policy_rejected')}** = **{pct_text(val('timing_rejection_rate'))}**",
        f"- Rejected by data tier: **{val('data_tier_rejected')}** = **{pct_text(val('data_tier_rejection_rate'))}**",
        f"- Rejected by non-positive edge/EV: **{val('non_positive_edge_or_ev_rejected')}** = **{pct_text(val('non_positive_edge_or_ev_rejection_rate'))}**",
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
    ledger_snapshot = capture_file(RESEARCH_CANDIDATES_LEDGER)

    steps: list[dict[str, Any]] = []

    if not args.skip_bridge:
        bridge_cmd = [
            CHILD_PYTHON,
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
        ("08b_level3_stats_coverage_diagnostic", "fqis_level3_stats_coverage_diagnostic.py"),
        ("09_daily_audit", "fqis_daily_audit_report.py"),
        ("10_final_pipeline_audit", "fqis_final_pipeline_audit.py"),
        ("11_level3_invariant_report", "fqis_level3_invariant_report.py"),
        ("12_bucket_alpha_audit", "fqis_bucket_alpha_audit.py"),
        ("13_bucket_policy_audit", "fqis_bucket_policy_audit.py"),
        ("14_bucket_quarantine_dry_run", "fqis_bucket_quarantine_dry_run.py"),
        ("15_post_quarantine_pnl_simulation", "fqis_post_quarantine_pnl_simulation.py"),
        ("16_go_no_go_report", "fqis_go_no_go_report.py"),
        ("17_shadow_readiness_report", "fqis_shadow_readiness_report.py"),
        ("18_live_freshness_report", "fqis_live_freshness_report.py"),
        ("19_paper_signal_export", "fqis_paper_signal_export.py"),
        ("20_paper_alert_dedupe", "fqis_paper_alert_dedupe.py"),
        ("21_paper_alert_ranker", "fqis_paper_alert_ranker.py"),
        ("22_operator_paper_decision_sheet", "fqis_operator_paper_decision_sheet.py"),
        ("23_discord_paper_payload", "fqis_discord_paper_payload.py"),
        ("24_operator_shadow_console", "fqis_operator_shadow_console.py"),
        ("25_shadow_session_quality_report", "fqis_shadow_session_quality_report.py"),
    ]

    generated_at_utc = utc_now()

    def write_stage(exclude: set[str] | None = None) -> tuple[dict[str, Any], dict[str, Any], str, dict[str, Any]]:
        stage_reports = read_reports(exclude=exclude)
        stage_ledger_restore = restore_file(ledger_snapshot)
        stage_status = cycle_status(steps, stage_ledger_restore)
        stage_payload = build_payload(
            status=stage_status,
            generated_at_utc=generated_at_utc,
            run_dir=run_dir,
            steps=steps,
            reports=stage_reports,
            ledger_restore=stage_ledger_restore,
        )
        write_latest_payload(stage_payload)
        return stage_reports, stage_ledger_restore, stage_status, stage_payload

    for label, script in scripts[:16]:
        steps.append(run_step(label, [CHILD_PYTHON, str(ROOT / "scripts" / script)], run_dir))

    reports, ledger_restore, status, payload = write_stage(
        exclude={
            "shadow_readiness",
            "live_freshness",
            "live_opportunity_scanner",
            "paper_signal_export",
            "paper_alert_dedupe",
            "paper_alert_ranker",
            "operator_paper_decision_sheet",
            "discord_paper_payload",
            "operator_shadow_console",
            "shadow_session_quality",
        }
    )

    shadow_label, shadow_script = scripts[16]
    steps.append(run_step(shadow_label, [CHILD_PYTHON, str(ROOT / "scripts" / shadow_script)], run_dir))

    reports, ledger_restore, status, payload = write_stage(
        exclude={
            "live_freshness",
            "live_opportunity_scanner",
            "paper_signal_export",
            "paper_alert_dedupe",
            "paper_alert_ranker",
            "operator_paper_decision_sheet",
            "discord_paper_payload",
            "operator_shadow_console",
            "shadow_session_quality",
        }
    )

    freshness_label, freshness_script = scripts[17]
    steps.append(run_step(freshness_label, [CHILD_PYTHON, str(ROOT / "scripts" / freshness_script)], run_dir))

    reports, ledger_restore, status, payload = write_stage(
        exclude={
            "live_opportunity_scanner",
            "paper_signal_export",
            "paper_alert_dedupe",
            "paper_alert_ranker",
            "operator_paper_decision_sheet",
            "discord_paper_payload",
            "operator_shadow_console",
            "shadow_session_quality",
        }
    )

    steps.append(
        run_step(
            "18b_live_opportunity_scanner",
            [CHILD_PYTHON, str(ROOT / "scripts" / "fqis_live_opportunity_scanner.py")],
            run_dir,
        )
    )

    reports, ledger_restore, status, payload = write_stage(
        exclude={
            "paper_signal_export",
            "paper_alert_dedupe",
            "paper_alert_ranker",
            "operator_paper_decision_sheet",
            "discord_paper_payload",
            "operator_shadow_console",
            "shadow_session_quality",
        }
    )

    paper_label, paper_script = scripts[18]
    steps.append(run_step(paper_label, [CHILD_PYTHON, str(ROOT / "scripts" / paper_script)], run_dir))
    reports, ledger_restore, status, payload = write_stage(
        exclude={
            "paper_alert_dedupe",
            "paper_alert_ranker",
            "operator_paper_decision_sheet",
            "discord_paper_payload",
            "operator_shadow_console",
            "shadow_session_quality",
        }
    )

    dedupe_label, dedupe_script = scripts[19]
    steps.append(run_step(dedupe_label, [CHILD_PYTHON, str(ROOT / "scripts" / dedupe_script)], run_dir))
    reports, ledger_restore, status, payload = write_stage(
        exclude={
            "paper_alert_ranker",
            "operator_paper_decision_sheet",
            "discord_paper_payload",
            "operator_shadow_console",
            "shadow_session_quality",
        }
    )

    ranker_label, ranker_script = scripts[20]
    steps.append(run_step(ranker_label, [CHILD_PYTHON, str(ROOT / "scripts" / ranker_script)], run_dir))
    reports, ledger_restore, status, payload = write_stage(
        exclude={"operator_paper_decision_sheet", "discord_paper_payload", "operator_shadow_console", "shadow_session_quality"}
    )

    sheet_label, sheet_script = scripts[21]
    steps.append(run_step(sheet_label, [CHILD_PYTHON, str(ROOT / "scripts" / sheet_script)], run_dir))
    reports, ledger_restore, status, payload = write_stage(
        exclude={"discord_paper_payload", "operator_shadow_console", "shadow_session_quality"}
    )

    discord_label, discord_script = scripts[22]
    steps.append(run_step(discord_label, [CHILD_PYTHON, str(ROOT / "scripts" / discord_script)], run_dir))
    reports, ledger_restore, status, payload = write_stage(exclude={"operator_shadow_console", "shadow_session_quality"})

    operator_label, operator_script = scripts[23]
    steps.append(run_step(operator_label, [CHILD_PYTHON, str(ROOT / "scripts" / operator_script)], run_dir))
    reports, ledger_restore, status, payload = write_stage(exclude={"shadow_session_quality"})

    quality_label, quality_script = scripts[24]
    steps.append(run_step(quality_label, [CHILD_PYTHON, str(ROOT / "scripts" / quality_script)], run_dir))

    reports, ledger_restore, status, payload = write_stage()
    write_master_report(payload)

    verdict = ((reports.get("daily_audit") or {}).get("verdict") or {})
    go_no_go = reports.get("go_no_go") or {}
    shadow = reports.get("shadow_readiness") or {}
    freshness = reports.get("live_freshness") or {}
    scanner = reports.get("live_opportunity_scanner") or {}
    operator_console = reports.get("operator_shadow_console") or {}
    paper_export = reports.get("paper_signal_export") or {}
    paper_dedupe = reports.get("paper_alert_dedupe") or {}
    paper_ranker = reports.get("paper_alert_ranker") or {}
    discord_payload = reports.get("discord_paper_payload") or {}

    print(json.dumps({
        "status": status,
        "final_verdict": verdict.get("final_verdict"),
        "go_no_go_state": go_no_go.get("go_no_go_state"),
        "promotion_allowed": verdict.get("promotion_allowed"),
        "shadow_state": shadow.get("shadow_state"),
        "live_freshness_status": freshness.get("status"),
        "live_opportunity_read": scanner.get("operator_read"),
        "operator_state": operator_console.get("operator_state"),
        "paper_signals_total": paper_export.get("paper_signals_total") or paper_export.get("total_decisions"),
        "top_ranked_alert_count": paper_ranker.get("top_ranked_alert_count") or paper_ranker.get("ranked_alert_count"),
        "new_paper_alerts": paper_dedupe.get("new_alerts"),
        "sendable_discord_payload": discord_payload.get("sendable"),
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
        subprocess.run([CHILD_PYTHON, str(fixture_script)], check=False)

    inject_research_screening_diagnostics_into_latest_full_cycle_report()
    inject_fixture_level_research_into_latest_full_cycle_report()
    raise SystemExit(exit_code)

