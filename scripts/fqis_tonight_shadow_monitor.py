from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
FULL_CYCLE_JSON = ORCH_DIR / "latest_full_cycle_report.json"
GO_NO_GO_JSON = ORCH_DIR / "latest_go_no_go_report.json"
SHADOW_JSON = ORCH_DIR / "latest_shadow_readiness_report.json"
LIVE_FRESHNESS_JSON = ORCH_DIR / "latest_live_freshness_report.json"
PAPER_SIGNAL_EXPORT_JSON = ORCH_DIR / "latest_paper_signal_export.json"
PAPER_ALERT_DEDUPE_JSON = ORCH_DIR / "latest_paper_alert_dedupe.json"
PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
OPERATOR_PAPER_DECISION_SHEET_JSON = ORCH_DIR / "latest_operator_paper_decision_sheet.json"
DISCORD_PAPER_PAYLOAD_JSON = ORCH_DIR / "latest_discord_paper_payload.json"
OPERATOR_CONSOLE_JSON = ORCH_DIR / "latest_operator_shadow_console.json"
OPERATOR_CONSOLE_SCRIPT = ROOT / "scripts" / "fqis_operator_shadow_console.py"
OUT_JSON = ORCH_DIR / "latest_tonight_shadow_monitor.json"
OUT_MD = ORCH_DIR / "latest_tonight_shadow_monitor.md"
VENV_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
CHILD_PYTHON = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(value: Any) -> datetime | None:
    try:
        if not value:
            return None
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def fnum(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


def tail_text(text: str, line_count: int) -> str:
    if line_count <= 0 or not text:
        return ""
    return "\n".join(text.splitlines()[-line_count:])


def monitor_run_id() -> str:
    return datetime.now(timezone.utc).strftime("monitor_run_%Y%m%d_%H%M%S")


def run_full_cycle(
    *,
    discord: bool,
    cycle_number: int,
    monitor_run_dir: Path,
    child_log_mode: str,
    tail_lines: int,
) -> dict[str, Any]:
    cmd = [CHILD_PYTHON, str(ROOT / "scripts" / "fqis_run_full_audit_cycle.py")]
    if discord:
        cmd.append("--discord")

    monitor_run_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = monitor_run_dir / f"cycle_{cycle_number:03d}.stdout.log"
    stderr_path = monitor_run_dir / f"cycle_{cycle_number:03d}.stderr.log"

    started = time.monotonic()
    stdout_text = ""
    stderr_text = ""

    if child_log_mode == "capture":
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        stdout_text = proc.stdout or ""
        stderr_text = proc.stderr or ""
    elif child_log_mode == "none":
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    else:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        stdout_text = "Child stdout inherited by monitor terminal; not captured.\n"
        stderr_text = "Child stderr inherited by monitor terminal; not captured.\n"

    duration = round(time.monotonic() - started, 3)
    stdout_path.write_text(stdout_text, encoding="utf-8")
    stderr_path.write_text(stderr_text, encoding="utf-8")

    result: dict[str, Any] = {
        "returncode": proc.returncode,
        "stdout_log": str(stdout_path),
        "stderr_log": str(stderr_path),
        "full_cycle_duration_sec": duration,
    }
    if tail_lines > 0:
        result["stdout_tail"] = tail_text(stdout_text, tail_lines)
        result["stderr_tail"] = tail_text(stderr_text, tail_lines)
    return result


def refresh_operator_console_after_monitor_write() -> None:
    if not OPERATOR_CONSOLE_SCRIPT.exists():
        return
    subprocess.run(
        [CHILD_PYTHON, str(OPERATOR_CONSOLE_SCRIPT)],
        cwd=str(ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def build_cycle_row(cycle_number: int, run_result: dict[str, Any]) -> dict[str, Any]:
    full_cycle = read_json(FULL_CYCLE_JSON)
    go_no_go = read_json(GO_NO_GO_JSON)
    shadow = read_json(SHADOW_JSON)
    live_freshness = read_json(LIVE_FRESHNESS_JSON)
    paper_signal_export = read_json(PAPER_SIGNAL_EXPORT_JSON)
    paper_alert_dedupe = read_json(PAPER_ALERT_DEDUPE_JSON)
    paper_alert_ranker = read_json(PAPER_ALERT_RANKER_JSON)
    decision_sheet = read_json(OPERATOR_PAPER_DECISION_SHEET_JSON)
    discord_payload = read_json(DISCORD_PAPER_PAYLOAD_JSON)
    operator_console = read_json(OPERATOR_CONSOLE_JSON)

    full_cycle_status = full_cycle.get("status")
    returncode = int(run_result.get("returncode") or 0)
    if returncode != 0:
        full_cycle_status = "COMMAND_FAILED"

    invariants = full_cycle.get("invariants") or {}
    post_quarantine = shadow.get("post_quarantine") or {}

    row = {
        "cycle": cycle_number,
        "timestamp": utc_now(),
        "full_cycle_returncode": returncode,
        "full_cycle_status": full_cycle_status,
        "go_no_go_state": go_no_go.get("go_no_go_state"),
        "shadow_state": shadow.get("shadow_state"),
        "operator_state": operator_console.get("operator_state"),
        "promotion_allowed": go_no_go.get("promotion_allowed"),
        "live_staking_allowed": go_no_go.get("live_staking_allowed"),
        "can_execute_real_bets": shadow.get("can_execute_real_bets"),
        "can_enable_live_staking": shadow.get("can_enable_live_staking"),
        "ledger_preserved": invariants.get("research_candidates_ledger_preserved"),
        "post_quarantine_pnl": fnum(post_quarantine.get("pnl")),
        "post_quarantine_roi": fnum(post_quarantine.get("roi")),
        "run_dir": full_cycle.get("run_dir"),
        "stdout_log": run_result.get("stdout_log"),
        "stderr_log": run_result.get("stderr_log"),
        "full_cycle_duration_sec": run_result.get("full_cycle_duration_sec"),
        "live_freshness_status": live_freshness.get("status"),
        "candidates_this_cycle": live_freshness.get("candidates_this_cycle"),
        "new_snapshots_appended": live_freshness.get("new_snapshots_appended"),
        "decisions_total": live_freshness.get("decisions_total"),
        "freshness_flags": live_freshness.get("freshness_flags") or [],
        "paper_signal_export_status": paper_signal_export.get("status"),
        "paper_alert_dedupe_status": paper_alert_dedupe.get("status"),
        "paper_alert_ranker_status": paper_alert_ranker.get("status"),
        "operator_paper_decision_sheet_status": decision_sheet.get("status"),
        "discord_paper_payload_status": discord_payload.get("status"),
        "paper_signals_total": paper_signal_export.get("paper_signals_total")
        or paper_signal_export.get("total_decisions"),
        "ranked_alert_count": paper_alert_ranker.get("ranked_alert_count"),
        "raw_ranked_alert_count": paper_alert_ranker.get("raw_ranked_alert_count")
        or paper_alert_ranker.get("ranked_alert_count"),
        "grouped_ranked_alert_count": paper_alert_ranker.get("grouped_ranked_alert_count")
        or paper_alert_ranker.get("top_ranked_alert_count"),
        "top_ranked_alert_count": paper_alert_ranker.get("top_ranked_alert_count"),
        "new_paper_alerts": paper_alert_dedupe.get("new_alerts"),
        "raw_new_paper_alerts": paper_alert_dedupe.get("raw_new_alerts") or paper_alert_dedupe.get("new_alerts"),
        "new_canonical_alerts": paper_alert_dedupe.get("new_canonical_alerts") or 0,
        "updated_canonical_alerts": paper_alert_dedupe.get("updated_canonical_alerts") or 0,
        "repeated_canonical_alerts": paper_alert_dedupe.get("repeated_canonical_alerts") or 0,
        "material_updates": paper_alert_dedupe.get("material_updates") or 0,
        "repeated_paper_alerts": paper_alert_dedupe.get("repeated_alerts"),
        "sendable_discord_payload": discord_payload.get("sendable") is True,
    }
    if "stdout_tail" in run_result:
        row["stdout_tail"] = run_result.get("stdout_tail")
    if "stderr_tail" in run_result:
        row["stderr_tail"] = run_result.get("stderr_tail")
    return row


def stop_reason(row: dict[str, Any]) -> str:
    checks = [
        ("FULL_CYCLE_NOT_READY", row.get("full_cycle_status") != "READY"),
        ("GO_NO_GO_LIVE_READY", row.get("go_no_go_state") == "LIVE_READY"),
        ("SHADOW_NOT_READY", row.get("shadow_state") != "SHADOW_READY"),
        ("OPERATOR_STATE_BLOCKED", row.get("operator_state") == "PAPER_BLOCKED"),
        ("PAPER_SIGNAL_EXPORT_BLOCKED", row.get("paper_signal_export_status") == "BLOCKED"),
        ("PAPER_ALERT_DEDUPE_BLOCKED", row.get("paper_alert_dedupe_status") == "BLOCKED"),
        ("PAPER_ALERT_RANKER_BLOCKED", row.get("paper_alert_ranker_status") == "BLOCKED"),
        ("OPERATOR_PAPER_DECISION_SHEET_BLOCKED", row.get("operator_paper_decision_sheet_status") == "BLOCKED"),
        ("DISCORD_PAYLOAD_UNSAFE", row.get("discord_paper_payload_status") == "BLOCKED"),
        ("PROMOTION_ALLOWED_TRUE", row.get("promotion_allowed") is True),
        ("LIVE_STAKING_ALLOWED_TRUE", row.get("live_staking_allowed") is True),
        ("CAN_EXECUTE_REAL_BETS_TRUE", row.get("can_execute_real_bets") is True),
        ("CAN_ENABLE_LIVE_STAKING_TRUE", row.get("can_enable_live_staking") is True),
        ("LEDGER_NOT_PRESERVED", row.get("ledger_preserved") is not True),
    ]
    reasons = [reason for reason, failed in checks if failed]
    return "; ".join(reasons)


def numeric_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = fnum(row.get(key))
        if value is not None:
            values.append(value)
    return values


def unique_values(rows: list[dict[str, Any]], key: str) -> list[str]:
    return sorted(
        {
            str(row.get(key))
            for row in rows
            if row.get(key) is not None and row.get(key) != ""
        }
    )


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    first_timestamp = rows[0].get("timestamp") if rows else None
    last_timestamp = rows[-1].get("timestamp") if rows else None
    first_dt = parse_timestamp(first_timestamp)
    last_dt = parse_timestamp(last_timestamp)
    duration_minutes = None
    if first_dt is not None and last_dt is not None:
        duration_minutes = round(max(0.0, (last_dt - first_dt).total_seconds() / 60.0), 6)

    pnl_values = numeric_values(rows, "post_quarantine_pnl")
    roi_values = numeric_values(rows, "post_quarantine_roi")
    candidate_values = numeric_values(rows, "candidates_this_cycle")
    paper_signal_values = numeric_values(rows, "paper_signals_total")
    ranked_alert_values = numeric_values(rows, "ranked_alert_count")
    raw_ranked_alert_values = numeric_values(rows, "raw_ranked_alert_count")
    grouped_ranked_alert_values = numeric_values(rows, "grouped_ranked_alert_count")

    return {
        "first_timestamp": first_timestamp,
        "last_timestamp": last_timestamp,
        "duration_minutes": duration_minutes,
        "ready_cycles": sum(1 for row in rows if row.get("full_cycle_status") == "READY"),
        "stopped_cycles": sum(1 for row in rows if stop_reason(row)),
        "shadow_ready_cycles": sum(1 for row in rows if row.get("shadow_state") == "SHADOW_READY"),
        "unique_go_no_go_states": unique_values(rows, "go_no_go_state"),
        "unique_shadow_states": unique_values(rows, "shadow_state"),
        "unique_operator_states": unique_values(rows, "operator_state"),
        "min_post_quarantine_pnl": min(pnl_values) if pnl_values else None,
        "max_post_quarantine_pnl": max(pnl_values) if pnl_values else None,
        "min_post_quarantine_roi": min(roi_values) if roi_values else None,
        "max_post_quarantine_roi": max(roi_values) if roi_values else None,
        "min_paper_signals_total": min(paper_signal_values) if paper_signal_values else None,
        "max_paper_signals_total": max(paper_signal_values) if paper_signal_values else None,
        "min_ranked_alert_count": min(ranked_alert_values) if ranked_alert_values else None,
        "max_ranked_alert_count": max(ranked_alert_values) if ranked_alert_values else None,
        "min_raw_ranked_alert_count": min(raw_ranked_alert_values) if raw_ranked_alert_values else None,
        "max_raw_ranked_alert_count": max(raw_ranked_alert_values) if raw_ranked_alert_values else None,
        "min_grouped_ranked_alert_count": min(grouped_ranked_alert_values) if grouped_ranked_alert_values else None,
        "max_grouped_ranked_alert_count": max(grouped_ranked_alert_values) if grouped_ranked_alert_values else None,
        "total_new_paper_alerts": sum(int(row.get("new_paper_alerts") or 0) for row in rows),
        "total_raw_new_paper_alerts": sum(int(row.get("raw_new_paper_alerts") or row.get("new_paper_alerts") or 0) for row in rows),
        "total_canonical_new_alerts": sum(int(row.get("new_canonical_alerts") or 0) for row in rows),
        "total_material_updates": sum(int(row.get("material_updates") or 0) for row in rows),
        "total_repeated_paper_alerts": sum(int(row.get("repeated_paper_alerts") or 0) for row in rows),
        "any_sendable_discord_payload": any(row.get("sendable_discord_payload") is True for row in rows),
        "all_ledger_preserved": bool(rows) and all(row.get("ledger_preserved") is True for row in rows),
        "any_real_bets_enabled": any(row.get("can_execute_real_bets") is True for row in rows),
        "any_live_staking_enabled": any(
            row.get("live_staking_allowed") is True or row.get("can_enable_live_staking") is True
            for row in rows
        ),
        "any_promotion_allowed": any(row.get("promotion_allowed") is True for row in rows),
        "total_new_snapshots_appended": sum(int(row.get("new_snapshots_appended") or 0) for row in rows),
        "min_candidates_this_cycle": min(candidate_values) if candidate_values else None,
        "max_candidates_this_cycle": max(candidate_values) if candidate_values else None,
        "unique_live_freshness_statuses": unique_values(rows, "live_freshness_status"),
        "all_live_freshness_ready_or_review": bool(rows)
        and all(row.get("live_freshness_status") in {"READY", "STALE_REVIEW"} for row in rows),
    }


def build_payload(
    *,
    status: str,
    cycles_requested: int,
    rows: list[dict[str, Any]],
    discord_enabled: bool,
    stopped_reason: str,
    monitor_run_id: str,
    monitor_run_dir: Path,
    quiet: bool,
    child_log_mode: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "generated_at_utc": utc_now(),
        "monitor_run_id": monitor_run_id,
        "monitor_run_dir": str(monitor_run_dir),
        "cycles_requested": cycles_requested,
        "cycles_completed": len(rows),
        "discord_enabled": discord_enabled,
        "quiet": quiet,
        "child_log_mode": child_log_mode,
        "stopped_reason": stopped_reason,
        "summary": build_summary(rows),
        "rows": rows,
    }


def markdown_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else "[]"
    return str(value)


def write_markdown(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    summary_fields = [
        "first_timestamp",
        "last_timestamp",
        "duration_minutes",
        "ready_cycles",
        "stopped_cycles",
        "shadow_ready_cycles",
        "unique_go_no_go_states",
        "unique_shadow_states",
        "unique_operator_states",
        "min_post_quarantine_pnl",
        "max_post_quarantine_pnl",
        "min_post_quarantine_roi",
        "max_post_quarantine_roi",
        "min_paper_signals_total",
        "max_paper_signals_total",
        "min_ranked_alert_count",
        "max_ranked_alert_count",
        "min_raw_ranked_alert_count",
        "max_raw_ranked_alert_count",
        "min_grouped_ranked_alert_count",
        "max_grouped_ranked_alert_count",
        "total_new_paper_alerts",
        "total_raw_new_paper_alerts",
        "total_canonical_new_alerts",
        "total_material_updates",
        "total_repeated_paper_alerts",
        "any_sendable_discord_payload",
        "all_ledger_preserved",
        "any_real_bets_enabled",
        "any_live_staking_enabled",
        "any_promotion_allowed",
        "total_new_snapshots_appended",
        "min_candidates_this_cycle",
        "max_candidates_this_cycle",
        "unique_live_freshness_statuses",
        "all_live_freshness_ready_or_review",
    ]

    lines = [
        "# FQIS Tonight Shadow Monitor",
        "",
        f"- Status: **{payload['status']}**",
        f"- Monitor run ID: **{payload['monitor_run_id']}**",
        f"- Monitor run dir: `{payload['monitor_run_dir']}`",
        f"- Cycles requested: **{payload['cycles_requested']}**",
        f"- Cycles completed: **{payload['cycles_completed']}**",
        f"- Discord enabled: **{payload['discord_enabled']}**",
        f"- Quiet: **{payload['quiet']}**",
        f"- Child log mode: **{payload['child_log_mode']}**",
        f"- Stopped reason: **{payload['stopped_reason'] or 'NONE'}**",
        "",
        "## Summary",
        "",
        *[f"- {field}: **{markdown_value(summary.get(field))}**" for field in summary_fields],
        "",
        "## Cycles",
        "",
        "| Cycle | Timestamp | Full cycle | Go/No-Go | Shadow | Operator | Freshness | Decisions | Candidates | New snapshots | Paper signals | Ranked alerts | New alerts | Repeats | Discord sendable | Promotion | Live staking | Real bets | Enable staking | Ledger | PnL | ROI | Duration sec |",
        "|---:|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|---|---:|---:|---:|",
    ]

    for row in payload["rows"]:
        lines.append(
            "| {cycle} | {timestamp} | {full_cycle_status} | {go_no_go_state} | {shadow_state} | {operator_state} | {live_freshness_status} | {decisions_total} | {candidates_this_cycle} | {new_snapshots_appended} | {paper_signals_total} | {ranked_alert_count} | {new_paper_alerts} | {repeated_paper_alerts} | {sendable_discord_payload} | {promotion_allowed} | {live_staking_allowed} | {can_execute_real_bets} | {can_enable_live_staking} | {ledger_preserved} | {post_quarantine_pnl} | {post_quarantine_roi} | {full_cycle_duration_sec} |".format(
                **row
            )
        )

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_outputs(payload: dict[str, Any]) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    write_markdown(payload)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=60)
    parser.add_argument("--discord", action="store_true")
    parser.add_argument("--stop-on-blocked", action="store_true", default=True)
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--child-log-mode", choices=["inherit", "capture", "none"], default="inherit")
    parser.add_argument("--tail-lines", type=int, default=0)
    args = parser.parse_args()

    cycles_requested = max(0, args.cycles)
    tail_lines = max(0, args.tail_lines)
    child_log_mode = args.child_log_mode
    if args.quiet and child_log_mode == "inherit":
        child_log_mode = "capture"
    run_id = monitor_run_id()
    run_dir = ORCH_DIR / run_id
    rows: list[dict[str, Any]] = []
    status = "READY"
    stopped_reason = ""

    payload = build_payload(
        status=status,
        cycles_requested=cycles_requested,
        rows=rows,
        discord_enabled=bool(args.discord),
        stopped_reason=stopped_reason,
        monitor_run_id=run_id,
        monitor_run_dir=run_dir,
        quiet=bool(args.quiet),
        child_log_mode=child_log_mode,
    )

    try:
        write_outputs(payload)

        for index in range(cycles_requested):
            run_result = run_full_cycle(
                discord=bool(args.discord),
                cycle_number=index + 1,
                monitor_run_dir=run_dir,
                child_log_mode=child_log_mode,
                tail_lines=tail_lines,
            )
            row = build_cycle_row(cycle_number=index + 1, run_result=run_result)
            rows.append(row)

            reason = stop_reason(row)
            if reason:
                status = "STOPPED"
                stopped_reason = reason

            payload = build_payload(
                status=status,
                cycles_requested=cycles_requested,
                rows=rows,
                discord_enabled=bool(args.discord),
                stopped_reason=stopped_reason,
                monitor_run_id=run_id,
                monitor_run_dir=run_dir,
                quiet=bool(args.quiet),
                child_log_mode=child_log_mode,
            )
            write_outputs(payload)

            if status == "STOPPED" and args.stop_on_blocked:
                break
            if index < cycles_requested - 1:
                time.sleep(max(0.0, args.sleep_seconds))
    except KeyboardInterrupt:
        if status != "STOPPED":
            status = "MANUALLY_INTERRUPTED"
            stopped_reason = "KEYBOARD_INTERRUPT"
        payload = build_payload(
            status=status,
            cycles_requested=cycles_requested,
            rows=rows,
            discord_enabled=bool(args.discord),
            stopped_reason=stopped_reason,
            monitor_run_id=run_id,
            monitor_run_dir=run_dir,
            quiet=bool(args.quiet),
            child_log_mode=child_log_mode,
        )
        write_outputs(payload)
        if args.quiet:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        return 130 if status == "MANUALLY_INTERRUPTED" else 2

    refresh_operator_console_after_monitor_write()

    if args.quiet:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0 if status == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
