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
OUT_JSON = ORCH_DIR / "latest_tonight_shadow_monitor.json"
OUT_MD = ORCH_DIR / "latest_tonight_shadow_monitor.md"
VENV_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
CHILD_PYTHON = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def run_full_cycle(discord: bool) -> int:
    cmd = [CHILD_PYTHON, str(ROOT / "scripts" / "fqis_run_full_audit_cycle.py")]
    if discord:
        cmd.append("--discord")

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return proc.returncode


def build_cycle_row(cycle_number: int, returncode: int) -> dict[str, Any]:
    full_cycle = read_json(FULL_CYCLE_JSON)
    go_no_go = read_json(GO_NO_GO_JSON)
    shadow = read_json(SHADOW_JSON)

    full_cycle_status = full_cycle.get("status")
    if returncode != 0:
        full_cycle_status = "COMMAND_FAILED"

    invariants = full_cycle.get("invariants") or {}
    post_quarantine = shadow.get("post_quarantine") or {}

    return {
        "cycle": cycle_number,
        "timestamp": utc_now(),
        "full_cycle_returncode": returncode,
        "full_cycle_status": full_cycle_status,
        "go_no_go_state": go_no_go.get("go_no_go_state"),
        "shadow_state": shadow.get("shadow_state"),
        "promotion_allowed": go_no_go.get("promotion_allowed"),
        "live_staking_allowed": go_no_go.get("live_staking_allowed"),
        "can_execute_real_bets": shadow.get("can_execute_real_bets"),
        "can_enable_live_staking": shadow.get("can_enable_live_staking"),
        "ledger_preserved": invariants.get("research_candidates_ledger_preserved"),
        "post_quarantine_pnl": fnum(post_quarantine.get("pnl")),
        "post_quarantine_roi": fnum(post_quarantine.get("roi")),
        "run_dir": full_cycle.get("run_dir"),
    }


def stop_reason(row: dict[str, Any]) -> str:
    checks = [
        ("FULL_CYCLE_NOT_READY", row.get("full_cycle_status") != "READY"),
        ("GO_NO_GO_LIVE_READY", row.get("go_no_go_state") == "LIVE_READY"),
        ("SHADOW_NOT_READY", row.get("shadow_state") != "SHADOW_READY"),
        ("PROMOTION_ALLOWED_TRUE", row.get("promotion_allowed") is True),
        ("LIVE_STAKING_ALLOWED_TRUE", row.get("live_staking_allowed") is True),
        ("CAN_EXECUTE_REAL_BETS_TRUE", row.get("can_execute_real_bets") is True),
        ("CAN_ENABLE_LIVE_STAKING_TRUE", row.get("can_enable_live_staking") is True),
        ("LEDGER_NOT_PRESERVED", row.get("ledger_preserved") is not True),
    ]
    reasons = [reason for reason, failed in checks if failed]
    return "; ".join(reasons)


def write_markdown(payload: dict[str, Any]) -> None:
    lines = [
        "# FQIS Tonight Shadow Monitor",
        "",
        f"- Status: **{payload['status']}**",
        f"- Cycles requested: **{payload['cycles_requested']}**",
        f"- Cycles completed: **{payload['cycles_completed']}**",
        f"- Discord enabled: **{payload['discord_enabled']}**",
        f"- Stopped reason: **{payload['stopped_reason'] or 'NONE'}**",
        "",
        "| Cycle | Timestamp | Full cycle | Go/No-Go | Shadow | Promotion | Live staking | Real bets | Enable staking | Ledger | PnL | ROI |",
        "|---:|---|---|---|---|---|---|---|---|---|---:|---:|",
    ]

    for row in payload["rows"]:
        lines.append(
            "| {cycle} | {timestamp} | {full_cycle_status} | {go_no_go_state} | {shadow_state} | {promotion_allowed} | {live_staking_allowed} | {can_execute_real_bets} | {can_enable_live_staking} | {ledger_preserved} | {post_quarantine_pnl} | {post_quarantine_roi} |".format(
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
    args = parser.parse_args()

    cycles_requested = max(0, args.cycles)
    rows: list[dict[str, Any]] = []
    status = "READY"
    stopped_reason = ""

    payload = {
        "status": status,
        "generated_at_utc": utc_now(),
        "cycles_requested": cycles_requested,
        "cycles_completed": 0,
        "discord_enabled": bool(args.discord),
        "stopped_reason": stopped_reason,
        "rows": rows,
    }
    write_outputs(payload)

    for index in range(cycles_requested):
        returncode = run_full_cycle(discord=bool(args.discord))
        row = build_cycle_row(cycle_number=index + 1, returncode=returncode)
        rows.append(row)

        reason = stop_reason(row)
        if reason:
            status = "STOPPED"
            stopped_reason = reason

        payload = {
            "status": status,
            "generated_at_utc": utc_now(),
            "cycles_requested": cycles_requested,
            "cycles_completed": len(rows),
            "discord_enabled": bool(args.discord),
            "stopped_reason": stopped_reason,
            "rows": rows,
        }
        write_outputs(payload)

        if status == "STOPPED" and args.stop_on_blocked:
            break
        if index < cycles_requested - 1:
            time.sleep(max(0.0, args.sleep_seconds))

    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0 if status == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
