from __future__ import annotations

import csv
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

CRITICAL_SCRIPTS = [
    "fqis_api_sports_live_decision_bridge.py",
    "fqis_level3_live_state_probe.py",
    "fqis_operator_decision_report.py",
    "fqis_research_ledger.py",
    "fqis_research_settlement.py",
    "fqis_clv_tracker.py",
    "fqis_clv_horizon_audit.py",
    "fqis_research_performance_report.py",
    "fqis_fixture_level_research_report.py",
    "fqis_provider_coverage_report.py",
    "fqis_daily_audit_report.py",
    "fqis_run_full_audit_cycle.py",
]

FULL_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.md"
FULL_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.json"
DAILY_JSON = ROOT / "data" / "pipeline" / "api_sports" / "audit" / "latest_daily_audit_report.json"
BRIDGE_JSON = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json"
LEDGER_CSV = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
FIXTURE_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_fixture_level_research_report.json"

OUT_DIR = ROOT / "data" / "pipeline" / "api_sports" / "audit"
OUT_MD = OUT_DIR / "latest_night_health_check.md"
OUT_JSON = OUT_DIR / "latest_night_health_check.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def read_text(path: Path) -> str:
    try:
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def fint(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", ".").strip()))
    except Exception:
        return default


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def pct(value: Any) -> str:
    return f"{fnum(value) * 100:.2f}%"


def compile_scripts() -> list[dict[str, Any]]:
    results = []

    for script_name in CRITICAL_SCRIPTS:
        path = ROOT / "scripts" / script_name

        if not path.exists():
            results.append({
                "script": script_name,
                "status": "MISSING",
                "returncode": None,
                "stderr": "",
            })
            continue

        proc = subprocess.run(
            [sys.executable, "-m", "py_compile", str(path)],
            capture_output=True,
            text=True,
        )

        results.append({
            "script": script_name,
            "status": "OK" if proc.returncode == 0 else "FAIL",
            "returncode": proc.returncode,
            "stderr": proc.stderr.strip(),
        })

    return results


def main() -> int:
    issues: list[dict[str, str]] = []

    def issue(severity: str, message: str) -> None:
        issues.append({"severity": severity, "message": message})

    compile_results = compile_scripts()

    for result in compile_results:
        if result["status"] != "OK":
            issue("CRITICAL", f"Script compile problem: {result['script']} => {result['status']}")

    full = read_json(FULL_JSON)
    daily = read_json(DAILY_JSON)
    bridge = read_json(BRIDGE_JSON)
    fixture = read_json(FIXTURE_JSON)
    ledger_rows = read_csv(LEDGER_CSV)
    full_md = read_text(FULL_MD)

    daily_verdict = daily.get("verdict") or {}
    daily_flags = daily_verdict.get("flags") or []
    bridge_summary = bridge.get("summary") or {}

    promotion_allowed = daily_verdict.get("promotion_allowed")

    if promotion_allowed is not False:
        issue("CRITICAL", f"Promotion is not explicitly false: {promotion_allowed}")

    official = fint(bridge_summary.get("official_decisions"))
    watchlist = fint(bridge_summary.get("watchlist_decisions"))

    if official != 0:
        issue("CRITICAL", f"Official decisions not zero: {official}")

    if watchlist != 0:
        issue("CRITICAL", f"Watchlist decisions not zero: {watchlist}")

    if "NO_PUBLISHABLE_DECISION" not in daily_flags:
        issue("WARNING", "Daily audit does not contain NO_PUBLISHABLE_DECISION flag")

    if "## Step Execution" not in full_md:
        issue("WARNING", "Full cycle report has no Step Execution section")

    step_rows = re.findall(
        r"^\|\s*(\d+_[^|]+)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|",
        full_md,
        flags=re.MULTILINE,
    )

    for step_name, step_status, return_code in step_rows:
        step_status = step_status.strip()
        return_code = return_code.strip()

        if step_status != "OK" or return_code != "0":
            issue("CRITICAL", f"Step failed: {step_name} => {step_status} / {return_code}")

    if "## Fixture-Level Research Read" not in full_md:
        issue("WARNING", "Fixture-Level Research Read is not injected in full cycle report")

    fixture_snapshot = fixture.get("snapshot") or {}
    fixture_level = fixture.get("fixture") or {}

    snapshot_settled = fint(fixture_snapshot.get("settled"))
    fixture_settled = fint(fixture_level.get("settled"))

    if fixture_settled > snapshot_settled and snapshot_settled > 0:
        issue("CRITICAL", "Fixture-level settled is greater than snapshot settled")

    if fixture_settled < 50:
        issue("INFO", f"Fixture-level sample still small: {fixture_settled}")

    if not ledger_rows:
        issue("WARNING", "Research ledger is empty")
    else:
        blank_tier = 0
        bad_events_only = 0
        bad_strict = 0
        bad_final_status = 0

        for row in ledger_rows:
            tier = str(row.get("research_data_tier") or "").strip()
            promo = str(row.get("promotion_allowed") or "").strip().lower()
            final_status = str(row.get("final_operational_status") or "").strip()

            if not tier:
                blank_tier += 1

            if tier == "EVENTS_ONLY_RESEARCH" and promo != "false":
                bad_events_only += 1

            if tier == "STRICT_EVENTS_PLUS_STATS" and promo not in {"committee_only", "false"}:
                bad_strict += 1

            if final_status and final_status != "NO_BET":
                bad_final_status += 1

        if blank_tier:
            issue("WARNING", f"Ledger rows with blank research_data_tier: {blank_tier}")

        if bad_events_only:
            issue("CRITICAL", f"EVENTS_ONLY rows not locked to promotion_allowed=false: {bad_events_only}")

        if bad_strict:
            issue("CRITICAL", f"STRICT rows with invalid promotion_allowed value: {bad_strict}")

        if bad_final_status:
            issue("CRITICAL", f"Research rows with final_operational_status not NO_BET: {bad_final_status}")

    under_rows = sum(1 for r in ledger_rows if str(r.get("side") or "").upper() == "UNDER")
    over_rows = sum(1 for r in ledger_rows if str(r.get("side") or "").upper() == "OVER")
    total_rows = len(ledger_rows)

    under_share = under_rows / total_rows if total_rows else 0.0
    over_share = over_rows / total_rows if total_rows else 0.0

    if total_rows >= 50 and under_share >= 0.70 and "RESEARCH_SIDE_BIAS_UNDER_DOMINANT" not in daily_flags:
        issue("WARNING", "UNDER side bias exists but daily flag is missing")

    critical_count = sum(1 for i in issues if i["severity"] == "CRITICAL")
    warning_count = sum(1 for i in issues if i["severity"] == "WARNING")

    status = "PASS_SLEEP_SAFE" if critical_count == 0 else "FAIL_FIX_BEFORE_SLEEP"

    payload = {
        "mode": "FQIS_NIGHT_HEALTH_CHECK",
        "generated_at_utc": utc_now(),
        "status": status,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "issues": issues,
        "key_metrics": {
            "promotion_allowed": promotion_allowed,
            "official_decisions": official,
            "watchlist_decisions": watchlist,
            "ledger_rows": total_rows,
            "under_rows": under_rows,
            "over_rows": over_rows,
            "under_share": round(under_share, 6),
            "over_share": round(over_share, 6),
            "snapshot_settled": snapshot_settled,
            "fixture_level_settled": fixture_settled,
            "daily_flags": daily_flags,
        },
        "compile_results": compile_results,
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    lines = [
        "# FQIS Night Health Check",
        "",
        "## Status",
        "",
        f"- Status: **{status}**",
        f"- Generated at UTC: `{payload['generated_at_utc']}`",
        f"- Critical issues: **{critical_count}**",
        f"- Warnings: **{warning_count}**",
        "",
        "## Key Metrics",
        "",
        f"- Promotion allowed: **{promotion_allowed}**",
        f"- Official decisions: **{official}**",
        f"- Watchlist decisions: **{watchlist}**",
        f"- Ledger rows: **{total_rows}**",
        f"- Fixture-level settled: **{fixture_settled}**",
        f"- Snapshot settled: **{snapshot_settled}**",
        f"- UNDER rows: **{under_rows}** = **{pct(under_share)}**",
        f"- OVER rows: **{over_rows}** = **{pct(over_share)}**",
        "",
        "## Issues",
        "",
    ]

    if issues:
        for item in issues:
            lines.append(f"- **{item['severity']}** — {item['message']}")
    else:
        lines.append("- None")

    lines += [
        "",
        "## Compile Results",
        "",
        "| Script | Status | Return code |",
        "|---|---|---:|",
    ]

    for result in compile_results:
        lines.append(
            f"| {result['script']} | {result['status']} | {result['returncode']} |"
        )

    lines += [
        "",
        "## Sleep Decision",
        "",
    ]

    if status == "PASS_SLEEP_SAFE":
        lines.append("Safe to stop patching. Production is protected. Continue research collection only if desired.")
    else:
        lines.append("Do not leave unattended. Fix critical issues before sleep.")

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "status": status,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "output_md": str(OUT_MD),
        "output_json": str(OUT_JSON),
    }, indent=2, ensure_ascii=False))

    return 0 if critical_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
