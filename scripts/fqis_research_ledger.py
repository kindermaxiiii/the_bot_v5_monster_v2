from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json"
OUT_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"
LEDGER_CSV = OUT_DIR / "research_candidates_ledger.csv"
LATEST_JSON = OUT_DIR / "latest_research_candidates.json"
LATEST_MD = OUT_DIR / "latest_research_candidates.md"


FIELDS = [
    "snapshot_key",
    "signal_key",
    "observed_at_utc",
    "source_cycle_dir",
    "fixture_id",
    "match",
    "score",
    "minute",
    "market_key",
    "side",
    "line",
    "selection",
    "odds_decimal",
    "raw_probability",
    "calibrated_probability",
    "market_no_vig_probability",
    "edge",
    "expected_value",
    "l3_data_mode",
    "l3_state_ready",
    "l3_trade_ready",
    "pre_external_veto_real_status",
    "final_operational_status",
    "primary_veto",
    "primary_blocker_family",
    "vetoes",
    "research_bucket",
    "research_data_tier",
    "research_status",
    "paper_only",
    "promotion_allowed",
    "settlement_status",
    "closing_odds",
    "clv_decimal",
    "clv_status",
    "near_close_observed_at_utc",
    "near_close_source_cycle_dir",
    "result_status",
    "pnl_unit",
    "fixture_status_short",
    "fixture_status_long",
    "fixture_elapsed",
    "final_home_goals",
    "final_away_goals",
    "final_total_goals",
    "provisional_result_if_now",
    "provisional_pnl_if_now",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def load_payload() -> dict[str, Any]:
    return json.loads(SOURCE.read_text(encoding="utf-8"))


def sha256_key(parts: list[Any]) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def is_market_research_veto(veto: str) -> bool:
    v = str(veto or "")
    return (
        "under_0_5" in v
        or "under_1_5" in v
        or "under_2_5" in v
        or "under_one_goal" in v
        or "under_two_goal" in v
        or "under_goal_budget" in v
        or "red_card_doc_ban" in v
        or "red_card_real_ban" in v
    )


def has_market_research_veto(decision: dict[str, Any]) -> bool:
    return any(is_market_research_veto(str(v)) for v in decision.get("vetoes") or [])


def has_negative_value_veto(decision: dict[str, Any]) -> bool:
    return any(str(v) == "non_positive_edge" for v in decision.get("vetoes") or [])


def final_operational_status(decision: dict[str, Any]) -> str:
    if decision.get("vetoes"):
        return "NO_BET"
    return str(decision.get("real_status") or "NO_BET")


def research_data_tier(decision: dict[str, Any]) -> str:
    p = decision.get("payload") or {}
    mode = str(p.get("level3_data_mode") or "")

    if p.get("level3_trade_ready") is True and mode == "EVENTS_PLUS_STATS":
        return "STRICT_EVENTS_PLUS_STATS"

    if p.get("level3_state_ready") is True and mode == "EVENTS_ONLY":
        return "EVENTS_ONLY_RESEARCH"

    return "REJECTED_DATA_TIER"


def research_bucket(decision: dict[str, Any]) -> str:
    p = decision.get("payload") or {}
    data_tier = research_data_tier(decision)
    vetoes = [str(v) for v in decision.get("vetoes") or []]
    selection = str(decision.get("selection") or "").upper()

    if data_tier == "EVENTS_ONLY_RESEARCH":
        prefix = "EVENTS_ONLY"
    else:
        prefix = "STRICT"

    if "UNDER 0.5" in selection or any("under_0_5" in v for v in vetoes):
        return f"{prefix}_UNDER_0_5_RESEARCH"

    if "UNDER 1.5" in selection or any("under_1_5" in v for v in vetoes):
        return f"{prefix}_UNDER_1_5_RESEARCH"

    if "UNDER 2.5" in selection or any("under_2_5" in v for v in vetoes):
        return f"{prefix}_UNDER_2_5_RESEARCH"

    if "UNDER" in selection:
        return f"{prefix}_UNDER_GENERAL_RESEARCH"

    if "OVER" in selection:
        return f"{prefix}_OVER_RESEARCH"

    return f"{prefix}_MARKET_RESEARCH"



def passes_research_timing_policy(decision: dict[str, Any]) -> bool:
    """
    Institutional timing guard.
    Prevents very-early live Under candidates from entering the research ledger,
    even if the model sees apparent EV.

    Rationale:
    - Minute 0-10 Under edges are often unstable or mechanical.
    - Low Under lines need much more match evidence.
    - Research is aggressive, but not blind.
    """
    minute = fnum(decision.get("minute"), 0.0)
    side = str(decision.get("side") or "").upper()
    line = fnum(decision.get("line"), 0.0)

    if minute <= 0:
        return False

    if side == "UNDER":
        if minute < 8:
            return False

        if line <= 0.5 and minute < 55:
            return False

        if line <= 1.5 and minute < 45:
            return False

        if line <= 2.5 and minute < 20:
            return False

    if side == "OVER":
        if minute < 8:
            return False

    return True


def is_research_candidate(decision: dict[str, Any]) -> bool:
    p = decision.get("payload") or {}
    mode = str(p.get("level3_data_mode") or "")

    if final_operational_status(decision) != "NO_BET":
        return False

    if fnum(decision.get("edge")) <= 0:
        return False

    if fnum(decision.get("expected_value")) <= 0:
        return False

    if not passes_research_timing_policy(decision):
        return False

    if has_negative_value_veto(decision):
        return False

    data_tier = research_data_tier(decision)

    if data_tier == "STRICT_EVENTS_PLUS_STATS":
        return True

    if data_tier == "EVENTS_ONLY_RESEARCH":
        return True

    if mode == "SCORE_ONLY":
        return False

    return False



def ensure_ledger_schema(path: Path, required_fields: list[str]) -> dict[str, Any]:
    """
    Defensive CSV schema migration.

    If the ledger already exists with an old header, rewrite it with the current
    institutional schema while preserving existing rows and unknown extra columns.
    This prevents silent empty fields after adding new columns.
    """
    if not path.exists():
        return {
            "schema_migrated": False,
            "schema_backup": "",
            "schema_missing_fields": [],
            "schema_rows_rewritten": 0,
        }

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            original_fields = list(reader.fieldnames or [])
            rows = list(reader)
    except Exception:
        return {
            "schema_migrated": False,
            "schema_backup": "",
            "schema_missing_fields": ["READ_ERROR"],
            "schema_rows_rewritten": 0,
        }

    missing = [field for field in required_fields if field not in original_fields]

    if not missing:
        return {
            "schema_migrated": False,
            "schema_backup": "",
            "schema_missing_fields": [],
            "schema_rows_rewritten": len(rows),
        }

    extra_fields = [field for field in original_fields if field not in required_fields]
    final_fields = list(required_fields) + extra_fields

    backup = path.with_name(
        f"{path.stem}.schema_backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}{path.suffix}"
    )
    backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=final_fields)
        writer.writeheader()

        for row in rows:
            writer.writerow({field: row.get(field, "") for field in final_fields})

    return {
        "schema_migrated": True,
        "schema_backup": str(backup),
        "schema_missing_fields": missing,
        "schema_rows_rewritten": len(rows),
    }


def load_seen_snapshot_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()

    seen: set[str] = set()

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                key = row.get("snapshot_key")
                if key:
                    seen.add(key)
    except Exception:
        return set()

    return seen


def decision_to_row(decision: dict[str, Any], payload_root: dict[str, Any]) -> dict[str, Any]:
    p = decision.get("payload") or {}
    vetoes = [str(v) for v in decision.get("vetoes") or []]

    fixture_id = decision.get("fixture_id")
    market_key = decision.get("market_key")
    side = decision.get("side")
    line = decision.get("line")

    signal_key = sha256_key([
        fixture_id,
        market_key,
        side,
        line,
        decision.get("selection"),
    ])

    snapshot_key = sha256_key([
        fixture_id,
        decision.get("match"),
        decision.get("score"),
        decision.get("minute"),
        market_key,
        side,
        line,
        decision.get("selection"),
        round(fnum(decision.get("odds_decimal")), 4),
        round(fnum(decision.get("calibrated_probability")), 6),
        round(fnum(decision.get("market_no_vig_probability")), 6),
        round(fnum(decision.get("edge")), 6),
        round(fnum(decision.get("expected_value")), 6),
    ])

    primary_veto = str(p.get("primary_veto") or (vetoes[0] if vetoes else ""))
    data_tier = research_data_tier(decision)

    return {
        "snapshot_key": snapshot_key,
        "signal_key": signal_key,
        "observed_at_utc": payload_root.get("generated_at_utc") or utc_now(),
        "source_cycle_dir": payload_root.get("cycle_dir") or "",
        "fixture_id": fixture_id,
        "match": decision.get("match"),
        "score": decision.get("score"),
        "minute": decision.get("minute"),
        "market_key": market_key,
        "side": side,
        "line": line,
        "selection": decision.get("selection"),
        "odds_decimal": decision.get("odds_decimal"),
        "raw_probability": decision.get("raw_probability"),
        "calibrated_probability": decision.get("calibrated_probability"),
        "market_no_vig_probability": decision.get("market_no_vig_probability"),
        "edge": decision.get("edge"),
        "expected_value": decision.get("expected_value"),
        "l3_data_mode": p.get("level3_data_mode"),
        "l3_state_ready": p.get("level3_state_ready"),
        "l3_trade_ready": p.get("level3_trade_ready"),
        "pre_external_veto_real_status": p.get("pre_external_veto_real_status", decision.get("real_status")),
        "final_operational_status": final_operational_status(decision),
        "primary_veto": primary_veto,
        "primary_blocker_family": p.get("primary_blocker_family") or "",
        "vetoes": ",".join(vetoes),
        "research_bucket": research_bucket(decision),
        "research_data_tier": data_tier,
        "research_status": "OPEN",
        "paper_only": "true",
        "promotion_allowed": "false" if data_tier == "EVENTS_ONLY_RESEARCH" else "committee_only",
        "settlement_status": "UNSETTLED",
        "closing_odds": "",
        "clv_decimal": "",
        "clv_status": "",
        "near_close_observed_at_utc": "",
        "near_close_source_cycle_dir": "",
        "result_status": "",
        "pnl_unit": "",
        "fixture_status_short": "",
        "fixture_status_long": "",
        "fixture_elapsed": "",
        "final_home_goals": "",
        "final_away_goals": "",
        "final_total_goals": "",
        "provisional_result_if_now": "",
        "provisional_pnl_if_now": "",
    }


def append_rows(path: Path, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)

    seen = load_seen_snapshot_keys(path)
    new_rows = [r for r in rows if r["snapshot_key"] not in seen]

    if not new_rows:
        return 0

    exists = path.exists()

    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)

        if not exists:
            writer.writeheader()

        for row in new_rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})

    return len(new_rows)


def write_latest_markdown(path: Path, rows: list[dict[str, Any]], appended: int, total_seen: int, diagnostics: dict[str, Any] | None = None) -> None:
    diagnostics = diagnostics or {}
    strict_count = sum(1 for r in rows if r["research_data_tier"] == "STRICT_EVENTS_PLUS_STATS")
    events_only_count = sum(1 for r in rows if r["research_data_tier"] == "EVENTS_ONLY_RESEARCH")

    lines = [
        "# FQIS Research Ledger",
        "",
        "## Summary",
        "",
        f"- Candidates this cycle: **{len(rows)}**",
        f"- Strict events+stats candidates: **{strict_count}**",
        f"- Events-only research candidates: **{events_only_count}**",
        f"- New snapshots appended: **{appended}**",
        f"- Existing snapshots before append: **{total_seen}**",
        f"- Decisions screened: **{diagnostics.get('decisions_screened', 0)}**",
        f"- Rejected by timing policy: **{diagnostics.get('timing_policy_rejected', 0)}**",
        f"- Rejected by data tier: **{diagnostics.get('data_tier_rejected', 0)}**",
        f"- Rejected by non-positive edge/EV: **{diagnostics.get('non_positive_edge_or_ev_rejected', 0)}**",
        f"- Rejected by final status: **{diagnostics.get('final_status_rejected', 0)}**",
        f"- Rejected by negative-value veto: **{diagnostics.get('negative_value_veto_rejected', 0)}**",
        "",
        "> Research only. No live staking. No Discord production publication.",
        "",
        "## Candidates",
        "",
    ]

    if not rows:
        lines.append("No research candidate this cycle.")
    else:
        lines.append("| Tier | Bucket | Match | Score | Min | Selection | Odds | Model | Market | Edge | EV | Final | Vetoes |")
        lines.append("|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|---|---|")

        for r in rows[:80]:
            lines.append(
                "| {tier} | {bucket} | {match} | {score} | {minute} | {selection} | {odds:.3f} | {model:.2f}% | {market:.2f}% | {edge:.2f}% | {ev:.2f}% | {final} | {vetoes} |".format(
                    tier=str(r["research_data_tier"]).replace("|", "/"),
                    bucket=str(r["research_bucket"]).replace("|", "/"),
                    match=str(r["match"]).replace("|", "/"),
                    score=r["score"],
                    minute=r["minute"],
                    selection=r["selection"],
                    odds=fnum(r["odds_decimal"]),
                    model=fnum(r["calibrated_probability"]) * 100,
                    market=fnum(r["market_no_vig_probability"]) * 100,
                    edge=fnum(r["edge"]) * 100,
                    ev=fnum(r["expected_value"]) * 100,
                    final=r["final_operational_status"],
                    vetoes=str(r["vetoes"]).replace("|", "/"),
                )
            )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = load_payload()
    decisions = payload.get("decisions") or []

    rejection_counts = {
        "decisions_screened": len(decisions),
        "final_status_rejected": 0,
        "non_positive_edge_or_ev_rejected": 0,
        "timing_policy_rejected": 0,
        "negative_value_veto_rejected": 0,
        "data_tier_rejected": 0,
    }

    accepted_decisions = []

    for d in decisions:
        if final_operational_status(d) != "NO_BET":
            rejection_counts["final_status_rejected"] += 1
            continue

        if fnum(d.get("edge")) <= 0 or fnum(d.get("expected_value")) <= 0:
            rejection_counts["non_positive_edge_or_ev_rejected"] += 1
            continue

        if not passes_research_timing_policy(d):
            rejection_counts["timing_policy_rejected"] += 1
            continue

        if has_negative_value_veto(d):
            rejection_counts["negative_value_veto_rejected"] += 1
            continue

        if research_data_tier(d) == "REJECTED_DATA_TIER":
            rejection_counts["data_tier_rejected"] += 1
            continue

        accepted_decisions.append(d)

    candidates = [
        decision_to_row(d, payload)
        for d in accepted_decisions
    ]

    candidates.sort(
        key=lambda r: (
            1 if r["research_data_tier"] == "STRICT_EVENTS_PLUS_STATS" else 0,
            fnum(r["expected_value"]),
            fnum(r["edge"]),
        ),
        reverse=True,
    )

    schema_report = ensure_ledger_schema(LEDGER_CSV, FIELDS)

    seen_before = len(load_seen_snapshot_keys(LEDGER_CSV))
    appended = append_rows(LEDGER_CSV, candidates)

    latest_payload = {
        "mode": "FQIS_RESEARCH_LEDGER",
        "generated_at_utc": utc_now(),
        "source": str(SOURCE),
        "ledger_csv": str(LEDGER_CSV),
        "summary": {
            "candidates_this_cycle": len(candidates),
            "strict_events_plus_stats": sum(1 for r in candidates if r["research_data_tier"] == "STRICT_EVENTS_PLUS_STATS"),
            "events_only_research": sum(1 for r in candidates if r["research_data_tier"] == "EVENTS_ONLY_RESEARCH"),
            "new_snapshots_appended": appended,
            "existing_snapshots_before_append": seen_before,
            **rejection_counts,
            **schema_report,
        },
        "candidates": candidates,
    }

    LATEST_JSON.write_text(json.dumps(latest_payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    write_latest_markdown(LATEST_MD, candidates, appended, seen_before, rejection_counts)

    print(json.dumps({
        "status": "READY",
        "candidates_this_cycle": len(candidates),
        "strict_events_plus_stats": latest_payload["summary"]["strict_events_plus_stats"],
        "events_only_research": latest_payload["summary"]["events_only_research"],
        "new_snapshots_appended": appended,
        **rejection_counts,
        **schema_report,
        "ledger_csv": str(LEDGER_CSV),
        "latest_md": str(LATEST_MD),
    }, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
