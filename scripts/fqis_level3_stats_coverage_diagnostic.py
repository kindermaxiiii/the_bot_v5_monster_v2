from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DECISION_DIR = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live"
DEFAULT_LEVEL3_DIR = ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
OUT_JSON_NAME = "latest_level3_stats_coverage_diagnostic.json"
OUT_MD_NAME = "latest_level3_stats_coverage_diagnostic.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_text(value: Any) -> str:
    return str(value or "").replace("|", "/").replace("\n", " ").strip()


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace("%", "").replace(",", ".").strip()))
    except Exception:
        return default


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_read_error": str(exc), "response": []}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def latest_run_dir(source_dir: Path) -> Path | None:
    runs = sorted(
        [path for path in source_dir.glob("run_*") if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return runs[0] if runs else None


def unwrap_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("payload"), dict):
        return payload["payload"]
    return payload if isinstance(payload, dict) else {}


def response_count(payload: dict[str, Any]) -> int:
    response = payload.get("response")
    return len(response) if isinstance(response, list) else 0


def has_payload_errors(payload: dict[str, Any]) -> bool:
    if payload.get("_read_error"):
        return True
    errors = payload.get("errors")
    if isinstance(errors, dict):
        return bool(errors)
    if isinstance(errors, list):
        return bool(errors)
    if isinstance(errors, str):
        return bool(errors.strip())
    return False


def raw_payload_info(raw_dir: Path, cache_dir: Path, fixture_id: str, kind: str) -> dict[str, Any]:
    candidates = [
        ("raw", raw_dir / f"fixture_{fixture_id}_{kind}.json"),
        ("cache", cache_dir / f"fixture_{fixture_id}_{kind}.json"),
    ]
    for source, path in candidates:
        if not path.exists():
            continue
        wrapped = read_json(path)
        payload = unwrap_payload(wrapped)
        return {
            "exists": True,
            "source": source,
            "path": str(path),
            "payload": payload,
            "response_count": response_count(payload),
            "has_errors": has_payload_errors(payload),
            "last_write_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
        }
    return {
        "exists": False,
        "source": "missing",
        "path": "",
        "payload": {},
        "response_count": 0,
        "has_errors": False,
        "last_write_utc": "",
    }


def current_live_fixtures(
    decision_dir: Path,
    level3_payload: dict[str, Any],
    level3_path: Path,
) -> tuple[list[dict[str, Any]], str]:
    run_dir = latest_run_dir(decision_dir)
    if run_dir:
        fixtures_path = run_dir / "inplay_fixtures.json"
        if fixtures_path.exists():
            payload = read_json(fixtures_path)
            fixtures = payload.get("fixtures") if isinstance(payload, dict) else []
            if isinstance(fixtures, list):
                return [fixture for fixture in fixtures if isinstance(fixture, dict)], str(fixtures_path)

    fixtures = level3_payload.get("fixtures") if isinstance(level3_payload, dict) else []
    if isinstance(fixtures, list):
        return [fixture for fixture in fixtures if isinstance(fixture, dict)], str(level3_path)
    return [], ""


def fixture_id(row: dict[str, Any]) -> str:
    return str(row.get("fixture_id") or row.get("id") or "").strip()


def fixture_match(row: dict[str, Any]) -> str:
    match = row.get("match")
    if match:
        return str(match)
    home = row.get("home_team") or row.get("home")
    away = row.get("away_team") or row.get("away")
    if home or away:
        return f"{home or 'Home'} vs {away or 'Away'}"
    fid = fixture_id(row)
    return f"Fixture {fid}" if fid else "UNKNOWN"


def fixture_minute(row: dict[str, Any]) -> int:
    return safe_int(row.get("elapsed") or row.get("minute") or row.get("fixture_minute"))


def state_by_fixture(level3_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in level3_payload.get("fixtures") or []:
        if isinstance(item, dict):
            fid = fixture_id(item)
            if fid:
                out[fid] = item
    return out


def normalized_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def expected_team_names(fixture: dict[str, Any]) -> list[str]:
    names = [fixture.get("home_team"), fixture.get("away_team")]
    if not any(names) and fixture.get("match"):
        parts = re.split(r"\s+v(?:s|\.)?\s+", str(fixture.get("match")), maxsplit=1, flags=re.IGNORECASE)
        if len(parts) == 2:
            names = parts
    return [str(name) for name in names if str(name or "").strip()]


def raw_stats_team_names(stats_payload: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for item in stats_payload.get("response") or []:
        if not isinstance(item, dict):
            continue
        team = item.get("team") or {}
        name = team.get("name") if isinstance(team, dict) else None
        if name:
            names.append(str(name))
    return names


def team_mapping_status(fixture: dict[str, Any], stats_payload: dict[str, Any], has_stats_raw: bool) -> str:
    if not has_stats_raw:
        return "NO_RAW_STATS"

    expected = [normalized_name(name) for name in expected_team_names(fixture)]
    actual = [normalized_name(name) for name in raw_stats_team_names(stats_payload)]
    if not expected or not actual:
        return "MAPPING_UNKNOWN"

    def matched(expected_name: str) -> bool:
        return any(expected_name == actual_name or expected_name in actual_name or actual_name in expected_name for actual_name in actual)

    return "MATCHED_TEAMS" if all(matched(name) for name in expected) else "MAPPING_MISMATCH"


def provider_stats_status(stats_info: dict[str, Any]) -> str:
    if not stats_info["exists"]:
        return "STATS_ENDPOINT_MISSING"
    if stats_info["has_errors"]:
        return "STATS_RAW_ERROR"
    if int(stats_info["response_count"]) <= 0:
        return "STATS_EMPTY_RESPONSE"
    return "STATS_RAW_AVAILABLE"


def parser_status(state: dict[str, Any], has_stats_raw: bool, has_stats_parsed: bool) -> str:
    if not state:
        return "LEVEL3_STATE_MISSING"
    if has_stats_parsed:
        return "PARSED_STATS_AVAILABLE"
    if has_stats_raw:
        return "PARSER_DROPPED_RAW_STATS"
    return "NO_PARSED_STATS"


def fixture_reason(
    *,
    has_events: bool,
    has_stats_raw: bool,
    has_stats_parsed: bool,
    stats_info: dict[str, Any],
    mapping_status: str,
    minute: int,
    trade_ready_eligible: bool,
) -> str:
    if not stats_info["exists"]:
        return "STATS_ENDPOINT_MISSING"
    if stats_info["has_errors"]:
        return "STATS_RAW_ERROR"
    if mapping_status == "MAPPING_MISMATCH":
        return "FIXTURE_TEAM_MAPPING_MISMATCH"
    if has_stats_raw and not has_stats_parsed:
        return "STATS_RESPONSE_PRESENT_PARSER_EMPTY"
    if has_stats_parsed and trade_ready_eligible:
        return "STATS_AVAILABLE_TRADE_READY_ELIGIBLE"
    if has_stats_parsed:
        return "STATS_AVAILABLE"
    if has_events and not has_stats_raw and minute and minute <= 15:
        return "TIMING_EARLY_STATS_MAY_ARRIVE_LATER"
    if has_events and not has_stats_raw:
        return "PROVIDER_EVENTS_ONLY_NO_STATS"
    if not has_events and not has_stats_raw:
        return "NO_EVENTS_NO_STATS"
    return "UNKNOWN_STATS_COVERAGE_STATE"


def diagnostic_row(
    fixture: dict[str, Any],
    state: dict[str, Any],
    raw_dir: Path,
    cache_dir: Path,
) -> dict[str, Any]:
    fid = fixture_id(fixture)
    events_info = raw_payload_info(raw_dir, cache_dir, fid, "events")
    stats_info = raw_payload_info(raw_dir, cache_dir, fid, "statistics")
    has_events_raw = bool(events_info["response_count"] > 0)
    has_stats_raw = bool(stats_info["response_count"] > 0)
    has_events_parsed = bool(state.get("events_available") is True)
    has_stats_parsed = bool(state.get("stats_available") is True)
    has_events = has_events_raw or has_events_parsed
    minute = fixture_minute(fixture) or safe_int(state.get("minute"))
    mapping_status = team_mapping_status(fixture, stats_info["payload"], has_stats_raw)
    trade_ready_eligible = bool(state.get("trade_ready") is True or (
        state.get("state_ready") is True
        and state.get("events_available") is True
        and state.get("stats_available") is True
    ))

    return {
        "fixture_id": fid,
        "match": fixture_match(fixture) or safe_text(state.get("match")),
        "minute": minute,
        "has_events": has_events,
        "has_stats_raw": has_stats_raw,
        "has_stats_parsed": has_stats_parsed,
        "provider_stats_status": provider_stats_status(stats_info),
        "parser_status": parser_status(state, has_stats_raw, has_stats_parsed),
        "reason": fixture_reason(
            has_events=has_events,
            has_stats_raw=has_stats_raw,
            has_stats_parsed=has_stats_parsed,
            stats_info=stats_info,
            mapping_status=mapping_status,
            minute=minute,
            trade_ready_eligible=trade_ready_eligible,
        ),
        "trade_ready_eligible": trade_ready_eligible,
        "team_mapping_status": mapping_status,
        "raw_events_available": has_events_raw,
        "raw_events_count": int(events_info["response_count"]),
        "raw_stats_count": int(stats_info["response_count"]),
        "parsed_events_available": has_events_parsed,
        "level3_state_present": bool(state),
        "level3_trade_ready": bool(state.get("trade_ready") is True),
        "events_payload_status": "EVENTS_RAW_AVAILABLE" if has_events_raw else ("EVENTS_ENDPOINT_MISSING" if not events_info["exists"] else "EVENTS_EMPTY_RESPONSE"),
        "events_payload_path": events_info["path"],
        "stats_payload_path": stats_info["path"],
        "events_payload_last_write_utc": events_info["last_write_utc"],
        "stats_payload_last_write_utc": stats_info["last_write_utc"],
    }


def build_payload(decision_dir: Path, level3_dir: Path, output_dir: Path, max_fixtures: int | None = None) -> dict[str, Any]:
    level3_path = level3_dir / "latest_level3_live_state.json"
    level3_payload = read_json(level3_path)
    if not isinstance(level3_payload, dict):
        level3_payload = {}
    fixtures, fixtures_source = current_live_fixtures(decision_dir, level3_payload, level3_path)
    if max_fixtures and max_fixtures > 0:
        fixtures = fixtures[:max_fixtures]

    states = state_by_fixture(level3_payload)
    raw_dir = level3_dir / "raw"
    cache_dir = level3_dir / "cache"
    rows = [
        diagnostic_row(fixture, states.get(fixture_id(fixture), {}), raw_dir, cache_dir)
        for fixture in fixtures
        if fixture_id(fixture)
    ]

    reason_counts = Counter(row["reason"] for row in rows)
    summary = {
        "fixtures_seen": len(rows),
        "events_available": sum(1 for row in rows if row["has_events"]),
        "raw_stats_available": sum(1 for row in rows if row["has_stats_raw"]),
        "parsed_stats_available": sum(1 for row in rows if row["has_stats_parsed"]),
        "events_only_no_stats": sum(1 for row in rows if row["has_events"] and not row["has_stats_raw"] and not row["has_stats_parsed"]),
        "stats_parser_empty": sum(1 for row in rows if row["has_stats_raw"] and not row["has_stats_parsed"]),
        "stats_endpoint_missing": sum(1 for row in rows if row["provider_stats_status"] == "STATS_ENDPOINT_MISSING"),
        "reason_counts": dict(sorted(reason_counts.items())),
    }

    return {
        "mode": "FQIS_LEVEL3_STATS_COVERAGE_DIAGNOSTIC",
        "status": "READY",
        "generated_at_utc": utc_now(),
        "diagnostic_only": True,
        "source_files": {
            "current_live_fixtures": fixtures_source,
            "latest_level3_live_state": str(level3_path),
            "level3_raw_dir": str(raw_dir),
            "level3_cache_dir": str(cache_dir),
        },
        "summary": summary,
        "fixtures": rows,
        "safety": dict(SAFETY_BLOCK),
        **SAFETY_BLOCK,
        "read": {
            "purpose": "DIAGNOSTIC_ONLY",
            "decision_path_mutated": False,
            "thresholds_changed": False,
            "stake_sizing_performed": False,
            "ledger_mutation_performed": False,
            "bookmaker_execution_performed": False,
        },
        "output_json": str(output_dir / OUT_JSON_NAME),
        "output_md": str(output_dir / OUT_MD_NAME),
    }


def pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.00%"
    return f"{numerator / denominator * 100:.2f}%"


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    total = int(summary["fixtures_seen"])
    lines = [
        "# FQIS Level 3 Stats Coverage Diagnostic",
        "",
        "DIAGNOSTIC ONLY | NO REAL BET | NO STAKE | NO EXECUTION | NO LEDGER MUTATION",
        "",
        "## Summary",
        "",
        f"- Status: **{payload['status']}**",
        f"- Fixtures seen: **{summary['fixtures_seen']}**",
        f"- Events available: **{summary['events_available']}** / {total} = **{pct(summary['events_available'], total)}**",
        f"- Raw stats available: **{summary['raw_stats_available']}** / {total} = **{pct(summary['raw_stats_available'], total)}**",
        f"- Parsed stats available: **{summary['parsed_stats_available']}** / {total} = **{pct(summary['parsed_stats_available'], total)}**",
        f"- Events-only no stats: **{summary['events_only_no_stats']}**",
        f"- Stats parser empty: **{summary['stats_parser_empty']}**",
        f"- Stats endpoint missing: **{summary['stats_endpoint_missing']}**",
        f"- Generated at UTC: `{payload['generated_at_utc']}`",
        "",
        "## Reason Counts",
        "",
        "| Reason | Count |",
        "|---|---:|",
    ]

    for reason, count in (summary.get("reason_counts") or {}).items():
        lines.append(f"| {safe_text(reason)} | {count} |")

    lines += [
        "",
        "## Fixtures",
        "",
        "| Fixture | Match | Min | Events | Raw Stats | Parsed Stats | Provider Stats | Parser | Reason |",
        "|---:|---|---:|---|---|---|---|---|---|",
    ]

    for row in payload.get("fixtures") or []:
        lines.append(
            "| {fixture_id} | {match} | {minute} | {events} | {raw_stats} | {parsed_stats} | {provider} | {parser} | {reason} |".format(
                fixture_id=safe_text(row.get("fixture_id")),
                match=safe_text(row.get("match")),
                minute=row.get("minute", 0),
                events="yes" if row.get("has_events") else "no",
                raw_stats="yes" if row.get("has_stats_raw") else "no",
                parsed_stats="yes" if row.get("has_stats_parsed") else "no",
                provider=safe_text(row.get("provider_stats_status")),
                parser=safe_text(row.get("parser_status")),
                reason=safe_text(row.get("reason")),
            )
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--decision-dir", default=str(DEFAULT_DECISION_DIR))
    parser.add_argument("--level3-dir", default=str(DEFAULT_LEVEL3_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--max-fixtures", type=int, default=200)
    args = parser.parse_args()

    decision_dir = Path(args.decision_dir)
    level3_dir = Path(args.level3_dir)
    output_dir = Path(args.output_dir)
    payload = build_payload(decision_dir, level3_dir, output_dir, max_fixtures=args.max_fixtures)

    out_json = output_dir / OUT_JSON_NAME
    out_md = output_dir / OUT_MD_NAME
    write_json(out_json, payload)
    write_markdown(out_md, payload)

    print(json.dumps({
        "status": payload["status"],
        "summary": payload["summary"],
        "output_json": str(out_json),
        "output_md": str(out_md),
        "can_execute_real_bets": payload["can_execute_real_bets"],
        "can_enable_live_staking": payload["can_enable_live_staking"],
        "can_mutate_ledger": payload["can_mutate_ledger"],
    }, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
