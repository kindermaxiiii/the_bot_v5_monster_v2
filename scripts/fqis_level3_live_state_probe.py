from __future__ import annotations

import argparse
import json
import math
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIR = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def api_key_from_env_or_dotenv() -> str:
    names = [
        "APISPORTS_API_KEY",
        "APISPORTS_KEY",
        "API_SPORTS_KEY",
        "API_FOOTBALL_KEY",
        "RAPIDAPI_KEY",
    ]

    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()

    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8-sig").splitlines():
            if "=" not in line or line.strip().startswith("#"):
                continue
            key, value = line.split("=", 1)
            if key.strip() in names:
                return value.strip().strip('"').strip("'")

    raise RuntimeError("API-Sports key introuvable.")


def api_get(path: str, params: dict[str, Any], api_key: str) -> dict[str, Any]:
    query = urllib.parse.urlencode(params)
    url = f"https://v3.football.api-sports.io{path}?{query}"
    req = urllib.request.Request(
        url,
        headers={
            "x-apisports-key": api_key,
            "Accept": "application/json",
            "User-Agent": "FQIS-Level3-LiveState/1.0",
        },
        method="GET",
    )

    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))



def read_cached_api_payload(cache_path: Path, ttl_seconds: int) -> Any | None:
    if ttl_seconds <= 0:
        return None

    if not cache_path.exists():
        return None

    try:
        age_seconds = datetime.now(timezone.utc).timestamp() - cache_path.stat().st_mtime
        if age_seconds > ttl_seconds:
            return None

        wrapped = read_json(cache_path)
        if isinstance(wrapped, dict) and "payload" in wrapped:
            return wrapped["payload"]

        return wrapped
    except Exception:
        return None


def cached_api_get(
    *,
    output_dir: Path,
    fixture_id: str,
    kind: str,
    path: str,
    params: dict[str, Any],
    api_key: str,
    ttl_seconds: int,
) -> dict[str, Any]:
    cache_dir = output_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_path = cache_dir / f"fixture_{fixture_id}_{kind}.json"

    cached = read_cached_api_payload(cache_path, ttl_seconds)
    if isinstance(cached, dict):
        return cached

    payload = api_get(path, params, api_key)

    write_json(
        cache_path,
        {
            "cached_at_utc": utc_now(),
            "ttl_seconds": ttl_seconds,
            "fixture_id": fixture_id,
            "kind": kind,
            "payload": payload,
        },
    )

    return payload



def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace("%", "").strip()))
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace("%", "").replace(",", ".").strip())
    except Exception:
        return default


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def latest_run_dir(source_dir: Path) -> Path:
    runs = sorted(
        [p for p in source_dir.glob("run_*") if p.is_dir()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not runs:
        raise RuntimeError(f"Aucun run_* trouvé dans {source_dir}")
    return runs[0]


def fixture_map(fixtures_payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in (fixtures_payload or {}).get("fixtures", []) or []:
        fixture_id = str(item.get("fixture_id") or "")
        if fixture_id:
            out[fixture_id] = item
    return out


def raw_live_odds_fixture_ids(raw_payload: dict[str, Any] | None) -> list[str]:
    ids: list[str] = []
    seen = set()

    for item in (raw_payload or {}).get("response", []) or []:
        fixture = item.get("fixture") or {}
        fixture_id = str(fixture.get("id") or "")
        if not fixture_id or fixture_id in seen:
            continue
        seen.add(fixture_id)
        ids.append(fixture_id)

    return ids


def normalize_statistics(stats_payload: dict[str, Any] | None) -> dict[str, Any]:
    teams = []

    for team_block in (stats_payload or {}).get("response", []) or []:
        team = team_block.get("team") or {}
        stats = {}

        for stat in team_block.get("statistics", []) or []:
            raw_type = str(stat.get("type") or "").strip()
            key = (
                raw_type.lower()
                .replace(" ", "_")
                .replace("-", "_")
                .replace("/", "_")
            )
            stats[key] = stat.get("value")

        teams.append({
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "statistics": stats,
        })

    return {
        "available": len(teams) > 0,
        "teams": teams,
    }


def normalize_events(events_payload: dict[str, Any] | None) -> dict[str, Any]:
    events = []

    goals = 0
    red_cards = 0
    yellow_cards = 0

    for event in (events_payload or {}).get("response", []) or []:
        event_type = str(event.get("type") or "")
        detail = str(event.get("detail") or "")
        team = event.get("team") or {}
        time_obj = event.get("time") or {}

        if event_type.lower() == "goal":
            goals += 1

        if "red" in detail.lower():
            red_cards += 1

        if "yellow" in detail.lower():
            yellow_cards += 1

        events.append({
            "elapsed": time_obj.get("elapsed"),
            "extra": time_obj.get("extra"),
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "type": event_type,
            "detail": detail,
            "comments": event.get("comments"),
        })

    return {
        "available": len(events) > 0,
        "goals_events": goals,
        "red_cards": red_cards,
        "yellow_cards": yellow_cards,
        "events": events,
    }


def stat_value(team_stats: dict[str, Any], *names: str) -> float:
    stats = team_stats.get("statistics") or {}
    for name in names:
        key = name.lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        if key in stats:
            return safe_float(stats.get(key), 0.0)
    return 0.0


def build_state_for_fixture(
    fixture_id: str,
    fixture_meta: dict[str, Any] | None,
    stats: dict[str, Any],
    events: dict[str, Any],
) -> dict[str, Any]:
    match = fixture_meta.get("match") if fixture_meta else f"Fixture {fixture_id}"
    minute = safe_int(fixture_meta.get("elapsed") if fixture_meta else None, 0)
    home_score = safe_int(fixture_meta.get("score_home") if fixture_meta else None, 0)
    away_score = safe_int(fixture_meta.get("score_away") if fixture_meta else None, 0)

    teams = stats.get("teams") or []
    home_stats = teams[0] if len(teams) >= 1 else {}
    away_stats = teams[1] if len(teams) >= 2 else {}

    home_sot = stat_value(home_stats, "Shots on Goal", "Shots on Target")
    away_sot = stat_value(away_stats, "Shots on Goal", "Shots on Target")
    home_shots = stat_value(home_stats, "Total Shots")
    away_shots = stat_value(away_stats, "Total Shots")
    home_corners = stat_value(home_stats, "Corner Kicks")
    away_corners = stat_value(away_stats, "Corner Kicks")
    home_poss = stat_value(home_stats, "Ball Possession")
    away_poss = stat_value(away_stats, "Ball Possession")

    total_sot = home_sot + away_sot
    total_shots = home_shots + away_shots
    total_corners = home_corners + away_corners
    total_goals = home_score + away_score

    # If live statistics are missing, pressure must not be fake.
    # We use a conservative events/score proxy only.
    stats_available = bool(stats.get("available"))
    events_available = bool(events.get("available"))

    expected_goals_so_far = 2.58 * max(1.0, float(minute)) / 90.0
    goal_pace_ratio = (total_goals + 0.15) / max(0.35, expected_goals_so_far)

    if stats_available:
        pressure_index = clamp(
            0.10 * total_sot
            + 0.035 * total_shots
            + 0.04 * total_corners
            + 0.10 * total_goals,
            0.0,
            1.0,
        )
    else:
        pressure_index = clamp(
            0.08 * total_goals
            + 0.10 * max(0.0, goal_pace_ratio - 1.0)
            + 0.06 * safe_int(events.get("goals_events"), 0),
            0.0,
            0.55,
        )

    red_cards = safe_int(events.get("red_cards"), 0)

    if stats_available:
        chaos_index = clamp(
            0.35 * red_cards
            + 0.10 * total_goals
            + 0.06 * total_sot
            + 0.03 * total_corners,
            0.0,
            1.0,
        )
    else:
        chaos_index = clamp(
            0.35 * red_cards
            + 0.08 * total_goals
            + 0.08 * max(0.0, goal_pace_ratio - 1.0),
            0.0,
            0.65,
        )

    if red_cards > 0:
        regime_label = "RED_CARD_DISTORTED"
    elif chaos_index >= 0.70:
        regime_label = "CHAOTIC_TRANSITIONS"
    elif minute >= 75 and pressure_index <= 0.45:
        regime_label = "LATE_LOCKDOWN"
    elif minute >= 20 and total_sot <= 2 and total_goals <= 1:
        regime_label = "CLOSED_LOW_EVENT"
    else:
        regime_label = "NEUTRAL"

    data_mode = "EVENTS_PLUS_STATS" if stats_available else "EVENTS_ONLY" if events_available else "SCORE_ONLY"

    feed_quality = 0.30
    if fixture_meta:
        feed_quality += 0.15
    if stats_available:
        feed_quality += 0.30
    if events_available:
        feed_quality += 0.18
    if minute > 0:
        feed_quality += 0.10

    # Events-only can be usable for state reconstruction, but not equivalent to full live stats.
    if data_mode == "EVENTS_ONLY":
        feed_quality = min(feed_quality, 0.72)
    elif data_mode == "SCORE_ONLY":
        feed_quality = min(feed_quality, 0.48)

    feed_quality = clamp(feed_quality, 0.0, 1.0)

    regime_confidence = clamp(
        0.28
        + 0.42 * feed_quality
        + (0.10 * min(1.0, total_shots / 12.0) if stats_available else 0.0)
        + 0.08 * min(1.0, minute / 45.0)
        + (0.05 if events_available else 0.0),
        0.0,
        0.92,
    )

    state_ready = bool(fixture_meta) and minute > 0 and events_available and feed_quality >= 0.60
    trade_ready = state_ready and stats_available and regime_confidence >= 0.72 and feed_quality >= 0.75

    return {
        "fixture_id": fixture_id,
        "match": match,
        "minute": minute,
        "score_home": home_score,
        "score_away": away_score,
        "score": f"{home_score}-{away_score}",
        "regime_label": regime_label,
        "regime_confidence": round(regime_confidence, 4),
        "feed_quality": round(feed_quality, 4),
        "pressure_index": round(pressure_index, 4),
        "chaos_index": round(chaos_index, 4),
        "red_cards": red_cards,
        "total_shots": total_shots,
        "total_shots_on_target": total_sot,
        "total_corners": total_corners,
        "home_possession": home_poss,
        "away_possession": away_poss,
        "stats_available": stats_available,
        "events_available": events_available,
        "data_mode": data_mode,
        "goal_pace_ratio": round(goal_pace_ratio, 4),
        "state_ready": state_ready,
        "trade_ready": trade_ready,
        "state_warnings": [
            reason
            for reason, active in {
                "events_only_no_live_statistics": data_mode == "EVENTS_ONLY",
                "score_only_state": data_mode == "SCORE_ONLY",
                "low_regime_confidence": regime_confidence < 0.68,
                "low_feed_quality": feed_quality < 0.60,
            }.items()
            if active
        ],
        "vetoes": [
            reason
            for reason, active in {
                "missing_live_events": not events_available,
                "score_only_no_state": data_mode == "SCORE_ONLY",
                "not_trade_ready_without_statistics": not trade_ready,
            }.items()
            if active
        ],
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    rows = payload.get("fixtures", [])

    lines = [
        "# FQIS Level 3 Live State Probe",
        "",
        "## Summary",
        "",
        f"- Status: **{payload['status']}**",
        f"- Fixtures inspected: **{payload['summary']['fixtures_inspected']}**",
        f"- Stats available: **{payload['summary']['stats_available']}**",
        f"- Events available: **{payload['summary']['events_available']}**",
        f"- State ready: **{payload['summary']['state_ready']}**",
        f"- Trade ready: **{payload['summary']['trade_ready']}**",
        f"- Generated at UTC: `{payload['generated_at_utc']}`",
        "",
        "> PAPER ONLY. This is state reconstruction, not a betting signal.",
        "",
        "## Live States",
        "",
    ]

    if not rows:
        lines.append("No live states.")
    else:
        lines.append("| Fixture | Match | Score | Min | Mode | Regime | Regime conf | Feed quality | Pressure | Chaos | State | Trade | Warnings | Vetoes |")
        lines.append("|---:|---|---:|---:|---|---|---:|---:|---:|---:|---|---|---|---|")

        for r in rows:
            lines.append(
                "| {fixture_id} | {match} | {score} | {minute} | {mode} | {regime} | {conf:.1f}% | {quality:.1f}% | {pressure:.2f} | {chaos:.2f} | {state} | {trade} | {warnings} | {vetoes} |".format(
                    fixture_id=r["fixture_id"],
                    match=str(r["match"]).replace("|", "/"),
                    score=r["score"],
                    minute=r["minute"],
                    mode=r.get("data_mode", "NA"),
                    regime=r["regime_label"],
                    conf=float(r["regime_confidence"]) * 100,
                    quality=float(r["feed_quality"]) * 100,
                    pressure=float(r["pressure_index"]),
                    chaos=float(r["chaos_index"]),
                    state="yes" if r.get("state_ready") else "no",
                    trade="yes" if r.get("trade_ready") else "no",
                    warnings=", ".join(r.get("state_warnings") or []).replace("|", "/"),
                    vetoes=", ".join(r.get("vetoes") or []).replace("|", "/"),
                )
            )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", default=str(DEFAULT_SOURCE_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--max-fixtures", type=int, default=8)
    parser.add_argument("--cache-ttl-seconds", type=int, default=120)
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    output_dir = Path(args.output_dir)

    run_dir = latest_run_dir(source_dir)
    fixtures_payload = read_json(run_dir / "inplay_fixtures.json")
    raw_payload = read_json(run_dir / "diagnostics" / "odds_live_raw.json")

    fmap = fixture_map(fixtures_payload)
    fixture_ids = raw_live_odds_fixture_ids(raw_payload)[: max(1, args.max_fixtures)]

    api_key = api_key_from_env_or_dotenv()

    states = []
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for fixture_id in fixture_ids:
        stats_payload = None
        events_payload = None

        try:
            stats_payload = cached_api_get(
                output_dir=output_dir,
                fixture_id=fixture_id,
                kind="statistics",
                path="/fixtures/statistics",
                params={"fixture": fixture_id},
                api_key=api_key,
                ttl_seconds=args.cache_ttl_seconds,
            )
            write_json(raw_dir / f"fixture_{fixture_id}_statistics.json", stats_payload)
        except Exception as exc:
            stats_payload = {"errors": [str(exc)], "response": []}

        try:
            events_payload = cached_api_get(
                output_dir=output_dir,
                fixture_id=fixture_id,
                kind="events",
                path="/fixtures/events",
                params={"fixture": fixture_id},
                api_key=api_key,
                ttl_seconds=args.cache_ttl_seconds,
            )
            write_json(raw_dir / f"fixture_{fixture_id}_events.json", events_payload)
        except Exception as exc:
            events_payload = {"errors": [str(exc)], "response": []}

        state = build_state_for_fixture(
            fixture_id,
            fmap.get(fixture_id),
            normalize_statistics(stats_payload),
            normalize_events(events_payload),
        )
        states.append(state)

    payload = {
        "mode": "FQIS_LEVEL3_LIVE_STATE_PROBE",
        "status": "READY",
        "generated_at_utc": utc_now(),
        "source_run_dir": str(run_dir),
        "summary": {
            "fixtures_inspected": len(states),
            "stats_available": sum(1 for s in states if s["stats_available"]),
            "events_available": sum(1 for s in states if s["events_available"]),
            "state_ready": sum(1 for s in states if s.get("state_ready")),
            "trade_ready": sum(1 for s in states if s.get("trade_ready")),
            "ready_states": sum(1 for s in states if s.get("state_ready")),
        },
        "fixtures": states,
    }

    write_json(output_dir / "latest_level3_live_state.json", payload)
    write_markdown(output_dir / "latest_level3_live_state.md", payload)

    print(json.dumps({
        "mode": payload["mode"],
        "status": payload["status"],
        "source_run_dir": payload["source_run_dir"],
        "summary": payload["summary"],
        "output": str(output_dir / "latest_level3_live_state.md"),
    }, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
