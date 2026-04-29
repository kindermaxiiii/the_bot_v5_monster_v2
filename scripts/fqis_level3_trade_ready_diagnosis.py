from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LEVEL3_PATH = ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state" / "latest_level3_live_state.json"
DECISIONS_PATH = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json"
PROVIDER_PATH = ROOT / "data" / "pipeline" / "api_sports" / "provider_coverage" / "latest_provider_coverage_report.json"

OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state" / "latest_trade_ready_diagnosis.md"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state" / "latest_trade_ready_diagnosis.json"

MODES = ("SCORE_ONLY", "EVENTS_ONLY", "EVENTS_PLUS_STATS")
CACHE_STALE_SECONDS = 900.0


def main() -> int:
    level3 = _load_json(LEVEL3_PATH)
    decisions = _load_json(DECISIONS_PATH)
    provider = _load_json(PROVIDER_PATH)

    fixtures = [dict(item) for item in level3.get("fixtures", []) if isinstance(item, dict)]
    decision_rows = [dict(item) for item in decisions.get("decisions", []) if isinstance(item, dict)]
    provider_rows = [dict(item) for item in provider.get("fixtures", []) if isinstance(item, dict)]

    provider_by_fixture = _index_provider_rows(provider_rows)
    bridge_by_fixture = _bridge_vetoes_by_fixture(decision_rows)
    top_blockers = Counter(veto for row in decision_rows for veto in row.get("vetoes", []) or [])

    inspected = [_diagnose_fixture(row, provider_by_fixture, bridge_by_fixture) for row in fixtures]
    _annotate_cache_staleness(inspected)
    mode_counts = {mode: sum(1 for item in inspected if item["data_mode"] == mode) for mode in MODES}
    recommendations = _build_recommendations(inspected, top_blockers)

    payload = {
        "mode": "FQIS_LEVEL3_TRADE_READY_DIAGNOSIS",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "level3_live_state": str(LEVEL3_PATH),
            "decision_bridge_live": str(DECISIONS_PATH),
            "provider_coverage": str(PROVIDER_PATH),
        },
        "summary": {
            "fixtures_inspected": len(inspected),
            "state_ready_count": sum(1 for item in inspected if item["state_ready"]),
            "trade_ready_count": sum(1 for item in inspected if item["trade_ready"]),
            "events_available_count": sum(1 for item in inspected if item["events_available"]),
            "stats_available_count": sum(1 for item in inspected if item["stats_available"]),
            "modes": mode_counts,
            "provider_summary": provider.get("summary", {}),
            "decision_count": len(decision_rows),
        },
        "top_blockers": [
            {"veto": veto, "count": count}
            for veto, count in top_blockers.most_common(20)
        ],
        "events_plus_stats_not_trade_ready": [
            item for item in inspected
            if item["data_mode"] == "EVENTS_PLUS_STATS" and not item["trade_ready"]
        ],
        "events_only_not_trade_ready": [
            item for item in inspected
            if item["data_mode"] == "EVENTS_ONLY" and not item["trade_ready"]
        ],
        "fixtures": inspected,
        "recommendations": recommendations,
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    OUT_MD.write_text(_render_markdown(payload), encoding="utf-8")

    print(
        {
            "status": "READY",
            "fixtures_inspected": payload["summary"]["fixtures_inspected"],
            "state_ready_count": payload["summary"]["state_ready_count"],
            "trade_ready_count": payload["summary"]["trade_ready_count"],
            "events_available_count": payload["summary"]["events_available_count"],
            "stats_available_count": payload["summary"]["stats_available_count"],
            "output_md": str(OUT_MD),
            "output_json": str(OUT_JSON),
        }
    )
    return 0


def _diagnose_fixture(
    row: dict[str, Any],
    provider_by_fixture: dict[str, dict[str, Any]],
    bridge_by_fixture: dict[str, Counter],
) -> dict[str, Any]:
    fixture_id = _text(row.get("fixture_id"))
    provider_row = provider_by_fixture.get(fixture_id, {})
    provider_events_available = _bool(provider_row.get("events_available"))
    provider_stats_available = _bool(provider_row.get("statistics_available"))
    events_available = _bool(row.get("events_available"))
    stats_available = _bool(row.get("stats_available"))
    data_mode = _text(row.get("data_mode")) or _infer_mode(events_available, stats_available)
    provider_mode = _text(provider_row.get("coverage_label")) or _infer_mode(provider_events_available, provider_stats_available)
    bridge_vetoes = bridge_by_fixture.get(fixture_id, Counter())
    fixture_vetoes = [_text(veto) for veto in row.get("vetoes", []) or [] if _text(veto)]
    warnings = [_text(warning) for warning in row.get("state_warnings", []) or [] if _text(warning)]

    reasons = _diagnosis_reasons(
        row=row,
        data_mode=data_mode,
        provider_row=provider_row,
        provider_mode=provider_mode,
        provider_events_available=provider_events_available,
        provider_stats_available=provider_stats_available,
        bridge_vetoes=bridge_vetoes,
    )

    return {
        "fixture_id": fixture_id,
        "match": _text(row.get("match")),
        "score": _text(row.get("score")),
        "minute": _safe_int(row.get("minute")),
        "data_mode": data_mode,
        "state_ready": _bool(row.get("state_ready")),
        "trade_ready": _bool(row.get("trade_ready")),
        "events_available": events_available,
        "stats_available": stats_available,
        "provider_coverage_label": provider_mode,
        "provider_events_available": provider_events_available,
        "provider_events_count": _safe_int(provider_row.get("events_count")),
        "provider_statistics_available": provider_stats_available,
        "provider_statistics_count": _safe_int(provider_row.get("statistics_count")),
        "provider_latest_seen": _text(provider_row.get("latest_seen")),
        "provider_latest_seen_ts": _safe_float(provider_row.get("latest_seen_ts")),
        "vetoes": fixture_vetoes,
        "bridge_vetoes": [
            {"veto": veto, "count": count}
            for veto, count in bridge_vetoes.most_common()
        ],
        "state_warnings": warnings,
        "diagnosis_reasons": reasons,
    }


def _diagnosis_reasons(
    *,
    row: dict[str, Any],
    data_mode: str,
    provider_row: dict[str, Any],
    provider_mode: str,
    provider_events_available: bool,
    provider_stats_available: bool,
    bridge_vetoes: Counter,
) -> list[str]:
    reasons: list[str] = []
    events_available = _bool(row.get("events_available"))
    stats_available = _bool(row.get("stats_available"))
    state_ready = _bool(row.get("state_ready"))
    trade_ready = _bool(row.get("trade_ready"))
    fixture_vetoes = {_text(veto) for veto in row.get("vetoes", []) or []}
    bridge_veto_names = set(bridge_vetoes)

    if not events_available:
        reasons.append("missing events")
    if not stats_available:
        reasons.append("missing stats")
    if data_mode == "EVENTS_ONLY" and state_ready and not trade_ready:
        reasons.append("bridge requires statistics before trade_ready")
    if data_mode == "EVENTS_PLUS_STATS" and not trade_ready:
        reasons.append("events and stats are present but trade_ready is still false")
    if provider_mode == "STATS_ONLY_ANOMALY":
        reasons.append("provider problem: stats exist without events")
    if provider_mode == "SCORE_ONLY":
        reasons.append("provider problem: score-only coverage")
    if provider_events_available and not events_available:
        reasons.append("parser problem: provider events not reflected in level3 state")
    if provider_stats_available and not stats_available:
        reasons.append("parser problem: provider statistics not reflected in level3 state")
    if "not_trade_ready_without_statistics" in fixture_vetoes or "level3_not_trade_ready_without_statistics" in bridge_veto_names:
        reasons.append("blocking veto: statistics required")
    if "missing_live_events" in fixture_vetoes or "level3_missing_live_events" in bridge_veto_names:
        reasons.append("blocking veto: live events missing")
    if not state_ready:
        reasons.append("state not ready")

    return _unique(reasons)


def _build_recommendations(inspected: list[dict[str, Any]], top_blockers: Counter) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []

    provider_problem_fixtures = [
        item for item in inspected
        if item["provider_coverage_label"] in {"SCORE_ONLY", "STATS_ONLY_ANOMALY"}
    ]
    parser_problem_fixtures = [
        item for item in inspected
        if (
            item["provider_events_available"] and not item["events_available"]
        ) or (
            item["provider_statistics_available"] and not item["stats_available"]
        )
    ]
    cache_stale_fixtures = [item for item in inspected if "cache stale" in item["diagnosis_reasons"]]
    bridge_too_strict_fixtures = [
        item for item in inspected
        if (
            item["data_mode"] == "EVENTS_PLUS_STATS" and not item["trade_ready"]
        ) or (
            item["data_mode"] == "EVENTS_ONLY"
            and item["state_ready"]
            and not item["trade_ready"]
            and item["events_available"]
        )
    ]
    missing_events_fixtures = [item for item in inspected if not item["events_available"]]
    missing_stats_fixtures = [item for item in inspected if not item["stats_available"]]

    recommendations.append(_recommendation(
        "provider problem",
        provider_problem_fixtures,
        "Prioriser les compétitions/fixtures où le provider renvoie SCORE_ONLY ou STATS_ONLY_ANOMALY; ce n'est pas un problème de modèle.",
    ))
    recommendations.append(_recommendation(
        "cache stale",
        cache_stale_fixtures,
        "Rafraîchir le cache Level 3/provider si latest_seen est en retard par rapport aux autres fixtures inspectées.",
    ))
    recommendations.append(_recommendation(
        "parser problem",
        parser_problem_fixtures,
        "Auditer le mapping provider -> level3 quand provider_coverage voit events/stats mais latest_level3_live_state ne les reflète pas.",
    ))
    recommendations.append(_recommendation(
        "bridge too strict",
        bridge_too_strict_fixtures,
        "Revoir la règle qui impose stats pour trade_ready, surtout sur EVENTS_ONLY state_ready avec veto level3_not_trade_ready_without_statistics.",
    ))
    recommendations.append(_recommendation(
        "missing events",
        missing_events_fixtures,
        "Bloquer ou dégrader explicitement SCORE_ONLY; sans events, Level 3 ne peut pas valider le rythme live.",
    ))
    recommendations.append(_recommendation(
        "missing stats",
        missing_stats_fixtures,
        "Décider si EVENTS_ONLY peut créer une watchlist Level 3, ou maintenir le veto stats pour les signaux réels.",
    ))

    recommendations.append({
        "category": "top bridge blockers",
        "affected_fixtures": None,
        "sample_fixture_ids": [],
        "action": "Traiter d'abord les vetoes dominants du bridge.",
        "evidence": [
            {"veto": veto, "count": count}
            for veto, count in top_blockers.most_common(5)
        ],
    })

    return recommendations


def _recommendation(category: str, fixtures: list[dict[str, Any]], action: str) -> dict[str, Any]:
    return {
        "category": category,
        "affected_fixtures": len(fixtures),
        "sample_fixture_ids": [item["fixture_id"] for item in fixtures[:8]],
        "action": action,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# FQIS Level 3 Trade Ready Diagnosis",
        "",
        f"Generated at UTC: {payload['generated_at_utc']}",
        "",
        "## Summary",
        "",
        f"- Fixtures inspected: **{summary['fixtures_inspected']}**",
        f"- State ready: **{summary['state_ready_count']}**",
        f"- Trade ready: **{summary['trade_ready_count']}**",
        f"- Events available: **{summary['events_available_count']}**",
        f"- Stats available: **{summary['stats_available_count']}**",
        "",
        "## Modes",
        "",
        "| Mode | Count |",
        "|---|---:|",
    ]

    for mode, count in summary["modes"].items():
        lines.append(f"| {mode} | {count} |")

    lines.extend([
        "",
        "## Top Bridge Blockers",
        "",
        "| Veto | Count |",
        "|---|---:|",
    ])
    for item in payload["top_blockers"][:20]:
        lines.append(f"| {item['veto']} | {item['count']} |")

    lines.extend([
        "",
        "## EVENTS_PLUS_STATS But Not Trade Ready",
        "",
        "| Fixture | Match | Min | Score | Reasons | Vetoes |",
        "|---:|---|---:|---:|---|---|",
    ])
    lines.extend(_fixture_lines(payload["events_plus_stats_not_trade_ready"]))

    lines.extend([
        "",
        "## EVENTS_ONLY Not Trade Ready",
        "",
        "| Fixture | Match | Min | Score | Reasons | Vetoes |",
        "|---:|---|---:|---:|---|---|",
    ])
    lines.extend(_fixture_lines(payload["events_only_not_trade_ready"]))

    lines.extend([
        "",
        "## Actionable Recommendations",
        "",
        "| Category | Affected fixtures | Sample fixture ids | Action |",
        "|---|---:|---|---|",
    ])
    for item in payload["recommendations"]:
        affected = "-" if item.get("affected_fixtures") is None else str(item.get("affected_fixtures"))
        samples = ", ".join(item.get("sample_fixture_ids") or [])
        lines.append(
            "| {category} | {affected} | {samples} | {action} |".format(
                category=_md(item["category"]),
                affected=affected,
                samples=_md(samples),
                action=_md(item["action"]),
            )
        )

    lines.extend([
        "",
        "## Fixtures Inspected",
        "",
        "| Fixture | Match | Mode | State | Trade | Events | Stats | Provider | Reasons |",
        "|---:|---|---|---:|---:|---:|---:|---|---|",
    ])
    for item in payload["fixtures"]:
        lines.append(
            "| {fixture_id} | {match} | {mode} | {state} | {trade} | {events} | {stats} | {provider} | {reasons} |".format(
                fixture_id=_md(item["fixture_id"]),
                match=_md(item["match"]),
                mode=_md(item["data_mode"]),
                state=_yes_no(item["state_ready"]),
                trade=_yes_no(item["trade_ready"]),
                events=_yes_no(item["events_available"]),
                stats=_yes_no(item["stats_available"]),
                provider=_md(item["provider_coverage_label"]),
                reasons=_md(", ".join(item["diagnosis_reasons"])),
            )
        )

    return "\n".join(lines) + "\n"


def _fixture_lines(fixtures: list[dict[str, Any]]) -> list[str]:
    if not fixtures:
        return ["| - | - | - | - | - | - |"]

    lines = []
    for item in fixtures:
        vetoes = [entry["veto"] for entry in item["bridge_vetoes"][:6]]
        if not vetoes:
            vetoes = item["vetoes"][:6]
        lines.append(
            "| {fixture_id} | {match} | {minute} | {score} | {reasons} | {vetoes} |".format(
                fixture_id=_md(item["fixture_id"]),
                match=_md(item["match"]),
                minute=item["minute"],
                score=_md(item["score"]),
                reasons=_md(", ".join(item["diagnosis_reasons"])),
                vetoes=_md(", ".join(vetoes)),
            )
        )
    return lines


def _index_provider_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        fixture_id = _text(row.get("fixture_id"))
        if not fixture_id:
            continue
        current = indexed.get(fixture_id)
        if current is None or _safe_float(row.get("latest_seen_ts"), 0.0) > _safe_float(current.get("latest_seen_ts"), 0.0):
            indexed[fixture_id] = row
    return indexed


def _bridge_vetoes_by_fixture(rows: list[dict[str, Any]]) -> dict[str, Counter]:
    by_fixture: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        fixture_id = _text(row.get("fixture_id"))
        for veto in row.get("vetoes", []) or []:
            by_fixture[fixture_id][_text(veto)] += 1
    return by_fixture


def _annotate_cache_staleness(fixtures: list[dict[str, Any]]) -> None:
    timestamps = [
        _safe_float(item.get("provider_latest_seen_ts"))
        for item in fixtures
        if _safe_float(item.get("provider_latest_seen_ts")) is not None
    ]
    if not timestamps:
        return

    freshest = max(ts for ts in timestamps if ts is not None)
    stale_before = freshest - CACHE_STALE_SECONDS

    for item in fixtures:
        latest_seen_ts = _safe_float(item.get("provider_latest_seen_ts"))
        if latest_seen_ts is not None and latest_seen_ts < stale_before:
            item["diagnosis_reasons"] = _unique([*item["diagnosis_reasons"], "cache stale"])


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _infer_mode(events_available: bool, stats_available: bool) -> str:
    if events_available and stats_available:
        return "EVENTS_PLUS_STATS"
    if events_available:
        return "EVENTS_ONLY"
    return "SCORE_ONLY"


def _safe_int(value: Any) -> int:
    try:
        return int(float(str(value).replace(",", ".")))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return default


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _md(value: Any) -> str:
    return _text(value).replace("|", "/").replace("\n", " ")


if __name__ == "__main__":
    raise SystemExit(main())
