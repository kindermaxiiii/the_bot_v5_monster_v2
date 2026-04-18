from __future__ import annotations

from typing import Any, Dict, Optional


def _parse_percentage(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    if isinstance(value, str) and value.endswith("%"):
        value = value[:-1]
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    if value in (None, "", "null"):
        return None
    try:
        return int(str(value).replace("%", ""))
    except (TypeError, ValueError):
        return None


def _flatten_stats(team_stats: Dict[str, Any]) -> Dict[str, Any]:
    mapping: Dict[str, Any] = {}
    for stat in team_stats.get("statistics", []) or []:
        key = (stat.get("type") or "").strip().lower()
        mapping[key] = stat.get("value")
    return mapping


def normalize_fixture_statistics(raw: Dict[str, Any]) -> Dict[str, Any]:
    response = raw.get("response", []) or []
    if len(response) < 2:
        return {"raw_payload_json": raw}
    home = response[0]
    away = response[1]
    h = _flatten_stats(home)
    a = _flatten_stats(away)
    return {
        "home_shots_total": _parse_int(h.get("total shots")),
        "away_shots_total": _parse_int(a.get("total shots")),
        "home_shots_on": _parse_int(h.get("shots on goal")),
        "away_shots_on": _parse_int(a.get("shots on goal")),
        "home_shots_inside_box": _parse_int(h.get("shots insidebox")) or _parse_int(h.get("shots inside box")),
        "away_shots_inside_box": _parse_int(a.get("shots insidebox")) or _parse_int(a.get("shots inside box")),
        "home_blocked_shots": _parse_int(h.get("blocked shots")),
        "away_blocked_shots": _parse_int(a.get("blocked shots")),
        "home_corners": _parse_int(h.get("corner kicks")),
        "away_corners": _parse_int(a.get("corner kicks")),
        "home_saves": _parse_int(h.get("goalkeeper saves")),
        "away_saves": _parse_int(a.get("goalkeeper saves")),
        "home_possession": _parse_percentage(h.get("ball possession")),
        "away_possession": _parse_percentage(a.get("ball possession")),
        "home_pass_accuracy": _parse_percentage(h.get("passes %")),
        "away_pass_accuracy": _parse_percentage(a.get("passes %")),
        "home_dangerous_attacks": _parse_int(h.get("dangerous attacks")),
        "away_dangerous_attacks": _parse_int(a.get("dangerous attacks")),
        "home_attacks": _parse_int(h.get("attacks")),
        "away_attacks": _parse_int(a.get("attacks")),
        "raw_payload_json": raw,
    }
