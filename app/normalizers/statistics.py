from __future__ import annotations

from typing import Any, Dict, Optional


ALIASES = {
    # Shots
    "total shots": "shots_total",
    "shots total": "shots_total",
    "shots on goal": "shots_on",
    "shots on target": "shots_on",
    "shots off goal": "shots_off",
    "shots off target": "shots_off",
    "blocked shots": "blocked_shots",
    "shots insidebox": "shots_inside_box",
    "shots inside box": "shots_inside_box",
    "shots inside the box": "shots_inside_box",
    "shots outsidebox": "shots_outside_box",
    "shots outside box": "shots_outside_box",
    "shots outside the box": "shots_outside_box",

    # Volume / territory
    "corner kicks": "corners",
    "corners": "corners",
    "attacks": "attacks",
    "dangerous attacks": "dangerous_attacks",

    # Possession / passing
    "ball possession": "possession",
    "possession": "possession",
    "passes %": "pass_accuracy",
    "pass accuracy": "pass_accuracy",
    "pass accuracy %": "pass_accuracy",
    "total passes": "passes_total",
    "passes accurate": "passes_accurate",
    "accurate passes": "passes_accurate",

    # Discipline / misc
    "fouls": "fouls",
    "offsides": "offsides",
    "yellow cards": "yellow_cards",
    "red cards": "red_cards",
    "goalkeeper saves": "saves",
    "saves": "saves",

    # Model enrichments
    "expected goals": "xg",
    "xg": "xg",
}


def _parse_percentage(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    if isinstance(value, str):
        value = value.strip().replace("%", "")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    if value in (None, "", "null"):
        return None
    try:
        return int(float(str(value).replace("%", "").strip()))
    except (TypeError, ValueError):
        return None


def _normalize_key(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", " ").replace("_", " ")
    return " ".join(text.split())


def _safe_team_id(value: Any) -> Optional[int]:
    try:
        if value in (None, "", "null"):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _flatten_stats(team_stats: Dict[str, Any]) -> Dict[str, Any]:
    mapping: Dict[str, Any] = {}
    for stat in team_stats.get("statistics", []) or []:
        raw_key = _normalize_key(stat.get("type"))
        if not raw_key:
            continue
        key = ALIASES.get(raw_key, raw_key)
        mapping[key] = stat.get("value")
    return mapping


def _team_meta(team_block: Dict[str, Any]) -> Dict[str, Any]:
    team = team_block.get("team", {}) or {}
    return {
        "team_id": _safe_team_id(team.get("id")),
        "team_name": team.get("name"),
        "team_logo": team.get("logo"),
    }


def _present_count(source: Dict[str, Any]) -> int:
    count = 0
    for value in source.values():
        if value not in (None, "", "null"):
            count += 1
    return count


def _compute_pass_accuracy(passes_accurate: Any, passes_total: Any) -> Optional[float]:
    accurate = _parse_float(passes_accurate)
    total = _parse_float(passes_total)
    if accurate is None or total is None or total <= 0:
        return None
    return round((accurate / total) * 100.0, 2)


def _pick_home_away(
    response: list[dict[str, Any]],
    expected_home_team_id: int | None = None,
    expected_away_team_id: int | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    if len(response) < 2:
        return None, None, "missing"

    exp_home = _safe_team_id(expected_home_team_id)
    exp_away = _safe_team_id(expected_away_team_id)

    if exp_home is not None and exp_away is not None:
        by_team_id: dict[int, dict[str, Any]] = {}
        for row in response:
            team = row.get("team", {}) or {}
            team_id = _safe_team_id(team.get("id"))
            if team_id is not None:
                by_team_id[team_id] = row

        home = by_team_id.get(exp_home)
        away = by_team_id.get(exp_away)
        if home is not None and away is not None:
            return home, away, "team_id_match"

    # Fallback : ordre API
    return response[0], response[1], "response_order"


def normalize_fixture_statistics(
    raw: Dict[str, Any],
    expected_home_team_id: int | None = None,
    expected_away_team_id: int | None = None,
) -> Dict[str, Any]:
    response = raw.get("response", []) or []
    home_block, away_block, mapping_mode = _pick_home_away(
        response,
        expected_home_team_id=expected_home_team_id,
        expected_away_team_id=expected_away_team_id,
    )

    if home_block is None or away_block is None:
        return {
            "raw_payload_json": raw,
            "stats_mapping_mode": mapping_mode,
        }

    h = _flatten_stats(home_block)
    a = _flatten_stats(away_block)

    home_meta = _team_meta(home_block)
    away_meta = _team_meta(away_block)

    home_pass_accuracy = _parse_percentage(h.get("pass_accuracy"))
    away_pass_accuracy = _parse_percentage(a.get("pass_accuracy"))

    if home_pass_accuracy is None:
        home_pass_accuracy = _compute_pass_accuracy(h.get("passes_accurate"), h.get("passes_total"))
    if away_pass_accuracy is None:
        away_pass_accuracy = _compute_pass_accuracy(a.get("passes_accurate"), a.get("passes_total"))

    out = {
        # Metadata
        "stats_mapping_mode": mapping_mode,
        "stats_home_team_id": home_meta["team_id"],
        "stats_home_team_name": home_meta["team_name"],
        "stats_away_team_id": away_meta["team_id"],
        "stats_away_team_name": away_meta["team_name"],
        "home_stats_present_count": _present_count(h),
        "away_stats_present_count": _present_count(a),

        # Shots
        "home_shots_total": _parse_int(h.get("shots_total")),
        "away_shots_total": _parse_int(a.get("shots_total")),
        "home_shots_on": _parse_int(h.get("shots_on")),
        "away_shots_on": _parse_int(a.get("shots_on")),
        "home_shots_off": _parse_int(h.get("shots_off")),
        "away_shots_off": _parse_int(a.get("shots_off")),
        "home_shots_inside_box": _parse_int(h.get("shots_inside_box")),
        "away_shots_inside_box": _parse_int(a.get("shots_inside_box")),
        "home_shots_outside_box": _parse_int(h.get("shots_outside_box")),
        "away_shots_outside_box": _parse_int(a.get("shots_outside_box")),
        "home_blocked_shots": _parse_int(h.get("blocked_shots")),
        "away_blocked_shots": _parse_int(a.get("blocked_shots")),

        # Territory / volume
        "home_corners": _parse_int(h.get("corners")),
        "away_corners": _parse_int(a.get("corners")),
        "home_attacks": _parse_int(h.get("attacks")),
        "away_attacks": _parse_int(a.get("attacks")),
        "home_dangerous_attacks": _parse_int(h.get("dangerous_attacks")),
        "away_dangerous_attacks": _parse_int(a.get("dangerous_attacks")),

        # Defensive / keeper
        "home_saves": _parse_int(h.get("saves")),
        "away_saves": _parse_int(a.get("saves")),

        # Possession / passing
        "home_possession": _parse_percentage(h.get("possession")),
        "away_possession": _parse_percentage(a.get("possession")),
        "home_pass_accuracy": home_pass_accuracy,
        "away_pass_accuracy": away_pass_accuracy,
        "home_passes_total": _parse_int(h.get("passes_total")),
        "away_passes_total": _parse_int(a.get("passes_total")),
        "home_passes_accurate": _parse_int(h.get("passes_accurate")),
        "away_passes_accurate": _parse_int(a.get("passes_accurate")),

        # Discipline / misc
        "home_fouls": _parse_int(h.get("fouls")),
        "away_fouls": _parse_int(a.get("fouls")),
        "home_offsides": _parse_int(h.get("offsides")),
        "away_offsides": _parse_int(a.get("offsides")),
        "home_yellow_cards": _parse_int(h.get("yellow_cards")),
        "away_yellow_cards": _parse_int(a.get("yellow_cards")),
        "home_red_cards": _parse_int(h.get("red_cards")),
        "away_red_cards": _parse_int(a.get("red_cards")),

        # Optional enrichments
        "home_xg": _parse_float(h.get("xg")),
        "away_xg": _parse_float(a.get("xg")),

        "raw_payload_json": raw,
    }

    # Derived totals / diagnostics
    home_shots_total = out.get("home_shots_total") or 0
    away_shots_total = out.get("away_shots_total") or 0
    out["total_shots_total"] = home_shots_total + away_shots_total

    home_shots_on = out.get("home_shots_on") or 0
    away_shots_on = out.get("away_shots_on") or 0
    out["total_shots_on"] = home_shots_on + away_shots_on

    home_corners = out.get("home_corners") or 0
    away_corners = out.get("away_corners") or 0
    out["total_corners"] = home_corners + away_corners

    home_danger = out.get("home_dangerous_attacks") or 0
    away_danger = out.get("away_dangerous_attacks") or 0
    out["total_dangerous_attacks"] = home_danger + away_danger

    home_attacks = out.get("home_attacks") or 0
    away_attacks = out.get("away_attacks") or 0
    out["total_attacks"] = home_attacks + away_attacks

    home_saves = out.get("home_saves") or 0
    away_saves = out.get("away_saves") or 0
    out["total_saves"] = home_saves + away_saves

    return out