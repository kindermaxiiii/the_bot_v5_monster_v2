from __future__ import annotations

from typing import Any, Dict, List


def normalize_fixture_players(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for team_block in raw.get("response", []) or []:
        team = team_block.get("team", {}) or {}
        for player in team_block.get("players", []) or []:
            pdata = player.get("player", {}) or {}
            stats = (player.get("statistics", []) or [{}])[0] or {}
            games = stats.get("games", {}) or {}
            shots = stats.get("shots", {}) or {}
            goals = stats.get("goals", {}) or {}
            passes = stats.get("passes", {}) or {}
            out.append({
                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "player_id": pdata.get("id"),
                "player_name": pdata.get("name"),
                "minutes": games.get("minutes"),
                "position": games.get("position"),
                "rating": games.get("rating"),
                "shots_total": shots.get("total"),
                "shots_on": shots.get("on"),
                "goals": goals.get("total"),
                "assists": goals.get("assists"),
                "passes_accuracy": passes.get("accuracy"),
                "raw_payload_json": player,
            })
    return out
