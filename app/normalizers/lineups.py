from __future__ import annotations

from typing import Any, Dict, List


def normalize_fixture_lineups(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for row in raw.get("response", []) or []:
        team = row.get("team", {}) or {}
        coach = row.get("coach", {}) or {}
        start_xi = row.get("startXI", []) or []
        substitutes = row.get("substitutes", []) or []

        out.append(
            {
                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "team_logo": team.get("logo"),
                "formation": row.get("formation"),
                "coach_name": coach.get("name"),
                "start_xi_count": len(start_xi),
                "substitutes_count": len(substitutes),
                "raw_payload_json": row,
            }
        )

    return out
