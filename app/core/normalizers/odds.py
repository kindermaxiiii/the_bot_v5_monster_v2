from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from app.config import settings

ALLOWED_STANDARD_TOTAL_LINES = {0.5, 1.5, 2.5, 3.5, 4.5, 5.5}
ALLOWED_FIRST_HALF_LINES = {0.5, 1.5, 2.5}
TEAM_TOTAL_LINES = {0.5, 1.5, 2.5, 3.5}


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _normalize_text(value: Optional[str]) -> str:
    return " ".join((value or "").strip().lower().replace("-", " ").split())


def _scope_from_market_name(name: Optional[str]) -> str:
    n = _normalize_text(name)
    if any(token in n for token in ["1st half", "first half", "1h", "half time"]):
        return "1H"
    if any(token in n for token in ["2nd half", "second half", "2h"]):
        return "2H"
    return "FT"


def _bookmaker_name(*candidates: Any) -> str:
    for item in candidates:
        if isinstance(item, str) and item.strip():
            return item.strip()
        if isinstance(item, dict):
            for key in ["name", "bookmaker_name", "title"]:
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return settings.primary_bookmaker_name or "API-FOOTBALL"


def _iter_books(match_block: dict[str, Any]) -> Iterable[tuple[str, list[dict[str, Any]], dict[str, Any]]]:
    bookmakers = match_block.get("bookmakers") or []
    if bookmakers:
        for bm in bookmakers:
            bets = bm.get("bets") or bm.get("odds") or []
            yield _bookmaker_name(bm), bets, bm
        return
    bets = match_block.get("odds") or match_block.get("bets") or []
    yield _bookmaker_name(match_block.get("bookmaker"), match_block.get("bookmaker_name"), match_block.get("name")), bets, match_block


def _is_supported_market(name: Optional[str]) -> bool:
    n = _normalize_text(name)
    if not n:
        return False
    supported_tokens = [
        "over/under", "over under", "total goals", "match goals", "goals over", "both teams to score",
        "winner", "match result", "1x2", "correct score", "team total", "team goals"
    ]
    return any(token in n for token in supported_tokens)


def _market_key(name: Optional[str]) -> str:
    n = _normalize_text(name)
    if "correct score" in n:
        return "correct_score"
    if "both teams to score" in n or "btts" in n:
        return "btts"
    if "winner" in n or "match result" in n or "1x2" in n:
        return "1x2"
    if "team total" in n or "team goals" in n:
        return "team_total"
    return "ou"


def _allowed_line(market_key: str, scope: str, line_value: Optional[float]) -> bool:
    if market_key == "correct_score" or market_key == "1x2" or market_key == "btts":
        return True
    if line_value is None:
        return False
    lv = round(float(line_value), 2)
    if market_key == "team_total":
        return lv in TEAM_TOTAL_LINES
    if scope == "1H":
        return lv in ALLOWED_FIRST_HALF_LINES
    if scope == "2H":
        return False
    return lv in ALLOWED_STANDARD_TOTAL_LINES


def normalize_live_odds(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for match_block in raw.get("response", []) or []:
        match_status = match_block.get("status", {}) or {}
        blocked = bool(match_status.get("blocked")) if match_status.get("blocked") is not None else False
        stopped = bool(match_status.get("stopped")) if match_status.get("stopped") is not None else False
        finished = bool(match_status.get("finished")) if match_status.get("finished") is not None else False
        for bookmaker_name, bets, raw_book in _iter_books(match_block):
            for bet in bets or []:
                market_name = bet.get("name") or bet.get("label") or ""
                if not _is_supported_market(market_name):
                    continue
                market_key = _market_key(market_name)
                scope = _scope_from_market_name(market_name)
                if scope == "2H":
                    continue
                for value in bet.get("values", []) or []:
                    selection_name = str(value.get("value") or value.get("selection") or "").strip()
                    odd_value = _to_float(value.get("odd") or value.get("odds"))
                    if odd_value is None or odd_value <= 1.0:
                        continue
                    line_value = _to_float(value.get("handicap") or value.get("line"))
                    if not _allowed_line(market_key, scope, line_value):
                        continue
                    rows.append({
                        "bookmaker": bookmaker_name,
                        "market_key": market_key,
                        "market_name": market_name,
                        "market_scope": scope,
                        "line_value": line_value,
                        "selection_name": selection_name,
                        "odds_decimal": odd_value,
                        "is_main": bool(value.get("main")) if value.get("main") is not None else None,
                        "is_blocked": blocked,
                        "is_stopped": stopped,
                        "is_finished": finished,
                        "raw_payload_json": {"match_block": match_block, "bookmaker": raw_book, "bet": bet, "value": value},
                    })
    return rows
