from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

from app.config import settings

ALLOWED_STANDARD_TOTAL_LINES = {0.5, 1.5, 2.5, 3.5, 4.5, 5.5}
ALLOWED_FIRST_HALF_LINES = {0.5, 1.5, 2.5}
TEAM_TOTAL_LINES = {0.5, 1.5, 2.5, 3.5}


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_text(value: Optional[str]) -> str:
    return " ".join((value or "").strip().lower().replace("-", " ").replace("_", " ").split())


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on", "blocked", "suspended", "closed", "stopped", "finished"}


def _contains_any(text: str, tokens: Iterable[str]) -> bool:
    return any(token in text for token in tokens)


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


def _scope_from_market_name(name: Optional[str]) -> str:
    n = _normalize_text(name)
    if _contains_any(n, ["1st half", "first half", "1h", "half time", "halftime"]):
        return "1H"
    if _contains_any(n, ["2nd half", "second half", "2h"]):
        return "2H"
    return "FT"


def _is_supported_market(name: Optional[str]) -> bool:
    n = _normalize_text(name)
    if not n:
        return False

    supported_tokens = [
        "over under",
        "over/under",
        "total goals",
        "match goals",
        "goals over",
        "goals under",
        "both teams to score",
        "btts",
        "winner",
        "match result",
        "1x2",
        "correct score",
        "team total",
        "team goals",
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


def _extract_explicit_line(*values: Any) -> Optional[float]:
    """
    Utilise uniquement les champs explicitement numériques.
    Évite de lire le "1" de "1st half" ou le "2" de "team 2".
    """
    for value in values:
        direct = _to_float(value)
        if direct is not None:
            return round(direct, 2)
    return None


def _extract_line_from_text(text: str) -> Optional[float]:
    """
    N'extrait une ligne que si elle ressemble vraiment à une ligne de total.
    On évite les nombres parasites de type '1st', 'team 2', etc.
    """
    t = _normalize_text(text)
    if not t:
        return None

    patterns = [
        r"(?:over|under)\s*([0-9]+(?:\.[0-9]+)?)",
        r"([0-9]+(?:\.[0-9]+)?)\s*(?:goals|goal)",
        r"(?:total|totals)\s*([0-9]+(?:\.[0-9]+)?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, t)
        if match:
            try:
                return round(float(match.group(1)), 2)
            except ValueError:
                return None
    return None


def _extract_line(market_key: str, market_name: str, selection_name: str, value_block: dict[str, Any]) -> Optional[float]:
    if market_key in {"1x2", "btts", "correct_score"}:
        return None

    explicit = _extract_explicit_line(
        value_block.get("handicap"),
        value_block.get("line"),
        value_block.get("total"),
        value_block.get("points"),
    )
    if explicit is not None:
        return explicit

    from_selection = _extract_line_from_text(selection_name)
    if from_selection is not None:
        return from_selection

    from_market = _extract_line_from_text(market_name)
    if from_market is not None:
        return from_market

    return None


def _allowed_line(market_key: str, scope: str, line_value: Optional[float]) -> bool:
    if market_key in {"correct_score", "1x2", "btts"}:
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


def _infer_team_total_side(market_name: str, selection_name: str) -> str:
    m = _normalize_text(market_name)
    s = _normalize_text(selection_name)

    is_home = _contains_any(m, ["home team", "home total", "team 1", "home goals", "domicile"])
    is_away = _contains_any(m, ["away team", "away total", "team 2", "away goals", "extérieur"])

    if not (is_home or is_away):
        if "home" in s or "domicile" in s:
            is_home = True
        elif "away" in s or "extérieur" in s:
            is_away = True

    if "over" in s:
        return "HOME_OVER" if is_home else "AWAY_OVER" if is_away else "OVER"
    if "under" in s:
        return "HOME_UNDER" if is_home else "AWAY_UNDER" if is_away else "UNDER"

    return selection_name.strip().upper()


def _normalize_selection(market_key: str, market_name: str, selection_name: str) -> str:
    s = _normalize_text(selection_name)

    if market_key == "ou":
        if "over" in s:
            return "OVER"
        if "under" in s:
            return "UNDER"

    if market_key == "btts":
        if s in {"yes", "btts yes", "both teams to score yes", "oui"}:
            return "BTTS_YES"
        if s in {"no", "btts no", "both teams to score no", "non"}:
            return "BTTS_NO"

    if market_key == "1x2":
        if s in {"1", "home", "home win", "domicile"}:
            return "HOME"
        if s in {"x", "draw", "tie", "nul"}:
            return "DRAW"
        if s in {"2", "away", "away win", "extérieur"}:
            return "AWAY"

    if market_key == "team_total":
        return _infer_team_total_side(market_name, selection_name)

    if market_key == "correct_score":
        score_match = re.search(r"(\d+)\s*[:\-]\s*(\d+)", selection_name)
        if score_match:
            return f"{score_match.group(1)}-{score_match.group(2)}"
        return selection_name.strip()

    return selection_name.strip().upper()


def _is_selection_supported(market_key: str, selection_name: str) -> bool:
    s = _normalize_text(selection_name)

    if market_key == "ou":
        return "over" in s or "under" in s

    if market_key == "btts":
        return s in {
            "yes",
            "no",
            "btts yes",
            "btts no",
            "both teams to score yes",
            "both teams to score no",
            "oui",
            "non",
        }

    if market_key == "1x2":
        return s in {"1", "x", "2", "home", "draw", "away", "home win", "away win", "domicile", "nul", "extérieur"}

    if market_key == "team_total":
        return "over" in s or "under" in s

    if market_key == "correct_score":
        return bool(re.search(r"\d+\s*[:\-]\s*\d+", s))

    return False


def _is_blocked(match_block: dict[str, Any], raw_book: dict[str, Any], bet: dict[str, Any], value: dict[str, Any]) -> tuple[bool, bool, bool]:
    blocked = any(
        _truthy(x)
        for x in [
            (match_block.get("status") or {}).get("blocked"),
            match_block.get("blocked"),
            raw_book.get("blocked"),
            bet.get("blocked"),
            value.get("blocked"),
            value.get("is_blocked"),
        ]
    )

    stopped = any(
        _truthy(x)
        for x in [
            (match_block.get("status") or {}).get("stopped"),
            match_block.get("stopped"),
            raw_book.get("stopped"),
            bet.get("stopped"),
            value.get("stopped"),
            value.get("is_stopped"),
            value.get("suspended"),
            value.get("is_suspended"),
            value.get("closed"),
        ]
    )

    finished = any(
        _truthy(x)
        for x in [
            (match_block.get("status") or {}).get("finished"),
            match_block.get("finished"),
            raw_book.get("finished"),
            bet.get("finished"),
            value.get("finished"),
            value.get("is_finished"),
            value.get("settled"),
        ]
    )

    return blocked, stopped, finished


def _is_main_flag(value: dict[str, Any], bet: dict[str, Any]) -> Optional[bool]:
    for candidate in [
        value.get("main"),
        value.get("is_main"),
        bet.get("main"),
        bet.get("is_main"),
    ]:
        if candidate is None:
            continue
        return _truthy(candidate)
    return None


def _iter_books(match_block: dict[str, Any]) -> Iterable[tuple[str, list[dict[str, Any]], dict[str, Any]]]:
    bookmakers = match_block.get("bookmakers") or []
    if bookmakers:
        for bm in bookmakers:
            bets = bm.get("bets") or bm.get("odds") or []
            yield _bookmaker_name(bm), bets, bm
        return

    bets = match_block.get("odds") or match_block.get("bets") or []
    yield _bookmaker_name(match_block.get("bookmaker"), match_block.get("bookmaker_name"), match_block.get("name")), bets, match_block


def normalize_live_odds(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[tuple] = set()

    for match_block in raw.get("response", []) or []:
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
                    if not selection_name:
                        continue

                    if not _is_selection_supported(market_key, selection_name):
                        continue

                    odd_value = _to_float(value.get("odd") or value.get("odds"))
                    if odd_value is None or odd_value <= 1.0:
                        continue

                    line_value = _extract_line(
                        market_key=market_key,
                        market_name=market_name,
                        selection_name=selection_name,
                        value_block=value,
                    )
                    if not _allowed_line(market_key, scope, line_value):
                        continue

                    normalized_selection = _normalize_selection(market_key, market_name, selection_name)

                    blocked, stopped, finished = _is_blocked(match_block, raw_book, bet, value)
                    is_main = _is_main_flag(value, bet)

                    row = {
                        "bookmaker": bookmaker_name,
                        "market_key": market_key,
                        "market_name": market_name,
                        "market_scope": scope,
                        "line_value": line_value,
                        "selection_name": normalized_selection,
                        "selection_raw": selection_name,
                        "odds_decimal": odd_value,
                        "is_main": is_main,
                        "is_blocked": blocked,
                        "is_stopped": stopped,
                        "is_finished": finished,
                        "raw_payload_json": {
                            "match_block": match_block,
                            "bookmaker": raw_book,
                            "bet": bet,
                            "value": value,
                        },
                    }

                    dedupe_key = (
                        row["bookmaker"],
                        row["market_key"],
                        row["market_scope"],
                        row["line_value"],
                        row["selection_name"],
                        row["odds_decimal"],
                    )
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)

                    rows.append(row)

    return rows