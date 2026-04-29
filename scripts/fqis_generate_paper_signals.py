from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json"
OUTPUT = ROOT / "data" / "pipeline" / "api_sports" / "fqis_paper_signals" / "latest_paper_signals.md"

PROMOTED_MIN_EDGE = 0.03
PROMOTED_MIN_EXPECTED_VALUE = 0.02

TABLE_FIELDS = (
    "fixture_id",
    "match",
    "score",
    "minute",
    "side",
    "line",
    "selection",
    "odds_decimal",
    "model_probability",
    "edge",
    "expected_value",
    "vetoes",
)


@dataclass(frozen=True)
class PaperSignal:
    fixture_id: str
    match: str
    score: str
    minute: str
    side: str
    line: str
    selection: str
    odds_decimal: str
    model_probability: str
    edge: float
    expected_value: float
    vetoes: str
    executable: bool
    publication_status: str
    real_status: str

    def sort_key(self) -> tuple[float, float, str]:
        return (self.expected_value, self.edge, self.fixture_id)

    def to_markdown_row(self) -> str:
        values = (
            self.fixture_id,
            self.match,
            self.score,
            self.minute,
            self.side,
            self.line,
            self.selection,
            self.odds_decimal,
            self.model_probability,
            _format_decimal(self.edge),
            _format_decimal(self.expected_value),
            self.vetoes,
        )
        return "| " + " | ".join(_escape_markdown_cell(value) for value in values) + " |"


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    if not INPUT.exists():
        _write_markdown([], [], [], [], source_status="NO_DECISIONS_FILE")
        print(
            {
                "status": "NO_DECISIONS_FILE",
                "clean_signals": 0,
                "watchlist": 0,
                "promoted": 0,
                "no_bets": 0,
                "output": str(OUTPUT),
            }
        )
        return

    payload = json.loads(INPUT.read_text(encoding="utf-8"))
    decisions = payload.get("decisions") or []

    clean_signals: list[PaperSignal] = []
    watchlist: list[PaperSignal] = []
    no_bets: list[PaperSignal] = []

    for decision in decisions:
        signal = _signal_from_decision(decision)
        if signal.edge > 0.0 and signal.expected_value > 0.0:
            if _is_clean_signal(signal):
                clean_signals.append(signal)
            else:
                watchlist.append(signal)
        else:
            no_bets.append(signal)

    clean_signals.sort(key=PaperSignal.sort_key, reverse=True)
    watchlist.sort(key=PaperSignal.sort_key, reverse=True)
    no_bets.sort(key=PaperSignal.sort_key, reverse=True)

    promoted: list[PaperSignal] = []
    if not clean_signals and watchlist:
        top_watchlist = watchlist[0]
        if (
            top_watchlist.edge >= PROMOTED_MIN_EDGE
            and top_watchlist.expected_value >= PROMOTED_MIN_EXPECTED_VALUE
        ):
            promoted.append(top_watchlist)

    _write_markdown(clean_signals, watchlist, promoted, no_bets, source_status="OK")

    print(
        {
            "status": "OK",
            "clean_signals": len(clean_signals),
            "watchlist": len(watchlist),
            "promoted": len(promoted),
            "no_bets": len(no_bets),
            "output": str(OUTPUT),
        }
    )


def _signal_from_decision(decision: dict[str, Any]) -> PaperSignal:
    edge = _float_value(decision.get("edge"))
    expected_value = _float_value(decision.get("expected_value"))
    model_probability = _first_float(
        decision,
        "model_probability",
        "calibrated_probability",
        "raw_probability",
    )

    return PaperSignal(
        fixture_id=_text(decision.get("fixture_id")),
        match=_text(decision.get("match")),
        score=_text(decision.get("score")),
        minute=_text(decision.get("minute")),
        side=_text(decision.get("side")).upper(),
        line=_format_decimal(_float_value(decision.get("line"))),
        selection=_text(decision.get("selection")),
        odds_decimal=_format_decimal(_float_value(decision.get("odds_decimal"))),
        model_probability=_format_decimal(model_probability),
        edge=edge,
        expected_value=expected_value,
        vetoes=", ".join(_collect_vetoes(decision)),
        executable=bool(decision.get("executable")),
        publication_status=_text(decision.get("publication_status")).upper(),
        real_status=_text(decision.get("real_status")).upper(),
    )


def _is_clean_signal(signal: PaperSignal) -> bool:
    if signal.vetoes:
        return False
    if signal.publication_status in {"BLOCKED", "NO_BET"}:
        return False
    if signal.real_status in {"REAL_VALID", "TOP_BET"}:
        return True
    return signal.executable


def _write_markdown(
    clean_signals: list[PaperSignal],
    watchlist: list[PaperSignal],
    promoted: list[PaperSignal],
    no_bets: list[PaperSignal],
    *,
    source_status: str,
) -> None:
    lines = [
        "# FQIS PAPER SIGNALS",
        "",
        f"Generated at UTC: {datetime.now(timezone.utc).isoformat()}",
        f"Source status: {source_status}",
        f"Source file: {INPUT}",
        "",
        f"- clean_signals: **{len(clean_signals)}**",
        f"- watchlist: **{len(watchlist)}**",
        f"- promoted: **{len(promoted)}**",
        f"- no_bets: **{len(no_bets)}**",
        "",
    ]

    lines.extend(_section("CLEAN SIGNALS", clean_signals))
    lines.extend(_section("WATCHLIST", watchlist))
    lines.extend(_section("PROMOTED", promoted))
    lines.extend(_section("NO BETS", no_bets))

    OUTPUT.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _section(title: str, rows: list[PaperSignal]) -> list[str]:
    lines = [
        f"## {title}",
        "",
        "| " + " | ".join(TABLE_FIELDS) + " |",
        "| " + " | ".join("---" for _ in TABLE_FIELDS) + " |",
    ]

    if rows:
        lines.extend(row.to_markdown_row() for row in rows)
    else:
        lines.append("| " + " | ".join("-" for _ in TABLE_FIELDS) + " |")

    lines.append("")
    return lines


def _collect_vetoes(decision: dict[str, Any]) -> list[str]:
    vetoes: list[str] = []

    for value in decision.get("vetoes") or []:
        _append_unique(vetoes, value)

    payload = decision.get("payload") or {}
    if isinstance(payload, dict):
        primary_veto = payload.get("primary_veto")
        if primary_veto:
            _append_unique(vetoes, primary_veto)

        for value in payload.get("level3_state_vetoes") or []:
            text = _text(value)
            if text and not text.startswith("level3_"):
                text = f"level3_{text}"
            _append_unique(vetoes, text)

    return vetoes


def _append_unique(values: list[str], value: Any) -> None:
    text = _text(value)
    if text and text not in values:
        values.append(text)


def _first_float(row: dict[str, Any], *names: str) -> float:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return _float_value(value)
    return 0.0


def _float_value(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return default


def _format_decimal(value: float) -> str:
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return text if text not in {"", "-0"} else "0"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _escape_markdown_cell(value: Any) -> str:
    return _text(value).replace("|", "/").replace("\r", " ").replace("\n", " ")


if __name__ == "__main__":
    main()
