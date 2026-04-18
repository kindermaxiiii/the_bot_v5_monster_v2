from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from app.config import settings
from app.core.contracts import MarketProjection
from app.core.match_state import MatchState


class BoardManager:
    def __init__(self) -> None:
        self.rows_by_key: dict[str, dict[str, Any]] = {}
        self.export_dir = Path(settings.csv_export_path)
        self.export_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def reset(self) -> None:
        self.rows_by_key = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _safe_line_text(self, value: Any) -> str:
        try:
            if value is None:
                return "NA"
            return f"{float(value):.1f}"
        except (TypeError, ValueError):
            return str(value)

    def _row_key(self, state: MatchState, projection: MarketProjection) -> str:
        return "|".join(
            [
                str(getattr(state, "fixture_id", "")),
                str(getattr(projection, "market_key", "") or "").upper(),
                str(getattr(projection, "side", "") or "").upper(),
                self._safe_line_text(getattr(projection, "line", None)),
            ]
        )

    def _status_rank(self, row: dict[str, Any]) -> tuple:
        real_status = str(row.get("real_status") or "").upper()
        doc_status = str(row.get("documentary_status") or "").upper()

        top_bet = 1 if real_status == "TOP_BET" else 0
        real_valid = 1 if real_status in {"TOP_BET", "REAL_VALID"} else 0
        doc_strong = 1 if doc_status == "DOC_STRONG" else 0

        try:
            edge = float(row.get("edge") or 0.0)
        except (TypeError, ValueError):
            edge = 0.0

        try:
            ev = float(row.get("ev") or 0.0)
        except (TypeError, ValueError):
            ev = 0.0

        try:
            confidence = float(row.get("confidence_score") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0

        return (top_bet, real_valid, doc_strong, edge, ev, confidence)

    # ------------------------------------------------------------------
    # Public add/update
    # ------------------------------------------------------------------
    def add(self, state: MatchState, projection: MarketProjection) -> None:
        if projection.real_status == "NO_BET" and projection.documentary_status != "DOC_STRONG":
            return

        payload = getattr(projection, "payload", {}) or {}
        if not isinstance(payload, dict):
            payload = {}

        key = self._row_key(state, projection)

        row = {
            "fixture_id": state.fixture_id,
            "match": f"{state.home.name} vs {state.away.name}",
            "minute": state.minute,
            "score": f"{state.home_goals}-{state.away_goals}",
            "market": projection.market_key,
            "side": projection.side,
            "line": projection.line,
            "bookmaker": projection.bookmaker,
            "odds": projection.odds_decimal,
            "edge": round(float(projection.edge or 0.0), 4),
            "ev": round(float(projection.expected_value or 0.0), 4),
            "documentary_status": projection.documentary_status,
            "real_status": projection.real_status,
            "price_state": getattr(projection, "price_state", None),
            "executable": bool(getattr(projection, "executable", False)),
            "same_bookmaker": payload.get("same_bookmaker"),
            "synthetic_cross_book": payload.get("synthetic_cross_book"),
            "regime_label": payload.get("regime_label"),
            "regime_confidence": payload.get("regime_confidence"),
            "confidence_score": payload.get("display_confidence_score"),
        }

        previous = self.rows_by_key.get(key)
        if previous is None or self._status_rank(row) >= self._status_rank(previous):
            self.rows_by_key[key] = row
        else:
            # On conserve au moins le minute/score le plus récent
            previous["minute"] = row["minute"]
            previous["score"] = row["score"]
            previous["bookmaker"] = row["bookmaker"]
            previous["odds"] = row["odds"]
            previous["price_state"] = row["price_state"]
            previous["executable"] = row["executable"]
            previous["same_bookmaker"] = row["same_bookmaker"]
            previous["synthetic_cross_book"] = row["synthetic_cross_book"]
            previous["regime_label"] = row["regime_label"]
            previous["regime_confidence"] = row["regime_confidence"]
            previous["confidence_score"] = row["confidence_score"]

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------
    def snapshot_rows(self) -> list[dict[str, Any]]:
        rows = list(self.rows_by_key.values())
        rows.sort(
            key=lambda r: (
                1 if str(r.get("real_status") or "").upper() == "TOP_BET" else 0,
                1 if str(r.get("real_status") or "").upper() in {"TOP_BET", "REAL_VALID"} else 0,
                1 if str(r.get("documentary_status") or "").upper() == "DOC_STRONG" else 0,
                float(r.get("edge") or 0.0),
                float(r.get("ev") or 0.0),
                float(r.get("confidence_score") or 0.0),
            ),
            reverse=True,
        )
        return rows

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------
    def export_csv(self, filename: str = "board_v5.csv") -> Path:
        path = self.export_dir / filename
        rows = self.snapshot_rows()

        fieldnames = [
            "fixture_id",
            "match",
            "minute",
            "score",
            "market",
            "side",
            "line",
            "bookmaker",
            "odds",
            "edge",
            "ev",
            "documentary_status",
            "real_status",
            "price_state",
            "executable",
            "same_bookmaker",
            "synthetic_cross_book",
            "regime_label",
            "regime_confidence",
            "confidence_score",
        ]

        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        return path