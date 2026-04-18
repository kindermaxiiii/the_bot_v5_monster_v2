from __future__ import annotations

import time
from math import isfinite
from typing import Any

from app.config import settings
from app.core.contracts import MarketProjection
from app.core.match_state import MatchState


class GovernanceEngine:
    """
    Governance V8.

    Objectifs :
    - pas de réémission immédiate de même thèse
    - pas de lockout permanent du fixture entier
    - support d'un cooldown fixture réel
    - historique purgé proprement
    - distinction claire :
        * actif du cycle
        * historique dispatché
    """

    def __init__(self) -> None:
        self.active_by_match: dict[int, list[MarketProjection]] = {}
        self._sent_theses_by_fixture: dict[int, dict[str, dict[str, Any]]] = {}
        self._sent_families_by_fixture: dict[int, dict[str, dict[str, Any]]] = {}
        self._sent_fixture_meta: dict[int, dict[str, Any]] = {}
        self._cycle_index: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start_cycle(self, cycle_index: int | None = None) -> None:
        self.active_by_match = {}
        self._purge_old_history()
        if cycle_index is not None:
            self._cycle_index = int(cycle_index)

    # ------------------------------------------------------------------
    # Safe helpers
    # ------------------------------------------------------------------
    def _f(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            value = float(value)
            if not isfinite(value):
                return default
            return value
        except (TypeError, ValueError):
            return default

    def _i(self, value: Any, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _s(self, value: Any, default: str = "") -> str:
        if value is None:
            return default
        text = str(value).strip()
        return text if text else default

    def _now(self) -> float:
        return time.time()

    # ------------------------------------------------------------------
    # Projection keys
    # ------------------------------------------------------------------
    def _market_key(self, projection: MarketProjection) -> str:
        return self._s(getattr(projection, "market_key", None), "").upper()

    def _side(self, projection: MarketProjection) -> str:
        return self._s(getattr(projection, "side", None), "").upper()

    def _line(self, projection: MarketProjection) -> float | None:
        try:
            line = getattr(projection, "line", None)
            return None if line is None else float(line)
        except (TypeError, ValueError):
            return None

    def _minute(self, state: MatchState) -> int:
        return self._i(getattr(state, "minute", None), 0)

    def _fixture_id(self, state: MatchState) -> int:
        return self._i(getattr(state, "fixture_id", None), 0)

    def _is_real(self, projection: MarketProjection) -> bool:
        return self._s(getattr(projection, "real_status", None), "").upper() in {"REAL_VALID", "TOP_BET"}

    def _family_key(self, projection: MarketProjection) -> str:
        return self._market_key(projection)

    def _thesis_key(self, projection: MarketProjection) -> str:
        line = self._line(projection)
        line_text = "NA" if line is None else f"{line:.1f}"
        return f"{self._market_key(projection)}|{self._side(projection)}|{line_text}"

    def _payload(self, projection: MarketProjection) -> dict[str, Any]:
        payload = getattr(projection, "payload", None)
        return payload if isinstance(payload, dict) else {}

    def _score_snapshot(self, state: MatchState) -> str:
        home_goals = self._i(getattr(state, "home_goals", None), 0)
        away_goals = self._i(getattr(state, "away_goals", None), 0)
        home_reds = self._i(getattr(state, "home_reds", None), 0)
        away_reds = self._i(getattr(state, "away_reds", None), 0)
        return f"score={home_goals}-{away_goals}|reds={home_reds}-{away_reds}"

    def _state_snapshot_key(self, state: MatchState, projection: MarketProjection) -> str:
        payload = self._payload(projection)
        explicit = self._s(payload.get("repeat_state_key"), "")
        if explicit:
            return explicit

        regime_label = self._s(payload.get("regime_label"), "-").upper()
        price_state = self._s(getattr(projection, "price_state", None), "").upper() or self._s(
            payload.get("price_state"),
            "-",
        ).upper()
        return f"{self._score_snapshot(state)}|regime={regime_label}|price={price_state}"

    def _repeat_thesis_key(self, state: MatchState, projection: MarketProjection) -> str:
        return f"fixture={self._fixture_id(state)}|{self._thesis_key(projection)}|{self._score_snapshot(state)}"

    def _repeat_family_state_key(self, state: MatchState, projection: MarketProjection) -> str:
        payload = self._payload(projection)
        explicit = self._s(payload.get("repeat_family_state_key"), "")
        if explicit:
            return explicit
        return f"fixture={self._fixture_id(state)}|{self._family_key(projection)}|{self._state_snapshot_key(state, projection)}"

    # ------------------------------------------------------------------
    # Purge
    # ------------------------------------------------------------------
    def _purge_old_history(self) -> None:
        """
        Purge simple mémoire :
        - on garde environ 6 heures d'historique dispatch
        - suffisant pour un bot live local sans lock mémoire infini
        """
        ttl_seconds = 6 * 3600
        now = self._now()

        fixture_ids = set(self._sent_fixture_meta.keys()) | set(self._sent_theses_by_fixture.keys()) | set(self._sent_families_by_fixture.keys())
        stale_fixtures: list[int] = []

        for fixture_id in fixture_ids:
            meta = self._sent_fixture_meta.get(fixture_id, {})
            last_ts = self._f(meta.get("sent_ts"), 0.0)
            if last_ts <= 0.0:
                continue
            if now - last_ts > ttl_seconds:
                stale_fixtures.append(fixture_id)

        for fixture_id in stale_fixtures:
            self._sent_fixture_meta.pop(fixture_id, None)
            self._sent_theses_by_fixture.pop(fixture_id, None)
            self._sent_families_by_fixture.pop(fixture_id, None)

    # ------------------------------------------------------------------
    # Cooldown checks
    # ------------------------------------------------------------------
    def _fixture_recently_sent(self, state: MatchState, projection: MarketProjection) -> bool:
        fixture_id = self._fixture_id(state)
        minute = self._minute(state)
        now = self._now()

        meta = self._sent_fixture_meta.get(fixture_id)
        if not meta:
            return False

        current_state_key = self._state_snapshot_key(state, projection)
        stored_state_key = self._s(meta.get("state_key"), "")
        if stored_state_key and stored_state_key != current_state_key:
            return False
        if not stored_state_key:
            return False

        # 1) temps réel
        cooldown_minutes = max(0, self._i(getattr(settings, "fixture_cooldown_minutes", 0), 0))
        sent_ts = self._f(meta.get("sent_ts"), 0.0)
        if cooldown_minutes > 0 and sent_ts > 0.0:
            if now - sent_ts <= cooldown_minutes * 60:
                return True

        # 2) filet match-minute supplémentaire
        sent_match_minute = self._i(meta.get("minute"), -999)
        if abs(minute - sent_match_minute) <= cooldown_minutes:
            return True

        return False

    def _same_thesis_recently_sent(self, state: MatchState, projection: MarketProjection) -> bool:
        fixture_id = self._fixture_id(state)
        minute = self._minute(state)
        thesis_key = self._thesis_key(projection)
        repeat_key = self._repeat_thesis_key(state, projection)

        sent_theses = self._sent_theses_by_fixture.setdefault(fixture_id, {})
        if thesis_key not in sent_theses:
            return False

        row = sent_theses[thesis_key]
        if self._s(row.get("repeat_key"), "") != repeat_key:
            return False

        last_minute = self._i(row.get("minute"), -999)
        block = max(0, self._i(getattr(settings, "same_signal_block_minutes", 0), 0))
        return abs(minute - last_minute) <= block

    def _same_family_recently_sent(self, state: MatchState, projection: MarketProjection) -> bool:
        fixture_id = self._fixture_id(state)
        minute = self._minute(state)
        family_key = self._family_key(projection)
        repeat_key = self._repeat_family_state_key(state, projection)

        sent_families = self._sent_families_by_fixture.setdefault(fixture_id, {})
        if family_key not in sent_families:
            return False

        row = sent_families[family_key]
        if self._s(row.get("repeat_key"), "") != repeat_key:
            return False

        last_minute = self._i(row.get("minute"), -999)
        block = max(0, self._i(getattr(settings, "same_family_block_minutes", 0), 0))
        return abs(minute - last_minute) <= block

    # ------------------------------------------------------------------
    # Main allow
    # ------------------------------------------------------------------
    def allow(self, state: MatchState, projection: MarketProjection) -> tuple[bool, str | None]:
        fixture_id = self._fixture_id(state)
        active = self.active_by_match.setdefault(fixture_id, [])

        # cap global par match dans le cycle
        if len(active) >= settings.max_tickets_per_match:
            return False, "max_tickets_per_match"

        # cap réel par match dans le cycle
        if self._is_real(projection):
            real_active = sum(1 for row in active if self._is_real(row))
            if real_active >= settings.max_real_tickets_per_match:
                return False, "max_real_tickets_per_match"

        # doublons intra-cycle
        for row in active:
            if self._repeat_thesis_key(state, row) == self._repeat_thesis_key(state, projection):
                return False, "duplicate_thesis_same_cycle"
            if self._repeat_family_state_key(state, row) == self._repeat_family_state_key(state, projection):
                return False, "duplicate_family_same_cycle"

        # cooldown fixture
        if self._fixture_recently_sent(state, projection):
            return False, "fixture_recently_alerted"

        # même thèse récemment envoyée
        if self._same_thesis_recently_sent(state, projection):
            return False, "duplicate_thesis_recently_sent"

        # même famille récemment envoyée
        if self._same_family_recently_sent(state, projection):
            return False, "same_family_recently_sent"

        # filtre chaos pour le réel
        chaos = self._f((getattr(projection, "payload", {}) or {}).get("chaos"), 0.0)
        if self._is_real(projection) and chaos > settings.max_chaos_real:
            return False, "chaos_too_high"

        return True, None

    # ------------------------------------------------------------------
    # Register / dispatch
    # ------------------------------------------------------------------
    def register(self, state: MatchState, projection: MarketProjection) -> None:
        fixture_id = self._fixture_id(state)
        self.active_by_match.setdefault(fixture_id, []).append(projection)

    def mark_dispatched(self, state: MatchState, projection: MarketProjection) -> None:
        fixture_id = self._fixture_id(state)
        minute = self._minute(state)
        now = self._now()

        thesis_key = self._thesis_key(projection)
        family_key = self._family_key(projection)
        repeat_thesis_key = self._repeat_thesis_key(state, projection)
        repeat_family_state_key = self._repeat_family_state_key(state, projection)
        state_snapshot = self._state_snapshot_key(state, projection)

        self._sent_theses_by_fixture.setdefault(fixture_id, {})[thesis_key] = {
            "minute": minute,
            "cycle": self._cycle_index,
            "real": self._is_real(projection),
            "sent_ts": now,
            "repeat_key": repeat_thesis_key,
            "state_key": state_snapshot,
        }

        self._sent_families_by_fixture.setdefault(fixture_id, {})[family_key] = {
            "minute": minute,
            "cycle": self._cycle_index,
            "real": self._is_real(projection),
            "sent_ts": now,
            "repeat_key": repeat_family_state_key,
            "state_key": state_snapshot,
        }

        self._sent_fixture_meta[fixture_id] = {
            "minute": minute,
            "cycle": self._cycle_index,
            "real": self._is_real(projection),
            "sent_ts": now,
            "state_key": state_snapshot,
        }
