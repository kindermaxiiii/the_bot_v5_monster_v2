from __future__ import annotations

import logging
import time
from types import SimpleNamespace
from typing import Any

from app.clients.discord import discord_client
from app.config import settings
from app.core.contracts import MarketProjection
from app.core.match_state import MatchState

logger = logging.getLogger(__name__)


def _safe_str(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _projection_confidence(proj: MarketProjection) -> float:
    payload = getattr(proj, "payload", None) or {}
    raw = payload.get("display_confidence_score")
    try:
        if raw is not None:
            return max(0.0, min(10.0, float(raw)))
    except (TypeError, ValueError):
        pass
    return 0.0


def _display_state(state: MatchState) -> SimpleNamespace:
    raw = getattr(state, "raw", None) or {}
    used_truth = raw.get("used_truth", {}) if isinstance(raw, dict) else {}
    if not isinstance(used_truth, dict):
        used_truth = {}

    minute = used_truth.get("minute", getattr(state, "minute", None))
    phase = used_truth.get("phase", getattr(state, "phase", None))
    status = used_truth.get("status", getattr(state, "status", None))
    home_goals = used_truth.get("home_goals", getattr(state, "home_goals", None))
    away_goals = used_truth.get("away_goals", getattr(state, "away_goals", None))

    return SimpleNamespace(
        fixture_id=getattr(state, "fixture_id", None),
        minute=minute,
        phase=phase,
        status=status,
        home_goals=home_goals,
        away_goals=away_goals,
        score_text=f"{home_goals}-{away_goals}" if home_goals is not None and away_goals is not None else getattr(state, "score_text", None),
        league_name=getattr(state, "competition_name", None) or raw.get("league_name"),
        league_logo=getattr(state, "competition_logo", None) or raw.get("league_logo"),
        country_name=getattr(state, "country_name", None) or raw.get("country_name"),
        home_team_name=getattr(getattr(state, "home", None), "name", None),
        away_team_name=getattr(getattr(state, "away", None), "name", None),
        home_team_logo=getattr(getattr(state, "home", None), "logo", None),
        away_team_logo=getattr(getattr(state, "away", None), "logo", None),
        used_truth=used_truth,
        truth_source="used_truth" if used_truth else "state_fields",
    )


def _display_projection(proj: MarketProjection) -> SimpleNamespace:
    payload = getattr(proj, "payload", None) or {}
    if not isinstance(payload, dict):
        payload = {}

    return SimpleNamespace(
        market_key=getattr(proj, "market_key", None),
        side=getattr(proj, "side", None),
        line=getattr(proj, "line", None),
        edge=getattr(proj, "edge", None),
        expected_value=getattr(proj, "expected_value", None),
        bookmaker=getattr(proj, "bookmaker", None),
        odds_decimal=getattr(proj, "odds_decimal", None),
        price_state=getattr(proj, "price_state", None),
        documentary_status=getattr(proj, "documentary_status", None),
        real_status=getattr(proj, "real_status", None),
        top_bet_flag=getattr(proj, "top_bet_flag", None),
        reasons=list(getattr(proj, "reasons", []) or []),
        vetoes=list(getattr(proj, "vetoes", []) or []),
        payload=payload,
        confidence_score=_projection_confidence(proj),
    )


class Dispatcher:
    def __init__(self) -> None:
        self._recent_dispatches: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Dedupe
    # ------------------------------------------------------------------
    def _dedupe_ttl_seconds(self) -> int:
        raw = getattr(settings, "dispatch_dedupe_seconds", 300)
        try:
            return max(30, int(raw))
        except (TypeError, ValueError):
            return 300

    def _purge_old(self) -> None:
        now = time.time()
        ttl = self._dedupe_ttl_seconds()
        stale = [k for k, ts in self._recent_dispatches.items() if now - ts > ttl]
        for k in stale:
            self._recent_dispatches.pop(k, None)

    def _dispatch_key(self, state: MatchState, proj: MarketProjection, channel_kind: str) -> str:
        fixture_id = _safe_str(getattr(state, "fixture_id", None), "na")
        market_key = _safe_str(getattr(proj, "market_key", None), "na").upper()
        side = _safe_str(getattr(proj, "side", None), "na").upper()
        line = getattr(proj, "line", None)
        line_text = "na" if line is None else str(line)
        return f"{channel_kind}|{fixture_id}|{market_key}|{side}|{line_text}"

    def _already_sent_recently(self, state: MatchState, proj: MarketProjection, channel_kind: str) -> bool:
        self._purge_old()
        return self._dispatch_key(state, proj, channel_kind) in self._recent_dispatches

    def _mark_sent(self, state: MatchState, proj: MarketProjection, channel_kind: str) -> None:
        self._recent_dispatches[self._dispatch_key(state, proj, channel_kind)] = time.time()

    # ------------------------------------------------------------------
    # Eligibility
    # ------------------------------------------------------------------
    def _eligible_for_real(self, proj: MarketProjection) -> tuple[bool, str]:
        if not settings.discord_webhook_real:
            return False, "missing_real_webhook"

        real_status = str(getattr(proj, "real_status", "") or "").upper()
        if real_status not in {"REAL_VALID", "TOP_BET"}:
            return False, "projection_not_real"

        if getattr(settings, "require_executable_for_real", True) and not bool(getattr(proj, "executable", False)):
            return False, "projection_not_executable"

        if _safe_float(getattr(proj, "odds_decimal", None), 0.0) <= 1.0:
            return False, "invalid_odds"

        return True, "ok"

    def _eligible_for_doc(self, proj: MarketProjection) -> tuple[bool, str]:
        if not getattr(settings, "allow_documentary_dispatch", False):
            return False, "documentary_dispatch_disabled"

        if not settings.discord_webhook_doc:
            return False, "missing_doc_webhook"

        doc_status = str(getattr(proj, "documentary_status", "") or "").upper()
        if doc_status != "DOC_STRONG":
            return False, "projection_not_doc_strong"

        if getattr(settings, "documentary_requires_executable", True) and not bool(getattr(proj, "executable", False)):
            return False, "projection_not_executable_for_doc"

        return True, "ok"

    # ------------------------------------------------------------------
    # Result handling
    # ------------------------------------------------------------------
    def _send_result_ok(self, result: Any) -> bool:
        if result is None:
            return False
        if isinstance(result, bool):
            return result
        ok = getattr(result, "ok", None)
        if isinstance(ok, bool):
            return ok
        return False

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------
    def dispatch(self, state: MatchState, projection: MarketProjection) -> bool:
        try:
            view_state = _display_state(state)
            view_projection = _display_projection(projection)

            real_ok, real_reason = self._eligible_for_real(projection)
            doc_ok, doc_reason = self._eligible_for_doc(projection)

            if real_ok:
                if self._already_sent_recently(state, projection, "real"):
                    logger.info(
                        "dispatch_skip_recent channel=real fixture_id=%s market=%s side=%s line=%s score=%s truth_source=%s",
                        getattr(state, "fixture_id", None),
                        getattr(projection, "market_key", None),
                        getattr(projection, "side", None),
                        getattr(projection, "line", None),
                        getattr(view_state, "score_text", None),
                        getattr(view_state, "truth_source", None),
                    )
                    return False

                result = discord_client.send_projection_card(
                    settings.discord_webhook_real,
                    view_state,
                    view_projection,
                    channel_kind="real",
                )

                if self._send_result_ok(result):
                    self._mark_sent(state, projection, "real")
                    logger.info(
                        "dispatch_sent channel=real fixture_id=%s market=%s side=%s line=%s score=%s minute=%s truth_source=%s",
                        getattr(state, "fixture_id", None),
                        getattr(projection, "market_key", None),
                        getattr(projection, "side", None),
                        getattr(projection, "line", None),
                        getattr(view_state, "score_text", None),
                        getattr(view_state, "minute", None),
                        getattr(view_state, "truth_source", None),
                    )
                    return True

                logger.warning(
                    "dispatch_failed channel=real fixture_id=%s market=%s side=%s line=%s score=%s minute=%s truth_source=%s",
                    getattr(state, "fixture_id", None),
                    getattr(projection, "market_key", None),
                    getattr(projection, "side", None),
                    getattr(projection, "line", None),
                    getattr(view_state, "score_text", None),
                    getattr(view_state, "minute", None),
                    getattr(view_state, "truth_source", None),
                )
                return False

            if doc_ok:
                if self._already_sent_recently(state, projection, "doc"):
                    logger.info(
                        "dispatch_skip_recent channel=doc fixture_id=%s market=%s side=%s line=%s score=%s truth_source=%s",
                        getattr(state, "fixture_id", None),
                        getattr(projection, "market_key", None),
                        getattr(projection, "side", None),
                        getattr(projection, "line", None),
                        getattr(view_state, "score_text", None),
                        getattr(view_state, "truth_source", None),
                    )
                    return False

                result = discord_client.send_projection_card(
                    settings.discord_webhook_doc,
                    view_state,
                    view_projection,
                    channel_kind="doc",
                )

                if self._send_result_ok(result):
                    self._mark_sent(state, projection, "doc")
                    logger.info(
                        "dispatch_sent channel=doc fixture_id=%s market=%s side=%s line=%s score=%s minute=%s truth_source=%s",
                        getattr(state, "fixture_id", None),
                        getattr(projection, "market_key", None),
                        getattr(projection, "side", None),
                        getattr(projection, "line", None),
                        getattr(view_state, "score_text", None),
                        getattr(view_state, "minute", None),
                        getattr(view_state, "truth_source", None),
                    )
                    return True

                logger.warning(
                    "dispatch_failed channel=doc fixture_id=%s market=%s side=%s line=%s score=%s minute=%s truth_source=%s",
                    getattr(state, "fixture_id", None),
                    getattr(projection, "market_key", None),
                    getattr(projection, "side", None),
                    getattr(projection, "line", None),
                    getattr(view_state, "score_text", None),
                    getattr(view_state, "minute", None),
                    getattr(view_state, "truth_source", None),
                )
                return False

            logger.info(
                "dispatch_ineligible fixture_id=%s market=%s side=%s line=%s real_reason=%s doc_reason=%s real_status=%s doc_status=%s confidence=%.1f executable=%s truth_source=%s",
                getattr(state, "fixture_id", None),
                getattr(projection, "market_key", None),
                getattr(projection, "side", None),
                getattr(projection, "line", None),
                real_reason,
                doc_reason,
                getattr(projection, "real_status", None),
                getattr(projection, "documentary_status", None),
                _projection_confidence(projection),
                getattr(projection, "executable", None),
                getattr(view_state, "truth_source", None),
            )
            return False

        except Exception:
            logger.exception(
                "dispatch_exception fixture_id=%s market=%s side=%s line=%s score=%s minute=%s",
                getattr(state, "fixture_id", None),
                getattr(projection, "market_key", None),
                getattr(projection, "side", None),
                getattr(projection, "line", None),
                getattr(state, "score_text", None),
                getattr(state, "minute", None),
            )
            return False