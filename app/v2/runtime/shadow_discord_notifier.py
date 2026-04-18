from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable

from app.clients.discord import DiscordSendResult, discord_client
from app.config import settings
from app.core.match_state import MatchState


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _format_optional_float(value: Any, digits: int = 3, *, signed: bool = False) -> str:
    parsed = _safe_float(value, None)
    if parsed is None:
        return "-"
    prefix = "+" if signed and parsed >= 0 else ""
    return f"{prefix}{parsed:.{digits}f}"


def _format_line(value: Any) -> str:
    parsed = _safe_float(value, None)
    if parsed is None:
        return "-"
    return f"{parsed:.1f}"


def _goal_word(line: Any) -> str:
    parsed = _safe_float(line, None)
    if parsed is None:
        return "but"
    return "but" if parsed <= 1.5 else "buts"


@dataclass(slots=True)
class V2ShadowDiscordNotifyResult:
    sent: bool
    reason: str
    dedupe_key: str | None = None
    status_code: int | None = None
    stability_key: str | None = None
    stability_count: int = 0
    alert_tier: str | None = None


class V2ShadowDiscordNotifier:
    def __init__(
        self,
        *,
        webhook_url: str | None = None,
        send_all_board_best: bool | None = None,
        send_watchlist: bool | None = None,
        sender: Callable[[str, dict[str, Any]], DiscordSendResult] | None = None,
        required_confirmations: int = 2,
    ) -> None:
        self.webhook_url = webhook_url or os.getenv("V2_SHADOW_DISCORD_WEBHOOK") or settings.discord_webhook_doc
        self.send_all_board_best = (
            _env_bool("V2_SHADOW_DISCORD_SEND_ALL_BOARD_BEST", False)
            or _env_bool("V2_SHADOW_DISCORD_PERMISSIVE", False)
            if send_all_board_best is None
            else bool(send_all_board_best)
        )
        self.send_watchlist = (
            _env_bool("V2_SHADOW_DISCORD_SEND_WATCHLIST", True)
            if send_watchlist is None
            else bool(send_watchlist)
        )
        self.sender = sender or discord_client.send_embed
        self.required_confirmations = max(1, int(required_confirmations))
        self._last_sent_key: str | None = None
        self._last_stability_key: str | None = None
        self._stability_count: int = 0
        self._country_flags = {
            "france": "🇫🇷",
            "portugal": "🇵🇹",
            "spain": "🇪🇸",
            "germany": "🇩🇪",
            "italy": "🇮🇹",
            "england": "🇬🇧",
            "netherlands": "🇳🇱",
            "belgium": "🇧🇪",
            "turkey": "🇹🇷",
        }

    def _resolve_best_projection_context(
        self,
        payload: dict[str, Any],
        *,
        states_by_fixture: dict[int, MatchState] | None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, MatchState | None]:
        product_payload = dict(payload.get("product", {}) or {})
        board_best = dict(product_payload.get("board_best", {}) or payload.get("board_best", {}) or {})
        projection = dict(board_best.get("best_projection", {}) or {})
        if not projection:
            return None, None, None

        fixture_id = board_best.get("diagnostics", {}).get("best_fixture_id")
        match_result = next(
            (
                item
                for item in product_payload.get("match_results", []) or payload.get("match_results", []) or []
                if int(item.get("fixture_id", -1)) == int(fixture_id)
            ),
            None,
        )
        state = None if states_by_fixture is None else states_by_fixture.get(int(fixture_id))
        return projection, match_result, state

    def _home_name(self, state: MatchState | None) -> str:
        if state is None:
            return "Equipe domicile"
        return str(getattr(getattr(state, "home", None), "name", "") or "Equipe domicile")

    def _away_name(self, state: MatchState | None) -> str:
        if state is None:
            return "Equipe exterieure"
        return str(getattr(getattr(state, "away", None), "name", "") or "Equipe exterieure")

    def _match_label(self, state: MatchState | None, fixture_id: Any) -> str:
        if state is None:
            return f"Fixture {fixture_id}"
        return f"{self._home_name(state)} vs {self._away_name(state)}"

    def _minute_text(self, match_result: dict[str, Any] | None, state: MatchState | None) -> str:
        minute = None
        if match_result is not None:
            minute = match_result.get("minute")
        if minute is None and state is not None:
            minute = state.minute
        if minute is None:
            return "-"
        return f"{int(minute)}'"

    def _score_text(self, match_result: dict[str, Any] | None, state: MatchState | None) -> str:
        if state is not None:
            return state.score_text
        if match_result is not None:
            return str(match_result.get("score") or "-")
        return "-"

    def _competition_text(self, state: MatchState | None) -> str:
        if state is None:
            return "-"
        country_name = str(getattr(state, "country_name", "") or "").strip()
        competition_name = str(getattr(state, "competition_name", "") or "").strip()
        country_flag = self._country_flags.get(country_name.lower())
        if competition_name and country_flag:
            return f"{country_flag} {competition_name}"
        if country_name and competition_name:
            return f"{country_name} / {competition_name}"
        return competition_name or country_name or "-"

    def _human_reading(self, projection: dict[str, Any], state: MatchState | None) -> str:
        market_key = str(projection.get("market_key") or "").upper()
        side = str(projection.get("side") or "").upper()
        line_text = _format_line(projection.get("line"))
        goal_word = _goal_word(projection.get("line"))
        home_name = self._home_name(state)
        away_name = self._away_name(state)

        if market_key == "OU_FT":
            if side == "OVER":
                return f"Plus de {line_text} {goal_word} FT"
            if side == "UNDER":
                return f"Moins de {line_text} {goal_word} FT"

        if market_key == "OU_1H":
            if side == "OVER":
                return f"Plus de {line_text} {goal_word} 1re mi-temps"
            if side == "UNDER":
                return f"Moins de {line_text} {goal_word} 1re mi-temps"

        if market_key == "BTTS":
            if side == "YES":
                return "Les deux equipes marquent"
            if side == "NO":
                return "Les deux equipes ne marquent pas"

        if market_key == "TEAM_TOTAL":
            if side == "HOME_OVER":
                return f"{home_name} plus de {line_text} {goal_word}"
            if side == "HOME_UNDER":
                return f"{home_name} moins de {line_text} {goal_word}"
            if side == "AWAY_OVER":
                return f"{away_name} plus de {line_text} {goal_word}"
            if side == "AWAY_UNDER":
                return f"{away_name} moins de {line_text} {goal_word}"

        if market_key == "RESULT":
            if side == "HOME":
                return f"Victoire {home_name}"
            if side == "DRAW":
                return "Match nul"
            if side == "AWAY":
                return f"Victoire {away_name}"

        return f"{market_key} {side}".strip()

    def _model_market_text(self, projection: dict[str, Any]) -> str:
        market_key = str(projection.get("market_key") or "").upper()
        side = str(projection.get("side") or "").upper()
        line_text = _format_line(projection.get("line"))
        return f"{market_key} {side}" if line_text == "-" else f"{market_key} {side} {line_text}"

    def _execution_feed_code(self, projection: dict[str, Any]) -> str:
        bookmaker = str(projection.get("bookmaker") or "").strip()
        odds = _safe_float(projection.get("odds_decimal"), None)
        price_state = str(projection.get("price_state") or "").upper().strip()
        executable = bool(projection.get("executable"))

        if not bookmaker or odds is None:
            return "FEED_ONLY"
        if price_state == "VIVANT" and executable:
            return "FEED_PAIR_DETECTED"
        if price_state == "DEGRADE_MAIS_VIVANT":
            return "FEED_DEGRADED"
        return "MANUAL_NOT_CONFIRMED"

    def _feed_status_text(self, projection: dict[str, Any]) -> str:
        execution_feed_code = self._execution_feed_code(projection)
        if execution_feed_code == "FEED_PAIR_DETECTED":
            return "paire detectee dans le feed"
        if execution_feed_code == "FEED_DEGRADED":
            return "feed degrade ou incomplet"
        if execution_feed_code == "FEED_ONLY":
            return "presence feed observee seulement"
        return "detection feed non confirmee"

    def _book_feed_text(self, projection: dict[str, Any]) -> str:
        bookmaker = str(projection.get("bookmaker") or "").strip()
        odds = _safe_float(projection.get("odds_decimal"), None)
        if bookmaker and odds is not None:
            return f"{bookmaker} @ {odds:.2f}"
        if bookmaker:
            return bookmaker
        return "-"

    def _period_text(self, projection: dict[str, Any]) -> str:
        market_key = str(projection.get("market_key") or "").upper().strip()
        if market_key == "OU_1H":
            return "1re mi-temps"
        return "Fin de match"

    def _team_concerned_text(self, projection: dict[str, Any], state: MatchState | None) -> str:
        side = str(projection.get("side") or "").upper().strip()
        if side.startswith("HOME"):
            return self._home_name(state)
        if side.startswith("AWAY"):
            return self._away_name(state)
        if str(projection.get("market_key") or "").upper().strip() == "RESULT":
            if side == "HOME":
                return self._home_name(state)
            if side == "AWAY":
                return self._away_name(state)
        return "-"

    def _betify_search_hint(self, projection: dict[str, Any], state: MatchState | None) -> dict[str, str]:
        market_key = str(projection.get("market_key") or "").upper().strip()
        side = str(projection.get("side") or "").upper().strip()
        line_text = _format_line(projection.get("line"))
        goal_word = _goal_word(projection.get("line"))

        if market_key == "OU_FT":
            family = "Over/Under FT"
            probable_label = f"Plus de {line_text} {goal_word}" if side == "OVER" else f"Moins de {line_text} {goal_word}"
            return {
                "family": family,
                "probable_label": probable_label,
                "period": "Fin de match",
                "team_concerned": "-",
            }

        if market_key == "OU_1H":
            family = "Over/Under 1re mi-temps"
            probable_label = (
                f"Plus de {line_text} {goal_word} 1re mi-temps"
                if side == "OVER"
                else f"Moins de {line_text} {goal_word} 1re mi-temps"
            )
            return {
                "family": family,
                "probable_label": probable_label,
                "period": "1re mi-temps",
                "team_concerned": "-",
            }

        if market_key == "BTTS":
            probable_label = "Les deux equipes marquent (BTTS Oui)" if side == "YES" else "Les deux equipes ne marquent pas (BTTS Non)"
            return {
                "family": "BTTS",
                "probable_label": probable_label,
                "period": "Fin de match",
                "team_concerned": "-",
            }

        if market_key == "TEAM_TOTAL":
            if side == "HOME_OVER":
                probable_label = f"Total equipe domicile plus de {line_text}"
            elif side == "HOME_UNDER":
                probable_label = f"Total equipe domicile moins de {line_text}"
            elif side == "AWAY_OVER":
                probable_label = f"Total equipe exterieure plus de {line_text}"
            else:
                probable_label = f"Total equipe exterieure moins de {line_text}"
            return {
                "family": "Total equipe",
                "probable_label": probable_label,
                "period": "Fin de match",
                "team_concerned": self._team_concerned_text(projection, state),
            }

        if market_key == "RESULT":
            if side == "HOME":
                probable_label = "1X2 domicile"
            elif side == "DRAW":
                probable_label = "1X2 nul"
            else:
                probable_label = "1X2 exterieur"
            return {
                "family": "1X2",
                "probable_label": probable_label,
                "period": "Fin de match",
                "team_concerned": self._team_concerned_text(projection, state),
            }

        return {
            "family": market_key or "-",
            "probable_label": self._model_market_text(projection),
            "period": self._period_text(projection),
            "team_concerned": self._team_concerned_text(projection, state),
        }

    def _alert_tier(self, payload: dict[str, Any]) -> str:
        product_payload = dict(payload.get("product", {}) or {})
        tier = str(
            product_payload.get("shadow_alert_tier")
            or payload.get("shadow_alert_tier")
            or payload.get("shadow_governance", {}).get("shadow_alert_tier")
            or payload.get("board_best", {}).get("shadow_alert_tier")
            or "NO_BET"
        ).upper()
        return tier if tier in {"ELITE", "WATCHLIST"} else "NO_BET"

    def _alert_title(self, alert_tier: str) -> str:
        if alert_tier == "ELITE":
            return "🟡 ELITE"
        if alert_tier == "WATCHLIST":
            return "🟠 WATCHLIST"
        return "⚪ NO BET"

    def _alert_color(self, alert_tier: str) -> int:
        if alert_tier == "ELITE":
            return 0xF1C40F
        if alert_tier == "WATCHLIST":
            return 0xE67E22
        return 0x95A5A6

    def _best_match_priority(self, payload: dict[str, Any], match_result: dict[str, Any] | None) -> dict[str, Any]:
        governance_priority = payload.get("shadow_governance", {}).get("best_match_priority")
        if isinstance(governance_priority, dict) and governance_priority:
            return governance_priority
        if isinstance(match_result, dict):
            return dict(match_result.get("priority", {}) or {})
        return {}

    def _confidence_score(self, projection: dict[str, Any], priority: dict[str, Any]) -> int:
        q_match = _clamp(_safe_float(priority.get("q_match"), 0.0) / 10.0, 0.0, 1.0)
        calibrated_probability = _clamp(_safe_float(projection.get("calibrated_probability"), 0.0), 0.0, 1.0)
        edge = _safe_float(projection.get("edge"), 0.0) or 0.0
        edge_score = _clamp((edge + 0.05) / 0.35, 0.0, 1.0)
        executable_bonus = 1.0 if bool(projection.get("executable")) else 0.0
        score = 100.0 * (
            0.45 * q_match
            + 0.30 * calibrated_probability
            + 0.15 * edge_score
            + 0.10 * executable_bonus
        )
        return int(round(_clamp(score, 0.0, 100.0)))

    def _market_line_text(self, projection: dict[str, Any]) -> str:
        line_text = _format_line(projection.get("line"))
        return line_text if line_text != "-" else "Sans ligne"

    def _is_publishable_projection(self, projection: dict[str, Any]) -> bool:
        bookmaker = str(projection.get("bookmaker") or "").strip()
        odds = _safe_float(projection.get("odds_decimal"), None)
        price_state = str(projection.get("price_state") or "").upper().strip()
        executable = bool(projection.get("executable"))
        return bool(bookmaker) and odds is not None and price_state == "VIVANT" and executable

    def _stability_key(self, *, fixture_id: Any, projection: dict[str, Any]) -> str:
        return "|".join(
            [
                str(fixture_id or ""),
                str(projection.get("market_key") or "").upper(),
                str(projection.get("side") or "").upper(),
                _format_line(projection.get("line")),
            ]
        )

    def _dedupe_key(self, *, fixture_id: Any, projection: dict[str, Any], score: str, alert_tier: str) -> str:
        return "|".join([self._stability_key(fixture_id=fixture_id, projection=projection), str(score or ""), alert_tier])

    def _update_stability(self, stability_key: str) -> int:
        if stability_key == self._last_stability_key:
            self._stability_count += 1
        else:
            self._last_stability_key = stability_key
            self._stability_count = 1
        return self._stability_count

    def _should_send(self, payload: dict[str, Any], projection: dict[str, Any]) -> tuple[bool, str, str]:
        alert_tier = self._alert_tier(payload)
        if not projection:
            return False, "no_board_best_projection", alert_tier
        if not self.webhook_url:
            return False, "missing_shadow_webhook", alert_tier
        if not self._is_publishable_projection(projection):
            return False, "projection_not_publishable_for_shadow_discord", alert_tier
        if alert_tier == "ELITE":
            return True, "send_elite_shadow", alert_tier
        if alert_tier == "WATCHLIST" and (self.send_watchlist or self.send_all_board_best):
            return True, "send_watchlist_shadow", alert_tier
        if alert_tier == "WATCHLIST":
            return False, "watchlist_mode_disabled", alert_tier
        return False, "shadow_alert_tier_not_sendable", alert_tier

    def _build_embed(
        self,
        payload: dict[str, Any],
        *,
        projection: dict[str, Any],
        match_result: dict[str, Any] | None,
        state: MatchState | None,
        top_bet_guardrail: str,
        alert_tier: str,
    ) -> dict[str, Any]:
        minute_text = self._minute_text(match_result, state)
        score_text = self._score_text(match_result, state)
        match_label = self._match_label(
            state,
            (payload.get("product", {}).get("board_best", {}) or payload.get("board_best", {}) or {}).get("diagnostics", {}).get("best_fixture_id"),
        )
        competition_text = self._competition_text(state)
        search_hint = self._betify_search_hint(projection, state)
        priority = self._best_match_priority(payload, match_result)

        return {
            "title": self._alert_title(alert_tier),
            "description": (
                f"{match_label}\n"
                f"{competition_text} | {minute_text} | {score_text}\n"
                f"{self._human_reading(projection, state)}"
            )[:4096],
            "color": self._alert_color(alert_tier),
            "fields": [
                {
                    "name": "Marche",
                    "value": (
                        f"{self._human_reading(projection, state)}\n"
                        f"Ligne {self._market_line_text(projection)} | {self._model_market_text(projection)}"
                    )[:1024],
                    "inline": True,
                },
                {
                    "name": "Prix",
                    "value": (
                        f"{self._book_feed_text(projection)}\n"
                        f"{self._feed_status_text(projection)} | manuel non confirme"
                    )[:1024],
                    "inline": True,
                },
                {
                    "name": "Confiance",
                    "value": (
                        f"{self._confidence_score(projection, priority)}/100\n"
                        f"Q_match {_format_optional_float(priority.get('q_match'))} | {priority.get('priority_tier', '-')}"
                    )[:1024],
                    "inline": True,
                },
                {
                    "name": "Ou chercher",
                    "value": (
                        f"{search_hint['family']}\n"
                        f"{search_hint['probable_label']}\n"
                        f"{search_hint['period']} | {search_hint['team_concerned']}"
                    )[:1024],
                    "inline": False,
                },
                {
                    "name": "Modele",
                    "value": (
                        f"p_model {_format_optional_float(projection.get('calibrated_probability'))} | "
                        f"p_no_vig {_format_optional_float(projection.get('market_no_vig_probability'))}\n"
                        f"edge {_format_optional_float(projection.get('edge'), signed=True)} | "
                        f"EV {_format_optional_float(projection.get('expected_value'), signed=True)}"
                    )[:1024],
                    "inline": False,
                },
            ],
            "footer": {
                "text": (
                    "V2 shadow documentaire | feed detecte | disponibilite manuelle non garantie | aucune execution bookmaker"
                )[:2048]
            },
        }

    def notify_from_payload(
        self,
        payload: dict[str, Any],
        *,
        states_by_fixture: dict[int, MatchState] | None = None,
        top_bet_guardrail: str = "early_cycle_not_reliable",
    ) -> V2ShadowDiscordNotifyResult:
        projection, match_result, state = self._resolve_best_projection_context(payload, states_by_fixture=states_by_fixture)
        if projection is None:
            return V2ShadowDiscordNotifyResult(sent=False, reason="no_projection_context")

        allowed, reason, alert_tier = self._should_send(payload, projection)
        fixture_id = (payload.get("board_best", {}) or {}).get("diagnostics", {}).get("best_fixture_id")
        stability_key = self._stability_key(fixture_id=fixture_id, projection=projection)
        stability_count = self._update_stability(stability_key)

        if not allowed:
            return V2ShadowDiscordNotifyResult(
                sent=False,
                reason=reason,
                stability_key=stability_key,
                stability_count=stability_count,
                alert_tier=alert_tier,
            )

        if stability_count < self.required_confirmations:
            return V2ShadowDiscordNotifyResult(
                sent=False,
                reason="waiting_stability_confirmation",
                stability_key=stability_key,
                stability_count=stability_count,
                alert_tier=alert_tier,
            )

        score = self._score_text(match_result, state)
        dedupe_key = self._dedupe_key(fixture_id=fixture_id, projection=projection, score=score, alert_tier=alert_tier)
        if dedupe_key == self._last_sent_key:
            return V2ShadowDiscordNotifyResult(
                sent=False,
                reason="duplicate_board_best",
                dedupe_key=dedupe_key,
                stability_key=stability_key,
                stability_count=stability_count,
                alert_tier=alert_tier,
            )

        embed = self._build_embed(
            payload,
            projection=projection,
            match_result=match_result,
            state=state,
            top_bet_guardrail=top_bet_guardrail,
            alert_tier=alert_tier,
        )
        send_result = self.sender(self.webhook_url, embed)
        if not send_result.ok:
            return V2ShadowDiscordNotifyResult(
                sent=False,
                reason=send_result.error or "discord_send_failed",
                dedupe_key=dedupe_key,
                status_code=send_result.status_code,
                stability_key=stability_key,
                stability_count=stability_count,
                alert_tier=alert_tier,
            )

        self._last_sent_key = dedupe_key
        return V2ShadowDiscordNotifyResult(
            sent=True,
            reason="sent",
            dedupe_key=dedupe_key,
            status_code=send_result.status_code,
            stability_key=stability_key,
            stability_count=stability_count,
            alert_tier=alert_tier,
        )
