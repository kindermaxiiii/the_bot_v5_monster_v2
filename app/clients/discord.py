from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DiscordSendResult:
    ok: bool
    status_code: int | None = None
    response_json: dict[str, Any] | None = None
    error: str | None = None


class DiscordWebhookError(RuntimeError):
    pass


class DiscordClient:
    def __init__(self) -> None:
        self.session = requests.Session()

    # ------------------------------------------------------------------
    # HTTP robustness
    # ------------------------------------------------------------------
    def _timeout(self) -> float:
        try:
            return float(getattr(settings, "discord_timeout_seconds", 12) or 12)
        except (TypeError, ValueError):
            return 12.0

    def _retry_attempts(self) -> int:
        try:
            return max(1, int(getattr(settings, "discord_retry_attempts", 3) or 3))
        except (TypeError, ValueError):
            return 3

    def _retry_backoff(self) -> float:
        try:
            return float(getattr(settings, "discord_retry_backoff_seconds", 1.5) or 1.5)
        except (TypeError, ValueError):
            return 1.5

    def _rate_limit_cooldown(self) -> float:
        try:
            return float(getattr(settings, "discord_rate_limit_cooldown_seconds", 2.5) or 2.5)
        except (TypeError, ValueError):
            return 2.5

    def _post(self, webhook_url: str, payload: dict[str, Any]) -> DiscordSendResult:
        if not webhook_url:
            return DiscordSendResult(ok=False, error="missing_webhook_url")

        attempts = self._retry_attempts()
        timeout = self._timeout()
        backoff = self._retry_backoff()
        cooldown = self._rate_limit_cooldown()

        last_error: str | None = None
        last_status: int | None = None
        last_json: dict[str, Any] | None = None

        for attempt in range(1, attempts + 1):
            try:
                response = self.session.post(
                    webhook_url + "?wait=true",
                    json=payload,
                    timeout=timeout,
                )
                last_status = response.status_code

                if response.status_code == 429:
                    retry_after = cooldown
                    try:
                        data = response.json()
                        last_json = data if isinstance(data, dict) else None
                        retry_after = float(data.get("retry_after", cooldown))
                    except Exception:
                        pass

                    last_error = "429 rate limited"
                    logger.warning(
                        "discord rate limited attempt=%s retry_after=%.2fs",
                        attempt,
                        retry_after,
                    )
                    time.sleep(max(cooldown, retry_after))
                    continue

                response.raise_for_status()

                try:
                    data = response.json()
                    last_json = data if isinstance(data, dict) else None
                except Exception:
                    last_json = None

                return DiscordSendResult(
                    ok=True,
                    status_code=response.status_code,
                    response_json=last_json,
                    error=None,
                )

            except requests.RequestException as exc:
                last_error = str(exc)
                logger.error("discord send failed attempt=%s error=%s", attempt, exc)
                if attempt < attempts:
                    time.sleep(backoff * attempt)

        return DiscordSendResult(
            ok=False,
            status_code=last_status,
            response_json=last_json,
            error=last_error or "unknown_discord_error",
        )

    # ------------------------------------------------------------------
    # Basic send methods
    # ------------------------------------------------------------------
    def send_message(self, webhook_url: str, content: str) -> DiscordSendResult:
        content = (content or "").strip()
        if not content:
            return DiscordSendResult(ok=False, error="empty_content")
        return self._post(webhook_url, {"content": content[:1900]})

    def send_embed(self, webhook_url: str, embed: dict[str, Any]) -> DiscordSendResult:
        if not embed:
            return DiscordSendResult(ok=False, error="empty_embed")
        return self._post(webhook_url, {"embeds": [embed]})

    # ------------------------------------------------------------------
    # Safe helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_str(value: Any, default: str = "-") -> str:
        if value is None:
            return default
        text = str(value).strip()
        return text if text else default

    @staticmethod
    def _safe_float(value: Any, default: float | None = None) -> float | None:
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _is_http_url(value: Any) -> bool:
        if not value:
            return False
        text = str(value).strip().lower()
        return text.startswith("http://") or text.startswith("https://")

    def _first_attr(self, obj: Any, *names: str, default: Any = None) -> Any:
        for name in names:
            if hasattr(obj, name):
                value = getattr(obj, name)
                if value is not None:
                    return value
        return default

    def _payload(self, projection: Any) -> dict[str, Any]:
        payload = self._first_attr(projection, "payload", default={}) or {}
        return payload if isinstance(payload, dict) else {}

    # ------------------------------------------------------------------
    # Core formatting helpers
    # ------------------------------------------------------------------
    def _confidence_to_score(self, projection: Any) -> float | None:
        payload = self._payload(projection)

        raw = payload.get("display_confidence_score")
        score = self._safe_float(raw, None)
        if score is not None:
            if score <= 1.0:
                score *= 10.0
            return max(0.0, min(10.0, score))

        score = self._safe_float(
            self._first_attr(
                projection,
                "confidence_score",
                "confidence",
                "model_confidence",
                default=None,
            ),
            None,
        )
        if score is not None:
            if score <= 1.0:
                score *= 10.0
            return max(0.0, min(10.0, score))

        edge = self._safe_float(self._first_attr(projection, "edge", default=None), None)
        if edge is None:
            return None

        edge_pp = edge * 100.0
        if edge_pp >= 25:
            return 9.0
        if edge_pp >= 18:
            return 8.0
        if edge_pp >= 12:
            return 7.0
        if edge_pp >= 8:
            return 6.0
        if edge_pp >= 4:
            return 5.0
        return 4.0

    def _confidence_to_text(self, projection: Any) -> str:
        score = self._confidence_to_score(projection)
        return "n/d" if score is None else f"{score:.1f}/10"

    def _confidence_emoji(self, projection: Any) -> str:
        score = self._confidence_to_score(projection)
        if score is None:
            return "📈"
        if score >= 8.5:
            return "🔥"
        if score >= 7.0:
            return "✅"
        if score >= 5.5:
            return "⚠️"
        return "📉"

    def _selection_label(self, projection: Any) -> str:
        market_key = self._safe_str(self._first_attr(projection, "market_key", default="")).upper()
        side = self._safe_str(self._first_attr(projection, "side", default="Selection")).upper()
        line = self._first_attr(projection, "line", default=None)

        line_text = ""
        if line is not None:
            try:
                line_text = f" {float(line):.1f}"
            except (TypeError, ValueError):
                line_text = f" {line}"

        if market_key == "OU_FT":
            if "UNDER" in side:
                return f"Under{line_text} Fin de match"
            if "OVER" in side:
                return f"Over{line_text} Fin de match"

        if market_key == "OU_1H":
            if "UNDER" in side:
                return f"Under{line_text} 1re mi-temps"
            if "OVER" in side:
                return f"Over{line_text} 1re mi-temps"

        if market_key == "BTTS":
            if "NO" in side:
                return "Les deux équipes marquent — Non"
            if "YES" in side:
                return "Les deux équipes marquent — Oui"

        if market_key == "TEAM_TOTAL":
            if "HOME_OVER" in side:
                return f"Total équipe domicile — Over{line_text}"
            if "HOME_UNDER" in side:
                return f"Total équipe domicile — Under{line_text}"
            if "AWAY_OVER" in side:
                return f"Total équipe extérieure — Over{line_text}"
            if "AWAY_UNDER" in side:
                return f"Total équipe extérieure — Under{line_text}"

        if market_key in {"RESULT", "1X2"}:
            if side in {"HOME", "1", "HOME_WIN"}:
                return "Résultat du match — Domicile"
            if side in {"DRAW", "X"}:
                return "Résultat du match — Nul"
            if side in {"AWAY", "2", "AWAY_WIN"}:
                return "Résultat du match — Extérieur"

        if market_key == "CORRECT_SCORE":
            return f"Score exact — {self._safe_str(self._first_attr(projection, 'selection_name', default=side))}"

        return self._safe_str(side.title())

    def _market_family_emoji(self, projection: Any) -> str:
        market_key = self._safe_str(self._first_attr(projection, "market_key", default="")).upper()
        return {
            "OU_FT": "🎯",
            "OU_1H": "⏱️",
            "BTTS": "🤝",
            "TEAM_TOTAL": "🏁",
            "RESULT": "🏆",
            "1X2": "🏆",
            "CORRECT_SCORE": "🧩",
        }.get(market_key, "📊")

    def _badge(self, projection: Any, channel_kind: str) -> str:
        real_status = self._safe_str(self._first_attr(projection, "real_status", default="")).upper()
        if real_status == "TOP_BET" or bool(self._first_attr(projection, "top_bet", "top_bet_flag", default=False)):
            return "🏆 TOP BET"
        if channel_kind == "real":
            return "✅ BET LIVE"
        if channel_kind == "doc":
            return "🧪 DOC STRONG"
        return "🧾 LOG"

    def _color_for_projection(self, projection: Any, channel_kind: str) -> int:
        if channel_kind == "doc":
            return 0xF1C40F
        if channel_kind == "logs":
            return 0x95A5A6

        side = self._safe_str(self._first_attr(projection, "side", default="")).upper()
        if "UNDER" in side or "NO" in side:
            return 0x2ECC71
        if "OVER" in side or "YES" in side:
            return 0x3498DB
        return 0x1ABC9C

    def _clean_reason(self, text: str) -> str:
        t = str(text or "").strip()
        if not t:
            return ""

        replacements = {
            "regime=": "régime=",
            "minute=": "minute=",
            "current_total=": "score_total=",
            "score_total=": "score_total=",
            "fair_prob=": "proba_modèle=",
            "fair_odds=": "cote_juste=",
            "haircut=": "ajustement=",
            "line=": "ligne=",
            "side=": "côté=",
            "struct_mult=": "structure=",
            "same_bookmaker=True": "same-book=oui",
            "same_bookmaker=False": "same-book=non",
        }
        for old, new in replacements.items():
            t = t.replace(old, new)

        return t

    def _reasons_lines(self, projection: Any) -> list[str]:
        reasons = list(self._first_attr(projection, "reasons", default=[]) or [])
        cleaned: list[str] = []
        for reason in reasons[:6]:
            text = self._clean_reason(reason)
            if text:
                cleaned.append(f"• {text}")
        return cleaned[:4] if cleaned else ["• aucun détail"]

    def _league_name(self, state: Any) -> str:
        return self._safe_str(
            self._first_attr(state, "league_name", "competition_name", default="-"),
            "-",
        )

    def _home_name(self, state: Any) -> str:
        return self._safe_str(
            self._first_attr(state, "home_team_name", default=None)
            or self._first_attr(self._first_attr(state, "home", default=None), "name", default="Home"),
            "Home",
        )

    def _away_name(self, state: Any) -> str:
        return self._safe_str(
            self._first_attr(state, "away_team_name", default=None)
            or self._first_attr(self._first_attr(state, "away", default=None), "name", default="Away"),
            "Away",
        )

    def _match_name(self, state: Any) -> str:
        return f"{self._home_name(state)} vs {self._away_name(state)}"

    def _live_state_text(self, state: Any) -> str:
        minute = self._safe_str(self._first_attr(state, "minute", default="-"), "-")
        home_goals = self._safe_str(self._first_attr(state, "home_goals", default="?"), "?")
        away_goals = self._safe_str(self._first_attr(state, "away_goals", default="?"), "?")
        return f"{minute}' • {home_goals}-{away_goals}"

    def _execution_label(self, projection: Any) -> str:
        payload = self._payload(projection)
        same_book = payload.get("same_bookmaker")
        synthetic = payload.get("synthetic_cross_book")
        price_state = self._safe_str(self._first_attr(projection, "price_state", default="-"))

        if same_book is True:
            return f"Same-book réel ✅\nÉtat : {price_state}"
        if synthetic is True:
            return f"Cross-book analytique ⚠️\nÉtat : {price_state}"
        return f"État : {price_state}"

    def _price_block(self, projection: Any) -> str:
        bookmaker = self._safe_str(self._first_attr(projection, "bookmaker", default="-"))
        odds = self._safe_float(self._first_attr(projection, "odds_decimal", default=None), None)
        odds_text = "-" if odds is None else f"{odds:.2f}"
        execution_text = self._execution_label(projection)
        return f"Book : {bookmaker}\nCote : {odds_text}\n{execution_text}"

    def _value_block(self, projection: Any) -> str:
        payload = self._payload(projection)

        edge = self._safe_float(self._first_attr(projection, "edge", default=None), None)
        ev = self._safe_float(self._first_attr(projection, "expected_value", default=None), None)
        fair_odds = self._safe_float(payload.get("fair_odds"), None)

        edge_text = "-" if edge is None else f"{edge * 100:.1f} pts"
        ev_text = "-" if ev is None else f"{ev * 100:.1f}%"
        fair_text = "-" if fair_odds is None else f"{fair_odds:.2f}"

        return f"Edge : {edge_text}\nEV : {ev_text}\nCote juste : {fair_text}"

    def _model_block(self, projection: Any) -> str:
        payload = self._payload(projection)

        regime = self._safe_str(payload.get("regime_label"), "-")
        regime_conf = self._safe_float(payload.get("regime_confidence"), None)
        cal_conf = self._safe_float(payload.get("calibration_confidence"), None)
        feed_quality = self._safe_float(payload.get("feed_quality"), None)

        regime_conf_text = "-" if regime_conf is None else f"{regime_conf:.2f}"
        cal_conf_text = "-" if cal_conf is None else f"{cal_conf:.2f}"
        feed_text = "-" if feed_quality is None else f"{feed_quality:.2f}"

        return (
            f"Régime : {regime}\n"
            f"Conf. régime : {regime_conf_text}\n"
            f"Conf. calib. : {cal_conf_text}\n"
            f"Feed quality : {feed_text}"
        )

    def _status_block(self, projection: Any) -> str:
        real_status = self._safe_str(self._first_attr(projection, "real_status", default="-"))
        doc_status = self._safe_str(self._first_attr(projection, "documentary_status", default="-"))
        confidence = self._confidence_to_text(projection)
        return f"Réel : {real_status}\nDoc : {doc_status}\nConfiance : {confidence}"

    def _truth_block(self, state: Any) -> str:
        truth_source = self._safe_str(self._first_attr(state, "truth_source", default="state_fields"))
        used_truth = self._first_attr(state, "used_truth", default={}) or {}
        if not isinstance(used_truth, dict):
            used_truth = {}

        minute = self._safe_str(used_truth.get("minute", self._first_attr(state, "minute", default="-")), "-")
        home_goals = self._safe_str(used_truth.get("home_goals", self._first_attr(state, "home_goals", default="?")), "?")
        away_goals = self._safe_str(used_truth.get("away_goals", self._first_attr(state, "away_goals", default="?")), "?")

        label = "Vérité corrigée" if truth_source == "used_truth" else "État direct"
        return f"Source : {label}\nLive retenu : {minute}' • {home_goals}-{away_goals}"

    # ------------------------------------------------------------------
    # Embed builder
    # ------------------------------------------------------------------
    def build_projection_embed(self, state: Any, projection: Any, channel_kind: str = "real") -> dict[str, Any]:
        badge = self._badge(projection, channel_kind)
        market_emoji = self._market_family_emoji(projection)
        confidence_emoji = self._confidence_emoji(projection)

        league_name = self._league_name(state)
        home_name = self._home_name(state)
        away_name = self._away_name(state)
        live_state = self._live_state_text(state)
        selection = self._selection_label(projection)
        reasons_lines = self._reasons_lines(projection)

        league_logo = self._first_attr(state, "league_logo", "competition_logo", default=None)
        home_logo = self._first_attr(state, "home_team_logo", default=None)
        away_logo = self._first_attr(state, "away_team_logo", default=None)

        title = f"{badge} • {market_emoji} {selection}"
        description = (
            f"**🏟️ Ligue :** {league_name}\n"
            f"**⚔️ Match :** {home_name} vs {away_name}\n"
            f"**⏱️ Live :** {live_state}\n"
            f"**{confidence_emoji} Lecture :** {selection}"
        )[:4096]

        fields = [
            {
                "name": "💸 Prix & exécution",
                "value": self._price_block(projection)[:1024],
                "inline": True,
            },
            {
                "name": "📐 Value",
                "value": self._value_block(projection)[:1024],
                "inline": True,
            },
            {
                "name": "🧠 Modèle",
                "value": self._model_block(projection)[:1024],
                "inline": True,
            },
            {
                "name": "🧾 Statut",
                "value": self._status_block(projection)[:1024],
                "inline": True,
            },
            {
                "name": "🔥 Raisons clés",
                "value": "\n".join(reasons_lines)[:1024],
                "inline": False,
            },
        ]

        truth_source = self._safe_str(self._first_attr(state, "truth_source", default="state_fields"))
        if truth_source == "used_truth":
            fields.append(
                {
                    "name": "🛡️ Vérité live utilisée",
                    "value": self._truth_block(state)[:1024],
                    "inline": False,
                }
            )

        embed: dict[str, Any] = {
            "title": title[:256],
            "description": description,
            "color": self._color_for_projection(projection, channel_kind),
            "fields": fields,
        }

        if self._is_http_url(league_logo):
            embed["author"] = {
                "name": league_name[:256],
                "icon_url": str(league_logo),
            }
        else:
            embed["author"] = {
                "name": league_name[:256],
            }

        if self._is_http_url(home_logo):
            embed["thumbnail"] = {"url": str(home_logo)}

        footer_text = f"{home_name} vs {away_name}"[:2048]
        if self._is_http_url(away_logo):
            embed["footer"] = {
                "text": footer_text,
                "icon_url": str(away_logo),
            }
        else:
            embed["footer"] = {
                "text": footer_text,
            }

        return embed

    # ------------------------------------------------------------------
    # Public card send
    # ------------------------------------------------------------------
    def send_projection_card(
        self,
        webhook_url: str,
        state: Any,
        projection: Any,
        channel_kind: str = "real",
    ) -> DiscordSendResult:
        try:
            embed = self.build_projection_embed(state, projection, channel_kind=channel_kind)
            return self.send_embed(webhook_url, embed)
        except Exception as exc:
            logger.exception("discord embed build failed; fallback text send error=%s", exc)

            text = (
                f"{self._badge(projection, channel_kind)}\n"
                f"🏟️ Ligue : {self._league_name(state)}\n"
                f"⚔️ Match : {self._match_name(state)}\n"
                f"⏱️ Live : {self._live_state_text(state)}\n"
                f"{self._market_family_emoji(projection)} Sélection : {self._selection_label(projection)}\n"
                f"{self._confidence_emoji(projection)} Confiance : {self._confidence_to_text(projection)}\n"
                f"🧠 Raisons : {' | '.join(x.replace('• ', '') for x in self._reasons_lines(projection))}"
            )
            return self.send_message(webhook_url, text)


discord_client = DiscordClient()