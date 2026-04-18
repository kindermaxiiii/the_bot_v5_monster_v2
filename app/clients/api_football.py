from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import requests

from app.config import settings

logger = logging.getLogger(__name__)


class APIFootballRequestError(RuntimeError):
    pass


class APIFootballClient:
    def __init__(self) -> None:
        self.base_url = settings.api_football_base_url.rstrip("/")
        self.session = requests.Session()
        self._endpoint_cooldowns: dict[str, float] = {}
        self.session.headers.update(
            {
                settings.api_football_key_header: settings.api_football_key,
                "Accept": "application/json",
                "User-Agent": "THE-BOT-V5/Institutional",
            }
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _timeout(self) -> float:
        try:
            return float(getattr(settings, "api_timeout_seconds", 20) or 20)
        except (TypeError, ValueError):
            return 20.0

    def _max_retries(self) -> int:
        try:
            return max(1, int(getattr(settings, "api_retry_attempts", 3) or 3))
        except (TypeError, ValueError):
            return 3

    def _backoff(self, attempt: int) -> float:
        try:
            base = float(getattr(settings, "api_retry_backoff_seconds", 1.5) or 1.5)
        except (TypeError, ValueError):
            base = 1.5
        return max(0.5, base * attempt)

    def _endpoint_name(self, path: str) -> str:
        if path == "/fixtures/statistics":
            return "statistics"
        if path == "/odds/live":
            return "odds_live"
        return path.strip("/") or "default"

    def _cooldown_for_endpoint(self, endpoint_name: str) -> float:
        try:
            if endpoint_name == "statistics":
                value = float(getattr(settings, "api_rate_limit_cooldown_statistics_seconds", 25.0) or 25.0)
            elif endpoint_name == "odds_live":
                value = float(getattr(settings, "api_rate_limit_cooldown_odds_seconds", 10.0) or 10.0)
            else:
                value = float(getattr(settings, "api_rate_limit_cooldown_seconds", 25.0) or 25.0)
            return max(5.0, value)
        except (TypeError, ValueError):
            return 25.0

    def _raise_if_missing_key(self) -> None:
        if not settings.api_football_key:
            raise APIFootballRequestError("API_FOOTBALL_KEY missing")

    def _json_or_none(self, response: requests.Response) -> dict[str, Any] | None:
        try:
            payload = response.json()
            return payload if isinstance(payload, dict) else None
        except Exception:
            return None

    def _response_snippet(self, response: requests.Response, max_len: int = 240) -> str:
        try:
            text = (response.text or "").strip()
        except Exception:
            text = ""
        if not text:
            return ""
        return text[:max_len]

    def _extract_retry_after(self, response: requests.Response) -> float:
        header_value = response.headers.get("Retry-After")
        if header_value:
            try:
                wait_s = float(header_value)
                if wait_s > 1000:
                    wait_s = wait_s / 1000.0
                return max(0.5, min(wait_s, 20.0))
            except (TypeError, ValueError):
                pass

        payload = self._json_or_none(response) or {}
        retry_after = payload.get("retry_after")
        if retry_after is not None:
            try:
                wait_s = float(retry_after)
                if wait_s > 1000:
                    wait_s = wait_s / 1000.0
                return max(0.5, min(wait_s, 20.0))
            except (TypeError, ValueError):
                pass

        return 2.5

    def _rate_limit_error_text(self, value: Any) -> str:
        return str(value or "").strip().lower()

    def _is_rate_limit_error(self, value: Any) -> bool:
        text = self._rate_limit_error_text(value)
        return "ratelimit" in text or "too many requests" in text or "exceeded the limit" in text

    def _mark_endpoint_cooldown(self, endpoint_name: str, wait_s: float | None = None) -> float:
        cooldown = max(self._cooldown_for_endpoint(endpoint_name), float(wait_s or 0.0))
        self._endpoint_cooldowns[endpoint_name] = max(
            self._endpoint_cooldowns.get(endpoint_name, 0.0),
            time.monotonic() + cooldown,
        )
        return cooldown

    def _check_endpoint_cooldown(self, endpoint_name: str) -> None:
        until = self._endpoint_cooldowns.get(endpoint_name)
        if until is None:
            return

        remaining = until - time.monotonic()
        if remaining <= 0:
            self._endpoint_cooldowns.pop(endpoint_name, None)
            return

        raise APIFootballRequestError(f"API-Football cooldown active for {endpoint_name} ({remaining:.1f}s)")

    def _log_quota_headers(self, response: requests.Response, path: str) -> None:
        remaining = response.headers.get("X-RateLimit-Remaining") or response.headers.get("x-ratelimit-remaining")
        limit = response.headers.get("X-RateLimit-Limit") or response.headers.get("x-ratelimit-limit")
        if remaining or limit:
            logger.debug(
                "api quota path=%s remaining=%s limit=%s",
                path,
                remaining,
                limit,
            )

    def _safe_fixture_id(self, fixture_id: int) -> int:
        try:
            value = int(fixture_id)
        except (TypeError, ValueError) as exc:
            raise APIFootballRequestError(f"invalid fixture_id: {fixture_id}") from exc
        if value <= 0:
            raise APIFootballRequestError(f"invalid fixture_id: {fixture_id}")
        return value

    def _request_once(
        self,
        path: str,
        endpoint_name: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        response = self.session.get(url, params=params or {}, timeout=self._timeout())
        self._log_quota_headers(response, url)

        if response.status_code == 429:
            wait_s = self._extract_retry_after(response)
            cooldown = self._mark_endpoint_cooldown(endpoint_name, wait_s)
            logger.warning(
                "api-football rate limited endpoint=%s path=%s wait=%.2fs cooldown=%.2fs",
                endpoint_name,
                path,
                wait_s,
                cooldown,
            )
            raise APIFootballRequestError(f"API-Football rate limited (429) cooldown={cooldown:.1f}s")

        if 500 <= response.status_code <= 599:
            snippet = self._response_snippet(response)
            raise APIFootballRequestError(
                f"API-Football temporary server error ({response.status_code}) body={snippet}"
            )

        if response.status_code >= 400:
            payload = self._json_or_none(response) or {}
            snippet = self._response_snippet(response)
            errors = payload.get("errors")
            message = payload.get("message")
            raise APIFootballRequestError(
                f"API-Football client error ({response.status_code}) errors={errors} message={message} body={snippet}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            snippet = self._response_snippet(response)
            raise APIFootballRequestError(f"Invalid JSON payload body={snippet}") from exc

        if not isinstance(payload, dict):
            raise APIFootballRequestError("Unexpected API payload type")

        errors = payload.get("errors")
        if errors:
            if self._is_rate_limit_error(errors):
                cooldown = self._mark_endpoint_cooldown(endpoint_name)
                raise APIFootballRequestError(f"API payload rate limited cooldown={cooldown:.1f}s")
            raise APIFootballRequestError(f"API payload errors: {errors}")

        return payload

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._raise_if_missing_key()
        endpoint_name = self._endpoint_name(path)
        self._check_endpoint_cooldown(endpoint_name)

        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        attempts = self._max_retries()

        for attempt in range(1, attempts + 1):
            try:
                return self._request_once(path, endpoint_name, url, params)
            except (requests.Timeout, requests.ConnectionError, APIFootballRequestError) as exc:
                last_exc = exc

                retriable = True
                text = str(exc).lower()
                if (
                    "client error (400)" in text
                    or "client error (401)" in text
                    or "client error (403)" in text
                    or "client error (404)" in text
                    or "cooldown active" in text
                    or "rate limited" in text
                ):
                    retriable = False

                if not retriable or attempt >= attempts:
                    log_fn = logger.warning if ("cooldown active" in text or "rate limited" in text) else logger.error
                    log_fn(
                        "api request failed path=%s params=%s attempt=%s/%s error=%s",
                        path,
                        params,
                        attempt,
                        attempts,
                        exc,
                    )
                    break

                sleep_s = self._backoff(attempt)
                logger.warning(
                    "api request retry path=%s params=%s attempt=%s/%s sleep=%.1fs error=%s",
                    path,
                    params,
                    attempt,
                    attempts,
                    sleep_s,
                    exc,
                )
                time.sleep(sleep_s)

            except requests.HTTPError as exc:
                last_exc = exc
                logger.error(
                    "api http error path=%s params=%s attempt=%s/%s error=%s",
                    path,
                    params,
                    attempt,
                    attempts,
                    exc,
                )
                break

        raise APIFootballRequestError(f"API request failed for {path}: {last_exc}")

    # ------------------------------------------------------------------
    # Public endpoints
    # ------------------------------------------------------------------
    def get_live_fixtures(self) -> Dict[str, Any]:
        payload = self._get("/fixtures", {"live": "all"})
        live_limit = getattr(settings, "live_fetch_limit", 0)
        try:
            live_limit = int(live_limit or 0)
        except (TypeError, ValueError):
            live_limit = 0

        if live_limit > 0:
            response_rows = payload.get("response", []) or []
            if isinstance(response_rows, list):
                payload = dict(payload)
                payload["response"] = response_rows[:live_limit]
        return payload

    def get_live_odds(self, fixture_id: int, bet_id: Optional[int] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"fixture": self._safe_fixture_id(fixture_id)}
        if bet_id is not None:
            params["bet"] = int(bet_id)
        return self._get("/odds/live", params)

    def get_fixture(self, fixture_id: int) -> Dict[str, Any]:
        return self._get("/fixtures", {"id": self._safe_fixture_id(fixture_id)})

    def get_fixtures_by_ids(self, fixture_ids: list[int]) -> Dict[str, Any]:
        clean_ids: list[int] = []
        for fid in fixture_ids:
            try:
                clean_ids.append(self._safe_fixture_id(fid))
            except APIFootballRequestError:
                continue

        if not clean_ids:
            return {"response": []}

        chunks: list[list[int]] = []
        chunk_size = 20
        for i in range(0, len(clean_ids), chunk_size):
            chunks.append(clean_ids[i : i + chunk_size])

        merged = {"response": []}
        for chunk in chunks:
            joined = "-".join(str(fid) for fid in chunk)
            payload = self._get("/fixtures", {"ids": joined})
            merged["response"].extend(payload.get("response", []) or [])
        return merged

    def get_fixture_statistics(self, fixture_id: int) -> Dict[str, Any]:
        return self._get("/fixtures/statistics", {"fixture": self._safe_fixture_id(fixture_id)})

    def get_team_fixtures(
        self,
        team_id: int,
        *,
        season: int,
        league_id: int | None = None,
        last: int | None = None,
        status: str | None = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"team": int(team_id), "season": int(season)}
        if league_id is not None:
            params["league"] = int(league_id)
        if last is not None:
            params["last"] = int(last)
        if status:
            params["status"] = str(status)
        return self._get("/fixtures", params)

    def get_fixture_players(self, fixture_id: int) -> Dict[str, Any]:
        return self._get("/fixtures/players", {"fixture": self._safe_fixture_id(fixture_id)})

    def get_fixture_lineups(self, fixture_id: int) -> Dict[str, Any]:
        return self._get("/fixtures/lineups", {"fixture": self._safe_fixture_id(fixture_id)})

    def get_live_bets_reference(self) -> Dict[str, Any]:
        return self._get("/odds/live/bets")

    def get_bookmakers_reference(self) -> Dict[str, Any]:
        return self._get("/odds/bookmakers")

    def get_leagues_reference(self) -> Dict[str, Any]:
        return self._get("/leagues", {"current": "true"})


api_client = APIFootballClient()
