from __future__ import annotations

from typing import Any, Mapping

import requests

from app.fqis.integrations.api_sports.cache import ApiSportsJsonCache
from app.fqis.integrations.api_sports.config import ApiSportsConfig
from app.fqis.integrations.api_sports.endpoints import ApiSportsEndpoint, normalize_endpoint
from app.fqis.integrations.api_sports.rate_limit import ApiSportsRateLimitState
from app.fqis.integrations.api_sports.schemas import ApiSportsResponse


class ApiSportsHttpError(RuntimeError):
    """Raised when API-Sports returns an HTTP/network failure."""


class ApiSportsClient:
    def __init__(
        self,
        config: ApiSportsConfig,
        session: requests.Session | None = None,
        cache: ApiSportsJsonCache | None = None,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.cache = cache or ApiSportsJsonCache(config.cache_dir)
        self.last_rate_limit_state: ApiSportsRateLimitState | None = None

    def get(
        self,
        endpoint: ApiSportsEndpoint | str,
        params: Mapping[str, Any] | None = None,
        *,
        cache_ttl_seconds: int | None = None,
    ) -> ApiSportsResponse:
        normalized = normalize_endpoint(endpoint)
        request_params = _clean_params(params or {})

        if cache_ttl_seconds is not None:
            cached = self.cache.get(normalized, request_params, ttl_seconds=cache_ttl_seconds)
            if cached is not None:
                return ApiSportsResponse.from_payload(cached)

        url = f"{self.config.base_url}/{normalized}"

        try:
            response = self.session.get(
                url,
                params=request_params,
                headers=self.config.headers,
                timeout=self.config.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise ApiSportsHttpError(f"API-Sports request failed for {normalized}: {exc}") from exc

        self.last_rate_limit_state = ApiSportsRateLimitState.from_headers(response.headers)
        self.last_rate_limit_state.assert_safe(
            min_remaining_requests=self.config.min_remaining_requests
        )

        if response.status_code >= 400:
            raise ApiSportsHttpError(
                "API-Sports HTTP error "
                f"endpoint={normalized} status={response.status_code} body={_safe_body(response)}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiSportsHttpError(f"API-Sports returned invalid JSON for {normalized}") from exc

        if not isinstance(payload, Mapping):
            raise ApiSportsHttpError(f"API-Sports returned non-object JSON for {normalized}")

        parsed = ApiSportsResponse.from_payload(payload)

        if cache_ttl_seconds is not None:
            self.cache.set(normalized, request_params, payload)

        return parsed

    def status(self) -> ApiSportsResponse:
        return self.get(ApiSportsEndpoint.STATUS, cache_ttl_seconds=60)

    def countries(self) -> ApiSportsResponse:
        return self.get(ApiSportsEndpoint.COUNTRIES, cache_ttl_seconds=86_400)

    def fixtures_by_date(self, date: str, timezone: str = "Europe/Paris") -> ApiSportsResponse:
        return self.get(
            ApiSportsEndpoint.FIXTURES,
            {"date": date, "timezone": timezone},
            cache_ttl_seconds=120,
        )

    def live_fixtures(self) -> ApiSportsResponse:
        return self.get(ApiSportsEndpoint.FIXTURES, {"live": "all"}, cache_ttl_seconds=15)

    def odds_by_date(
        self,
        date: str,
        timezone: str = "Europe/Paris",
        page: int = 1,
    ) -> ApiSportsResponse:
        return self.get(
            ApiSportsEndpoint.ODDS,
            {"date": date, "timezone": timezone, "page": page},
            cache_ttl_seconds=180,
        )

    def live_odds(self) -> ApiSportsResponse:
        return self.get(ApiSportsEndpoint.ODDS_LIVE, cache_ttl_seconds=5)


def _clean_params(params: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): value for key, value in params.items() if value is not None and value != ""}


def _safe_body(response: requests.Response) -> str:
    text = response.text or ""
    return text[:1_000]
