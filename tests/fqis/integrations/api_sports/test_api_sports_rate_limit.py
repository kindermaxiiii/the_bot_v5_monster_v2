import pytest

from app.fqis.integrations.api_sports.rate_limit import (
    ApiSportsRateLimitError,
    ApiSportsRateLimitState,
)


def test_rate_limit_from_headers():
    state = ApiSportsRateLimitState.from_headers(
        {
            "x-ratelimit-requests-limit": "75000",
            "x-ratelimit-requests-remaining": "74999",
            "x-ratelimit-limit": "300",
            "x-ratelimit-remaining": "299",
        }
    )

    assert state.requests_limit == 75000
    assert state.requests_remaining == 74999
    assert state.per_minute_limit == 300
    assert state.per_minute_remaining == 299


def test_rate_limit_blocks_low_daily_remaining():
    state = ApiSportsRateLimitState(requests_remaining=99)

    with pytest.raises(ApiSportsRateLimitError):
        state.assert_safe(min_remaining_requests=100)


def test_rate_limit_blocks_per_minute_zero():
    state = ApiSportsRateLimitState(per_minute_remaining=0)

    with pytest.raises(ApiSportsRateLimitError):
        state.assert_safe(min_remaining_requests=100)


def test_rate_limit_ignores_missing_headers():
    state = ApiSportsRateLimitState.from_headers({})

    state.assert_safe(min_remaining_requests=100)
