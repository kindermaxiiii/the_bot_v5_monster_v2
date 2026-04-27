import pytest

from app.fqis.integrations.api_sports.endpoints import (
    ApiSportsEndpoint,
    MARKET_PRIORITIES,
    normalize_endpoint,
)


def test_normalize_endpoint_enum():
    assert normalize_endpoint(ApiSportsEndpoint.FIXTURES) == "fixtures"


def test_normalize_endpoint_string():
    assert normalize_endpoint("/odds/live/") == "odds/live"


def test_normalize_endpoint_rejects_empty():
    with pytest.raises(ValueError):
        normalize_endpoint("/")


def test_market_priorities_order():
    names = [item.name for item in MARKET_PRIORITIES]

    assert names[:5] == [
        "Over / Under match",
        "Over / Under mi-temps",
        "BTTS",
        "Team totals",
        "1X2",
    ]
