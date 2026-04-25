from __future__ import annotations

from app.vnext.runtime.runner import (
    MAX_PUBLISHED_ODDS,
    MIN_PUBLISHED_ODDS,
    _published_odds_allowed,
)


def test_published_odds_guard_bounds() -> None:
    assert _published_odds_allowed(1.49) is False
    assert _published_odds_allowed(1.50) is True
    assert _published_odds_allowed(2.80) is True
    assert _published_odds_allowed(2.81) is False


def test_published_odds_guard_rejects_missing_or_invalid_values() -> None:
    assert _published_odds_allowed(None) is False
    assert _published_odds_allowed("") is False
    assert _published_odds_allowed("abc") is False


def test_published_odds_constants_match_product_rule() -> None:
    assert MIN_PUBLISHED_ODDS == 1.50
    assert MAX_PUBLISHED_ODDS == 2.80
    