import math

import pytest

from app.fqis.pricing.math import compute_edge, compute_ev, implied_probability


def test_implied_probability() -> None:
    assert math.isclose(implied_probability(2.0), 0.5, rel_tol=1e-9)


def test_compute_edge() -> None:
    assert math.isclose(compute_edge(0.60, 0.50), 0.10, rel_tol=1e-9)


def test_compute_ev() -> None:
    assert math.isclose(compute_ev(0.60, 2.0), 0.20, rel_tol=1e-9)


def test_implied_probability_rejects_invalid_odds() -> None:
    with pytest.raises(ValueError):
        implied_probability(1.0)


def test_compute_ev_rejects_invalid_probability() -> None:
    with pytest.raises(ValueError):
        compute_ev(1.2, 2.0)