from __future__ import annotations

import math

import pytest

from app.fqis.probability.poisson import (
    poisson_cdf,
    poisson_pmf,
    poisson_tail_gt,
    truncated_poisson_probabilities,
)


def test_poisson_pmf_known_values() -> None:
    assert math.isclose(poisson_pmf(0, 2.0), math.exp(-2.0), rel_tol=1e-12)
    assert math.isclose(poisson_pmf(1, 2.0), 2.0 * math.exp(-2.0), rel_tol=1e-12)
    assert math.isclose(poisson_pmf(2, 2.0), 2.0 * math.exp(-2.0), rel_tol=1e-12)


def test_poisson_cdf_and_tail_are_complementary() -> None:
    cdf = poisson_cdf(2, 1.4)
    tail = poisson_tail_gt(2, 1.4)

    assert math.isclose(cdf + tail, 1.0, rel_tol=1e-12)


def test_truncated_poisson_probabilities_sum_to_one() -> None:
    probabilities = truncated_poisson_probabilities(1.8, 10)

    assert len(probabilities) == 11
    assert math.isclose(sum(probabilities), 1.0, rel_tol=1e-12)


def test_zero_lambda_places_all_mass_at_zero() -> None:
    probabilities = truncated_poisson_probabilities(0.0, 5)

    assert probabilities[0] == 1.0
    assert sum(probabilities[1:]) == 0.0


def test_negative_lambda_is_rejected() -> None:
    with pytest.raises(ValueError):
        poisson_pmf(0, -0.1)


def test_negative_k_has_zero_pmf() -> None:
    assert poisson_pmf(-1, 1.0) == 0.0

    