from __future__ import annotations

import math


def poisson_pmf(k: int, lambda_: float) -> float:
    _validate_lambda(lambda_)

    if k < 0:
        return 0.0

    return math.exp(-lambda_) * (lambda_**k) / math.factorial(k)


def poisson_cdf(k: int, lambda_: float) -> float:
    _validate_lambda(lambda_)

    if k < 0:
        return 0.0

    return sum(poisson_pmf(i, lambda_) for i in range(k + 1))


def poisson_tail_gt(k: int, lambda_: float) -> float:
    _validate_lambda(lambda_)

    if k < 0:
        return 1.0

    return max(0.0, 1.0 - poisson_cdf(k, lambda_))


def truncated_poisson_probabilities(lambda_: float, max_k: int) -> tuple[float, ...]:
    _validate_lambda(lambda_)

    if max_k < 0:
        raise ValueError("max_k must be >= 0")

    probabilities = [poisson_pmf(k, lambda_) for k in range(max_k)]

    tail_probability = max(0.0, 1.0 - sum(probabilities))
    probabilities.append(tail_probability)

    return tuple(probabilities)


def _validate_lambda(lambda_: float) -> None:
    if lambda_ < 0:
        raise ValueError("lambda must be >= 0")
    if not math.isfinite(lambda_):
        raise ValueError("lambda must be finite")

        