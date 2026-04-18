from __future__ import annotations

from math import exp, factorial, isfinite, lgamma, sqrt
from typing import Dict


def clamp(value: float, low: float, high: float) -> float:
    if value < low:
        return low
    if value > high:
        return high
    return value


def safe_prob(value: float, floor: float = 1e-6, ceil: float = 1.0 - 1e-6) -> float:
    if not isfinite(value):
        return floor
    return clamp(value, floor, ceil)


def safe_non_negative(value: float) -> float:
    if not isfinite(value):
        return 0.0
    return max(0.0, value)


def poisson_pmf(k: int, lam: float) -> float:
    """
    Stable Poisson PMF.
    Uses lgamma for better numerical behavior than factorial on larger k.
    """
    if k < 0:
        return 0.0
    lam = safe_non_negative(lam)
    if lam == 0.0:
        return 1.0 if k == 0 else 0.0
    return exp(-lam + k * __import__("math").log(lam) - lgamma(k + 1))


def poisson_cdf(k: int, lam: float) -> float:
    if k < 0:
        return 0.0
    lam = safe_non_negative(lam)
    return clamp(sum(poisson_pmf(i, lam) for i in range(k + 1)), 0.0, 1.0)


def poisson_sf(k: int, lam: float) -> float:
    """
    Survival function: P(X > k)
    """
    return clamp(1.0 - poisson_cdf(k, lam), 0.0, 1.0)


def poisson_at_least_one(lam: float) -> float:
    lam = safe_non_negative(lam)
    if lam >= 20:
        return 1.0
    return 1.0 - exp(-lam)


def poisson_exactly_zero(lam: float) -> float:
    lam = safe_non_negative(lam)
    if lam >= 20:
        return 0.0
    return exp(-lam)


def poisson_exactly_one(lam: float) -> float:
    lam = safe_non_negative(lam)
    if lam >= 20:
        return 0.0
    return lam * exp(-lam)


def poisson_exactly_two(lam: float) -> float:
    lam = safe_non_negative(lam)
    if lam >= 20:
        return 0.0
    return (lam * lam / 2.0) * exp(-lam)


def truncated_poisson_distribution(lam: float, max_k: int) -> Dict[int, float]:
    """
    Returns a 0..max_k distribution where the last bucket absorbs the tail.
    """
    lam = safe_non_negative(lam)
    max_k = max(0, int(max_k))

    if max_k == 0:
        return {0: 1.0}

    out: Dict[int, float] = {}
    cumulative = 0.0

    for k in range(max_k):
        p = max(0.0, poisson_pmf(k, lam))
        out[k] = p
        cumulative += p

    out[max_k] = max(0.0, 1.0 - cumulative)

    total = sum(out.values()) or 1.0
    return {k: v / total for k, v in out.items()}


def adaptive_poisson_cap(lam: float, minute: int | None = None, floor_cap: int = 6, ceil_cap: int = 12) -> int:
    """
    Useful for scoreline distributions. The later the match, the smaller the useful tail.
    """
    lam = safe_non_negative(lam)
    minute = int(minute or 0)

    raw = lam + 4.0 * sqrt(max(lam, 0.10))
    if minute >= 80:
        raw -= 1.0
    elif minute <= 25:
        raw += 1.0

    return int(clamp(round(raw), floor_cap, ceil_cap))
