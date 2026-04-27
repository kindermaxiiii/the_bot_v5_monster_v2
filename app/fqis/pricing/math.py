from __future__ import annotations


def implied_probability(odds_decimal: float) -> float:
    if odds_decimal <= 1.0:
        raise ValueError("decimal odds must be > 1.0")
    return 1.0 / odds_decimal


def compute_edge(p_real: float, p_implied: float) -> float:
    _validate_probability(p_real, "p_real")
    _validate_probability(p_implied, "p_implied")
    return p_real - p_implied


def compute_ev(p_real: float, odds_decimal: float) -> float:
    _validate_probability(p_real, "p_real")
    if odds_decimal <= 1.0:
        raise ValueError("decimal odds must be > 1.0")
    return (p_real * odds_decimal) - 1.0


def _validate_probability(value: float, name: str) -> None:
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be between 0 and 1")

        