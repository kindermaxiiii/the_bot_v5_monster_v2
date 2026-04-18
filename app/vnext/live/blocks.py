from __future__ import annotations

from statistics import mean

from app.vnext.live.events import detect_break_events
from app.vnext.live.models import (
    LiveBalanceBlock,
    LiveContextPack,
    LivePressureBlock,
    LiveSnapshot,
    LiveStateBlock,
    LiveThreatBlock,
)
from app.vnext.live.timebands import classify_time_band


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _norm(value: int | float | None, *, scale: float) -> float:
    if value is None or scale <= 0:
        return 0.0
    return _clip(float(value) / scale)


def _time_weight(time_band: str) -> float:
    if time_band == "EARLY":
        return 0.08
    if time_band == "MID":
        return 0.16
    return 0.28


def _leading_side(snapshot: LiveSnapshot) -> str:
    if snapshot.home_goals > snapshot.away_goals:
        return "HOME"
    if snapshot.away_goals > snapshot.home_goals:
        return "AWAY"
    return "DRAW"


def build_live_threat(snapshot: LiveSnapshot) -> LiveThreatBlock:
    home = (
        _norm(snapshot.home_xg, scale=1.8) * 0.40
        + _norm(snapshot.home_shots_on, scale=7.0) * 0.28
        + _norm(snapshot.home_shots_total, scale=18.0) * 0.12
        + _norm(snapshot.home_dangerous_attacks, scale=75.0) * 0.20
    )
    away = (
        _norm(snapshot.away_xg, scale=1.8) * 0.40
        + _norm(snapshot.away_shots_on, scale=7.0) * 0.28
        + _norm(snapshot.away_shots_total, scale=18.0) * 0.12
        + _norm(snapshot.away_dangerous_attacks, scale=75.0) * 0.20
    )
    return LiveThreatBlock(
        home_threat_raw=round(home, 4),
        away_threat_raw=round(away, 4),
        threat_edge=round(home - away, 4),
    )


def build_live_pressure(snapshot: LiveSnapshot) -> LivePressureBlock:
    home = (
        _norm(snapshot.home_dangerous_attacks, scale=80.0) * 0.35
        + _norm(snapshot.home_attacks, scale=120.0) * 0.18
        + _norm(snapshot.home_corners, scale=10.0) * 0.17
        + _norm(snapshot.home_shots_total, scale=18.0) * 0.15
        + _norm(snapshot.home_possession, scale=100.0) * 0.15
    )
    away = (
        _norm(snapshot.away_dangerous_attacks, scale=80.0) * 0.35
        + _norm(snapshot.away_attacks, scale=120.0) * 0.18
        + _norm(snapshot.away_corners, scale=10.0) * 0.17
        + _norm(snapshot.away_shots_total, scale=18.0) * 0.15
        + _norm(snapshot.away_possession, scale=100.0) * 0.15
    )
    return LivePressureBlock(
        home_pressure_raw=round(home, 4),
        away_pressure_raw=round(away, 4),
        pressure_edge=round(home - away, 4),
    )


def build_live_state(snapshot: LiveSnapshot) -> LiveStateBlock:
    time_band = classify_time_band(snapshot.minute)
    leading_side = _leading_side(snapshot)
    score_diff = snapshot.home_goals - snapshot.away_goals
    state_weight = _time_weight(time_band)
    score_signal = _clip(score_diff / 3.0, low=-1.0, high=1.0)
    red_signal = _clip((snapshot.away_red_cards - snapshot.home_red_cards) / 2.0, low=-1.0, high=1.0)
    home_raw = _clip(0.5 + ((score_signal * state_weight) + (red_signal * 0.20)))
    away_raw = _clip(0.5 - ((score_signal * state_weight) + (red_signal * 0.20)))
    coherence = mean(
        [
            1.0 if 0 <= snapshot.minute <= 130 else 0.0,
            1.0 if snapshot.home_goals >= 0 and snapshot.away_goals >= 0 else 0.0,
            1.0 if snapshot.home_red_cards >= 0 and snapshot.away_red_cards >= 0 else 0.0,
            snapshot.live_snapshot_quality_score,
        ]
    )
    return LiveStateBlock(
        time_band=time_band,
        leading_side=leading_side,  # type: ignore[arg-type]
        score_diff=score_diff,
        home_state_raw=round(home_raw, 4),
        away_state_raw=round(away_raw, 4),
        state_edge=round(home_raw - away_raw, 4),
        state_coherence_score=round(_clip(coherence), 4),
    )


def build_live_balance(
    snapshot: LiveSnapshot,
    threat: LiveThreatBlock,
    pressure: LivePressureBlock,
    state: LiveStateBlock,
) -> LiveBalanceBlock:
    state_weight = _time_weight(state.time_band)
    home = _clip(
        (threat.home_threat_raw * 0.42)
        + (pressure.home_pressure_raw * 0.36)
        + (state.home_state_raw * state_weight)
        + (_clip((snapshot.away_red_cards - snapshot.home_red_cards) / 2.0, low=-1.0, high=1.0) * 0.10)
    )
    away = _clip(
        (threat.away_threat_raw * 0.42)
        + (pressure.away_pressure_raw * 0.36)
        + (state.away_state_raw * state_weight)
        + (_clip((snapshot.home_red_cards - snapshot.away_red_cards) / 2.0, low=-1.0, high=1.0) * 0.10)
    )
    return LiveBalanceBlock(
        home_balance_raw=round(home, 4),
        away_balance_raw=round(away, 4),
        balance_edge=round(home - away, 4),
    )


def build_live_context_pack(
    current_snapshot: LiveSnapshot,
    previous_snapshot: LiveSnapshot | None = None,
) -> LiveContextPack:
    threat = build_live_threat(current_snapshot)
    pressure = build_live_pressure(current_snapshot)
    state = build_live_state(current_snapshot)
    balance = build_live_balance(current_snapshot, threat, pressure, state)
    break_events = detect_break_events(current_snapshot, previous_snapshot)
    return LiveContextPack(
        current_snapshot=current_snapshot,
        previous_snapshot=previous_snapshot,
        threat=threat,
        pressure=pressure,
        balance=balance,
        state=state,
        break_events=break_events,
    )
