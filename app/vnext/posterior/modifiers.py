from __future__ import annotations

from app.vnext.live.models import LiveContextPack
from app.vnext.posterior.models import ScenarioModifier


HOME_KEYS = {"HOME_CONTROL", "HOME_ATTACKING_BIAS", "HOME_DEFENSIVE_HOLD_BIAS"}
AWAY_KEYS = {"AWAY_CONTROL", "AWAY_ATTACKING_BIAS", "AWAY_DEFENSIVE_HOLD_BIAS"}
BALANCED_KEYS = {"OPEN_BALANCED", "CAGEY_BALANCED", "DUAL_SCORING_BIAS"}


def _clip(value: float, *, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _state_weight(time_band: str) -> float:
    if time_band == "EARLY":
        return 0.08
    if time_band == "MID":
        return 0.14
    return 0.22


def _balanced_closeness(edge: float) -> float:
    return 1.0 - min(abs(edge), 1.0)


def _convergence_bonus(*signals: float) -> float:
    positive = sum(1 for signal in signals if signal > 0.06)
    negative = sum(1 for signal in signals if signal < -0.06)
    dominant = max(positive, negative)
    if dominant >= 3:
        return 0.10
    if dominant == 2:
        return 0.05
    return 0.0


def _balanced_signal(*edges: float) -> float:
    closeness = sum(_balanced_closeness(edge) for edge in edges) / len(edges)
    max_abs_edge = max(abs(edge) for edge in edges)
    return (closeness - 0.72) - (max_abs_edge * 0.18)


def _flags(context: LiveContextPack) -> tuple[str, ...]:
    flags = []
    events = context.break_events
    if events.goal_scored:
        flags.append("goal_scored")
    if events.home_goal_scored:
        flags.append("home_goal_scored")
    if events.away_goal_scored:
        flags.append("away_goal_scored")
    if events.equalizer_event:
        flags.append("equalizer_event")
    if events.lead_change_event:
        flags.append("lead_change_event")
    if events.two_goal_gap_event:
        flags.append("two_goal_gap_event")
    if events.red_card_occurred:
        flags.append("red_card_occurred")
    if events.home_red_card:
        flags.append("home_red_card")
    if events.away_red_card:
        flags.append("away_red_card")
    return tuple(flags)


def scenario_live_alignment(key: str, context: LiveContextPack) -> tuple[float, tuple[str, ...]]:
    threat_edge = context.threat.threat_edge
    pressure_edge = context.pressure.pressure_edge
    balance_edge = context.balance.balance_edge
    state_edge = context.state.state_edge
    state_weight = _state_weight(context.state.time_band)
    openness = (
        context.threat.home_threat_raw
        + context.threat.away_threat_raw
        + context.pressure.home_pressure_raw
        + context.pressure.away_pressure_raw
    ) / 4.0
    low_tempo = 1.0 - openness
    balanced_env = _balanced_signal(threat_edge, pressure_edge, balance_edge)
    suppress_away = 1.0 - context.threat.away_threat_raw
    suppress_home = 1.0 - context.threat.home_threat_raw
    home_convergence = _convergence_bonus(threat_edge, pressure_edge, balance_edge)
    away_convergence = _convergence_bonus(-threat_edge, -pressure_edge, -balance_edge)
    balanced_state_bonus = 0.08 if context.state.leading_side == "DRAW" else -0.12
    state_neutrality = _balanced_closeness(state_edge) - 0.78
    home_hold_lead_bonus = 0.10 if context.state.leading_side == "HOME" else (-0.02 if context.state.leading_side == "DRAW" else -0.12)
    away_hold_lead_bonus = 0.10 if context.state.leading_side == "AWAY" else (-0.02 if context.state.leading_side == "DRAW" else -0.12)

    reasons: list[str] = []
    if key == "HOME_CONTROL":
        score = (
            (threat_edge * 0.28)
            + (pressure_edge * 0.22)
            + (balance_edge * 0.28)
            + (state_edge * state_weight)
            + ((suppress_away - 0.50) * 0.10)
            + home_convergence
        )
        reasons = ["threat_edge", "pressure_edge", "balance_edge", "home_convergence"]
    elif key == "AWAY_CONTROL":
        score = (
            ((-threat_edge) * 0.28)
            + ((-pressure_edge) * 0.22)
            + ((-balance_edge) * 0.28)
            + ((-state_edge) * state_weight)
            + ((suppress_home - 0.50) * 0.10)
            + away_convergence
        )
        reasons = ["threat_edge", "pressure_edge", "balance_edge", "away_convergence"]
    elif key == "HOME_ATTACKING_BIAS":
        score = (
            (threat_edge * 0.44)
            + (pressure_edge * 0.28)
            + (context.threat.home_threat_raw * 0.14)
            + (state_edge * (state_weight * 0.4))
            + home_convergence
        )
        reasons = ["threat_edge", "pressure_edge", "home_threat_raw", "home_convergence"]
    elif key == "AWAY_ATTACKING_BIAS":
        score = (
            ((-threat_edge) * 0.44)
            + ((-pressure_edge) * 0.28)
            + (context.threat.away_threat_raw * 0.14)
            + ((-state_edge) * (state_weight * 0.4))
            + away_convergence
        )
        reasons = ["threat_edge", "pressure_edge", "away_threat_raw", "away_convergence"]
    elif key == "HOME_DEFENSIVE_HOLD_BIAS":
        score = (
            ((suppress_away - 0.50) * 0.40)
            + (balance_edge * 0.16)
            + (state_edge * (state_weight * 0.8))
            + ((low_tempo - 0.52) * 0.22)
            + home_hold_lead_bonus
        )
        reasons = ["away_threat_raw", "balance_edge", "home_hold_lead_bonus"]
    elif key == "AWAY_DEFENSIVE_HOLD_BIAS":
        score = (
            ((suppress_home - 0.50) * 0.40)
            + ((-balance_edge) * 0.16)
            + ((-state_edge) * (state_weight * 0.8))
            + ((low_tempo - 0.52) * 0.22)
            + away_hold_lead_bonus
        )
        reasons = ["home_threat_raw", "balance_edge", "away_hold_lead_bonus"]
    elif key == "OPEN_BALANCED":
        score = (
            ((openness - 0.46) * 0.40)
            + (balanced_env * 0.34)
            + (state_neutrality * 0.12)
            + balanced_state_bonus
        )
        reasons = ["openness", "balanced_env", "balanced_state_bonus"]
    elif key == "CAGEY_BALANCED":
        score = (
            ((low_tempo - 0.52) * 0.44)
            + (balanced_env * 0.34)
            + (state_neutrality * 0.10)
            + balanced_state_bonus
        )
        reasons = ["low_tempo", "balanced_env", "balanced_state_bonus"]
    else:  # DUAL_SCORING_BIAS
        dual_scoring = min(context.threat.home_threat_raw, context.threat.away_threat_raw) - 0.28
        draw_bonus = 0.04 if context.state.leading_side == "DRAW" else -0.06
        score = (dual_scoring * 0.42) + ((openness - 0.46) * 0.22) + (balanced_env * 0.18) + draw_bonus
        reasons = ["dual_scoring", "openness", "balanced_env", "draw_bonus"]

    return _clip(score, low=-1.0, high=1.0), tuple(reasons)


def break_event_impact(key: str, context: LiveContextPack) -> float:
    events = context.break_events
    if not any(
        [
            events.goal_scored,
            events.red_card_occurred,
            events.equalizer_event,
            events.lead_change_event,
            events.two_goal_gap_event,
        ]
    ):
        return 0.0

    impact = 0.0
    if key in HOME_KEYS:
        if events.home_goal_scored:
            impact += 0.18
        if events.away_goal_scored:
            impact -= 0.18
        if events.away_red_card:
            impact += 0.20
        if events.home_red_card:
            impact -= 0.20
    elif key in AWAY_KEYS:
        if events.away_goal_scored:
            impact += 0.18
        if events.home_goal_scored:
            impact -= 0.18
        if events.home_red_card:
            impact += 0.20
        if events.away_red_card:
            impact -= 0.20
    else:
        if events.equalizer_event:
            impact += 0.10
        if events.goal_scored and not events.equalizer_event:
            impact -= 0.10
        if events.lead_change_event:
            impact -= 0.18
        if events.two_goal_gap_event:
            impact -= 0.22
        if events.red_card_occurred:
            impact -= 0.08

    if events.lead_change_event:
        if key in HOME_KEYS and context.state.leading_side == "HOME":
            impact += 0.12
        elif key in AWAY_KEYS and context.state.leading_side == "AWAY":
            impact += 0.12
        else:
            impact -= 0.12
    if events.two_goal_gap_event:
        if key in HOME_KEYS and context.state.leading_side == "HOME":
            impact += 0.08
        elif key in AWAY_KEYS and context.state.leading_side == "AWAY":
            impact += 0.08
    return _clip(impact, low=-0.36, high=0.36)


def build_modifier(
    *,
    key: str,
    context: LiveContextPack,
) -> ScenarioModifier:
    live_alignment_score, reasons = scenario_live_alignment(key, context)
    event_impact = break_event_impact(key, context)
    total_alignment = _clip(live_alignment_score + event_impact)

    if total_alignment >= 0.40:
        status = "CONFIRME"
    elif total_alignment >= 0.08:
        status = "ATTENDU"
    elif total_alignment <= -0.45:
        status = "RUPTURE"
    else:
        status = "CONTRARIE"

    magnitude = abs(total_alignment)
    if magnitude < 0.18:
        intensity = "FAIBLE"
    elif magnitude < 0.38:
        intensity = "MOYENNE"
    else:
        intensity = "FORTE"

    rationale = list(reasons)
    if event_impact:
        rationale.append("break_event_impact")
    return ScenarioModifier(
        status=status,  # type: ignore[arg-type]
        intensity=intensity,  # type: ignore[arg-type]
        phase=context.state.time_band,
        live_alignment_score=round(total_alignment, 4),
        break_event_impact=round(event_impact, 4),
        active_event_flags=_flags(context),
        rationale=tuple(rationale),
    )
