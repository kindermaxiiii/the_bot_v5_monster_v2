from __future__ import annotations

from dataclasses import replace

from app.vnext.markets.blockers import evaluate_candidate_blockers
from app.vnext.markets.families import family_maturity
from app.vnext.markets.lines import line_template
from app.vnext.markets.models import MarketCandidate, MarketSupportBreakdown, MarketTranslationResult
from app.vnext.posterior.models import ScenarioPosteriorResult


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _scenario_map(posterior_result: ScenarioPosteriorResult) -> dict[str, float]:
    return {candidate.key: candidate.posterior_score for candidate in posterior_result.scenarios}


def _conflict_score(primary: float, opposite: float) -> float:
    if primary < 0.45 and opposite < 0.45:
        return 0.0
    return _clip(min(primary, opposite) - abs(primary - opposite) + 0.12)


def _build_support_breakdown(
    *,
    posterior_result: ScenarioPosteriorResult,
    scenario_support_score: float,
    attack_support_score: float,
    defensive_support_score: float,
    directionality_score: float,
    live_support_score: float,
    conflict_score: float,
    supporting_scenarios: tuple[str, ...],
    supporting_signals: tuple[str, ...],
) -> MarketSupportBreakdown:
    return MarketSupportBreakdown(
        scenario_support_score=round(_clip(scenario_support_score), 4),
        attack_support_score=round(_clip(attack_support_score), 4),
        defensive_support_score=round(_clip(defensive_support_score), 4),
        directionality_score=round(_clip(directionality_score), 4),
        live_support_score=round(_clip(live_support_score), 4),
        reliability_score=round(posterior_result.posterior_reliability.posterior_reliability_score, 4),
        conflict_score=round(_clip(conflict_score), 4),
        supporting_scenarios=supporting_scenarios,
        supporting_signals=supporting_signals,
    )


def _base_scores(posterior_result: ScenarioPosteriorResult) -> dict[str, float]:
    scenario_scores = _scenario_map(posterior_result)
    live = posterior_result.live_context
    subscores = posterior_result.prior_result.subscores
    home_attack_live = _clip((live.threat.home_threat_raw * 0.62) + (live.pressure.home_pressure_raw * 0.38))
    away_attack_live = _clip((live.threat.away_threat_raw * 0.62) + (live.pressure.away_pressure_raw * 0.38))
    home_defense_live = _clip(((1.0 - live.threat.away_threat_raw) * 0.68) + ((1.0 - live.pressure.away_pressure_raw) * 0.32))
    away_defense_live = _clip(((1.0 - live.threat.home_threat_raw) * 0.68) + ((1.0 - live.pressure.home_pressure_raw) * 0.32))

    home_attack_support = _clip(
        (scenario_scores.get("HOME_ATTACKING_BIAS", 0.0) * 0.50)
        + (scenario_scores.get("HOME_CONTROL", 0.0) * 0.18)
        + (home_attack_live * 0.22)
        + (max(live.threat.threat_edge, 0.0) * 0.10)
    )
    away_attack_support = _clip(
        (scenario_scores.get("AWAY_ATTACKING_BIAS", 0.0) * 0.50)
        + (scenario_scores.get("AWAY_CONTROL", 0.0) * 0.18)
        + (away_attack_live * 0.22)
        + (max(-live.threat.threat_edge, 0.0) * 0.10)
    )
    home_defense_support = _clip(
        (scenario_scores.get("HOME_DEFENSIVE_HOLD_BIAS", 0.0) * 0.48)
        + (scenario_scores.get("HOME_CONTROL", 0.0) * 0.14)
        + (home_defense_live * 0.24)
        + (subscores.clean_sheet_home_affinity * 0.14)
    )
    away_defense_support = _clip(
        (scenario_scores.get("AWAY_DEFENSIVE_HOLD_BIAS", 0.0) * 0.48)
        + (scenario_scores.get("AWAY_CONTROL", 0.0) * 0.14)
        + (away_defense_live * 0.24)
        + (subscores.clean_sheet_away_affinity * 0.14)
    )
    open_support = _clip(
        (scenario_scores.get("OPEN_BALANCED", 0.0) * 0.32)
        + (scenario_scores.get("DUAL_SCORING_BIAS", 0.0) * 0.24)
        + (home_attack_support * 0.18)
        + (away_attack_support * 0.18)
        + (subscores.over_2_5_affinity * 0.08)
    )
    cagey_support = _clip(
        (scenario_scores.get("CAGEY_BALANCED", 0.0) * 0.34)
        + (home_defense_support * 0.20)
        + (away_defense_support * 0.20)
        + (subscores.under_2_5_affinity * 0.16)
        + ((1.0 - ((live.threat.home_threat_raw + live.threat.away_threat_raw) / 2.0)) * 0.10)
    )
    both_teams_attack_support = _clip(
        (min(home_attack_support, away_attack_support) * 0.68)
        + (scenario_scores.get("DUAL_SCORING_BIAS", 0.0) * 0.20)
        + (subscores.btts_affinity * 0.12)
    )

    return {
        "home_attack_support": home_attack_support,
        "away_attack_support": away_attack_support,
        "home_defense_support": home_defense_support,
        "away_defense_support": away_defense_support,
        "open_support": open_support,
        "cagey_support": cagey_support,
        "both_teams_attack_support": both_teams_attack_support,
    }


def _candidate(
    *,
    posterior_result: ScenarioPosteriorResult,
    template_key: str,
    scenario_support_score: float,
    attack_support_score: float,
    defensive_support_score: float,
    directionality_score: float,
    live_support_score: float,
    conflict_score: float,
    supporting_scenarios: tuple[str, ...],
    supporting_signals: tuple[str, ...],
) -> MarketCandidate:
    template = line_template(template_key)
    support = _build_support_breakdown(
        posterior_result=posterior_result,
        scenario_support_score=scenario_support_score,
        attack_support_score=attack_support_score,
        defensive_support_score=defensive_support_score,
        directionality_score=directionality_score,
        live_support_score=live_support_score,
        conflict_score=conflict_score,
        supporting_scenarios=supporting_scenarios,
        supporting_signals=supporting_signals,
    )
    support_score = _clip(
        (support.scenario_support_score * 0.40)
        + (support.attack_support_score * 0.20)
        + (support.defensive_support_score * 0.15)
        + (support.directionality_score * 0.15)
        + (support.live_support_score * 0.10)
    )
    confidence_score = _clip(
        (support.reliability_score * 0.55)
        + (posterior_result.posterior_reliability.live_snapshot_quality_score * 0.25)
        + (support.directionality_score * 0.20)
    )
    explanation = ", ".join((*support.supporting_scenarios[:2], *support.supporting_signals[:2]))
    base_candidate = MarketCandidate(
        fixture_id=posterior_result.prior_result.fixture_id,
        family=template.family,
        maturity=family_maturity(template.family),
        line_template=template,
        exists=True,
        is_blocked=False,
        is_selectable=True,
        support_score=round(support_score, 4),
        confidence_score=round(confidence_score, 4),
        support_breakdown=support,
        blockers=(),
        explanation=explanation,
    )
    blockers = evaluate_candidate_blockers(base_candidate, posterior_result)
    return replace(
        base_candidate,
        is_blocked=bool(blockers),
        is_selectable=base_candidate.exists and not blockers,
        blockers=blockers,
    )


def translate_market_candidates(posterior_result: ScenarioPosteriorResult) -> MarketTranslationResult:
    live = posterior_result.live_context
    scores = _scenario_map(posterior_result)
    base = _base_scores(posterior_result)
    candidates: list[MarketCandidate] = []

    over_support = base["open_support"]
    under_support = base["cagey_support"]
    if max(over_support, under_support) >= 0.42:
        if over_support >= 0.42:
            candidates.append(
                _candidate(
                    posterior_result=posterior_result,
                    template_key="OU_FT_OVER_CORE",
                    scenario_support_score=over_support,
                    attack_support_score=(base["home_attack_support"] + base["away_attack_support"]) / 2.0,
                    defensive_support_score=(1.0 - ((base["home_defense_support"] + base["away_defense_support"]) / 2.0)),
                    directionality_score=_clip(0.50 + ((over_support - under_support) * 1.30)),
                    live_support_score=(live.threat.home_threat_raw + live.threat.away_threat_raw) / 2.0,
                    conflict_score=_conflict_score(over_support, under_support),
                    supporting_scenarios=("OPEN_BALANCED", "DUAL_SCORING_BIAS", "HOME_ATTACKING_BIAS", "AWAY_ATTACKING_BIAS"),
                    supporting_signals=("open_support", "attack_pressure_convergence"),
                )
            )
        if under_support >= 0.42:
            candidates.append(
                _candidate(
                    posterior_result=posterior_result,
                    template_key="OU_FT_UNDER_CORE",
                    scenario_support_score=under_support,
                    attack_support_score=1.0 - ((base["home_attack_support"] + base["away_attack_support"]) / 2.0),
                    defensive_support_score=(base["home_defense_support"] + base["away_defense_support"]) / 2.0,
                    directionality_score=_clip(0.50 + ((under_support - over_support) * 1.30)),
                    live_support_score=1.0 - ((live.threat.home_threat_raw + live.threat.away_threat_raw) / 2.0),
                    conflict_score=_conflict_score(under_support, over_support),
                    supporting_scenarios=("CAGEY_BALANCED", "HOME_DEFENSIVE_HOLD_BIAS", "AWAY_DEFENSIVE_HOLD_BIAS"),
                    supporting_signals=("cagey_support", "defensive_hold"),
                )
            )

    btts_yes_support = _clip((base["both_teams_attack_support"] * 0.62) + (scores.get("OPEN_BALANCED", 0.0) * 0.20) + (scores.get("DUAL_SCORING_BIAS", 0.0) * 0.18))
    btts_no_support = _clip((max(base["home_defense_support"], base["away_defense_support"]) * 0.46) + (under_support * 0.28) + ((1.0 - base["both_teams_attack_support"]) * 0.26))
    if max(btts_yes_support, btts_no_support) >= 0.40:
        if btts_yes_support >= 0.40:
            candidates.append(
                _candidate(
                    posterior_result=posterior_result,
                    template_key="BTTS_YES_CORE",
                    scenario_support_score=btts_yes_support,
                    attack_support_score=base["both_teams_attack_support"],
                    defensive_support_score=1.0 - max(base["home_defense_support"], base["away_defense_support"]),
                    directionality_score=_clip(0.50 + ((btts_yes_support - btts_no_support) * 1.35)),
                    live_support_score=min(live.threat.home_threat_raw, live.threat.away_threat_raw),
                    conflict_score=_conflict_score(btts_yes_support, btts_no_support),
                    supporting_scenarios=("DUAL_SCORING_BIAS", "OPEN_BALANCED"),
                    supporting_signals=("dual_attack_support", "both_sides_threat"),
                )
            )
        if btts_no_support >= 0.40:
            candidates.append(
                _candidate(
                    posterior_result=posterior_result,
                    template_key="BTTS_NO_CORE",
                    scenario_support_score=btts_no_support,
                    attack_support_score=1.0 - base["both_teams_attack_support"],
                    defensive_support_score=max(base["home_defense_support"], base["away_defense_support"]),
                    directionality_score=_clip(0.50 + ((btts_no_support - btts_yes_support) * 1.35)),
                    live_support_score=max(1.0 - live.threat.home_threat_raw, 1.0 - live.threat.away_threat_raw),
                    conflict_score=_conflict_score(btts_no_support, btts_yes_support),
                    supporting_scenarios=("HOME_DEFENSIVE_HOLD_BIAS", "AWAY_DEFENSIVE_HOLD_BIAS", "CAGEY_BALANCED"),
                    supporting_signals=("defensive_hold", "suppressed_opponent"),
                )
            )

    home_over_support = _clip((base["home_attack_support"] * 0.60) + (scores.get("HOME_CONTROL", 0.0) * 0.18) + (scores.get("OPEN_BALANCED", 0.0) * 0.10) + (live.threat.home_threat_raw * 0.12))
    away_over_support = _clip((base["away_attack_support"] * 0.60) + (scores.get("AWAY_CONTROL", 0.0) * 0.18) + (scores.get("OPEN_BALANCED", 0.0) * 0.10) + (live.threat.away_threat_raw * 0.12))
    home_under_support = _clip((base["away_defense_support"] * 0.46) + (scores.get("AWAY_CONTROL", 0.0) * 0.18) + (under_support * 0.20) + ((1.0 - base["home_attack_support"]) * 0.16))
    away_under_support = _clip((base["home_defense_support"] * 0.46) + (scores.get("HOME_CONTROL", 0.0) * 0.18) + (under_support * 0.20) + ((1.0 - base["away_attack_support"]) * 0.16))

    if home_over_support >= 0.40:
        candidates.append(
            _candidate(
                posterior_result=posterior_result,
                template_key="TEAM_TOTAL_HOME_OVER_CORE",
                scenario_support_score=home_over_support,
                attack_support_score=base["home_attack_support"],
                defensive_support_score=1.0 - base["away_defense_support"],
                directionality_score=_clip(0.50 + ((base["home_attack_support"] - base["away_attack_support"]) * 1.40)),
                live_support_score=live.threat.home_threat_raw,
                conflict_score=_conflict_score(home_over_support, home_under_support),
                supporting_scenarios=("HOME_ATTACKING_BIAS", "HOME_CONTROL"),
                supporting_signals=("home_attack_support", "home_threat_edge"),
            )
        )
    if away_over_support >= 0.40:
        candidates.append(
            _candidate(
                posterior_result=posterior_result,
                template_key="TEAM_TOTAL_AWAY_OVER_CORE",
                scenario_support_score=away_over_support,
                attack_support_score=base["away_attack_support"],
                defensive_support_score=1.0 - base["home_defense_support"],
                directionality_score=_clip(0.50 + ((base["away_attack_support"] - base["home_attack_support"]) * 1.40)),
                live_support_score=live.threat.away_threat_raw,
                conflict_score=_conflict_score(away_over_support, away_under_support),
                supporting_scenarios=("AWAY_ATTACKING_BIAS", "AWAY_CONTROL"),
                supporting_signals=("away_attack_support", "away_threat_edge"),
            )
        )
    if home_under_support >= 0.40:
        candidates.append(
            _candidate(
                posterior_result=posterior_result,
                template_key="TEAM_TOTAL_HOME_UNDER_CORE",
                scenario_support_score=home_under_support,
                attack_support_score=1.0 - base["home_attack_support"],
                defensive_support_score=base["away_defense_support"],
                directionality_score=_clip(0.50 + ((home_under_support - home_over_support) * 1.25)),
                live_support_score=1.0 - live.threat.home_threat_raw,
                conflict_score=_conflict_score(home_under_support, home_over_support),
                supporting_scenarios=("AWAY_DEFENSIVE_HOLD_BIAS", "AWAY_CONTROL", "CAGEY_BALANCED"),
                supporting_signals=("away_defensive_hold", "suppressed_home_attack"),
            )
        )
    if away_under_support >= 0.40:
        candidates.append(
            _candidate(
                posterior_result=posterior_result,
                template_key="TEAM_TOTAL_AWAY_UNDER_CORE",
                scenario_support_score=away_under_support,
                attack_support_score=1.0 - base["away_attack_support"],
                defensive_support_score=base["home_defense_support"],
                directionality_score=_clip(0.50 + ((away_under_support - away_over_support) * 1.25)),
                live_support_score=1.0 - live.threat.away_threat_raw,
                conflict_score=_conflict_score(away_under_support, away_over_support),
                supporting_scenarios=("HOME_DEFENSIVE_HOLD_BIAS", "HOME_CONTROL", "CAGEY_BALANCED"),
                supporting_signals=("home_defensive_hold", "suppressed_away_attack"),
            )
        )

    if scores.get("HOME_CONTROL", 0.0) >= 0.52 or scores.get("HOME_ATTACKING_BIAS", 0.0) >= 0.58:
        candidates.append(
            _candidate(
                posterior_result=posterior_result,
                template_key="RESULT_HOME_CORE",
                scenario_support_score=max(scores.get("HOME_CONTROL", 0.0), scores.get("HOME_ATTACKING_BIAS", 0.0)),
                attack_support_score=base["home_attack_support"],
                defensive_support_score=base["home_defense_support"],
                directionality_score=_clip(0.50 + ((live.balance.balance_edge + live.threat.threat_edge) * 0.90)),
                live_support_score=max(live.threat.home_threat_raw, live.balance.home_balance_raw),
                conflict_score=_conflict_score(scores.get("HOME_CONTROL", 0.0), scores.get("AWAY_CONTROL", 0.0)),
                supporting_scenarios=("HOME_CONTROL", "HOME_ATTACKING_BIAS"),
                supporting_signals=("result_lab_only",),
            )
        )
    if scores.get("AWAY_CONTROL", 0.0) >= 0.52 or scores.get("AWAY_ATTACKING_BIAS", 0.0) >= 0.58:
        candidates.append(
            _candidate(
                posterior_result=posterior_result,
                template_key="RESULT_AWAY_CORE",
                scenario_support_score=max(scores.get("AWAY_CONTROL", 0.0), scores.get("AWAY_ATTACKING_BIAS", 0.0)),
                attack_support_score=base["away_attack_support"],
                defensive_support_score=base["away_defense_support"],
                directionality_score=_clip(0.50 + (((-live.balance.balance_edge) + (-live.threat.threat_edge)) * 0.90)),
                live_support_score=max(live.threat.away_threat_raw, live.balance.away_balance_raw),
                conflict_score=_conflict_score(scores.get("AWAY_CONTROL", 0.0), scores.get("HOME_CONTROL", 0.0)),
                supporting_scenarios=("AWAY_CONTROL", "AWAY_ATTACKING_BIAS"),
                supporting_signals=("result_lab_only",),
            )
        )

    return MarketTranslationResult(
        posterior_result=posterior_result,
        candidates=tuple(candidates),
    )
