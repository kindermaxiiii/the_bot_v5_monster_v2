from __future__ import annotations

from app.fqis.contracts.core import StatisticalThesis
from app.fqis.contracts.enums import ThesisKey
from app.fqis.thesis.features import SimpleMatchFeatures


def build_statistical_theses(features: SimpleMatchFeatures) -> tuple[StatisticalThesis, ...]:
    theses: list[StatisticalThesis] = []

    away_threat = _away_threat_score(features)
    home_threat = _home_threat_score(features)
    game_openness = _game_openness_score(features)

    if away_threat < 0.42:
        theses.append(
            StatisticalThesis(
                event_id=features.event_id,
                thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
                strength=1.0 - away_threat,
                confidence=_confidence_from_minute(features.minute),
                rationale=("away threat suppressed",),
                features={
                    "away_xg_live": features.away_xg_live,
                    "away_shots_on_target": float(features.away_shots_on_target),
                    "minute": float(features.minute),
                },
            )
        )

    if home_threat < 0.42:
        theses.append(
            StatisticalThesis(
                event_id=features.event_id,
                thesis_key=ThesisKey.LOW_HOME_SCORING_HAZARD,
                strength=1.0 - home_threat,
                confidence=_confidence_from_minute(features.minute),
                rationale=("home threat suppressed",),
                features={
                    "home_xg_live": features.home_xg_live,
                    "home_shots_on_target": float(features.home_shots_on_target),
                    "minute": float(features.minute),
                },
            )
        )

    if game_openness > 0.58:
        theses.append(
            StatisticalThesis(
                event_id=features.event_id,
                thesis_key=ThesisKey.OPEN_GAME,
                strength=game_openness,
                confidence=_confidence_from_minute(features.minute),
                rationale=("game is open",),
                features={
                    "total_xg_live": features.total_xg_live,
                    "minute": float(features.minute),
                },
            )
        )
    else:
        theses.append(
            StatisticalThesis(
                event_id=features.event_id,
                thesis_key=ThesisKey.CAGEY_GAME,
                strength=1.0 - game_openness,
                confidence=_confidence_from_minute(features.minute),
                rationale=("game remains cagey",),
                features={
                    "total_xg_live": features.total_xg_live,
                    "minute": float(features.minute),
                },
            )
        )

    return tuple(theses)


def _away_threat_score(features: SimpleMatchFeatures) -> float:
    raw = (0.7 * features.away_xg_live) + (0.08 * features.away_shots_on_target)
    return max(0.0, min(1.0, raw))


def _home_threat_score(features: SimpleMatchFeatures) -> float:
    raw = (0.7 * features.home_xg_live) + (0.08 * features.home_shots_on_target)
    return max(0.0, min(1.0, raw))


def _game_openness_score(features: SimpleMatchFeatures) -> float:
    raw = (0.45 * features.total_xg_live) + (
        0.05 * (features.home_shots_on_target + features.away_shots_on_target)
    )
    return max(0.0, min(1.0, raw))


def _confidence_from_minute(minute: int) -> float:
    if minute < 15:
        return 0.45
    if minute < 30:
        return 0.58
    if minute < 60:
        return 0.72
    return 0.82