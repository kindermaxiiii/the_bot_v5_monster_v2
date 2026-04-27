from app.fqis.contracts.enums import ThesisKey
from app.fqis.thesis.builder import build_statistical_theses
from app.fqis.thesis.features import SimpleMatchFeatures


def test_builds_low_away_scoring_hazard_when_away_threat_is_low() -> None:
    features = SimpleMatchFeatures(
        event_id=101,
        home_xg_live=0.9,
        away_xg_live=0.2,
        home_shots_on_target=4,
        away_shots_on_target=1,
        minute=54,
        home_score=1,
        away_score=0,
    )

    theses = build_statistical_theses(features)
    keys = {thesis.thesis_key for thesis in theses}

    assert ThesisKey.LOW_AWAY_SCORING_HAZARD in keys


def test_builds_open_game_when_total_threat_is_high() -> None:
    features = SimpleMatchFeatures(
        event_id=102,
        home_xg_live=1.1,
        away_xg_live=1.0,
        home_shots_on_target=5,
        away_shots_on_target=4,
        minute=61,
        home_score=1,
        away_score=1,
    )

    theses = build_statistical_theses(features)
    keys = {thesis.thesis_key for thesis in theses}

    assert ThesisKey.OPEN_GAME in keys