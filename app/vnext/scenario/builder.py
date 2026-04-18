from __future__ import annotations

from app.vnext.prior.models import HistoricalPriorPack
from app.vnext.scenario.catalog import SCENARIO_CATALOG_VERSION
from app.vnext.scenario.models import ScenarioPriorResult
from app.vnext.scenario.ranker import rank_scenarios
from app.vnext.scenario.subscores import build_historical_subscores, build_prior_reliability


def build_scenario_prior_result(pack: HistoricalPriorPack) -> ScenarioPriorResult:
    prior_reliability = build_prior_reliability(pack)
    subscores = build_historical_subscores(pack)
    ranked = rank_scenarios(subscores)
    top = ranked[0]
    return ScenarioPriorResult(
        fixture_id=pack.fixture_id,
        competition_id=pack.competition_id,
        competition_name=pack.competition_context.competition.competition_name,
        season=pack.season,
        as_of_date=pack.as_of_date,
        kickoff_utc=pack.kickoff_utc,
        home_team_id=pack.home_team_id,
        away_team_id=pack.away_team_id,
        home_team_name=pack.home_team_name,
        away_team_name=pack.away_team_name,
        source_version="scenario_prior.v1",
        catalog_version=SCENARIO_CATALOG_VERSION,
        prior_source_version=pack.source_version,
        prior_reliability=prior_reliability,
        subscores=subscores,
        scenarios=ranked,
        top_scenario=top,
        data_quality_flag=pack.competition_context.data_quality_flag,
        notes=(),
    )
