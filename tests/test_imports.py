from app.clients.api_football import APIFootballClient
from app.clients.discord import DiscordClient
from app.core.feature_engine import FeatureEngine
from app.core.hazard_engine import HazardEngine
from app.core.intensity_engine import IntensityEngine
from app.core.match_state import MatchState
from app.core.regime_engine import RegimeEngine
from app.core.scoreline_distribution import ScorelineDistributionEngine
from app.jobs.runtime_cycle import RuntimeCycle
from app.markets.btts_engine import BTTSEngine
from app.markets.correct_score_engine import CorrectScoreEngine
from app.markets.first_half_engine import FirstHalfEngine
from app.markets.over_under_engine import OverUnderEngine
from app.markets.result_engine import ResultEngine
from app.markets.team_totals_engine import TeamTotalsEngine
from app.services.board_manager import BoardManager
from app.services.dispatcher import Dispatcher
from app.services.execution_layer import ExecutionLayer
from app.services.governance import GovernanceEngine
from app.services.market_engine import MarketEngine


def test_imports() -> None:
    assert APIFootballClient
    assert DiscordClient
    assert MatchState
    assert FeatureEngine
    assert RegimeEngine
    assert IntensityEngine
    assert HazardEngine
    assert ScorelineDistributionEngine
    assert MarketEngine
    assert GovernanceEngine
    assert ExecutionLayer
    assert Dispatcher
    assert BoardManager
    assert OverUnderEngine
    assert FirstHalfEngine
    assert BTTSEngine
    assert TeamTotalsEngine
    assert ResultEngine
    assert CorrectScoreEngine
    assert RuntimeCycle
