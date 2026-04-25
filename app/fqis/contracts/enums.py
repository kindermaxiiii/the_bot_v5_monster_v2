from __future__ import annotations

from enum import Enum


class MarketFamily(str, Enum):
    MATCH_TOTAL = "MATCH_TOTAL"
    TEAM_TOTAL_HOME = "TEAM_TOTAL_HOME"
    TEAM_TOTAL_AWAY = "TEAM_TOTAL_AWAY"
    BTTS = "BTTS"
    RESULT = "RESULT"


class MarketSide(str, Enum):
    OVER = "OVER"
    UNDER = "UNDER"
    YES = "YES"
    NO = "NO"
    HOME = "HOME"
    AWAY = "AWAY"
    DRAW = "DRAW"


class Period(str, Enum):
    FT = "FT"


class TeamRole(str, Enum):
    HOME = "HOME"
    AWAY = "AWAY"
    NONE = "NONE"


class ThesisKey(str, Enum):
    LOW_AWAY_SCORING_HAZARD = "LOW_AWAY_SCORING_HAZARD"
    LOW_HOME_SCORING_HAZARD = "LOW_HOME_SCORING_HAZARD"
    OPEN_GAME = "OPEN_GAME"
    CAGEY_GAME = "CAGEY_GAME"
    DUAL_SCORING = "DUAL_SCORING"
    HOME_SUPERIORITY = "HOME_SUPERIORITY"
    AWAY_SUPERIORITY = "AWAY_SUPERIORITY"