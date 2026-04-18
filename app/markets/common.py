from __future__ import annotations

from app.core.calibration import CalibrationLayer
from app.services.execution_layer import ExecutionLayer
from app.services.market_engine import MarketEngine

calibration_layer = CalibrationLayer()
market_engine = MarketEngine()
execution_layer = ExecutionLayer()
