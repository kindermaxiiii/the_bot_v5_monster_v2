from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class MatchSnapshot(Base):
    __tablename__ = "match_snapshots"
    id: Mapped[int] = mapped_column(primary_key=True)
    fixture_id: Mapped[int] = mapped_column(Integer, index=True)
    minute: Mapped[int | None] = mapped_column(Integer, nullable=True)
    phase: Mapped[str | None] = mapped_column(String(16), nullable=True)
    status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    home_goals: Mapped[int] = mapped_column(Integer, default=0)
    away_goals: Mapped[int] = mapped_column(Integer, default=0)
    payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(), default=datetime.utcnow)


class DecisionLog(Base):
    __tablename__ = "decision_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    fixture_id: Mapped[int] = mapped_column(Integer, index=True)
    market_key: Mapped[str] = mapped_column(String(32), index=True)
    side: Mapped[str] = mapped_column(String(64))
    line_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    odds_decimal: Mapped[float | None] = mapped_column(Float, nullable=True)
    bookmaker: Mapped[str | None] = mapped_column(String(64), nullable=True)
    regime_label: Mapped[str | None] = mapped_column(String(64), nullable=True)
    p_raw: Mapped[float | None] = mapped_column(Float, nullable=True)
    p_cal: Mapped[float | None] = mapped_column(Float, nullable=True)
    p_market_no_vig: Mapped[float | None] = mapped_column(Float, nullable=True)
    edge: Mapped[float | None] = mapped_column(Float, nullable=True)
    ev: Mapped[float | None] = mapped_column(Float, nullable=True)
    executable: Mapped[bool] = mapped_column(Boolean, default=False)
    documentary_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    real_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    reasons_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    vetoes_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(), default=datetime.utcnow)
