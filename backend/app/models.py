from datetime import datetime
from sqlalchemy import BigInteger, Boolean, DateTime, Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase): pass

class Session(Base):
    __tablename__ = "experiment_sessions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    event_ticker: Mapped[str] = mapped_column(String, index=True)
    home_team: Mapped[str] = mapped_column(String)
    away_team: Mapped[str] = mapped_column(String)
    scheduled_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    score_source: Mapped[str] = mapped_column(String, default="MANUAL")
    app_version: Mapped[str] = mapped_column(String)
    git_commit: Mapped[str] = mapped_column(String, default="unknown")
    notes: Mapped[str] = mapped_column(Text, default="")
    session_type: Mapped[str] = mapped_column(String, default="MOCK")
    series_ticker: Mapped[str | None] = mapped_column(String)
    mock_mode: Mapped[bool] = mapped_column(Boolean, default=True)
    trading_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    kalshi_ws_status: Mapped[str] = mapped_column(String, default="DISCONNECTED")

class Market(Base):
    __tablename__ = "markets"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    event_ticker: Mapped[str] = mapped_column(String, index=True)
    title: Mapped[str] = mapped_column(String)
    group_name: Mapped[str] = mapped_column(String, default="OTHER")
    status: Mapped[str] = mapped_column(String, default="open")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)

class RawMessage(Base):
    __tablename__ = "raw_messages"
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(String, index=True)
    local_recv_ts_ns: Mapped[int] = mapped_column(BigInteger)
    kalshi_ts_ms: Mapped[int | None] = mapped_column(BigInteger)
    market_ticker: Mapped[str | None] = mapped_column(String, index=True)
    channel: Mapped[str] = mapped_column(String)
    sequence_number: Mapped[int | None] = mapped_column(BigInteger)
    payload: Mapped[dict] = mapped_column(JSON)

class Quote(Base):
    __tablename__ = "quotes"
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(String, index=True)
    local_recv_ts_ns: Mapped[int] = mapped_column(BigInteger, index=True)
    market_ticker: Mapped[str] = mapped_column(String, index=True)
    yes_bid: Mapped[float | None] = mapped_column(Float); yes_bid_size: Mapped[float | None] = mapped_column(Float)
    yes_ask: Mapped[float | None] = mapped_column(Float); yes_ask_size: Mapped[float | None] = mapped_column(Float)
    no_bid: Mapped[float | None] = mapped_column(Float); no_bid_size: Mapped[float | None] = mapped_column(Float)
    no_ask: Mapped[float | None] = mapped_column(Float); no_ask_size: Mapped[float | None] = mapped_column(Float)
    last_price: Mapped[float | None] = mapped_column(Float); volume: Mapped[float | None] = mapped_column(Float)
    open_interest: Mapped[float | None] = mapped_column(Float); market_status: Mapped[str | None] = mapped_column(String)
    source: Mapped[str] = mapped_column(String)
    provenance: Mapped[dict | None] = mapped_column(JSON)

class Trade(Base):
    __tablename__ = "trades"; __table_args__ = (UniqueConstraint("session_id", "trade_id"),)
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(String, index=True); local_recv_ts_ns: Mapped[int] = mapped_column(BigInteger)
    market_ticker: Mapped[str] = mapped_column(String, index=True); kalshi_ts_ms: Mapped[int | None] = mapped_column(BigInteger)
    price: Mapped[float | None] = mapped_column(Float); size: Mapped[float | None] = mapped_column(Float)
    side: Mapped[str | None] = mapped_column(String); trade_id: Mapped[str | None] = mapped_column(String)

class BookEvent(Base):
    __tablename__ = "orderbook_events"
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(String, index=True); local_recv_ts_ns: Mapped[int] = mapped_column(BigInteger, index=True)
    market_ticker: Mapped[str] = mapped_column(String, index=True); kind: Mapped[str] = mapped_column(String)
    sequence_number: Mapped[int | None] = mapped_column(BigInteger); payload: Mapped[dict] = mapped_column(JSON)

class HumanEvent(Base):
    __tablename__ = "human_events"
    id: Mapped[str] = mapped_column(String, primary_key=True); event_group_id: Mapped[str] = mapped_column(String, index=True)
    session_id: Mapped[str] = mapped_column(String, index=True); match_id: Mapped[str] = mapped_column(String)
    device_wall_ts_ms: Mapped[float] = mapped_column(Float); device_perf_ts_ms: Mapped[float | None] = mapped_column(Float)
    pointerdown_perf_ts_ms: Mapped[float | None] = mapped_column(Float)
    server_request_entry_ts_ns: Mapped[int | None] = mapped_column(BigInteger)
    server_receive_ts_ns: Mapped[int] = mapped_column(BigInteger); phone_to_backend_latency_ms: Mapped[float | None] = mapped_column(Float)
    db_commit_complete_ts_ns: Mapped[int | None] = mapped_column(BigInteger)
    human_raw_fsync_complete_ts_ns: Mapped[int | None] = mapped_column(BigInteger)
    calibration_id: Mapped[str | None] = mapped_column(String, index=True)
    event_type: Mapped[str] = mapped_column(String); team: Mapped[str | None] = mapped_column(String)
    score_at_click: Mapped[dict | None] = mapped_column(JSON); kalshi_match_clock_at_click: Mapped[str | None] = mapped_column(String)
    target_event_id: Mapped[str | None] = mapped_column(String); detail: Mapped[dict] = mapped_column(JSON, default=dict)

class ClockCalibration(Base):
    __tablename__ = "clock_calibrations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    session_id: Mapped[str] = mapped_column(String, index=True)
    created_at_ns: Mapped[int] = mapped_column(BigInteger, index=True)
    client_created_ts_ms: Mapped[float] = mapped_column(Float)
    samples: Mapped[list] = mapped_column(JSON)
    offset_ms: Mapped[float] = mapped_column(Float)
    rtt_last_ms: Mapped[float] = mapped_column(Float)
    rtt_p50_ms: Mapped[float] = mapped_column(Float)
    rtt_p95_ms: Mapped[float] = mapped_column(Float)
    rtt_p99_ms: Mapped[float] = mapped_column(Float)
    estimated_one_way_ms: Mapped[float] = mapped_column(Float)
    jitter_ms: Mapped[float] = mapped_column(Float)

class SystemEvent(Base):
    __tablename__ = "system_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True); session_id: Mapped[str] = mapped_column(String, index=True)
    timestamp_ns: Mapped[int] = mapped_column(BigInteger); kind: Mapped[str] = mapped_column(String); detail: Mapped[dict] = mapped_column(JSON)
