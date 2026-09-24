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
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
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
    client_enqueue_perf_ts_ms: Mapped[float | None] = mapped_column(Float)
    client_fetch_start_perf_ts_ms: Mapped[float | None] = mapped_column(Float)
    client_ack_perf_ts_ms: Mapped[float | None] = mapped_column(Float)
    client_ack_wall_ts_ms: Mapped[float | None] = mapped_column(Float)
    operator_session_id: Mapped[str | None] = mapped_column(String, index=True)
    realtime_eligible: Mapped[bool] = mapped_column(Boolean, default=False)

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

class AuthSession(Base):
    __tablename__ = "auth_sessions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    role: Mapped[str | None] = mapped_column(String, index=True)
    token_hash: Mapped[str] = mapped_column(String, unique=True, index=True)
    csrf_hash: Mapped[str] = mapped_column(String)
    created_at_ns: Mapped[int] = mapped_column(BigInteger)
    expires_at_ns: Mapped[int] = mapped_column(BigInteger, index=True)
    last_seen_at_ns: Mapped[int] = mapped_column(BigInteger)
    remembered: Mapped[bool] = mapped_column(Boolean, default=False)
    revoked_at_ns: Mapped[int | None] = mapped_column(BigInteger, index=True)
    admin_verified_until_ns: Mapped[int | None] = mapped_column(BigInteger)
    user_agent: Mapped[str] = mapped_column(String, default="")

class NetworkTestRun(Base):
    """Isolated TEST ONLY authorization envelope; never references a trading Session."""
    __tablename__ = "network_test_runs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    label: Mapped[str] = mapped_column(String, default="Chongqing → AWS Ohio")
    status: Mapped[str] = mapped_column(String, index=True, default="ACTIVE")
    created_at_ns: Mapped[int] = mapped_column(BigInteger, index=True)
    started_at_ns: Mapped[int] = mapped_column(BigInteger)
    ended_at_ns: Mapped[int | None] = mapped_column(BigInteger, index=True)
    created_by_auth_session_id: Mapped[str] = mapped_column(String, index=True)
    max_events: Mapped[int] = mapped_column(Integer, default=600)
    expires_at_ns: Mapped[int] = mapped_column(BigInteger, index=True)
    notes: Mapped[str] = mapped_column(Text, default="")

class NetworkTestCalibration(Base):
    __tablename__ = "network_test_calibrations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    test_run_id: Mapped[str] = mapped_column(String, ForeignKey("network_test_runs.id", ondelete="CASCADE"), index=True)
    operator_auth_session_id: Mapped[str] = mapped_column(String, index=True)
    created_at_ns: Mapped[int] = mapped_column(BigInteger, index=True)
    client_created_wall_ms: Mapped[float] = mapped_column(Float)
    samples: Mapped[list] = mapped_column(JSON)
    offset_ms: Mapped[float] = mapped_column(Float)
    uncertainty_ms: Mapped[float] = mapped_column(Float)
    rtt_p50_ms: Mapped[float] = mapped_column(Float)
    rtt_p95_ms: Mapped[float] = mapped_column(Float)
    rtt_p99_ms: Mapped[float] = mapped_column(Float)
    rtt_max_ms: Mapped[float] = mapped_column(Float)
    jitter_ms: Mapped[float] = mapped_column(Float)

class NetworkTestEvent(Base):
    """Network measurements only. No trading/recorder code consumes this table."""
    __tablename__ = "network_test_events"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    test_run_id: Mapped[str] = mapped_column(String, ForeignKey("network_test_runs.id", ondelete="CASCADE"), index=True)
    operator_auth_session_id: Mapped[str] = mapped_column(String, index=True)
    calibration_id: Mapped[str | None] = mapped_column(String, ForeignKey("network_test_calibrations.id", ondelete="SET NULL"), index=True)
    scenario: Mapped[str] = mapped_column(String, default="MANUAL")
    client_attempt_count: Mapped[int] = mapped_column(Integer, default=1)
    vpn_confirmed: Mapped[bool | None] = mapped_column(Boolean)
    client_pointerdown_perf_ms: Mapped[float | None] = mapped_column(Float)
    client_pointerdown_wall_ms: Mapped[float | None] = mapped_column(Float)
    client_created_perf_ms: Mapped[float] = mapped_column(Float)
    client_created_wall_ms: Mapped[float] = mapped_column(Float)
    client_enqueue_perf_ms: Mapped[float] = mapped_column(Float)
    client_enqueue_wall_ms: Mapped[float] = mapped_column(Float)
    client_fetch_start_perf_ms: Mapped[float] = mapped_column(Float)
    client_fetch_start_wall_ms: Mapped[float] = mapped_column(Float)
    server_request_entry_ns: Mapped[int] = mapped_column(BigInteger, index=True)
    server_validation_complete_ns: Mapped[int] = mapped_column(BigInteger)
    server_ack_ready_ns: Mapped[int] = mapped_column(BigInteger)
    server_persist_complete_ns: Mapped[int | None] = mapped_column(BigInteger)
    client_ack_perf_ms: Mapped[float | None] = mapped_column(Float)
    client_ack_wall_ms: Mapped[float | None] = mapped_column(Float)
    client_ui_update_perf_ms: Mapped[float | None] = mapped_column(Float)
    client_ui_update_wall_ms: Mapped[float | None] = mapped_column(Float)
    expired: Mapped[bool] = mapped_column(Boolean, default=False)
    duplicate_count: Mapped[int] = mapped_column(Integer, default=0)
    intentional_duplicate_count: Mapped[int] = mapped_column(Integer, default=0)
    persistence_status: Mapped[str] = mapped_column(String, default="PERSISTED")
