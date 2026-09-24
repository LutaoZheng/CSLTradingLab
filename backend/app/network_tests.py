"""Isolated network-path diagnostics.

Nothing in this module imports Kalshi, Recorder, HumanEvent, or experiment Session
models. The API is intentionally incapable of activating or executing trading.
"""
import asyncio
import csv
import io
import json
import logging
import math
import statistics
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select

from .models import NetworkTestCalibration, NetworkTestEvent, NetworkTestRun

log = logging.getLogger("csl.network_test")
router = APIRouter(prefix="/api/network-tests")


class CreateRunReq(BaseModel):
    label: str = Field(default="Chongqing → AWS Ohio", min_length=1, max_length=100)
    notes: str = Field(default="", max_length=500)
    max_events: int = Field(default=600, ge=1, le=1200)


class CalibrationReq(BaseModel):
    calibration_id: str
    client_created_wall_ms: float
    samples: list[dict]
    offset_ms: float
    uncertainty_ms: float = Field(ge=0, le=60000)
    rtt_p50_ms: float = Field(ge=0)
    rtt_p95_ms: float = Field(ge=0)
    rtt_p99_ms: float = Field(ge=0)
    rtt_max_ms: float = Field(ge=0)
    jitter_ms: float = Field(ge=0)


class NetworkEventReq(BaseModel):
    event_id: str
    calibration_id: str | None = None
    scenario: str = Field(default="MANUAL", pattern="^(MANUAL|BURST_20|FIXED_1M|SOAK_5M|RECOVERY|DUPLICATE|BACKGROUND)$")
    client_attempt_count: int = Field(default=1, ge=1, le=1000)
    vpn_confirmed: bool | None = None
    pointerdown_perf_ms: float | None = None
    pointerdown_wall_ms: float | None = None
    created_perf_ms: float
    created_wall_ms: float
    enqueue_perf_ms: float
    enqueue_wall_ms: float
    fetch_start_perf_ms: float
    fetch_start_wall_ms: float


class ClientAckReq(BaseModel):
    ack_perf_ms: float
    ack_wall_ms: float
    ui_update_perf_ms: float
    ui_update_wall_ms: float


class PingReq(BaseModel):
    sequence: int
    client_send_wall_ms: float


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile
    low, high = math.floor(rank), math.ceil(rank)
    if low == high:
        return round(ordered[low], 3)
    return round(ordered[low] + (ordered[high] - ordered[low]) * (rank - low), 3)


def _iso_ns(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1e9, timezone.utc).isoformat(timespec="milliseconds")


class NetworkTestService:
    def __init__(self, maker, settings):
        self.maker = maker
        self.settings = settings
        self.lock = asyncio.Lock()
        self.run_lock = asyncio.Lock()
        self.pending: dict[str, dict] = {}
        self.tasks: set[asyncio.Task] = set()
        self.rate: dict[str, deque[float]] = defaultdict(deque)

    async def cleanup(self):
        cutoff = time.time_ns() - self.settings.network_test_retention_days * 86400 * 1_000_000_000
        async with self.maker() as db:
            ids = (await db.execute(select(NetworkTestRun.id).where(NetworkTestRun.ended_at_ns.is_not(None), NetworkTestRun.ended_at_ns < cutoff))).scalars().all()
            if ids:
                await db.execute(delete(NetworkTestEvent).where(NetworkTestEvent.test_run_id.in_(ids)))
                await db.execute(delete(NetworkTestCalibration).where(NetworkTestCalibration.test_run_id.in_(ids)))
                await db.execute(delete(NetworkTestRun).where(NetworkTestRun.id.in_(ids)))
                await db.commit()
                log.info("network_test_retention_cleanup runs=%d", len(ids))

    async def shutdown(self):
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)

    def check_rate(self, key: str):
        now = time.monotonic()
        bucket = self.rate[key]
        while bucket and bucket[0] <= now - 60:
            bucket.popleft()
        if len(bucket) >= self.settings.network_test_rate_per_minute:
            raise HTTPException(429, "NETWORK TEST rate limit exceeded")
        bucket.append(now)

    def background(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def active_run(self, run_id: str | None = None) -> NetworkTestRun:
        async with self.maker() as db:
            if run_id:
                run = await db.get(NetworkTestRun, run_id)
            else:
                run = (await db.execute(select(NetworkTestRun).where(NetworkTestRun.status == "ACTIVE").order_by(NetworkTestRun.started_at_ns.desc()).limit(1))).scalar_one_or_none()
        if not run or run.status != "ACTIVE" or run.expires_at_ns <= time.time_ns():
            raise HTTPException(409, "No authorized active NETWORK TEST")
        return run

    async def persist_event(self, payload: dict):
        event_id = payload["id"]
        try:
            async with self.lock:
                pending = self.pending.get(event_id, {})
                payload.update(pending.get("client_ack", {}))
            async with self.maker() as db:
                db.add(NetworkTestEvent(**payload))
                await db.commit()
                persisted_ns = time.time_ns()
                row = await db.get(NetworkTestEvent, event_id)
                async with self.lock:
                    pending = self.pending.get(event_id, {})
                    pending["server_persist_complete_ns"] = persisted_ns
                    pending["persistence_status"] = "PERSISTED"
                    latest_ack = pending.get("client_ack", {})
                for key, value in latest_ack.items():
                    setattr(row, key, value)
                row.client_attempt_count = pending.get("client_attempt_count", row.client_attempt_count)
                row.duplicate_count = pending.get("duplicate_count", row.duplicate_count)
                row.intentional_duplicate_count = pending.get("intentional_duplicate_count", row.intentional_duplicate_count)
                row.server_persist_complete_ns = persisted_ns
                row.persistence_status = "PERSISTED"
                await db.commit()
            log.info(json.dumps({"kind": "NETWORK_TEST_PERSISTED", "test_run_id": payload["test_run_id"], "event_id": event_id, "server_persist_complete_ns": persisted_ns}, separators=(",", ":")))
        except Exception as exc:
            async with self.lock:
                if event_id in self.pending:
                    self.pending[event_id]["persistence_status"] = "FAILED"
            log.error("network test persistence failed event_id=%s error_type=%s", event_id, type(exc).__name__)

    async def record_client_ack(self, event_id: str, run_id: str, principal_session_id: str, req: ClientAckReq):
        values = {"client_ack_perf_ms": req.ack_perf_ms, "client_ack_wall_ms": req.ack_wall_ms, "client_ui_update_perf_ms": req.ui_update_perf_ms, "client_ui_update_wall_ms": req.ui_update_wall_ms}
        async with self.lock:
            pending = self.pending.get(event_id)
            if pending:
                if pending["test_run_id"] != run_id or pending["operator_auth_session_id"] != principal_session_id:
                    raise HTTPException(404, "NETWORK TEST event not found")
                pending.setdefault("client_ack", values)
                if pending["persistence_status"] == "PENDING":
                    return
        async with self.maker() as db:
            row = await db.get(NetworkTestEvent, event_id)
            if not row or row.test_run_id != run_id or row.operator_auth_session_id != principal_session_id:
                raise HTTPException(404, "NETWORK TEST event not found")
            if row.client_ack_perf_ms is not None:
                return
            for key, value in values.items():
                setattr(row, key, value)
            await db.commit()


service: NetworkTestService | None = None


def configure(maker, settings):
    global service
    service = NetworkTestService(maker, settings)
    return service


def _service() -> NetworkTestService:
    if service is None:
        raise RuntimeError("NETWORK TEST service is not configured")
    return service


def _run_dict(run: NetworkTestRun) -> dict:
    return {"test_run_id": run.id, "label": run.label, "status": run.status, "started_at_ns": str(run.started_at_ns), "started_at": _iso_ns(run.started_at_ns), "ended_at_ns": str(run.ended_at_ns) if run.ended_at_ns else None, "expires_at_ns": str(run.expires_at_ns), "max_events": run.max_events, "notes": run.notes, "test_only": True}


@router.post("/admin/runs")
async def create_run(req: CreateRunReq, request: Request):
    svc = _service()
    now = time.time_ns()
    async with svc.run_lock:
        async with svc.maker() as db:
            active = (await db.execute(select(NetworkTestRun).where(NetworkTestRun.status == "ACTIVE").limit(1))).scalar_one_or_none()
            if active:
                raise HTTPException(409, "End the active NETWORK TEST before creating another")
            run = NetworkTestRun(id=str(uuid.uuid4()), label=req.label, status="ACTIVE", created_at_ns=now, started_at_ns=now, ended_at_ns=None, created_by_auth_session_id=request.state.principal.session.id, max_events=min(req.max_events, svc.settings.network_test_max_events_per_run), expires_at_ns=now + svc.settings.network_test_run_max_minutes * 60 * 1_000_000_000, notes=req.notes)
            db.add(run)
            await db.commit()
    return {"ok": True, "run": _run_dict(run)}


@router.post("/admin/runs/{run_id}/end")
async def end_run(run_id: str):
    svc = _service()
    async with svc.maker() as db:
        run = await db.get(NetworkTestRun, run_id)
        if not run:
            raise HTTPException(404, "NETWORK TEST not found")
        if run.status == "ACTIVE":
            run.status, run.ended_at_ns = "ENDED", time.time_ns()
            await db.commit()
    return {"ok": True, "run": _run_dict(run)}


@router.get("/operator/bootstrap")
async def operator_bootstrap():
    svc = _service()
    try:
        run = await svc.active_run()
    except HTTPException:
        return {"active_run": None, "test_only": True, "limits": {"per_minute": svc.settings.network_test_rate_per_minute, "max_events": svc.settings.network_test_max_events_per_run}}
    return {"active_run": _run_dict(run), "test_only": True, "limits": {"per_minute": svc.settings.network_test_rate_per_minute, "max_events": run.max_events}}


@router.post("/operator/runs/{run_id}/ping")
async def calibration_ping(run_id: str, req: PingReq, request: Request):
    svc = _service()
    await svc.active_run(run_id)
    svc.check_rate(f"ping:{request.state.principal.session.id}:{run_id}")
    receive_ns = request.state.request_entry_ts_ns
    send_ns = time.time_ns()
    return {"sequence": req.sequence, "client_send_wall_ms": req.client_send_wall_ms, "server_receive_ts_ns": str(receive_ns), "server_send_ts_ns": str(send_ns)}


@router.post("/operator/runs/{run_id}/calibrations")
async def save_calibration(run_id: str, req: CalibrationReq, request: Request):
    svc = _service()
    await svc.active_run(run_id)
    if not 5 <= len(req.samples) <= 20:
        raise HTTPException(400, "Clock calibration requires 5 to 20 samples")
    try:
        uuid.UUID(req.calibration_id)
    except ValueError:
        raise HTTPException(400, "Invalid calibration_id")
    async with svc.maker() as db:
        existing = await db.get(NetworkTestCalibration, req.calibration_id)
        if existing:
            if existing.test_run_id != run_id or existing.operator_auth_session_id != request.state.principal.session.id:
                raise HTTPException(409, "Calibration id belongs to another test")
            return {"ok": True, "duplicate": True}
        row = NetworkTestCalibration(id=req.calibration_id, test_run_id=run_id, operator_auth_session_id=request.state.principal.session.id, created_at_ns=time.time_ns(), **req.model_dump(exclude={"calibration_id"}))
        db.add(row)
        await db.commit()
    return {"ok": True, "duplicate": False}


@router.post("/operator/runs/{run_id}/events")
async def submit_event(run_id: str, req: NetworkEventReq, request: Request):
    svc = _service()
    run = await svc.active_run(run_id)
    principal_id = request.state.principal.session.id
    svc.check_rate(f"event:{principal_id}:{run_id}")
    try:
        uuid.UUID(req.event_id)
    except ValueError:
        raise HTTPException(400, "Invalid event_id")
    if req.fetch_start_perf_ms < req.enqueue_perf_ms or req.enqueue_perf_ms < req.created_perf_ms:
        raise HTTPException(400, "Invalid client monotonic timestamp order")
    calibration = None
    async with svc.maker() as db:
        existing = await db.get(NetworkTestEvent, req.event_id)
        if existing:
            if existing.test_run_id != run_id or existing.operator_auth_session_id != principal_id:
                raise HTTPException(409, "event_id belongs to another test or operator session")
            existing.duplicate_count += 1
            existing.client_attempt_count = max(existing.client_attempt_count, req.client_attempt_count)
            if req.scenario == "DUPLICATE":
                existing.intentional_duplicate_count += 1
            await db.commit()
            return {"ok": True, "duplicate": True, "event_id": req.event_id, "server_request_entry_ts_ns": str(existing.server_request_entry_ns), "server_validation_complete_ts_ns": str(existing.server_validation_complete_ns), "server_ack_ready_ts_ns": str(existing.server_ack_ready_ns), "server_persist_complete_ts_ns": str(existing.server_persist_complete_ns) if existing.server_persist_complete_ns else None, "expired": existing.expired, "test_only": True}
        count = await db.scalar(select(func.count()).select_from(NetworkTestEvent).where(NetworkTestEvent.test_run_id == run_id)) or 0
        if req.calibration_id:
            calibration = await db.get(NetworkTestCalibration, req.calibration_id)
            if not calibration or calibration.test_run_id != run_id or calibration.operator_auth_session_id != principal_id:
                raise HTTPException(400, "Unknown calibration_id for this NETWORK TEST")
    async with svc.lock:
        pending = svc.pending.get(req.event_id)
        if pending:
            if pending["test_run_id"] != run_id or pending["operator_auth_session_id"] != principal_id:
                raise HTTPException(409, "event_id belongs to another test or operator session")
            pending["duplicate_count"] += 1
            pending["client_attempt_count"] = max(pending["client_attempt_count"], req.client_attempt_count)
            if req.scenario == "DUPLICATE":
                pending["intentional_duplicate_count"] += 1
            return {"ok": True, "duplicate": True, "event_id": req.event_id, "server_request_entry_ts_ns": str(pending["server_request_entry_ns"]), "server_validation_complete_ts_ns": str(pending["server_validation_complete_ns"]), "server_ack_ready_ts_ns": str(pending["server_ack_ready_ns"]), "server_persist_complete_ts_ns": str(pending.get("server_persist_complete_ns")) if pending.get("server_persist_complete_ns") else None, "expired": pending["expired"], "test_only": True}
        run_pending = sum(1 for item in svc.pending.values() if item["test_run_id"] == run_id and item["persistence_status"] == "PENDING")
        if count + run_pending >= run.max_events:
            raise HTTPException(409, "NETWORK TEST event limit reached")
        validation_ns = time.time_ns()
        monotonic_age_ms = req.fetch_start_perf_ms - req.created_perf_ms
        calibrated_age_ms = validation_ns / 1e6 - (req.created_wall_ms + calibration.offset_ms) if calibration else None
        expired = monotonic_age_ms > svc.settings.network_test_event_max_age_ms or (calibrated_age_ms is not None and calibrated_age_ms > svc.settings.network_test_event_max_age_ms + calibration.uncertainty_ms)
        ack_ready_ns = time.time_ns()
        pending = {"test_run_id": run_id, "operator_auth_session_id": principal_id, "server_request_entry_ns": request.state.request_entry_ts_ns, "server_validation_complete_ns": validation_ns, "server_ack_ready_ns": ack_ready_ns, "server_persist_complete_ns": None, "expired": expired, "duplicate_count": 0, "intentional_duplicate_count": 0, "client_attempt_count": req.client_attempt_count, "persistence_status": "PENDING"}
        svc.pending[req.event_id] = pending
    payload = {"id": req.event_id, "test_run_id": run_id, "operator_auth_session_id": principal_id, "calibration_id": req.calibration_id, "scenario": req.scenario, "client_attempt_count": req.client_attempt_count, "vpn_confirmed": req.vpn_confirmed, "client_pointerdown_perf_ms": req.pointerdown_perf_ms, "client_pointerdown_wall_ms": req.pointerdown_wall_ms, "client_created_perf_ms": req.created_perf_ms, "client_created_wall_ms": req.created_wall_ms, "client_enqueue_perf_ms": req.enqueue_perf_ms, "client_enqueue_wall_ms": req.enqueue_wall_ms, "client_fetch_start_perf_ms": req.fetch_start_perf_ms, "client_fetch_start_wall_ms": req.fetch_start_wall_ms, "server_request_entry_ns": request.state.request_entry_ts_ns, "server_validation_complete_ns": validation_ns, "server_ack_ready_ns": ack_ready_ns, "server_persist_complete_ns": None, "client_ack_perf_ms": None, "client_ack_wall_ms": None, "client_ui_update_perf_ms": None, "client_ui_update_wall_ms": None, "expired": expired, "duplicate_count": 0, "intentional_duplicate_count": 0, "persistence_status": "PENDING"}
    svc.background(svc.persist_event(payload))
    return {"ok": True, "duplicate": False, "event_id": req.event_id, "server_request_entry_ts_ns": str(request.state.request_entry_ts_ns), "server_validation_complete_ts_ns": str(validation_ns), "server_ack_ready_ts_ns": str(ack_ready_ns), "server_persist_complete_ts_ns": None, "expired": expired, "test_only": True}


@router.post("/operator/runs/{run_id}/events/{event_id}/client-ack")
async def client_ack(run_id: str, event_id: str, req: ClientAckReq, request: Request):
    svc = _service()
    await svc.active_run(run_id)
    await svc.record_client_ack(event_id, run_id, request.state.principal.session.id, req)
    return {"ok": True}


async def _summary(run_id: str) -> dict:
    svc = _service()
    async with svc.maker() as db:
        run = await db.get(NetworkTestRun, run_id)
        if not run:
            raise HTTPException(404, "NETWORK TEST not found")
        events = (await db.execute(select(NetworkTestEvent).where(NetworkTestEvent.test_run_id == run_id).order_by(NetworkTestEvent.server_request_entry_ns))).scalars().all()
        calibrations = (await db.execute(select(NetworkTestCalibration).where(NetworkTestCalibration.test_run_id == run_id))).scalars().all()
    calibration_by_id = {item.id: item for item in calibrations}
    rtts = [item.client_ack_perf_ms - item.client_fetch_start_perf_ms for item in events if item.client_ack_perf_ms is not None and item.client_ack_perf_ms >= item.client_fetch_start_perf_ms]
    queues = [item.client_fetch_start_perf_ms - item.client_enqueue_perf_ms for item in events]
    server = [(item.server_ack_ready_ns - item.server_request_entry_ns) / 1e6 for item in events]
    uplinks, uncertainties = [], []
    for item in events:
        calibration = calibration_by_id.get(item.calibration_id)
        if calibration:
            uplinks.append(item.server_request_entry_ns / 1e6 - (item.client_fetch_start_wall_ms + calibration.offset_ms))
            uncertainties.append(calibration.uncertainty_ms)
    pending_failures = sum(1 for item in svc.pending.values() if item["test_run_id"] == run_id and item["persistence_status"] == "FAILED")
    now_perf_deadline_ns = time.time_ns() - 10_000_000_000
    timeouts = sum(1 for item in events if item.client_ack_perf_ms is None and item.server_request_entry_ns < now_perf_deadline_ns)
    attempts = [max(item.client_attempt_count, item.duplicate_count + 1) for item in events]
    retry_failures = sum(max(0, attempts[index] - 1 - item.intentional_duplicate_count) for index, item in enumerate(events))
    return {"run": _run_dict(run), "counts": {"sent": sum(attempts) + pending_failures, "received": len(events), "ack": len(rtts), "duplicate": sum(item.duplicate_count for item in events), "failed": retry_failures + pending_failures + sum(item.persistence_status == "FAILED" for item in events), "timeout": timeouts, "expired": sum(item.expired for item in events)}, "rtt_ms": {"p50": _percentile(rtts, .5), "p95": _percentile(rtts, .95), "p99": _percentile(rtts, .99), "max": round(max(rtts), 3) if rtts else None}, "uplink_calibrated_ms": {"p50": _percentile(uplinks, .5), "p95": _percentile(uplinks, .95), "uncertainty_p95": _percentile(uncertainties, .95), "method": "NTP midpoint offset; uncertainty derives from calibration RTT/2 and offset spread"}, "client_queue_ms": {"p50": _percentile(queues, .5), "p95": _percentile(queues, .95)}, "server_processing_ms": {"p50": _percentile(server, .5), "p95": _percentile(server, .95)}, "vpn": {"confirmed_yes": sum(item.vpn_confirmed is True for item in events), "confirmed_no": sum(item.vpn_confirmed is False for item in events), "unknown": sum(item.vpn_confirmed is None for item in events), "source": "operator confirmation; never inferred from IP"}, "calibrations": len(calibrations), "test_only": True}


@router.get("/admin/runs")
async def list_runs():
    svc = _service()
    async with svc.maker() as db:
        runs = (await db.execute(select(NetworkTestRun).order_by(NetworkTestRun.started_at_ns.desc()).limit(50))).scalars().all()
    return {"items": [_run_dict(run) for run in runs], "retention_days": svc.settings.network_test_retention_days, "test_only": True}


@router.get("/admin/runs/{run_id}/summary")
async def summary(run_id: str):
    return await _summary(run_id)


@router.get("/admin/runs/{run_id}/export.{format}")
async def export(run_id: str, format: str):
    svc = _service()
    async with svc.maker() as db:
        run = await db.get(NetworkTestRun, run_id)
        if not run:
            raise HTTPException(404, "NETWORK TEST not found")
        events = (await db.execute(select(NetworkTestEvent).where(NetworkTestEvent.test_run_id == run_id).order_by(NetworkTestEvent.server_request_entry_ns))).scalars().all()
        calibrations = (await db.execute(select(NetworkTestCalibration).where(NetworkTestCalibration.test_run_id == run_id))).scalars().all()
    rows = [{column.name: getattr(item, column.name) for column in item.__table__.columns} for item in events]
    if format == "json":
        body = json.dumps({"schema_version": 1, "test_only": True, "run": _run_dict(run), "summary": await _summary(run_id), "calibrations": [{column.name: getattr(item, column.name) for column in item.__table__.columns} for item in calibrations], "events": rows}, separators=(",", ":"))
        return Response(body, media_type="application/json", headers={"Content-Disposition": f'attachment; filename="network-test-{run_id}.json"'})
    if format == "csv":
        output = io.StringIO()
        fields = list(rows[0]) if rows else [column.name for column in NetworkTestEvent.__table__.columns]
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader(); writer.writerows(rows)
        return Response(output.getvalue(), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="network-test-{run_id}.csv"'})
    raise HTTPException(404, "Export format must be csv or json")
