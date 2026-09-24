import asyncio, csv, io, json, logging, os, re, secrets, shutil, sqlite3, subprocess, tempfile, time, uuid, zipfile
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response, JSONResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel
from sqlalchemy import delete, event, func, select, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from .config import settings
from .models import Base, Session, Market, HumanEvent, ClockCalibration, Quote, Trade, BookEvent, RawMessage, SystemEvent, AuthSession
from .network_tests import configure as configure_network_tests, router as network_test_router
from .recorder import Recorder
from .kalshi import Discovery, KalshiEngine, ScoreAdapter
from .auth import ADMIN_ROLE, OPERATOR_ROLE, AuthManager, SESSION_COOKIE, LoginLimiter, login_limiter, verify_password, set_auth_cookies, clear_auth_cookies
from .manual_matches import configured_match, load_match_config

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(name)s %(message)s")

engine=create_async_engine(settings.database_url)
if settings.database_url.startswith("sqlite"):
    @event.listens_for(engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=FULL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
maker=async_sessionmaker(engine,expire_on_commit=False)
clients=set()
async def broadcast(data):
    dead=[]
    for ws in clients:
        try: await ws.send_json(data)
        except Exception: dead.append(ws)
    for ws in dead: clients.discard(ws) # slow/broken UI never backpressures recorder beyond this coalesced task
recorder=Recorder(maker,settings.data_dir,broadcast); discovery=Discovery(settings); score=ScoreAdapter()
kalshi=KalshiEngine(settings,discovery,recorder,maker,broadcast)
focus_lock=asyncio.Lock()
human_event_lock=asyncio.Lock()
manual_active_session_id:str|None=None
app=FastAPI(title="CSL Trading Lab",version=settings.app_version)
app.add_middleware(CORSMiddleware,allow_origins=[settings.public_origin],allow_credentials=True,allow_methods=["GET","POST","DELETE"],allow_headers=["content-type","x-csrf-token"])
auth=AuthManager(maker,settings)
admin_limiter=LoginLimiter(limit=5,window_seconds=900)
network_tests=configure_network_tests(maker,settings)
app.include_router(network_test_router)

@app.middleware("http")
async def security_boundary(request:Request,call_next):
    request.state.request_entry_ts_ns=time.time_ns()
    path=request.url.path
    if request.method=="OPTIONS": return await call_next(request)
    try:
        if path=="/api/auth/login":
            pass
        elif path=="/api/auth/me":
            try: request.state.principal=await auth.principal(request)
            except HTTPException: request.state.principal=None
        elif path=="/api/auth/logout":
            auth.validate_origin(request)
            try:
                request.state.principal=await auth.principal(request)
                auth.validate_csrf(request,request.state.principal)
            except HTTPException as exc:
                if exc.status_code!=401: raise
                request.state.principal=None
        elif path.startswith("/api/operator/"):
            request.state.principal=await auth.require(request,csrf=request.method not in {"GET","HEAD"})
        elif path.startswith("/api/network-tests/operator/"):
            request.state.principal=await auth.require(request,csrf=request.method not in {"GET","HEAD"})
        elif path.startswith("/api/network-tests/admin/"):
            request.state.principal=await auth.require(request,required_role=ADMIN_ROLE,csrf=request.method not in {"GET","HEAD"})
        elif path=="/api/auth/admin/verify":
            request.state.principal=await auth.require(request,required_role=ADMIN_ROLE,csrf=True)
        elif request.method=="DELETE" and path.startswith("/api/sessions/"):
            request.state.principal=await auth.require(request,required_role=ADMIN_ROLE,csrf=True,high_risk=True)
        elif path.startswith("/api/") or path in {"/docs","/openapi.json","/redoc"}:
            request.state.principal=await auth.require(request,required_role=ADMIN_ROLE,csrf=request.method not in {"GET","HEAD"})
    except HTTPException as exc:
        return JSONResponse({"detail":exc.detail},status_code=exc.status_code)
    return await call_next(request)

@app.on_event("startup")
async def startup():
    if settings.trading_enabled:
        raise RuntimeError("Refusing startup: CSL Trading Lab is read-only and TRADING_ENABLED must be false")
    settings.data_dir.mkdir(parents=True,exist_ok=True)
    async with engine.begin() as c:
        await c.run_sync(Base.metadata.create_all)
        if settings.database_url.startswith("sqlite"):
            columns={row[1] for row in (await c.execute(text("PRAGMA table_info(quotes)"))).all()}
            if "provenance" not in columns: await c.execute(text("ALTER TABLE quotes ADD COLUMN provenance JSON"))
            human_columns={row[1] for row in (await c.execute(text("PRAGMA table_info(human_events)"))).all()}
            human_additions={"pointerdown_perf_ts_ms":"FLOAT","server_request_entry_ts_ns":"BIGINT","db_commit_complete_ts_ns":"BIGINT","human_raw_fsync_complete_ts_ns":"BIGINT","calibration_id":"VARCHAR","client_enqueue_perf_ts_ms":"FLOAT","client_fetch_start_perf_ts_ms":"FLOAT","client_ack_perf_ts_ms":"FLOAT","client_ack_wall_ts_ms":"FLOAT","operator_session_id":"VARCHAR","realtime_eligible":"BOOLEAN DEFAULT 0"}
            for name,column_type in human_additions.items():
                if name not in human_columns: await c.execute(text(f"ALTER TABLE human_events ADD COLUMN {name} {column_type}"))
            await c.execute(text("CREATE INDEX IF NOT EXISTS ix_human_events_calibration_id ON human_events(calibration_id)"))
            await c.execute(text("CREATE INDEX IF NOT EXISTS ix_human_events_operator_session_id ON human_events(operator_session_id)"))
            auth_columns={row[1] for row in (await c.execute(text("PRAGMA table_info(auth_sessions)"))).all()}
            if "role" not in auth_columns: await c.execute(text("ALTER TABLE auth_sessions ADD COLUMN role VARCHAR"))
            await c.execute(text("CREATE INDEX IF NOT EXISTS ix_auth_sessions_role ON auth_sessions(role)"))
            await c.execute(text("UPDATE auth_sessions SET revoked_at_ns=:now WHERE role IS NULL AND revoked_at_ns IS NULL"),{"now":time.time_ns()})
            session_columns={row[1] for row in (await c.execute(text("PRAGMA table_info(experiment_sessions)"))).all()}
            if "archived_at" not in session_columns: await c.execute(text("ALTER TABLE experiment_sessions ADD COLUMN archived_at DATETIME"))
            await c.execute(text("CREATE INDEX IF NOT EXISTS ix_experiment_sessions_archived_at ON experiment_sessions(archived_at)"))
    await recorder.start()
    await network_tests.cleanup()
@app.on_event("shutdown")
async def shutdown(): await network_tests.shutdown(); await kalshi.stop(); await recorder.stop(); await engine.dispose()

class StartReq(BaseModel): event_ticker:str; notes:str=""; session_mode:str="TEST"
class EventReq(BaseModel):
    event_id:str; event_group_id:str; event_type:str; team:str|None=None
    device_wall_ts_ms:float; device_perf_ts_ms:float|None=None; pointerdown_perf_ts_ms:float|None=None
    calibration_id:str|None=None; score_at_click:dict|None=None
    kalshi_match_clock_at_click:str|None=None; target_event_id:str|None=None; detail:dict={}
    client_enqueue_perf_ts_ms:float|None=None; client_fetch_start_perf_ts_ms:float|None=None
    authorized_event_ticker:str|None=None
class PingReq(BaseModel): sequence:int
class CalibrationReq(BaseModel):
    calibration_id:str; client_created_ts_ms:float; samples:list[dict]
    offset_ms:float; rtt_last_ms:float; rtt_p50_ms:float; rtt_p95_ms:float; rtt_p99_ms:float
    estimated_one_way_ms:float; jitter_ms:float
class ScoreReq(BaseModel): side:str; delta:int
class DeleteReq(BaseModel): confirmation:str=""
class LoginReq(BaseModel): username:str; password:str; remember:bool=False
class AdminVerifyReq(BaseModel): password:str
class AckReq(BaseModel): client_ack_perf_ts_ms:float; client_ack_wall_ts_ms:float

@app.post("/api/auth/login")
async def login(req:LoginReq,request:Request):
    auth.validate_origin(request)
    if not auth.configured(): raise HTTPException(503,"Authentication is not configured")
    client_key=f'ip:{request.client.host if request.client else "unknown"}:{req.username.casefold()}'
    account_key=f'account:{req.username.casefold()}'
    login_limiter.check(client_key); login_limiter.check(account_key)
    operator_user=secrets.compare_digest(req.username,settings.operator_username)
    admin_user=secrets.compare_digest(req.username,auth.admin_username)
    operator_password=verify_password(req.password,settings.auth_password_hash)
    admin_password=verify_password(req.password,settings.admin_password_hash)
    role=OPERATOR_ROLE if operator_user and operator_password else ADMIN_ROLE if admin_user and admin_password else None
    if not role:
        login_limiter.fail(client_key); login_limiter.fail(account_key); raise HTTPException(401,"Invalid username or password")
    login_limiter.success(client_key); login_limiter.success(account_key)
    row,raw,csrf,max_age=await auth.create(role,req.remember,request.headers.get("user-agent"))
    destination="/dashboard" if role==ADMIN_ROLE else "/live/chongqing"
    response=JSONResponse({"ok":True,"authenticated":True,"role":role,"high_risk_verified":False,"destination":destination})
    set_auth_cookies(response,raw,csrf,max_age); response.headers["Cache-Control"]="no-store"; return response

@app.get("/api/auth/me")
async def auth_me(request:Request):
    principal=getattr(request.state,"principal",None)
    if not principal: return {"authenticated":False,"role":None,"high_risk_verified":False,"destination":"/login"}
    destination="/dashboard" if principal.role==ADMIN_ROLE else "/live/chongqing"
    return {"authenticated":True,"role":principal.role,"high_risk_verified":principal.high_risk_verified,"remembered":principal.session.remembered,"destination":destination}

@app.post("/api/auth/logout")
async def logout(request:Request):
    principal=getattr(request.state,"principal",None)
    if principal: await auth.revoke(principal.session.id)
    response=JSONResponse({"ok":True}); clear_auth_cookies(response); return response

@app.post("/api/auth/admin/verify")
async def admin_verify(req:AdminVerifyReq,request:Request):
    key=f'admin:{request.state.principal.session.id}'; admin_limiter.check(key)
    if not verify_password(req.password,settings.admin_password_hash): admin_limiter.fail(key); raise HTTPException(401,"Administrator verification failed")
    admin_limiter.success(key)
    until=await auth.elevate(request.state.principal)
    return {"ok":True,"high_risk_verified_until_ns":str(until),"destination":"/dashboard"}

@app.post("/api/auth/sessions/revoke-others")
async def revoke_other_sessions(request:Request):
    return {"ok":True,"revoked":await auth.revoke_all_except(request.state.principal.session.id)}

async def _stop_session_locked(sid:str,status:str="STOPPED"):
    boundary=datetime.now(timezone.utc)
    async with maker() as db:
        s=await db.get(Session,sid)
        if not s: raise HTTPException(404,"Session not found")
        if s.ended_at is not None: return s,False
    if kalshi.session_id==sid: await kalshi.stop_focus(sid)
    else: await recorder.finalize_session(sid)
    async with maker() as db:
        s=await db.get(Session,sid); s.ended_at=boundary; s.kalshi_ws_status=status; await db.commit()
    return s,True

async def _operator_session(required=True):
    if not manual_active_session_id:
        if required: raise HTTPException(409,"No active match")
        return None
    async with maker() as db:
        s=await db.get(Session,manual_active_session_id)
    if not s or s.ended_at is not None or kalshi.session_id!=s.id:
        if required: raise HTTPException(409,"No active match")
        return None
    return s

@app.get("/api/operator/bootstrap")
async def operator_bootstrap(request:Request):
    s=await _operator_session(required=False); principal=request.state.principal
    if not s: return {"match":None,"connection":{"server":True,"kalshi_connected":False,"kalshi_message_age_ms":None},"events":[]}
    definition=configured_match(settings.match_config_path,s.event_ticker)
    if not definition: return {"match":None,"connection":{"server":True,"kalshi_connected":False,"kalshi_message_age_ms":None},"events":[]}
    async with maker() as db:
        events=(await db.execute(select(HumanEvent).where(HumanEvent.session_id==s.id,HumanEvent.operator_session_id==principal.session.id).order_by(HumanEvent.server_receive_ts_ns.desc()).limit(20))).scalars().all()
    age_ms=(time.time_ns()-kalshi.last_message_ns)/1e6 if kalshi.last_message_ns else None
    return {"match":{"session_id":s.id,"event_ticker":s.event_ticker,"home_label":definition.home_team,"away_label":definition.away_team,"start_time":definition.start_time,"timezone":definition.timezone,"allowed_event_buttons":[x.model_dump() for x in definition.allowed_event_buttons]},"connection":{"server":True,"kalshi_connected":kalshi.connected,"kalshi_message_age_ms":age_ms},"events":[serialize_operator_event(x) for x in events]}

OPERATOR_EVENTS={"BALL_IN_NET","PENALTY_EVENT","RED_CARD_EVENT","VAR_CHECK","EVENT_VOIDED"}
@app.post("/api/operator/events")
async def operator_event(req:EventReq,request:Request):
    s=await _operator_session(); principal=request.state.principal
    definition=configured_match(settings.match_config_path,s.event_ticker)
    if not definition: raise HTTPException(409,"Active match configuration is unavailable")
    allowed={button.event_type:button for button in definition.allowed_event_buttons}
    if req.event_type not in allowed: raise HTTPException(400,"Event type is not enabled for this match")
    if req.authorized_event_ticker!=s.event_ticker: raise HTTPException(409,"Signal belongs to a different or inactive match")
    request_entry_ns=request.state.request_entry_ts_ns; server_ns=time.time_ns()
    elapsed_ms=(req.client_fetch_start_perf_ts_ms-req.client_enqueue_perf_ts_ms) if req.client_fetch_start_perf_ts_ms is not None and req.client_enqueue_perf_ts_ms is not None else None
    wall_age_ms=server_ns/1e6-req.device_wall_ts_ms
    realtime=bool(elapsed_ms is not None and 0<=elapsed_ms<=settings.realtime_signal_max_age_ms and -2000<=wall_age_ms<=settings.realtime_signal_max_age_ms)
    team={definition.home_team:"HOME",definition.away_team:"AWAY","HOME":"HOME","AWAY":"AWAY"}.get(req.team or "")
    if req.team and not team: raise HTTPException(400,"Team is not authorized for this match")
    if req.team and req.team not in allowed[req.event_type].teams and req.team not in {"HOME","AWAY"}: raise HTTPException(400,"Team button is not enabled for this match")
    async with human_event_lock:
        async with maker() as db:
            existing=await db.get(HumanEvent,req.event_id)
            if existing:
                if existing.operator_session_id!=principal.session.id: raise HTTPException(409,"Event id already exists")
                return {"ok":True,"duplicate":True,"item":serialize_operator_event(existing)}
            e=HumanEvent(id=req.event_id,event_group_id=req.event_group_id,session_id=s.id,match_id=s.event_ticker,device_wall_ts_ms=req.device_wall_ts_ms,device_perf_ts_ms=req.device_perf_ts_ms,pointerdown_perf_ts_ms=req.pointerdown_perf_ts_ms,client_enqueue_perf_ts_ms=req.client_enqueue_perf_ts_ms,client_fetch_start_perf_ts_ms=req.client_fetch_start_perf_ts_ms,server_request_entry_ts_ns=request_entry_ns,server_receive_ts_ns=server_ns,phone_to_backend_latency_ms=wall_age_ms,calibration_id=None,event_type=req.event_type,team=team,score_at_click=None,kalshi_match_clock_at_click=None,target_event_id=req.target_event_id,detail=req.detail,operator_session_id=principal.session.id,realtime_eligible=realtime)
            db.add(e); await db.commit()
        db_commit_ns=time.time_ns()
        asyncio.create_task(_finish_operator_event(e,s,request_entry_ns,server_ns,db_commit_ns))
    return {"ok":True,"duplicate":False,"event_id":e.id,"server_request_entry_ts_ns":str(request_entry_ns),"server_receive_ts_ns":str(server_ns),"db_commit_complete_ts_ns":str(db_commit_ns),"realtime_eligible":realtime}

async def _finish_operator_event(e:HumanEvent,s:Session,request_entry_ns:int,server_ns:int,db_commit_ns:int):
    try:
        raw_path=settings.data_dir/"raw"/f"match_{s.event_ticker}"/s.id/"human_events.ndjson"
        raw_line=json.dumps({"record_type":"HUMAN_EVENT","server_receive_ts_ns":server_ns,**obj(e)},default=str,separators=(",",":"))+"\n"
        await asyncio.to_thread(_append_text,raw_path,raw_line); fsync_ns=time.time_ns()
        persistence_line=json.dumps({"record_type":"HUMAN_EVENT_PERSISTENCE","event_id":e.id,"server_request_entry_ts_ns":request_entry_ns,"server_receive_ts_ns":server_ns,"db_commit_complete_ts_ns":db_commit_ns,"human_raw_fsync_complete_ts_ns":fsync_ns},separators=(",",":"))+"\n"
        await asyncio.to_thread(_append_text,raw_path,persistence_line)
        async with maker() as db:
            stored=await db.get(HumanEvent,e.id); stored.db_commit_complete_ts_ns=db_commit_ns; stored.human_raw_fsync_complete_ts_ns=fsync_ns; await db.commit()
        asyncio.create_task(broadcast({"type":"human_event","item":obj(e)}))
    except Exception as exc: logging.getLogger("csl.operator").error("operator persistence failed event_id=%s error_type=%s",e.id,type(exc).__name__)

@app.post("/api/operator/events/{event_id}/ack")
async def operator_ack(event_id:str,req:AckReq,request:Request):
    async with maker() as db:
        e=await db.get(HumanEvent,event_id)
        if not e or e.operator_session_id!=request.state.principal.session.id: raise HTTPException(404,"Event not found")
        e.client_ack_perf_ts_ms=req.client_ack_perf_ts_ms; e.client_ack_wall_ts_ms=req.client_ack_wall_ts_ms; await db.commit()
    return {"ok":True}

def serialize_operator_event(e:HumanEvent):
    return {"event_id":e.id,"event_type":e.event_type,"team":e.team,"device_wall_ts_ms":e.device_wall_ts_ms,"client_enqueue_perf_ts_ms":e.client_enqueue_perf_ts_ms,"client_fetch_start_perf_ts_ms":e.client_fetch_start_perf_ts_ms,"client_ack_perf_ts_ms":e.client_ack_perf_ts_ms,"server_request_entry_ts_ns":str(e.server_request_entry_ts_ns) if e.server_request_entry_ts_ns else None,"server_receive_ts_ns":str(e.server_receive_ts_ns),"db_commit_complete_ts_ns":str(e.db_commit_complete_ts_ns) if e.db_commit_complete_ts_ns else None,"realtime_eligible":e.realtime_eligible}

@app.get("/api/matches")
async def matches():
    config=load_match_config(settings.match_config_path)
    return [{"event_ticker":m.event_ticker,"home_team":m.home_team,"away_team":m.away_team,"scheduled_start":m.start_time,"timezone":m.timezone,"status":"ACTIVE" if kalshi.focus==m.event_ticker and manual_active_session_id else "CONFIGURED","markets":[x.model_dump() for x in m.markets],"allowed_event_buttons":[x.model_dump() for x in m.allowed_event_buttons]} for m in config.matches]
@app.post("/api/latency/ping")
async def latency_ping(req:PingReq,request:Request):
    receive_ns=request.state.request_entry_ts_ns
    send_ns=time.time_ns()
    # Decimal strings preserve nanosecond integers across JavaScript's 53-bit number boundary.
    payload={"sequence":req.sequence,"server_receive_ts_ns":str(receive_ns),"server_send_ts_ns":str(send_ns)}
    return Response(content=json.dumps(payload,separators=(",",":")),media_type="application/json")
@app.get("/api/sessions")
async def session_history(limit:int=100,include_archived:bool=False):
    async with maker() as db:
        query=select(Session)
        if not include_archived: query=query.where(Session.archived_at.is_(None))
        sessions=(await db.execute(query.order_by(Session.created_at.desc()).limit(limit))).scalars().all()
        archived_count=await db.scalar(select(func.count()).select_from(Session).where(Session.archived_at.is_not(None)))
        async def counts(model):
            rows=(await db.execute(select(model.session_id,func.count()).group_by(model.session_id))).all()
            return dict(rows)
        human_counts=await counts(HumanEvent); quote_counts=await counts(Quote); raw_counts=await counts(RawMessage)
        items=[{
                **obj(s),
                "status":"ACTIVE" if s.id==manual_active_session_id else ("INACTIVE" if s.ended_at is None else "STOPPED"),
                "human_events":human_counts.get(s.id,0),
                "quote_rows":quote_counts.get(s.id,0),
                "kalshi_ws_messages":raw_counts.get(s.id,0),
                "recorder_on":bool(s.id==manual_active_session_id and recorder.health(s.id)["raw_recorder"]),
                "ws_markets":kalshi.health(s.id)["subscribed_market_count"] if s.id==manual_active_session_id else 0,
            } for s in sessions]
    active=next((item for item in items if item["status"]=="ACTIVE"),None)
    return {"items":items,"active_session":active,"archived_count":archived_count or 0}
@app.post("/api/sessions")
async def start(req:StartReq):
    global manual_active_session_id
    if req.session_mode not in {"TEST","MATCH_DAY"}: raise HTTPException(400,"session_mode must be TEST or MATCH_DAY")
    definition=configured_match(settings.match_config_path,req.event_ticker)
    if not definition: raise HTTPException(404,"Exact event ticker is not present in the manual match configuration")
    try:
        exact=await discovery.event_exact(req.event_ticker)
    except Exception as exc:
        raise HTTPException(502,f"Exact configured event lookup failed: {type(exc).__name__}") from exc
    available={m["ticker"]:m for m in exact["markets"]}; missing=[m.ticker for m in definition.markets if m.ticker not in available]
    if missing: raise HTTPException(409,{"message":"Configured market tickers are missing from the exact event","missing":missing})
    event={"event_ticker":definition.event_ticker,"series_ticker":exact.get("series_ticker"),"title":f"{definition.home_team} vs {definition.away_team}","home_team":definition.home_team,"away_team":definition.away_team,"scheduled_start":definition.start_time,"markets":[{"ticker":rule.ticker,"title":rule.title,"group":rule.group,"raw":{**available[rule.ticker].get("raw",{}),"configured_direction":rule.direction,"configured_settlement_rule":rule.settlement_rule}} for rule in definition.markets]}
    async with focus_lock:
        async with maker() as db:
            active=(await db.execute(select(Session).where(Session.ended_at.is_(None)).limit(1))).scalar_one_or_none()
        if active and active.event_ticker==req.event_ticker:
            sid=active.id; resumed=True; replaced_session_id=None; existing=active
        else:
            # Validate/read the target metadata before ending the current experiment.
            # This does not create a Focus subscription; it only prevents a typo or
            # upstream REST failure from stopping a healthy ACTIVE Session.
            replaced_session_id=active.id if active else None
            if active: await _stop_session_locked(active.id,"FOCUS_REPLACED")
            now=datetime.now(timezone.utc); sid=str(uuid.uuid4()); resumed=False; existing=None
            try: commit=subprocess.check_output(["git","rev-parse","HEAD"],text=True,stderr=subprocess.DEVNULL).strip()
            except Exception: commit="unknown"
            async with maker() as db:
                db.add(Session(id=sid,event_ticker=event["event_ticker"],series_ticker=event.get("series_ticker"),home_team=event["home_team"],away_team=event["away_team"],scheduled_start=datetime.fromisoformat(event["scheduled_start"].replace("Z","+00:00")) if event.get("scheduled_start") else None,created_at=now,started_at=now,score_source=score.source,app_version=settings.app_version,git_commit=commit,notes=req.notes,session_type=req.session_mode,mock_mode=settings.mock_mode,trading_enabled=settings.trading_enabled,kalshi_ws_status="CONNECTING")); await db.commit()
        if kalshi.session_id!=sid or not kalshi.connected:
            await kalshi.focus_match(event,sid)
        manual_active_session_id=sid
        return {"session_id":sid,"match":None if resumed else event,"resumed":resumed,"replaced_session_id":replaced_session_id,"session_mode":existing.session_type if existing else req.session_mode}

@app.post("/api/sessions/{sid}/focus")
async def restore_focus(sid:str):
    global manual_active_session_id
    async with focus_lock:
        async with maker() as db: s=await db.get(Session,sid)
        if not s: raise HTTPException(404)
        if s.ended_at is not None: raise HTTPException(409,"Session has ended")
        definition=configured_match(settings.match_config_path,s.event_ticker)
        if not definition: raise HTTPException(404,"Session event is not in the manual match configuration")
        exact=await discovery.event_exact(s.event_ticker); available={m["ticker"]:m for m in exact["markets"]}
        missing=[m.ticker for m in definition.markets if m.ticker not in available]
        if missing: raise HTTPException(409,"Configured exact markets are unavailable")
        event={"event_ticker":definition.event_ticker,"series_ticker":exact.get("series_ticker"),"title":f"{definition.home_team} vs {definition.away_team}","home_team":definition.home_team,"away_team":definition.away_team,"scheduled_start":definition.start_time,"markets":[{"ticker":rule.ticker,"title":rule.title,"group":rule.group,"raw":{**available[rule.ticker].get("raw",{}),"configured_direction":rule.direction,"configured_settlement_rule":rule.settlement_rule}} for rule in definition.markets]}
        if kalshi.session_id!=sid or not kalshi.connected: await kalshi.focus_match(event,sid)
        manual_active_session_id=sid
        return {"ok":True,"session_id":sid,"upstream_connected":kalshi.connected}

@app.post("/api/matches/{event_ticker}/deactivate")
async def deactivate_match(event_ticker:str):
    global manual_active_session_id
    async with focus_lock:
        if not manual_active_session_id: return {"ok":True,"changed":False}
        async with maker() as db: s=await db.get(Session,manual_active_session_id)
        if not s or s.event_ticker!=event_ticker: raise HTTPException(409,"A different match is active")
        await _stop_session_locked(s.id,"MANUALLY_DEACTIVATED"); manual_active_session_id=None
    return {"ok":True,"changed":True}
@app.post("/api/sessions/{sid}/calibrations")
async def save_calibration(sid:str,req:CalibrationReq):
    if len(req.samples)!=20: raise HTTPException(400,"A calibration must contain exactly 20 raw samples")
    async with maker() as db:
        s=await db.get(Session,sid)
        if not s: raise HTTPException(404,"Session not found")
        existing=await db.get(ClockCalibration,req.calibration_id)
        if existing:
            if existing.session_id!=sid: raise HTTPException(409,"Calibration id belongs to another Session")
            return {"ok":True,"duplicate":True,"calibration":obj(existing)}
        calibration=ClockCalibration(id=req.calibration_id,session_id=sid,created_at_ns=time.time_ns(),client_created_ts_ms=req.client_created_ts_ms,samples=req.samples,offset_ms=req.offset_ms,rtt_last_ms=req.rtt_last_ms,rtt_p50_ms=req.rtt_p50_ms,rtt_p95_ms=req.rtt_p95_ms,rtt_p99_ms=req.rtt_p99_ms,estimated_one_way_ms=req.estimated_one_way_ms,jitter_ms=req.jitter_ms)
        db.add(calibration); await db.commit()
    return {"ok":True,"duplicate":False,"calibration":obj(calibration)}
@app.get("/api/sessions/{sid}/state")
async def state(sid:str):
    async with maker() as db:
        s=await db.get(Session,sid)
        if not s: raise HTTPException(404)
        ms=(await db.execute(select(Market).where(Market.event_ticker==s.event_ticker))).scalars().all()
        ev=(await db.execute(select(HumanEvent).where(HumanEvent.session_id==sid).order_by(HumanEvent.server_receive_ts_ns))).scalars().all()
        calibration=(await db.execute(select(ClockCalibration).where(ClockCalibration.session_id==sid).order_by(ClockCalibration.created_at_ns.desc()).limit(1))).scalar_one_or_none()
    quotes=recorder.current_quotes(sid)
    return {"session":obj(s),"markets":[obj(x) for x in ms],"events":[obj(x) for x in ev],"latest_calibration":obj(calibration) if calibration else None,"quotes":quotes,"score":score.get(s.event_ticker),"health":kalshi.health(sid),"recorder":recorder.health(sid),"preflight":preflight(sid)}
@app.get("/api/sessions/{sid}/preflight")
async def get_preflight(sid:str): return preflight(sid)
def preflight(session_id=None):
    disk=shutil.disk_usage(settings.data_dir)
    h=kalshi.health()
    rh=recorder.health(session_id); active=bool(session_id and session_id==kalshi.session_id==rh["session_id"])
    checks={"mode":"MOCK" if settings.mock_mode else "PRODUCTION_READ_ONLY","active_focus_session":active,"kalshi_api_auth":h["auth_ok"] if active else False,"csl_series":bool(kalshi.focus) if active else False,"event_discovery":bool(kalshi.focus) if active else False,"ticker_ws":h["ticker_subscribed"] if active else False,"orderbook_snapshot":h["snapshot_markets"]>0 if active else False,"orderbook_delta":h["delta_markets"]>0 or (settings.mock_mode and h["snapshot_markets"]>0) if active else False,"trade_subscription":h["trade_subscribed"] if active else False,"raw_recorder":rh["raw_recorder"] if active else False,"sqlite_writer":rh["database_writer"] if active else False,"dynamic_discovery":((not settings.auto_discovery_enabled) or h["dynamic_discovery"]) if active else False,"auto_discovery_enabled":settings.auto_discovery_enabled,"trading_disabled":not settings.trading_enabled,"score_source":score.source,"clock":None,"markets":len(kalshi.markets) if active else 0,"disk_ok":disk.free>500_000_000,"disk_free_gb":round(disk.free/1e9,1)}
    checks["critical_ok"]=all(checks[k] for k in ("kalshi_api_auth","csl_series","event_discovery","ticker_ws","orderbook_snapshot","trade_subscription","raw_recorder","sqlite_writer","dynamic_discovery","trading_disabled","disk_ok"))
    return checks

@app.post("/api/admin/controlled-reconnect")
async def controlled_reconnect():
    if settings.mock_mode: raise HTTPException(400,"Use the mock disconnect endpoint in Mock Mode")
    await kalshi.controlled_reconnect(); return {"ok":True,"trading_enabled":False}

@app.post("/api/sessions/{sid}/end")
async def end_session(sid:str):
    global manual_active_session_id
    async with focus_lock: s,changed=await _stop_session_locked(sid)
    if manual_active_session_id==sid: manual_active_session_id=None
    return {"ok":True,"changed":changed,"ended_at":s.ended_at,"focus_cleared":kalshi.session_id is None}
@app.post("/api/sessions/{sid}/archive")
async def archive_session(sid:str):
    async with focus_lock:
        if sid==manual_active_session_id: raise HTTPException(409,"Stop the active Session before archiving it")
        async with maker() as db:
            s=await db.get(Session,sid)
            if not s: raise HTTPException(404,"Session not found")
            if s.archived_at is None: s.archived_at=datetime.now(timezone.utc); await db.commit()
            return {"ok":True,"session_id":sid,"archived_at":s.archived_at}
@app.post("/api/sessions/{sid}/restore")
async def restore_session(sid:str):
    async with maker() as db:
        s=await db.get(Session,sid)
        if not s: raise HTTPException(404,"Session not found")
        if s.archived_at is not None: s.archived_at=None; await db.commit()
        return {"ok":True,"session_id":sid}
@app.delete("/api/sessions/{sid}")
async def delete_session(sid:str,req:DeleteReq):
    async with focus_lock:
        async with maker() as db:
            s=await db.get(Session,sid)
            if not s: raise HTTPException(404,"Session not found")
            if sid==manual_active_session_id: raise HTTPException(409,"Stop the active Session before deleting it")
            if s.archived_at is None: raise HTTPException(409,"Archive the Session before permanently deleting it")
            if req.confirmation!=f"DELETE {sid}": raise HTTPException(400,"Permanent deletion requires the exact Session confirmation")
            event_ticker=s.event_ticker
        raw_dir=settings.data_dir/"raw"/f"match_{event_ticker}"/sid
        holding=settings.data_dir/".deleting"/f"{sid}-{uuid.uuid4()}"; moved=False
        if raw_dir.exists():
            holding.parent.mkdir(parents=True,exist_ok=True); await asyncio.to_thread(shutil.move,str(raw_dir),str(holding)); moved=True
        try:
            async with maker() as db:
                deleted={}
                for model in (HumanEvent,ClockCalibration,Quote,Trade,BookEvent,RawMessage,SystemEvent):
                    result=await db.execute(delete(model).where(model.session_id==sid)); deleted[model.__tablename__]=result.rowcount
                await db.execute(delete(Session).where(Session.id==sid)); await db.commit()
        except Exception:
            if moved: await asyncio.to_thread(shutil.move,str(holding),str(raw_dir))
            raise
        raw_directory_deleted=moved
        if moved:
            try: await asyncio.to_thread(shutil.rmtree,holding)
            except OSError as exc:
                raw_directory_deleted=False
                logging.getLogger("csl.sessions").warning("deleted Session raw cleanup deferred session_id=%s error_type=%s",sid,type(exc).__name__)
        return {"ok":True,"session_id":sid,"deleted_rows":deleted,"raw_directory_deleted":raw_directory_deleted}
@app.post("/api/sessions/{sid}/events")
async def human_event(sid:str,req:EventReq,request:Request):
    request_entry_ns=request.state.request_entry_ts_ns
    server_receive_ns=time.time_ns()
    async with human_event_lock:
        return await _append_human_event(sid,req,request_entry_ns,server_receive_ns)

async def _append_human_event(sid:str,req:EventReq,request_entry_ns:int|None=None,server_receive_ns:int|None=None):
    request_entry_ns=request_entry_ns or time.time_ns(); server_ns=server_receive_ns or time.time_ns()
    latency=server_ns/1e6-req.device_wall_ts_ms
    async with maker() as db:
        if not await db.get(Session,sid): raise HTTPException(404)
        existing=await db.get(HumanEvent,req.event_id)
        if existing: return {"ok":True,"duplicate":True,"item":obj(existing)}
        s=await db.get(Session,sid)
        if s.ended_at is not None: raise HTTPException(409,"Session has stopped")
        if req.calibration_id:
            calibration=await db.get(ClockCalibration,req.calibration_id)
            if not calibration or calibration.session_id!=sid: raise HTTPException(400,"Unknown calibration_id for this Session")
        if req.event_type=="VAR_CHECK" and req.detail.get("parent_event_id"):
            linked=(await db.execute(select(HumanEvent.id).where(HumanEvent.session_id==sid,HumanEvent.event_group_id==req.event_group_id,HumanEvent.event_type=="VAR_CHECK").limit(1))).scalar_one_or_none()
            if linked: return {"ok":True,"duplicate":True,"duplicate_reason":"LINKED_VAR_ALREADY_RECORDED","existing_event_id":linked}
        e=HumanEvent(id=req.event_id,event_group_id=req.event_group_id,session_id=sid,match_id=s.event_ticker,device_wall_ts_ms=req.device_wall_ts_ms,device_perf_ts_ms=req.device_perf_ts_ms,pointerdown_perf_ts_ms=req.pointerdown_perf_ts_ms,server_request_entry_ts_ns=request_entry_ns,server_receive_ts_ns=server_ns,phone_to_backend_latency_ms=latency,calibration_id=req.calibration_id,event_type=req.event_type,team=req.team,score_at_click=req.score_at_click,kalshi_match_clock_at_click=req.kalshi_match_clock_at_click,target_event_id=req.target_event_id,detail=req.detail); db.add(e); await db.commit()
    db_commit_ns=time.time_ns(); e.db_commit_complete_ts_ns=db_commit_ns
    raw_path=settings.data_dir/"raw"/f"match_{s.event_ticker}"/sid/"human_events.ndjson"
    raw_line=json.dumps({"record_type":"HUMAN_EVENT","server_receive_ts_ns":server_ns,**obj(e)},default=str,separators=(",",":"))+"\n"
    await asyncio.to_thread(_append_text,raw_path,raw_line)
    fsync_ns=time.time_ns(); e.human_raw_fsync_complete_ts_ns=fsync_ns
    persistence_line=json.dumps({"record_type":"HUMAN_EVENT_PERSISTENCE","event_id":e.id,"server_request_entry_ts_ns":request_entry_ns,"server_receive_ts_ns":server_ns,"db_commit_complete_ts_ns":db_commit_ns,"human_raw_fsync_complete_ts_ns":fsync_ns},separators=(",",":"))+"\n"
    await asyncio.to_thread(_append_text,raw_path,persistence_line)
    async with maker() as db:
        stored=await db.get(HumanEvent,e.id); stored.db_commit_complete_ts_ns=db_commit_ns; stored.human_raw_fsync_complete_ts_ns=fsync_ns; await db.commit()
    item=obj(e); await broadcast({"type":"human_event","item":item})
    return {"ok":True,"server_request_entry_ts_ns":request_entry_ns,"server_receive_ts_ns":server_ns,"db_commit_complete_ts_ns":db_commit_ns,"human_raw_fsync_complete_ts_ns":fsync_ns,"phone_to_backend_latency_ms":latency}

@app.get("/api/sessions/{sid}/data")
async def session_data(sid:str,limit:int=200):
    async with maker() as db:
        s=await db.get(Session,sid)
        if not s: raise HTTPException(404)
        models={"kalshi_ws_messages":RawMessage,"orderbook_events":BookEvent,"quote_rows":Quote,"trade_rows":Trade,"human_events":HumanEvent}
        counts={name:await db.scalar(select(func.count()).select_from(model).where(model.session_id==sid)) for name,model in models.items()}
        humans=(await db.execute(select(HumanEvent).where(HumanEvent.session_id==sid).order_by(HumanEvent.server_receive_ts_ns.desc()).limit(limit))).scalars().all()
        quotes=(await db.execute(select(Quote).where(Quote.session_id==sid).order_by(Quote.local_recv_ts_ns.desc()).limit(limit))).scalars().all()
        trades=(await db.execute(select(Trade).where(Trade.session_id==sid).order_by(Trade.local_recv_ts_ns.desc()).limit(limit))).scalars().all()
        markets=(await db.execute(select(Market).where(Market.event_ticker==s.event_ticker))).scalars().all()
        calibrations=(await db.execute(select(ClockCalibration).where(ClockCalibration.session_id==sid).order_by(ClockCalibration.created_at_ns.desc()))).scalars().all()
    titles={m.ticker:m.title for m in markets}; timeline=[]
    for e in humans: timeline.append({"timestamp_ns":e.server_receive_ts_ns,"kind":"HUMAN","label":e.event_type,"team":e.team,"target_event_id":e.target_event_id,"event_id":e.id})
    for q in quotes: timeline.append({"timestamp_ns":q.local_recv_ts_ns,"kind":"BOOK" if q.source.startswith("orderbook") else "QUOTE","label":titles.get(q.market_ticker,q.market_ticker),"market_ticker":q.market_ticker,"yes_bid":q.yes_bid,"yes_ask":q.yes_ask,"no_bid":q.no_bid,"no_ask":q.no_ask,"source":q.source,"provenance":q.provenance})
    for t in trades: timeline.append({"timestamp_ns":t.local_recv_ts_ns,"kind":"TRADE","label":titles.get(t.market_ticker,t.market_ticker),"market_ticker":t.market_ticker,"price":t.price,"size":t.size,"side":t.side})
    timeline=sorted(timeline,key=lambda x:x["timestamp_ns"],reverse=True)[:limit]
    calibration_by_id={x.id:x for x in calibrations}; measurements=[]
    for e in humans:
        calibration=calibration_by_id.get(e.calibration_id)
        calibrated_click_ns=int((e.device_wall_ts_ms+calibration.offset_ms)*1_000_000) if calibration else None
        measurements.append({"event_id":e.id,"event_type":e.event_type,"team":e.team,"calibration_id":e.calibration_id,"client_click_epoch_ms":e.device_wall_ts_ms,"client_click_perf_ms":e.device_perf_ts_ms,"pointerdown_perf_ts_ms":e.pointerdown_perf_ts_ms,"server_request_entry_ts_ns":e.server_request_entry_ts_ns,"server_receive_ts_ns":e.server_receive_ts_ns,"db_commit_complete_ts_ns":e.db_commit_complete_ts_ns,"human_raw_fsync_complete_ts_ns":e.human_raw_fsync_complete_ts_ns,"calibrated_client_click_ts_ns":calibrated_click_ns,"client_to_server_calibrated_ms":(e.server_receive_ts_ns-calibrated_click_ns)/1e6 if calibrated_click_ns else None,"request_entry_to_receive_ms":(e.server_receive_ts_ns-e.server_request_entry_ts_ns)/1e6 if e.server_request_entry_ts_ns else None,"server_receive_to_db_commit_ms":(e.db_commit_complete_ts_ns-e.server_receive_ts_ns)/1e6 if e.db_commit_complete_ts_ns else None,"db_commit_to_raw_fsync_ms":(e.human_raw_fsync_complete_ts_ns-e.db_commit_complete_ts_ns)/1e6 if e.human_raw_fsync_complete_ts_ns and e.db_commit_complete_ts_ns else None,"request_entry_to_raw_fsync_ms":(e.human_raw_fsync_complete_ts_ns-e.server_request_entry_ts_ns)/1e6 if e.human_raw_fsync_complete_ts_ns and e.server_request_entry_ts_ns else None})
    rh=recorder.health(sid); duration_end=_utc(s.ended_at) or datetime.now(timezone.utc); duration_start=_utc(s.started_at)
    runtime_status="ACTIVE" if s.id==manual_active_session_id else ("INACTIVE" if s.ended_at is None else "STOPPED")
    return {"session":obj(s),"status":runtime_status,"duration_seconds":max(0,(duration_end-duration_start).total_seconds()) if duration_start else 0,"counts":counts,"recorder":rh,"latest_calibration":obj(calibrations[0]) if calibrations else None,"measurements":measurements,"timeline":timeline}
@app.post("/api/sessions/{sid}/score")
async def change_score(sid:str,req:ScoreReq):
    async with maker() as db: s=await db.get(Session,sid)
    if not s: raise HTTPException(404)
    result=score.adjust(s.event_ticker,req.side,req.delta); await broadcast({"type":"score","item":result}); return result
@app.get("/api/sessions/{sid}/replay")
async def replay(sid:str,event_id:str,before:float=10,after:float=30,reference:str="server"):
    async with maker() as db:
        e=await db.get(HumanEvent,event_id)
        if not e or e.session_id!=sid: raise HTTPException(404)
        if reference=="server": anchor_ns=e.server_receive_ts_ns
        elif reference=="device": anchor_ns=int(e.device_wall_ts_ms*1e6)
        elif reference=="calibrated":
            calibration=await db.get(ClockCalibration,e.calibration_id) if e.calibration_id else None
            if not calibration: raise HTTPException(409,"Human Event has no clock calibration")
            anchor_ns=int((e.device_wall_ts_ms+calibration.offset_ms)*1e6)
        else: raise HTTPException(400,"reference must be server, device, or calibrated")
        lo=int(anchor_ns-before*1e9); hi=int(anchor_ns+after*1e9)
        qs=(await db.execute(select(Quote).where(Quote.session_id==sid,Quote.local_recv_ts_ns.between(lo,hi)).order_by(Quote.local_recv_ts_ns))).scalars().all()
    return {"event":obj(e),"reference":reference,"reference_ts_ns":anchor_ns,"quotes":[{**obj(q),"relative_ms":(q.local_recv_ts_ns-anchor_ns)/1e6} for q in qs],"sampling":"exact event-driven updates; no interpolation"}
@app.get("/api/sessions/{sid}/export")
async def export(sid:str):
    async with maker() as db:
        s=await db.get(Session,sid)
        if not s: raise HTTPException(404)
        session_record=obj(s)
    if not settings.database_url.startswith("sqlite"):
        raise HTTPException(501,"Streaming Session export currently requires SQLite Native Mode")
    fd,tmp_name=tempfile.mkstemp(prefix=f"csl-export-{sid[:8]}-",suffix=".zip"); os.close(fd)
    try:
        await asyncio.to_thread(_build_session_export,Path(tmp_name),sid,session_record)
    except Exception:
        Path(tmp_name).unlink(missing_ok=True)
        raise
    started=_utc(s.started_at) or _utc(s.created_at) or datetime.now(timezone.utc)
    safe=lambda value:re.sub(r"[^A-Za-z0-9._-]+","-",value).strip("-") or "unknown"
    filename=f"CSL_{started.date().isoformat()}_{safe(s.home_team)}_vs_{safe(s.away_team)}_{sid[:8]}.zip"
    snapshot_status="active" if s.id==manual_active_session_id else ("inactive" if s.ended_at is None else "stopped")
    return FileResponse(tmp_name,media_type="application/zip",filename=filename,background=BackgroundTask(Path(tmp_name).unlink,missing_ok=True),headers={"X-Session-Export-Snapshot":snapshot_status})
@app.post("/api/mock/{kind}")
async def mock(kind:str):
    if not settings.mock_mode: raise HTTPException(403)
    await kalshi.inject(kind); return {"ok":True}
@app.websocket("/ws")
async def ws(websocket:WebSocket):
    if websocket.headers.get("origin")!=settings.public_origin:
        await websocket.close(code=1008,reason="Origin rejected"); return
    try: await auth.websocket_principal(websocket,required_role=ADMIN_ROLE)
    except HTTPException:
        await websocket.close(code=1008,reason="Authentication required"); return
    await websocket.accept(); clients.add(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect: clients.discard(websocket)
def obj(x): return {c.name:getattr(x,c.name) for c in x.__table__.columns}
def _utc(value): return value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value
def _append_text(path:Path,text:str):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("a",encoding="utf-8") as f: f.write(text); f.flush(); os.fsync(f.fileno())

def _iso_ns(value):
    if value in (None,""): return ""
    return datetime.fromtimestamp(int(value)/1_000_000_000,tz=timezone.utc).isoformat(timespec="microseconds")

def _iso_ms(value):
    if value in (None,""): return ""
    return datetime.fromtimestamp(float(value)/1000,tz=timezone.utc).isoformat(timespec="milliseconds")

def _build_session_export(target:Path,sid:str,session_record:dict):
    """Build from SQLite/read-only files; never touches Recorder queues or state."""
    db_path=Path(engine.url.database or "")
    conn=sqlite3.connect(f"file:{db_path}?mode=ro",uri=True)
    conn.row_factory=sqlite3.Row
    conn.execute("BEGIN")  # one consistent SQLite snapshot; WAL writers continue independently
    exported_at=datetime.now(timezone.utc).isoformat()
    try:
        markets=[dict(x) for x in conn.execute("SELECT ticker,title,group_name,status,metadata_json FROM markets WHERE event_ticker=? ORDER BY ticker",(session_record["event_ticker"],))]
        for market in markets:
            try: market["metadata_json"]=json.loads(market["metadata_json"] or "{}")
            except (TypeError,json.JSONDecodeError): pass
        calibrations=[dict(x) for x in conn.execute("SELECT * FROM clock_calibrations WHERE session_id=? ORDER BY created_at_ns",(sid,))]
        for calibration in calibrations:
            if isinstance(calibration.get("samples"),str): calibration["samples"]=json.loads(calibration["samples"])
        status="ACTIVE" if session_record.get("ended_at") is None else "STOPPED"
        family=lambda m:"GAME" if m["group_name"]=="MATCH RESULT" else ("TOTAL" if m["group_name"] in {"TOTAL","TEAM TOTAL"} else m["group_name"])
        families=sorted({family(m) for m in markets})
        session_json={**session_record,"session_id":sid,"mode":session_record.get("session_type"),"status":status,"match_title":f'{session_record["home_team"]} vs {session_record["away_team"]}',"exported_at":exported_at,"market_tickers":[m["ticker"] for m in markets],"market_families":families,"market_family_counts":{name:sum(family(m)==name for m in markets) for name in families},"clock_calibration_ids":[x["id"] for x in calibrations]}
        health=recorder.health(sid)
        manifest_files=[]
        with zipfile.ZipFile(target,"w",zipfile.ZIP_DEFLATED,allowZip64=True) as z:
            def json_file(name,data,description):
                z.writestr(name,json.dumps(data,default=str,indent=2,ensure_ascii=False))
                info=z.getinfo(name); manifest_files.append({"filename":name,"row_count":len(data) if isinstance(data,list) else 1,"size_bytes":info.file_size,"description":description})
            def csv_query(name,headers,sql,params,description,transform=None):
                count=0
                raw=z.open(name,"w",force_zip64=True); stream=io.TextIOWrapper(raw,encoding="utf-8",newline="")
                writer=csv.DictWriter(stream,fieldnames=headers,extrasaction="ignore"); writer.writeheader()
                cursor=conn.execute(sql,params)
                while True:
                    batch=cursor.fetchmany(2000)
                    if not batch: break
                    for row in batch:
                        item=dict(row)
                        if transform: item=transform(item)
                        writer.writerow({k:json.dumps(v,ensure_ascii=False,separators=(",",":")) if isinstance(v,(dict,list)) else ("" if v is None else v) for k,v in item.items()}); count+=1
                stream.flush(); stream.detach(); raw.close()
                info=z.getinfo(name); manifest_files.append({"filename":name,"row_count":count,"size_bytes":info.file_size,"description":description})
            def raw_file(path:Path,archive_name,description):
                if not path.exists(): return
                count=size=0
                with path.open("rb") as src,z.open(archive_name,"w",force_zip64=True) as dst:
                    for line in src:
                        dst.write(line); size+=len(line); count+=1
                manifest_files.append({"filename":archive_name,"row_count":count,"size_bytes":size,"description":description})

            json_file("session.json",session_json,"Session identity, lifecycle, code version, and market inventory")
            json_file("market_metadata.json",markets,"Kalshi market metadata captured for the event")
            json_file("clock_calibrations.json",calibrations,"Clock calibration raw ping samples and derived RTT/offset metrics; separate from Human Events")
            human_headers=["event_id","event_group_id","event_type","team","device_wall_timestamp","device_wall_timestamp_ms","performance_timestamp_ms","pointerdown_performance_timestamp_ms","server_request_entry_timestamp","server_request_entry_timestamp_ns","server_receive_timestamp","server_receive_timestamp_ns","db_commit_complete_timestamp","db_commit_complete_timestamp_ns","human_raw_fsync_complete_timestamp","human_raw_fsync_complete_timestamp_ns","calibration_id","phone_to_backend_latency_ms","match_id","score_at_click","kalshi_match_clock_at_click","target_event_id","reason","detail"]
            def human_transform(x):
                detail=json.loads(x["detail"] or "{}") if isinstance(x.get("detail"),str) else (x.get("detail") or {})
                return {**x,"event_id":x.pop("id"),"device_wall_timestamp":_iso_ms(x["device_wall_ts_ms"]),"device_wall_timestamp_ms":x.pop("device_wall_ts_ms"),"performance_timestamp_ms":x.pop("device_perf_ts_ms"),"pointerdown_performance_timestamp_ms":x.pop("pointerdown_perf_ts_ms"),"server_request_entry_timestamp":_iso_ns(x["server_request_entry_ts_ns"]),"server_request_entry_timestamp_ns":x.pop("server_request_entry_ts_ns"),"server_receive_timestamp":_iso_ns(x["server_receive_ts_ns"]),"server_receive_timestamp_ns":x.pop("server_receive_ts_ns"),"db_commit_complete_timestamp":_iso_ns(x["db_commit_complete_ts_ns"]),"db_commit_complete_timestamp_ns":x.pop("db_commit_complete_ts_ns"),"human_raw_fsync_complete_timestamp":_iso_ns(x["human_raw_fsync_complete_ts_ns"]),"human_raw_fsync_complete_timestamp_ns":x.pop("human_raw_fsync_complete_ts_ns"),"reason":detail.get("reason","")}
            csv_query("human_events.csv",human_headers,"SELECT * FROM human_events WHERE session_id=? ORDER BY server_receive_ts_ns",(sid,),"Append-only human event timeline with device and server timestamp precision",human_transform)
            quote_headers=["timestamp","timestamp_ns","market_ticker","market_title","source","yes_bid","yes_bid_size","yes_ask","yes_ask_size","no_bid","no_bid_size","no_ask","no_ask_size","last_price","volume","open_interest","market_status","provenance"]
            csv_query("quotes.csv",quote_headers,"SELECT q.local_recv_ts_ns AS timestamp_ns,q.market_ticker,m.title AS market_title,q.source,q.yes_bid,q.yes_bid_size,q.yes_ask,q.yes_ask_size,q.no_bid,q.no_bid_size,q.no_ask,q.no_ask_size,q.last_price,q.volume,q.open_interest,q.market_status,q.provenance FROM quotes q LEFT JOIN markets m ON m.ticker=q.market_ticker WHERE q.session_id=? ORDER BY q.local_recv_ts_ns",(sid,),"Structured top-of-book read model with RAW/DERIVED provenance",lambda x:{**x,"timestamp":_iso_ns(x["timestamp_ns"])})
            trade_headers=["timestamp","timestamp_ns","kalshi_timestamp_ms","market_ticker","market_title","trade_id","price","size","side"]
            csv_query("trades.csv",trade_headers,"SELECT t.local_recv_ts_ns AS timestamp_ns,t.kalshi_ts_ms AS kalshi_timestamp_ms,t.market_ticker,m.title AS market_title,t.trade_id,t.price,t.size,t.side FROM trades t LEFT JOIN markets m ON m.ticker=t.market_ticker WHERE t.session_id=? ORDER BY t.local_recv_ts_ns",(sid,),"Observed Kalshi trades; price/side retain the wire schema semantics",lambda x:{**x,"timestamp":_iso_ns(x["timestamp_ns"])})
            book_headers=["timestamp","timestamp_ns","market_ticker","market_title","kind","sequence_number","payload"]
            csv_query("orderbook_events.csv",book_headers,"SELECT b.local_recv_ts_ns AS timestamp_ns,b.market_ticker,m.title AS market_title,b.kind,b.sequence_number,b.payload FROM orderbook_events b LEFT JOIN markets m ON m.ticker=b.market_ticker WHERE b.session_id=? ORDER BY b.local_recv_ts_ns",(sid,),"Full snapshot/delta/resync orderbook event payloads, including depth and size changes",lambda x:{**x,"timestamp":_iso_ns(x["timestamp_ns"])})
            timeline_headers=["timestamp","timestamp_ns","type","source","market_ticker","market_title","team","human_event_type","event_group_id","yes_bid","yes_ask","no_bid","no_ask","trade_price","trade_size"]
            timeline_sql="""SELECT server_receive_ts_ns timestamp_ns,'HUMAN' type,'PHONE' source,'' market_ticker,'' market_title,team,event_type human_event_type,event_group_id,NULL yes_bid,NULL yes_ask,NULL no_bid,NULL no_ask,NULL trade_price,NULL trade_size FROM human_events WHERE session_id=? UNION ALL SELECT q.local_recv_ts_ns,CASE WHEN q.source LIKE 'orderbook%' THEN 'BOOK' ELSE 'QUOTE' END,q.source,q.market_ticker,COALESCE(m.title,''),'','','',q.yes_bid,q.yes_ask,q.no_bid,q.no_ask,NULL,NULL FROM quotes q LEFT JOIN markets m ON m.ticker=q.market_ticker WHERE q.session_id=? UNION ALL SELECT t.local_recv_ts_ns,'TRADE','KALSHI',t.market_ticker,COALESCE(m.title,''),'','','',NULL,NULL,NULL,NULL,t.price,t.size FROM trades t LEFT JOIN markets m ON m.ticker=t.market_ticker WHERE t.session_id=? UNION ALL SELECT timestamp_ns,'SYSTEM',kind,'','','','','',NULL,NULL,NULL,NULL,NULL,NULL FROM system_events WHERE session_id=? ORDER BY timestamp_ns"""
            csv_query("timeline.csv",timeline_headers,timeline_sql,(sid,sid,sid,sid),"Unified timestamp-ordered HUMAN/BOOK/QUOTE/TRADE/SYSTEM analysis timeline",lambda x:{**x,"timestamp":_iso_ns(x["timestamp_ns"])})
            raw_dir=settings.data_dir/"raw"/f'match_{session_record["event_ticker"]}'/sid
            raw_file(raw_dir/"kalshi_ws.ndjson","raw/kalshi_ws.ndjson","Append-only original Kalshi WebSocket messages")
            raw_file(raw_dir/"human_events.ndjson","raw/human_events.ndjson","Append-only original human event records")
            manifest_files.append({"filename":"manifest.json","row_count":1,"size_bytes":0,"description":"Export schema, file inventory, row counts, sizes, and data-quality counters"})
            manifest={"schema_version":"1.0","export_version":"1.0","session_id":sid,"session_status":status,"exported_at":exported_at,"data_quality":{"queue_drop_count":health.get("queue_drop_count"),"db_write_failures":health.get("db_write_failures"),"counter_scope":"current recorder process at export time"},"files":manifest_files}
            while True:
                manifest_bytes=json.dumps(manifest,indent=2,ensure_ascii=False).encode("utf-8")
                if manifest_files[-1]["size_bytes"]==len(manifest_bytes): break
                manifest_files[-1]["size_bytes"]=len(manifest_bytes)
            z.writestr("manifest.json",manifest_bytes)
    finally:
        conn.close()
