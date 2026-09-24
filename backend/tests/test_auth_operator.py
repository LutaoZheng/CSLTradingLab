import asyncio, json, sqlite3, time, uuid
from datetime import datetime, timezone
import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from app import main as main_module
from app.auth import hash_password, login_limiter, token_hash
from app.models import AuthSession, Base, HumanEvent, Session
from app.manual_matches import load_match_config

ORIGIN="https://csltradinglab.duckdns.org"

@pytest.fixture
async def secured_app(tmp_path,monkeypatch):
    engine=create_async_engine(f"sqlite+aiosqlite:///{tmp_path/'auth.db'}")
    maker=async_sessionmaker(engine,expire_on_commit=False)
    async with engine.begin() as connection: await connection.run_sync(Base.metadata.create_all)
    monkeypatch.setattr(main_module,"maker",maker); monkeypatch.setattr(main_module.auth,"maker",maker)
    monkeypatch.setattr(main_module.settings,"data_dir",tmp_path)
    monkeypatch.setattr(main_module.settings,"public_origin",ORIGIN)
    monkeypatch.setattr(main_module.settings,"operator_username","operator")
    monkeypatch.setattr(main_module.settings,"auth_username","michael")
    monkeypatch.setattr(main_module.settings,"admin_username","")
    monkeypatch.setattr(main_module.settings,"auth_password_hash",hash_password("operator-pass"))
    monkeypatch.setattr(main_module.settings,"admin_password_hash",hash_password("admin-pass"))
    config_path=tmp_path/"matches.v1.json"
    config_path.write_text(json.dumps({"schema_version":1,"matches":[{"event_ticker":"CHN-MDV","home_team":"CHINA","away_team":"MALDIVES","start_time":"2026-09-24T19:00:00+08:00","timezone":"Asia/Shanghai","markets":[{"ticker":"CHN-MDV-WIN","title":"China win","group":"MATCH RESULT","direction":{"CHINA_GOAL":"YES_UP"},"settlement_rule":"test only"}],"allowed_event_buttons":[{"event_type":"BALL_IN_NET","label":"GOAL","teams":["CHINA","MALDIVES"]},{"event_type":"VAR_CHECK","label":"VAR","teams":[]},{"event_type":"EVENT_VOIDED","label":"CORRECTION","teams":[]}]}]}))
    monkeypatch.setattr(main_module.settings,"match_config_path",config_path)
    login_limiter.failures.clear()
    now=datetime.now(timezone.utc)
    async with maker() as db:
        db.add(Session(id="match-session",event_ticker="CHN-MDV",home_team="China",away_team="Maldives",scheduled_start=now,created_at=now,started_at=now,ended_at=None,score_source="MANUAL",app_version="test",git_commit="test",notes="",session_type="MATCH_DAY",series_ticker="TEST",mock_mode=False,trading_enabled=False,kalshi_ws_status="CONNECTED")); await db.commit()
    monkeypatch.setattr(main_module,"manual_active_session_id","match-session")
    monkeypatch.setattr(main_module.kalshi,"session_id","match-session")
    yield maker
    await asyncio.sleep(.1); await engine.dispose()

async def login(client,role="OPERATOR",remember=False):
    username,password=("michael","admin-pass") if role=="ADMIN" else ("operator","operator-pass")
    return await client.post("/api/auth/login",json={"username":username,"password":password,"remember":remember},headers={"origin":ORIGIN})

def csrf_headers(client): return {"origin":ORIGIN,"x-csrf-token":client.cookies.get("csl_csrf")}

@pytest.mark.asyncio
async def test_auth_roles_remember_logout_and_revocation(secured_app):
    transport=httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport,base_url=ORIGIN) as first, httpx.AsyncClient(transport=transport,base_url=ORIGIN) as admin:
        assert (await first.get("/api/sessions")).status_code==401
        remembered=await login(first,remember=True); assert remembered.status_code==200
        assert remembered.json()["role"]=="OPERATOR" and remembered.json()["destination"]=="/live/chongqing"
        assert "Max-Age=2592000" in remembered.headers.get_list("set-cookie")[0]
        assert (await first.get("/api/operator/bootstrap")).status_code==200
        assert (await first.get("/api/sessions")).status_code==403
        assert (await first.get("/api/sessions/match-session/export")).status_code==403
        assert (await first.post("/api/auth/sessions/revoke-others",json={},headers=csrf_headers(first))).status_code==403
        assert (await first.post("/api/auth/admin/verify",json={"password":"admin-pass"},headers=csrf_headers(first))).status_code==403

        admin_login=await login(admin,"ADMIN"); assert admin_login.status_code==200
        assert admin_login.json()["role"]=="ADMIN" and admin_login.json()["destination"]=="/dashboard"
        assert "Max-Age=43200" in admin_login.headers.get_list("set-cookie")[0]
        async with secured_app() as db:
            rows=(await db.execute(select(AuthSession))).scalars().all()
            assert {row.role for row in rows}=={"OPERATOR","ADMIN"}
            assert all(row.token_hash not in {first.cookies.get("csl_session"),admin.cookies.get("csl_session")} for row in rows)
        assert (await admin.get("/api/sessions")).status_code==200
        assert (await admin.get("/api/operator/bootstrap")).status_code==200
        assert (await admin.post("/api/auth/admin/verify",json={"password":"wrong"},headers=csrf_headers(admin))).status_code==401
        elevated=await admin.post("/api/auth/admin/verify",json={"password":"admin-pass"},headers=csrf_headers(admin)); assert elevated.status_code==200
        assert elevated.json()["destination"]=="/dashboard"
        revoked=await admin.post("/api/auth/sessions/revoke-others",json={},headers=csrf_headers(admin)); assert revoked.status_code==200 and revoked.json()["revoked"]==1
        assert (await first.get("/api/operator/bootstrap")).status_code==401
        logged_out=await admin.post("/api/auth/logout",json={},headers=csrf_headers(admin)); assert logged_out.status_code==200
        assert (await admin.get("/api/sessions")).status_code==401

@pytest.mark.asyncio
async def test_logout_revokes_only_current_device_and_expired_cookie_is_cleared(secured_app):
    transport=httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport,base_url=ORIGIN) as first, httpx.AsyncClient(transport=transport,base_url=ORIGIN) as second:
        assert (await login(first,"ADMIN")).status_code==200
        assert (await login(second,"ADMIN")).status_code==200
        first_token=first.cookies.get("csl_session")
        logged_out=await first.post("/api/auth/logout",json={},headers=csrf_headers(first))
        assert logged_out.status_code==200
        assert "csl_session=" in logged_out.headers.get("set-cookie","")
        assert (await first.get("/api/sessions")).status_code==401
        assert (await second.get("/api/sessions")).status_code==200
        async with secured_app() as db:
            first_row=(await db.execute(select(AuthSession).where(AuthSession.token_hash==token_hash(first_token)))).scalar_one()
            assert first_row.revoked_at_ns is not None
            second_row=(await db.execute(select(AuthSession).where(AuthSession.token_hash==token_hash(second.cookies.get("csl_session"))))).scalar_one()
            second_row.expires_at_ns=time.time_ns()-1
            await db.commit()
        expired_logout=await second.post("/api/auth/logout",json={},headers=csrf_headers(second))
        assert expired_logout.status_code==200
        assert "csl_session=" in expired_logout.headers.get("set-cookie","")

@pytest.mark.asyncio
async def test_auth_request_does_not_block_on_last_seen_sqlite_writer(secured_app):
    maker=secured_app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app),base_url=ORIGIN) as admin:
        assert (await login(admin,"ADMIN")).status_code==200
        async with maker() as db:
            row=(await db.execute(select(AuthSession).where(AuthSession.role=="ADMIN"))).scalar_one()
            row.last_seen_at_ns=0
            await db.commit()
        lock=sqlite3.connect(main_module.settings.data_dir/"auth.db",timeout=.1)
        lock.execute("BEGIN IMMEDIATE")
        started=time.monotonic()
        try:
            response=await admin.get("/api/sessions")
            assert response.status_code==200
            assert time.monotonic()-started<1
        finally:
            lock.rollback(); lock.close()
        await asyncio.sleep(.1)

@pytest.mark.asyncio
async def test_admin_archive_restore_and_permanent_delete_preserve_other_sessions(secured_app):
    maker=secured_app; now=datetime.now(timezone.utc); sid="historical-session"
    raw_dir=main_module.settings.data_dir/"raw"/"match_OLD-EVENT"/sid
    raw_dir.mkdir(parents=True); (raw_dir/"human_events.ndjson").write_text('{"historical":true}\n')
    async with maker() as db:
        db.add(Session(id=sid,event_ticker="OLD-EVENT",home_team="Old Home",away_team="Old Away",scheduled_start=now,created_at=now,started_at=now,ended_at=None,score_source="MANUAL",app_version="test",git_commit="test",notes="legacy inactive",session_type="MATCH_DAY",series_ticker="OLD",mock_mode=False,trading_enabled=False,kalshi_ws_status="DISCONNECTED"))
        db.add(HumanEvent(id="old-event",event_group_id="old-group",session_id=sid,match_id="OLD-EVENT",device_wall_ts_ms=time.time()*1000,server_receive_ts_ns=time.time_ns(),event_type="VAR_CHECK",detail={},operator_session_id=None,realtime_eligible=False))
        await db.commit()
    transport=httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport,base_url=ORIGIN) as operator, httpx.AsyncClient(transport=transport,base_url=ORIGIN) as admin:
        assert (await login(operator)).status_code==200
        assert (await operator.post(f"/api/sessions/{sid}/archive",json={},headers=csrf_headers(operator))).status_code==403
        assert (await operator.request("DELETE",f"/api/sessions/{sid}",json={"confirmation":f"DELETE {sid}"},headers=csrf_headers(operator))).status_code==403
        assert (await login(admin,"ADMIN")).status_code==200
        headers=csrf_headers(admin)
        archived=await admin.post(f"/api/sessions/{sid}/archive",json={},headers=headers)
        assert archived.status_code==200
        default_ids={item["id"] for item in (await admin.get("/api/sessions")).json()["items"]}
        assert sid not in default_ids and "match-session" in default_ids
        included=(await admin.get("/api/sessions?include_archived=true")).json()
        assert included["archived_count"]==1
        assert next(item for item in included["items"] if item["id"]==sid)["archived_at"] is not None
        async with maker() as db:
            assert await db.get(HumanEvent,"old-event") is not None
        assert raw_dir.is_dir()
        assert (await admin.request("DELETE",f"/api/sessions/{sid}",json={"confirmation":f"DELETE {sid}"},headers=headers)).status_code==403
        assert (await admin.post("/api/auth/admin/verify",json={"password":"admin-pass"},headers=headers)).status_code==200
        assert (await admin.request("DELETE",f"/api/sessions/{sid}",json={"confirmation":"DELETE"},headers=headers)).status_code==400
        restored=await admin.post(f"/api/sessions/{sid}/restore",json={},headers=headers)
        assert restored.status_code==200
        assert (await admin.request("DELETE",f"/api/sessions/{sid}",json={"confirmation":f"DELETE {sid}"},headers=headers)).status_code==409
        assert (await admin.post(f"/api/sessions/{sid}/archive",json={},headers=headers)).status_code==200
        deleted=await admin.request("DELETE",f"/api/sessions/{sid}",json={"confirmation":f"DELETE {sid}"},headers=headers)
        assert deleted.status_code==200 and deleted.json()["deleted_rows"]["human_events"]==1
    async with maker() as db:
        assert await db.get(Session,sid) is None
        assert await db.get(HumanEvent,"old-event") is None
        assert await db.get(Session,"match-session") is not None
    assert not raw_dir.exists()

@pytest.mark.asyncio
async def test_passwords_are_bound_to_server_side_roles_and_legacy_sessions_fail_closed(secured_app):
    maker=secured_app; transport=httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport,base_url=ORIGIN) as client:
        assert (await client.post("/api/auth/login",json={"username":"operator","password":"admin-pass"},headers={"origin":ORIGIN})).status_code==401
        assert (await client.post("/api/auth/login",json={"username":"michael","password":"operator-pass"},headers={"origin":ORIGIN})).status_code==401
        async with maker() as db:
            now=time.time_ns(); db.add(AuthSession(id="legacy",role=None,token_hash=token_hash("legacy-token"),csrf_hash=token_hash("legacy-csrf"),created_at_ns=now,expires_at_ns=now+60_000_000_000,last_seen_at_ns=now,remembered=True,revoked_at_ns=None,admin_verified_until_ns=now+60_000_000_000,user_agent="legacy")); await db.commit()
        client.cookies.set("csl_session","legacy-token")
        me=await client.get("/api/auth/me"); assert me.status_code==200 and not me.json()["authenticated"]
        assert (await client.get("/api/operator/bootstrap")).status_code==401
        assert (await client.get("/api/sessions")).status_code==401
        client.cookies.set("csl_session","forged-admin-role")
        assert (await client.get("/api/sessions")).status_code==401

@pytest.mark.asyncio
async def test_operator_twenty_acks_dedup_and_historical_exclusion(secured_app):
    maker=secured_app; transport=httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport,base_url=ORIGIN) as client:
        assert (await login(client)).status_code==200
        headers=csrf_headers(client); ids=[]
        for index in range(20):
            event_id=str(uuid.uuid4()); ids.append(event_id); now=time.time()*1000
            payload={"event_id":event_id,"event_group_id":str(uuid.uuid4()),"event_type":"BALL_IN_NET","team":"CHINA","authorized_event_ticker":"CHN-MDV","device_wall_ts_ms":now,"device_perf_ts_ms":100+index,"pointerdown_perf_ts_ms":99+index,"client_enqueue_perf_ts_ms":100+index,"client_fetch_start_perf_ts_ms":101+index,"detail":{}}
            response=await client.post("/api/operator/events",json=payload,headers=headers)
            assert response.status_code==200 and response.json()["ok"] and response.json()["realtime_eligible"]
        duplicate=await client.post("/api/operator/events",json={"event_id":ids[0],"event_group_id":str(uuid.uuid4()),"event_type":"BALL_IN_NET","team":"CHINA","authorized_event_ticker":"CHN-MDV","device_wall_ts_ms":time.time()*1000,"client_enqueue_perf_ts_ms":1,"client_fetch_start_perf_ts_ms":2,"detail":{}},headers=headers)
        assert duplicate.status_code==200 and duplicate.json()["duplicate"]
        stale=await client.post("/api/operator/events",json={"event_id":str(uuid.uuid4()),"event_group_id":str(uuid.uuid4()),"event_type":"VAR_CHECK","authorized_event_ticker":"CHN-MDV","device_wall_ts_ms":time.time()*1000-60_000,"client_enqueue_perf_ts_ms":1,"client_fetch_start_perf_ts_ms":60_001,"detail":{}},headers=headers)
        assert stale.status_code==200 and not stale.json()["realtime_eligible"]
        bootstrap=(await client.get("/api/operator/bootstrap")).json()
        assert "credentials" not in str(bootstrap).lower() and "private_key" not in str(bootstrap).lower()
    async with maker() as db:
        assert await db.scalar(select(func.count()).select_from(HumanEvent))==21
        assert await db.scalar(select(func.count()).select_from(HumanEvent).where(HumanEvent.realtime_eligible.is_(False)))==1

@pytest.mark.asyncio
async def test_login_rate_limit_and_origin_csrf(secured_app):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app),base_url=ORIGIN) as client:
        no_origin=await client.post("/api/auth/login",json={"username":"operator","password":"operator-pass"}); assert no_origin.status_code==403
        for _ in range(5): assert (await client.post("/api/auth/login",json={"username":"operator","password":"bad"},headers={"origin":ORIGIN})).status_code==401
        assert (await client.post("/api/auth/login",json={"username":"operator","password":"bad"},headers={"origin":ORIGIN})).status_code==429

def test_empty_versioned_config_has_no_active_match(tmp_path):
    path=tmp_path/"matches.v1.json"; path.write_text('{"schema_version":1,"matches":[]}')
    assert load_match_config(path).matches==[]
    assert main_module.settings.auto_discovery_enabled is False

@pytest.mark.asyncio
async def test_manual_activation_uses_only_exact_markets_and_configured_buttons(secured_app,monkeypatch):
    captured={}
    async def exact(ticker):
        assert ticker=="CHN-MDV"
        return {"event_ticker":ticker,"series_ticker":"SERIES","markets":[{"ticker":"CHN-MDV-WIN","title":"wire","group":"OTHER","raw":{"ticker":"CHN-MDV-WIN"}},{"ticker":"SIMILAR-OTHER","title":"must not subscribe","group":"OTHER","raw":{}}]}
    async def focus(event,sid): captured.update(event=event,sid=sid); main_module.kalshi.session_id=sid; main_module.kalshi.connected=True
    monkeypatch.setattr(main_module,"manual_active_session_id",None); monkeypatch.setattr(main_module.discovery,"event_exact",exact); monkeypatch.setattr(main_module.kalshi,"focus_match",focus); monkeypatch.setattr(main_module.kalshi,"connected",False)
    result=await main_module.start(main_module.StartReq(event_ticker="CHN-MDV",session_mode="MATCH_DAY"))
    assert result["session_id"]=="match-session"
    assert [m["ticker"] for m in captured["event"]["markets"]]==["CHN-MDV-WIN"]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app),base_url=ORIGIN) as client:
        assert (await login(client)).status_code==200
        boot=(await client.get("/api/operator/bootstrap")).json()
        assert boot["match"]["home_label"]=="CHINA" and boot["match"]["away_label"]=="MALDIVES"
        assert [b["label"] for b in boot["match"]["allowed_event_buttons"]]==["GOAL","VAR","CORRECTION"]

@pytest.mark.asyncio
async def test_deactivation_blocks_signals_without_deleting_history(secured_app,monkeypatch):
    maker=secured_app; before=time.time_ns()
    async with maker() as db:
        db.add(HumanEvent(id="historical",event_group_id="historical-group",session_id="match-session",match_id="CHN-MDV",device_wall_ts_ms=before/1e6,server_receive_ts_ns=before,event_type="VAR_CHECK",detail={},operator_session_id=None,realtime_eligible=False)); await db.commit()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_module.app),base_url=ORIGIN) as client:
        assert (await login(client,"ADMIN")).status_code==200
        headers=csrf_headers(client)
        stopped=await client.post("/api/matches/CHN-MDV/deactivate",json={},headers=headers); assert stopped.status_code==200 and stopped.json()["changed"]
        rejected=await client.post("/api/operator/events",json={"event_id":str(uuid.uuid4()),"event_group_id":str(uuid.uuid4()),"event_type":"VAR_CHECK","authorized_event_ticker":"CHN-MDV","device_wall_ts_ms":time.time()*1000,"client_enqueue_perf_ts_ms":1,"client_fetch_start_perf_ts_ms":2,"detail":{}},headers=headers)
        assert rejected.status_code==409
    async with maker() as db:
        row=await db.get(HumanEvent,"historical"); assert row and row.server_receive_ts_ns==before
        assert await db.scalar(select(func.count()).select_from(HumanEvent))==1
