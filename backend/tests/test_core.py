import asyncio, json, time
from pathlib import Path
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select, func
from app.models import Base, RawMessage, Quote
from app.recorder import Recorder, OrderBooks
from app.config import Settings, PROJECT_ROOT
from app.kalshi import Discovery, KalshiEngine, group_market

@pytest.mark.asyncio
async def test_ingest_is_nonblocking_and_raw_lossless(tmp_path):
    eng=create_async_engine("sqlite+aiosqlite:///:memory:"); maker=async_sessionmaker(eng,expire_on_commit=False)
    async with eng.begin() as c: await c.run_sync(Base.metadata.create_all)
    async def broadcast(_): pass
    r=Recorder(maker,tmp_path,broadcast); await r.start(); start=time.perf_counter()
    for i in range(1000): r.ingest("s","E",{"type":"ticker","seq":i,"msg":{"market_ticker":"M","yes_bid_dollars":"0.50","ts_ms":i}})
    assert time.perf_counter()-start < .25
    await asyncio.wait_for(r.raw_q.join(),5); await asyncio.wait_for(r.db_q.join(),5)
    lines=(tmp_path/"raw/match_E/s/kalshi_ws.ndjson").read_text().splitlines(); assert len(lines)==1000
    assert json.loads(lines[0])["local_recv_ts_ns"]>0
    async with maker() as db: assert (await db.scalar(select(func.count()).select_from(RawMessage)))==1000
    await r.stop(); await eng.dispose()

def test_orderbook_snapshot_delta_and_gap():
    b=OrderBooks(); assert not b.apply({"type":"orderbook_snapshot","seq":10,"msg":{"market_ticker":"M","yes_dollars_fp":[["0.50","10"]],"no_dollars_fp":[["0.47","20"]]}})
    assert not b.apply({"type":"orderbook_delta","seq":11,"msg":{"market_ticker":"M","side":"yes","price_dollars":"0.50","delta_fp":"-3"}})
    assert b.quote("M")["yes_bid_size"]==7
    assert b.apply({"type":"orderbook_delta","seq":13,"msg":{"market_ticker":"M","side":"yes","price_dollars":"0.51","delta_fp":"4"}})
    assert b.gaps==1

def test_native_defaults_are_project_local_sqlite(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False); monkeypatch.delenv("DATA_DIR", raising=False)
    cfg=Settings(_env_file=None)
    assert cfg.database_url == f"sqlite+aiosqlite:///{PROJECT_ROOT / 'data/csl_trading_lab.db'}"
    assert cfg.data_dir == PROJECT_ROOT / "data"
    assert "postgres" not in cfg.database_url

def test_production_market_classifier_and_binary_complements():
    assert group_market({"ticker":"KXCHNSLGAME-26AUG28SHSSHT-SHS","title":"Shanghai Shenhua wins"})=="MATCH RESULT"
    assert group_market({"ticker":"KXCHNSLTOTAL-26AUG28SHSSHT-T2.5","title":"Total goals over 2.5"})=="TOTAL"
    assert group_market({"ticker":"KXCHNSLBTTS-26AUG28SHSSHT","title":"Both teams to score"})=="BTTS"
    assert group_market({"ticker":"KXCHNSLSPREAD-26AUG28SHSSHT-H1.5","title":"Shanghai Shenhua spread"})=="SPREAD"
    b=OrderBooks(); b.apply({"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"M","yes_dollars_fp":[["0.55","10"]],"no_dollars_fp":[["0.44","20"]]}})
    q=b.quote("M")
    assert q["yes_bid"]==.55 and q["yes_ask"]==.56
    assert q["no_bid"]==.44 and q["no_ask"]==.45
    assert q["provenance"]=={
        "yes_bid":{"kind":"RAW","source":"ORDERBOOK_YES_BID"},
        "yes_ask":{"kind":"DERIVED","source":"ORDERBOOK_NO_BID_COMPLEMENT"},
        "no_bid":{"kind":"RAW","source":"ORDERBOOK_NO_BID"},
        "no_ask":{"kind":"DERIVED","source":"ORDERBOOK_YES_BID_COMPLEMENT"}}

def test_team_identity_is_generic_and_kickoff_unverified():
    d=Discovery(Settings(_env_file=None))
    for home,away in (("Shanghai Shenhua","Shandong Taishan"),("Shenzhen Peng City","Shanghai Port"),("Dalian Yingbo FC","Beijing Guoan")):
        raw={"event_ticker":"E","title":f"{home} vs {away}","markets":[
            {"ticker":"H","title":f"{home} wins","yes_sub_title":home,"occurrence_datetime":"2026-08-28T14:35:00Z"},
            {"ticker":"A","title":f"{away} wins","yes_sub_title":away},
            {"ticker":"T","title":"Tie wins","yes_sub_title":"Tie"}]}
        match=d._normalize(raw)
        assert (match["home_team"],match["away_team"])==(home,away)
        assert match["team_source"]=="event.title verified by outcome yes_sub_title"
        assert match["kalshi_occurrence_datetime"]=="2026-08-28T14:35:00Z"
        assert match["display_kickoff_datetime"] is None and not match["kickoff_verified"]

@pytest.mark.asyncio
async def test_orderbook_subscription_locks_legacy_outcome_side_pricing():
    class WS:
        async def send(self,value): self.value=json.loads(value)
    engine=object.__new__(KalshiEngine); engine.ws=WS(); engine.reconnect_count=0
    async def system(*_): pass
    engine._system=system
    await engine._subscribe(["M"])
    assert engine.ws.value["params"]["use_yes_price"] is False

@pytest.mark.asyncio
async def test_dynamic_discovery_adds_only_focus_market_without_reconnect(tmp_path):
    eng=create_async_engine("sqlite+aiosqlite:///:memory:"); maker=async_sessionmaker(eng,expire_on_commit=False)
    async with eng.begin() as c: await c.run_sync(Base.metadata.create_all)
    class D:
        async def event(self,_): return {"markets":[{"ticker":"KXCHNSLBTTS-26AUG28SHSSHT","title":"Both teams to score","group":"BTTS","raw":{}}]}
    class R:
        class B: gaps=0
        books=B()
        def ingest(self,*_): pass
    class WS:
        def __init__(self): self.sent=[]
        async def send(self,value): self.sent.append(json.loads(value))
    emitted=[]
    async def broadcast(value): emitted.append(value)
    engine=KalshiEngine(Settings(_env_file=None),D(),R(),maker,broadcast); engine.focus="KXCHNSLGAME-26AUG28SHSSHT"; engine.session_id="s"; engine.connected=True; engine.ws=WS(); engine.subscriptions={"ticker":1,"trade":2,"orderbook_delta":3}; engine.markets={"GAME-A","GAME-B","GAME-C"}; engine.market_groups={x:"MATCH RESULT" for x in engine.markets}
    new=await engine.discovery_scan_once()
    assert [x["ticker"] for x in new]==["KXCHNSLBTTS-26AUG28SHSSHT"]
    assert engine._family_counts()=={"GAME":3,"BTTS":1,"TOTAL":0,"SPREAD":0}
    assert engine.ws.sent[0]["cmd"]=="update_subscription" and engine.ws.sent[0]["params"]["action"]=="add_markets"
    assert emitted[0]["type"]=="markets_added"
    await eng.dispose()
