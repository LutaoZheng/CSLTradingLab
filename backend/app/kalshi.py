import asyncio, base64, json, logging, random, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import httpx, websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from .config import Settings
from .models import Market, Session

log=logging.getLogger("csl.focus")

MOCK_EVENT={"event_ticker":"KXCSL-MOCK-SHST","title":"Shanghai Shenhua vs Shandong Taishan","home_team":"Shanghai Shenhua","away_team":"Shandong Taishan","scheduled_start":(datetime.now(timezone.utc)+timedelta(hours=2)).isoformat(),"status":"PRE","markets":[
 {"ticker":"KXCSL-MOCK-HOME","title":"Shanghai Shenhua Win","group":"MATCH RESULT"},
 {"ticker":"KXCSL-MOCK-DRAW","title":"Draw","group":"MATCH RESULT"},
 {"ticker":"KXCSL-MOCK-AWAY","title":"Shandong Taishan Win","group":"MATCH RESULT"}]}

class Discovery:
    def __init__(self,cfg:Settings): self.cfg=cfg; self._series_cache=[]
    async def csl_series(self):
        configured=[x.strip() for x in self.cfg.csl_series_tickers.split(",") if x.strip()]
        if configured:
            allowed={"KXCHNSLGAME","KXCHNSLTOTAL","KXCHNSLBTTS","KXCHNSLSPREAD"}
            if not set(configured) <= allowed: raise RuntimeError("CSL_SERIES_TICKERS contains a non-CSL or unsupported series")
            return sorted(set(configured) | ({"KXCHNSLTOTAL","KXCHNSLBTTS","KXCHNSLSPREAD"} if "KXCHNSLGAME" in configured else set()))
        if self._series_cache: return self._series_cache
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.get(f"{self.cfg.kalshi_rest_url}/series",params={"category":"Sports","include_product_metadata":"true"}); r.raise_for_status()
        exact=[]
        for s in r.json().get("series",[]):
            ticker=s.get("ticker",""); title=s.get("title",""); meta=s.get("product_metadata") or {}
            if ticker.startswith("KXCHNSL") and (title=="Chinese Super League Game" or ticker in {"KXCHNSLGAME","KXCHNSLTOTAL","KXCHNSLBTTS","KXCHNSLSPREAD"}): exact.append(ticker)
        if "KXCHNSLGAME" not in exact: raise RuntimeError("Exact Chinese Super League Game series not found")
        self._series_cache=sorted(set(exact)); return self._series_cache
    async def matches(self):
        if self.cfg.mock_mode: return [MOCK_EVENT]
        # Only the exact CSL GAME series produces home-page matches; sibling series are derivatives.
        series=[x for x in await self.csl_series() if x=="KXCHNSLGAME" or x.endswith("GAME")]
        out=[]
        async with httpx.AsyncClient(timeout=10) as c:
            for s in series:
                cursor=None
                while True:
                    params={"series_ticker":s,"with_nested_markets":"true","limit":200}
                    if cursor: params["cursor"]=cursor
                    r=await c.get(f"{self.cfg.kalshi_rest_url}/events",params=params); r.raise_for_status(); data=r.json()
                    for e in data.get("events",[]):
                        if (e.get("product_metadata") or {}).get("competition") == "Chinese Super League" and e.get("markets"):
                            normalized=self._normalize(e)
                            if normalized["status"]!="CLOSED": out.append(normalized)
                    cursor=data.get("cursor")
                    if not cursor: break
        return out
    async def event(self,ticker):
        if self.cfg.mock_mode: return MOCK_EVENT
        async with httpx.AsyncClient(timeout=10) as c:
            r=await c.get(f"{self.cfg.kalshi_rest_url}/events/{ticker}",params={"with_nested_markets":"true"}); r.raise_for_status()
            d=r.json(); e=d["event"]; e["markets"]=e.get("markets") or d.get("markets",[]); base=self._normalize(e)
        related=await self.related_markets(base)
        base["markets"]=related; return base
    async def related_markets(self,base):
        found={m["ticker"]:m for m in base["markets"]}; target={base["home_team"],base["away_team"]}
        async with httpx.AsyncClient(timeout=20) as c:
            for series in await self.csl_series():
                if series==base.get("series_ticker"): continue
                r=await c.get(f"{self.cfg.kalshi_rest_url}/events",params={"series_ticker":series,"with_nested_markets":"true","limit":200}); r.raise_for_status()
                for e in r.json().get("events",[]):
                    title=e.get("title",""); parts={x.strip() for x in title.split(":",1)[0].split(" vs ")}
                    if parts!=target: continue
                    for m in e.get("markets",[]):
                        n=self._market(m); found[n["ticker"]]=n
        return list(found.values())
    def _market(self,m):
        return {"ticker":m["ticker"],"title":m.get("title") or m.get("yes_sub_title") or m["ticker"],"group":group_market(m),"raw":m}
    def _normalize(self,e):
        title=e.get("title",""); parts=title.split(" vs ",1); markets=[]
        for m in e.get("markets",[]): markets.append(self._market(m))
        home=parts[0].strip() if len(parts)==2 else "Home"; away=parts[1].split(":")[0].strip() if len(parts)==2 else "Away"
        outcome_names={m.get("yes_sub_title") for m in e.get("markets",[]) if m.get("yes_sub_title") not in {None,"Tie"}}
        team_verified={home,away} <= outcome_names
        scheduled=next((m.get("occurrence_datetime") or m.get("expected_expiration_time") for m in e.get("markets",[]) if m.get("occurrence_datetime") or m.get("expected_expiration_time")),None)
        occurrence=scheduled or e.get("strike_date")
        return {"event_ticker":e["event_ticker"],"series_ticker":e.get("series_ticker"),"title":title,"home_team":home,"away_team":away,"team_source":"event.title verified by outcome yes_sub_title" if team_verified else "event.title","team_parse_risk":not team_verified,"scheduled_start":occurrence,"kalshi_occurrence_datetime":occurrence,"display_kickoff_datetime":None,"display_kickoff_source":None,"kickoff_verified":False,"scheduled_time_source":"kalshi_market.occurrence_datetime" if scheduled else "kalshi_event.strike_date","scheduled_time_verified":False,"status":normalize_status(markets,scheduled),"markets":markets}

def normalize_status(markets,scheduled=None):
    statuses={m.get("raw",{}).get("status") for m in markets}
    if statuses and statuses <= {"closed","settled","finalized"}: return "CLOSED"
    if scheduled:
        try:
            start=datetime.fromisoformat(scheduled.replace("Z","+00:00"))
            if datetime.now(timezone.utc)<start: return "PRE"
        except ValueError: pass
    if statuses & {"active","open"}: return "LIVE"
    return "PRE"
def group_market(m):
    ticker=m.get("ticker","").upper(); s=" ".join(str(m.get(k,"")) for k in ("title","subtitle","yes_sub_title","rules_primary")).lower()
    if "BTTS" in ticker or "btts" in s or "both teams" in s: return "BTTS"
    if "SPREAD" in ticker or "spread" in s or "handicap" in s: return "SPREAD"
    if "TOTAL" in ticker:
        if "team total" in s or "goals by" in s: return "TEAM TOTAL"
        return "TOTAL"
    if "btts" in s or "both teams" in s: return "BTTS"
    if "total" in s and ("home" in s or "away" in s or "team" in s): return "TEAM TOTAL"
    if "total" in s or "over" in s: return "TOTAL"
    if "GAME" in ticker or any(x in s for x in (" wins","tie wins")): return "MATCH RESULT"
    return "OTHER"

class KalshiEngine:
    def __init__(self,cfg,discovery,recorder,maker,broadcast):
        self.cfg=cfg; self.discovery=discovery; self.recorder=recorder; self.maker=maker; self.broadcast=broadcast
        self.task=None; self.discovery_task=None; self.focus=None; self.session_id=None; self.markets=set(); self.ws=None
        self.connected=False; self.auth_ok=False; self.reconnect_count=0; self.last_message_ns=None; self.stop_flag=False
        self.subscriptions={}; self.subscription_requested=set(); self.snapshots=set(); self.deltas_seen=set(); self.trade_subscribed=False; self.ticker_subscribed=False
        self.dynamic_discovery_ok=False; self.new_markets_observed=0; self.disconnect_ts_ns=None; self.reconnect_ts_ns=None; self.resync_complete_ts_ns=None
        self.market_groups={}; self.market_focus={}; self.last_discovery_scan_ns=None; self.last_discovery_duration_ms=None; self.discovery_error=None
    async def focus_match(self,event,session_id):
        await self.stop(); self.focus=event["event_ticker"]; self.session_id=session_id; self.markets={m["ticker"] for m in event["markets"]}; self.stop_flag=False
        self.market_groups={m["ticker"]:m.get("group","OTHER") for m in event["markets"]}; self.market_focus={m["ticker"]:self.focus for m in event["markets"]}; self.dynamic_discovery_ok=False; self.discovery_error=None
        self.recorder.activate(session_id)
        log.info("FOCUS_SESSION event_ticker=%s session_id=%s",self.focus,self.session_id)
        log.info("WS_SUBSCRIPTIONS focus_markets=%d other_game_markets=0 tickers=%s",len(self.markets),sorted(self.markets))
        log.info("DISCOVERY %s",self._family_counts())
        await self._persist_markets(event["markets"]); self.task=asyncio.create_task(self._run()); self.discovery_task=asyncio.create_task(self._discover())
    async def stop(self):
        self.stop_flag=True
        for t in (self.task,self.discovery_task):
            if t: t.cancel()
        self.task=self.discovery_task=None; self.connected=False
    async def stop_focus(self,session_id=None):
        if session_id is not None and self.session_id!=session_id: return False
        await self.stop()
        if self.session_id: await self.recorder.finalize_session(self.session_id)
        self.focus=None; self.session_id=None; self.markets.clear(); self.market_groups.clear(); self.market_focus.clear()
        self.subscriptions.clear(); self.subscription_requested.clear(); self.snapshots.clear(); self.deltas_seen.clear()
        self.ws=None; self.auth_ok=False; self.ticker_subscribed=False; self.trade_subscribed=False; self.dynamic_discovery_ok=False
        return True
    async def _persist_markets(self,markets):
        async with self.maker() as db:
            for m in markets:
                if not await db.get(Market,m["ticker"]): db.add(Market(ticker=m["ticker"],event_ticker=self.focus,title=m["title"],group_name=m["group"],metadata_json=m.get("raw",{})))
            await db.commit()
    async def _run(self):
        if self.cfg.mock_mode: return await self._mock()
        delay=1
        while not self.stop_flag:
            try:
                headers=self._headers()
                async with websockets.connect(self.cfg.kalshi_ws_url,additional_headers=headers,ping_interval=15,ping_timeout=10,max_queue=None) as ws:
                    self.ws=ws; self.connected=True; self.auth_ok=True; self.reconnect_ts_ns=time.time_ns(); delay=1
                    self.subscriptions.clear(); self.subscription_requested=set(self.markets); self.snapshots.clear(); self.deltas_seen.clear(); self.recorder.books.books.clear(); self.recorder.books.last_seq.clear()
                    await self._system("WS_RECONNECTED" if self.reconnect_count else "WS_CONNECTED",{}); await self._system("AUTH_OK",{})
                    await self._subscribe(sorted(self.markets))
                    async for text in ws:
                        self.last_message_ns=time.time_ns(); payload=json.loads(text); self._observe_protocol(payload); self.recorder.ingest(self.session_id,self.focus,payload)
                    if not self.stop_flag: raise ConnectionError("WebSocket closed")
            except asyncio.CancelledError: break
            except Exception as exc:
                self.connected=False; self.auth_ok=False; self.disconnect_ts_ns=time.time_ns(); self.reconnect_count+=1; await self._system("WS_DISCONNECTED",{"error_type":type(exc).__name__,"error":str(exc),"reconnect_count":self.reconnect_count}); await self._system("WS_RECONNECTING",{"delay_seconds":delay})
                await asyncio.sleep(delay); delay=min(delay*2,30)
                await self._system("ORDERBOOK_RESYNC_REQUESTED",{})
    def _headers(self):
        if not self.cfg.kalshi_api_key_id or not self.cfg.kalshi_private_key_path: raise RuntimeError("Kalshi credentials missing")
        ts=str(int(time.time()*1000)); msg=(ts+"GET"+"/trade-api/ws/v2").encode()
        key=serialization.load_pem_private_key(Path(self.cfg.kalshi_private_key_path).read_bytes(),password=None)
        sig=key.sign(msg,padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=padding.PSS.DIGEST_LENGTH),hashes.SHA256())
        return {"KALSHI-ACCESS-KEY":self.cfg.kalshi_api_key_id,"KALSHI-ACCESS-TIMESTAMP":ts,"KALSHI-ACCESS-SIGNATURE":base64.b64encode(sig).decode()}
    async def _subscribe(self,tickers):
        await self.ws.send(json.dumps({"id":int(time.time()*1000)%1000000,"cmd":"subscribe","params":{"channels":["ticker","trade","orderbook_delta"],"market_tickers":tickers,"use_yes_price":False}})); await self._system("RESUBSCRIBE" if self.reconnect_count else "SUBSCRIBE_SENT",{"market_count":len(tickers),"channels":["ticker","trade","orderbook_delta"],"orderbook_price_mode":"LEGACY_OUTCOME_SIDE","use_yes_price":False})
    async def add_markets(self,tickers):
        if not (self.ws and self.connected): return
        sids=list(self.subscriptions.values())
        if sids:
            await self.ws.send(json.dumps({"id":int(time.time()*1000)%1000000,"cmd":"update_subscription","params":{"sids":sids,"market_tickers":tickers,"action":"add_markets"}}))
        else: await self._subscribe(tickers)
        self.subscription_requested.update(tickers)
        for ticker in tickers: log.info("WS_SUBSCRIPTION_ADDED ticker=%s",ticker)
    def _observe_protocol(self,p):
        typ=p.get("type"); msg=p.get("msg",{})
        if typ=="subscribed":
            channel=msg.get("channel"); sid=msg.get("sid")
            if channel and sid is not None: self.subscriptions[channel]=sid
            self.ticker_subscribed="ticker" in self.subscriptions; self.trade_subscribed="trade" in self.subscriptions
        elif typ=="orderbook_snapshot":
            ticker=msg.get("market_ticker")
            if ticker: self.snapshots.add(ticker)
            if self.subscription_requested and self.subscription_requested <= self.snapshots:
                self.resync_complete_ts_ns=time.time_ns()
                asyncio.create_task(self._system("ORDERBOOK_RESYNC",{"market_count":len(self.snapshots)}))
        elif typ=="orderbook_delta":
            ticker=msg.get("market_ticker")
            if ticker: self.deltas_seen.add(ticker)
    async def _discover(self):
        while True:
            await asyncio.sleep(self.cfg.discovery_interval_seconds); started=time.perf_counter()
            try:
                new=await self.discovery_scan_once()
                self.last_discovery_scan_ns=time.time_ns(); self.last_discovery_duration_ms=round((time.perf_counter()-started)*1000,2)
                log.info("DISCOVERY_SCAN duration_ms=%.2f new_markets=%d",self.last_discovery_duration_ms,len(new))
            except asyncio.CancelledError: raise
            except Exception as exc:
                self.dynamic_discovery_ok=False; self.discovery_error=f"{type(exc).__name__}: {exc}"; self.last_discovery_scan_ns=time.time_ns(); self.last_discovery_duration_ms=round((time.perf_counter()-started)*1000,2)
                log.warning("DISCOVERY_SCAN duration_ms=%.2f error_type=%s",self.last_discovery_duration_ms,type(exc).__name__)
    async def discovery_scan_once(self):
        e=await self.discovery.event(self.focus); self.dynamic_discovery_ok=True; self.discovery_error=None
        new=[m for m in e["markets"] if m["ticker"] not in self.markets]
        if new:
            self.new_markets_observed+=len(new); await self._persist_markets(new)
            for m in new:
                self.markets.add(m["ticker"]); self.market_groups[m["ticker"]]=m.get("group","OTHER"); self.market_focus[m["ticker"]]=self.focus
                log.info("NEW_MARKET_DISCOVERED family=%s ticker=%s",m.get("group","OTHER"),m["ticker"]); await self._system("NEW_MARKET_DISCOVERED",m)
            await self.add_markets([m["ticker"] for m in new]); await self.broadcast({"type":"markets_added","items":new})
        return new
    async def _mock(self):
        self.connected=True; self.auth_ok=True; self.subscriptions={"ticker":2,"trade":3,"orderbook_delta":1}; self.ticker_subscribed=True; self.trade_subscribed=True; self.dynamic_discovery_ok=True
        await self._system("WS_CONNECTED",{"mock":True}); seq=0; books={t:50+i*3 for i,t in enumerate(sorted(self.markets))}
        for t,p in books.items():
            seq+=1; self.recorder.ingest(self.session_id,self.focus,{"type":"orderbook_snapshot","sid":1,"seq":seq,"msg":{"market_ticker":t,"yes_dollars_fp":[[f"{p/100:.2f}","800"]],"no_dollars_fp":[[f"{(97-p)/100:.2f}","600"]]}})
            self.snapshots.add(t)
        while True:
            await asyncio.sleep(.12); t=random.choice(list(self.markets)); seq+=1; side=random.choice(["yes","no"]); price=(books.get(t,50) if side=="yes" else 97-books.get(t,50))/100
            self.last_message_ns=time.time_ns(); self.recorder.ingest(self.session_id,self.focus,{"type":"orderbook_delta","sid":1,"seq":seq,"msg":{"market_ticker":t,"price_dollars":f"{price:.2f}","delta_fp":str(random.choice([-15,-5,5,20])),"side":side,"ts_ms":int(time.time()*1000)}})
            if seq%15==0: self.recorder.ingest(self.session_id,self.focus,{"type":"ticker","sid":2,"seq":seq,"msg":{"market_ticker":t,"yes_bid_dollars":f"{price:.2f}","yes_ask_dollars":f"{min(price+.03,.99):.2f}","volume_fp":str(seq),"ts_ms":int(time.time()*1000)}})
            if seq%29==0: self.recorder.ingest(self.session_id,self.focus,{"type":"trade","sid":3,"seq":seq,"msg":{"market_ticker":t,"yes_price_dollars":f"{price:.2f}","count_fp":"10","taker_side":"yes","trade_id":f"mock-{seq}","ts_ms":int(time.time()*1000)}})
    async def _system(self,kind,msg):
        if self.session_id: self.recorder.ingest(self.session_id,self.focus,{"type":kind,"msg":msg})
    async def inject(self,kind):
        if kind=="disconnect":
            await self._system("WS_DISCONNECTED",{"mock":True}); self.connected=False; self.reconnect_count+=1; await asyncio.sleep(.2); self.connected=True; await self._system("WS_RECONNECTED",{"mock":True}); await self._system("ORDERBOOK_RESYNC",{"mock":True})
        elif kind=="new_market":
            t="KXCSL-MOCK-BTTS";
            if t not in self.markets:
                m={"ticker":t,"title":"Both teams to score","group":"BTTS"}; await self._persist_markets([m]); self.markets.add(t); await self._system("NEW_MARKET_DISCOVERED",m); await self.broadcast({"type":"markets_added","items":[m]})
    async def controlled_reconnect(self):
        if not self.ws or not self.connected: raise RuntimeError("WebSocket is not connected")
        await self.ws.close(code=1000,reason="controlled read-only dry run reconnect")
    def health(self,session_id=None):
        gap_ms=(self.resync_complete_ts_ns-self.disconnect_ts_ns)/1e6 if self.disconnect_ts_ns and self.resync_complete_ts_ns and self.resync_complete_ts_ns>=self.disconnect_ts_ns else None
        active=session_id is None or session_id==self.session_id
        subscribed=set(self.subscription_requested) if active else set()
        other=sum(1 for ticker in subscribed if self.market_focus.get(ticker)!=self.focus) if active else 0
        return {"session_id":self.session_id if active else None,"focus_event_ticker":self.focus if active else None,"connected":self.connected if active else False,"auth_ok":self.auth_ok if active else False,"reconnect_count":self.reconnect_count if active else 0,"last_ws_message_age_ms":(time.time_ns()-self.last_message_ns)/1e6 if active and self.last_message_ns else None,"sequence_gap_count":self.recorder.books.gaps if active else 0,"subscriptions":self.subscriptions if active else {},"subscribed_market_count":len(subscribed),"subscribed_tickers":sorted(subscribed),"other_game_markets":other,"market_family_counts":self._family_counts() if active else {x:0 for x in ("GAME","BTTS","TOTAL","SPREAD")},"ticker_subscribed":self.ticker_subscribed if active else False,"trade_subscribed":self.trade_subscribed if active else False,"snapshot_markets":len(self.snapshots) if active else 0,"delta_markets":len(self.deltas_seen) if active else 0,"dynamic_discovery":self.dynamic_discovery_ok if active else False,"discovery_status":("ACTIVE" if self.dynamic_discovery_ok else "ERROR") if active else "INACTIVE","discovery_error":self.discovery_error if active else None,"last_discovery_scan_age_ms":(time.time_ns()-self.last_discovery_scan_ns)/1e6 if active and self.last_discovery_scan_ns else None,"last_discovery_duration_ms":self.last_discovery_duration_ms if active else None,"new_markets_observed":self.new_markets_observed if active else 0,"disconnect_ts_ns":self.disconnect_ts_ns if active else None,"reconnect_ts_ns":self.reconnect_ts_ns if active else None,"resync_complete_ts_ns":self.resync_complete_ts_ns if active else None,"gap_duration_ms":gap_ms if active else None}
    def _family_counts(self):
        counts={x:0 for x in ("GAME","BTTS","TOTAL","SPREAD")}
        for ticker in self.markets:
            group=self.market_groups.get(ticker,"OTHER"); key="GAME" if group=="MATCH RESULT" else group
            if key in counts: counts[key]+=1
        return counts

class ScoreAdapter:
    # No documented Kalshi score endpoint exists. Manual is truthful default; adapter boundary is stable.
    source="MANUAL"
    def __init__(self): self.scores={}
    def get(self,event): return {"source":self.source,"home":self.scores.get(event,[0,0])[0],"away":self.scores.get(event,[0,0])[1],"clock":None,"confirmed":False}
    def adjust(self,event,side,delta):
        v=self.scores.setdefault(event,[0,0]); i=0 if side=="home" else 1; v[i]=max(0,v[i]+delta); return self.get(event)
