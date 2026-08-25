import asyncio, json, os, time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from .models import RawMessage, Quote, Trade, BookEvent, SystemEvent

def num(v):
    try: return float(v) if v is not None else None
    except (TypeError, ValueError): return None

@dataclass(slots=True)
class Envelope:
    session_id: str; event_ticker: str; local_ns: int; payload: dict

class OrderBooks:
    def __init__(self): self.books = {}; self.last_seq = {}; self.gaps = 0
    def apply(self, p):
        typ, msg, seq = p.get("type"), p.get("msg", {}), p.get("seq")
        ticker = msg.get("market_ticker"); gap = False; stream = p.get("sid", ticker)
        if not ticker: return gap
        if seq is not None and stream in self.last_seq and seq != self.last_seq[stream] + 1:
            self.gaps += 1; gap = True
        if seq is not None: self.last_seq[stream] = seq
        if typ == "orderbook_snapshot":
            self.books[ticker] = {
                "yes": {str(x[0]): num(x[1]) for x in msg.get("yes_dollars_fp", msg.get("yes", []))},
                "no": {str(x[0]): num(x[1]) for x in msg.get("no_dollars_fp", msg.get("no", []))}}
        elif typ == "orderbook_delta":
            book = self.books.setdefault(ticker, {"yes": {}, "no": {}})
            side, price = msg.get("side"), str(msg.get("price_dollars", msg.get("price")))
            delta = num(msg.get("delta_fp", msg.get("delta"))) or 0
            if side in book:
                value = (book[side].get(price) or 0) + delta
                if value <= 0: book[side].pop(price, None)
                else: book[side][price] = value
        return gap
    def quote(self, ticker):
        b = self.books.get(ticker, {"yes":{},"no":{}}); yes=b["yes"]; no=b["no"]
        ykey=max(yes,key=lambda x:float(x),default=None); nkey=max(no,key=lambda x:float(x),default=None)
        yb=float(ykey) if ykey is not None else None; nb=float(nkey) if nkey is not None else None
        return {"yes_bid":yb,"yes_bid_size":yes.get(ykey),
          "yes_ask":round(1-nb,10) if nb is not None else None,"yes_ask_size":no.get(nkey),
          "no_bid":nb,"no_bid_size":no.get(nkey),
          "no_ask":round(1-yb,10) if yb is not None else None,"no_ask_size":yes.get(ykey),
          "provenance":{"yes_bid":{"kind":"RAW","source":"ORDERBOOK_YES_BID"},"yes_ask":{"kind":"DERIVED","source":"ORDERBOOK_NO_BID_COMPLEMENT"},"no_bid":{"kind":"RAW","source":"ORDERBOOK_NO_BID"},"no_ask":{"kind":"DERIVED","source":"ORDERBOOK_YES_BID_COMPLEMENT"}}}

class Recorder:
    """Receive path only stamps, updates memory, and put_nowait()s unbounded lossless queues."""
    def __init__(self, maker: async_sessionmaker[AsyncSession], data_dir: Path, broadcast):
        self.maker=maker; self.data_dir=data_dir; self.broadcast=broadcast
        self.raw_q=asyncio.Queue(); self.db_q=asyncio.Queue(); self.ui_latest={}; self.books=OrderBooks(); self.tasks=[]
        self.raw_handles={}
        self.raw_ok=False; self.db_ok=False; self.running=False; self.active_session_id=None
        self.metrics_by_session=defaultdict(self._new_metrics); self.raw_ok_by_session=defaultdict(bool); self.db_ok_by_session=defaultdict(bool)
    @staticmethod
    def _new_metrics(): return {"ws_messages_received":0,"raw_queue_enqueued":0,"raw_messages_written":0,"db_queue_enqueued":0,"db_rows_written":0,"ui_frames_emitted":0,"queue_drop_count":0,"db_write_failures":0}
    def activate(self,session_id):
        self.active_session_id=session_id; self.raw_ok=self.raw_ok_by_session[session_id]; self.db_ok=self.db_ok_by_session[session_id]
    async def start(self):
        self.running=True; self.tasks=[asyncio.create_task(self._raw()),asyncio.create_task(self._db()),asyncio.create_task(self._ui())]
        await asyncio.sleep(0)
    async def stop(self):
        self.running=False; await self.raw_q.join(); await self.db_q.join()
        for t in self.tasks: t.cancel()
    async def finalize_session(self,session_id):
        await self.raw_q.join(); await self.db_q.join()
        handle=self.raw_handles.pop(session_id,None)
        if handle: handle.close()
        if self.active_session_id==session_id:
            self.active_session_id=None; self.raw_ok=False; self.db_ok=False
    def ingest(self, session_id, event_ticker, payload):
        env=Envelope(session_id,event_ticker,time.time_ns(),payload)
        metrics=self.metrics_by_session[session_id]; metrics["ws_messages_received"]+=1
        self.raw_q.put_nowait(env); self.db_q.put_nowait(env)
        metrics["raw_queue_enqueued"]+=1; metrics["db_queue_enqueued"]+=1
        typ=payload.get("type", "unknown"); msg=payload.get("msg",{})
        if typ.startswith("orderbook_"):
            gap=self.books.apply(payload); self.ui_latest[msg.get("market_ticker","")]={"type":"quote","ticker":msg.get("market_ticker"),**self.books.quote(msg.get("market_ticker"))}
            if gap: self.db_q.put_nowait(Envelope(session_id,event_ticker,time.time_ns(),{"type":"SEQUENCE_GAP","msg":{"market_ticker":msg.get("market_ticker")}}))
        elif typ=="ticker": self.ui_latest[msg.get("market_ticker","")]={"type":"ticker",**msg}
        elif typ=="trade": self.ui_latest["trade:"+str(msg.get("market_ticker",""))]={"type":"trade",**msg}
        return env.local_ns
    async def _raw(self):
        try:
            while True:
                e=await self.raw_q.get()
                try:
                    path=self.data_dir/"raw"/f"match_{e.event_ticker}"/e.session_id/"kalshi_ws.ndjson"
                    if e.session_id not in self.raw_handles:
                        path.parent.mkdir(parents=True,exist_ok=True); self.raw_handles[e.session_id]=open(path,"a",buffering=1,encoding="utf-8")
                    p=e.payload; msg=p.get("msg",{})
                    self.raw_handles[e.session_id].write(json.dumps({"local_recv_ts_ns":e.local_ns,"channel":p.get("type"),"market_ticker":msg.get("market_ticker"),"payload":p},separators=(",",":"))+"\n")
                    self.raw_handles[e.session_id].flush()
                    self.metrics_by_session[e.session_id]["raw_messages_written"]+=1; self.raw_ok_by_session[e.session_id]=True
                    if e.session_id==self.active_session_id: self.raw_ok=True
                finally: self.raw_q.task_done()
        finally:
            for f in self.raw_handles.values(): f.close()
            self.raw_handles.clear()
    async def _db(self):
        while True:
            batch=[]; e=await self.db_q.get(); batch.append(e)
            while len(batch)<500:
                try: batch.append(self.db_q.get_nowait())
                except asyncio.QueueEmpty: break
            try:
                async with self.maker() as db:
                    for e in batch: self._add(db,e)
                    await db.commit()
                    for e in batch: self.metrics_by_session[e.session_id]["db_rows_written"]+=1; self.db_ok_by_session[e.session_id]=True
                    if self.active_session_id and self.db_ok_by_session[self.active_session_id]: self.db_ok=True
            except Exception:
                for e in batch: self.metrics_by_session[e.session_id]["db_write_failures"]+=1
                if any(e.session_id==self.active_session_id for e in batch): self.db_ok=False
            finally:
                for _ in batch: self.db_q.task_done()
    def _add(self,db,e):
        p=e.payload; typ=p.get("type","unknown"); m=p.get("msg",{}); ticker=m.get("market_ticker")
        ts=m.get("ts_ms"); seq=p.get("seq")
        db.add(RawMessage(session_id=e.session_id,local_recv_ts_ns=e.local_ns,kalshi_ts_ms=ts,market_ticker=ticker,channel=typ,sequence_number=seq,payload=p))
        if typ in ("orderbook_snapshot","orderbook_delta"):
            db.add(BookEvent(session_id=e.session_id,local_recv_ts_ns=e.local_ns,market_ticker=ticker,kind=typ,sequence_number=seq,payload=m))
            q=self.books.quote(ticker); db.add(Quote(session_id=e.session_id,local_recv_ts_ns=e.local_ns,market_ticker=ticker,source=typ,**q))
        elif typ=="ticker":
            vals={"yes_bid":num(m.get("yes_bid_dollars",m.get("yes_bid"))),"yes_bid_size":num(m.get("yes_bid_size_fp")),"yes_ask":num(m.get("yes_ask_dollars",m.get("yes_ask"))),"yes_ask_size":num(m.get("yes_ask_size_fp")),"no_bid":num(m.get("no_bid_dollars",m.get("no_bid"))),"no_bid_size":num(m.get("no_bid_size_fp")),"no_ask":num(m.get("no_ask_dollars",m.get("no_ask"))),"no_ask_size":num(m.get("no_ask_size_fp")),"last_price":num(m.get("last_price_dollars",m.get("price"))),"volume":num(m.get("volume_fp",m.get("volume"))),"open_interest":num(m.get("open_interest_fp",m.get("open_interest"))),"market_status":m.get("market_status"),"provenance":{field:{"kind":"RAW","source":"TICKER"} for field in ("yes_bid","yes_ask","no_bid","no_ask") if m.get(field+"_dollars",m.get(field)) is not None}}
            db.add(Quote(session_id=e.session_id,local_recv_ts_ns=e.local_ns,market_ticker=ticker,source="ticker",**vals))
        elif typ=="trade":
            db.add(Trade(session_id=e.session_id,local_recv_ts_ns=e.local_ns,market_ticker=ticker,kalshi_ts_ms=ts,price=num(m.get("yes_price_dollars",m.get("price"))),size=num(m.get("count_fp",m.get("count"))),side=m.get("taker_outcome_side",m.get("taker_side")),trade_id=str(m.get("trade_id") or f'{e.local_ns}')))
        elif typ.isupper(): db.add(SystemEvent(session_id=e.session_id,timestamp_ns=e.local_ns,kind=typ,detail=m))
    async def _ui(self):
        while True:
            await asyncio.sleep(.075)
            if self.ui_latest:
                batch=list(self.ui_latest.values()); self.ui_latest.clear(); await self.broadcast({"type":"market_batch","items":batch})
                if self.active_session_id: self.metrics_by_session[self.active_session_id]["ui_frames_emitted"]+=1

    def health(self,session_id=None):
        sid=session_id or self.active_session_id; metrics=self.metrics_by_session[sid] if sid else self._new_metrics()
        return {**metrics,"session_id":sid,"is_active_session":bool(sid and sid==self.active_session_id),"raw_queue_depth":self.raw_q.qsize(),"db_queue_depth":self.db_q.qsize(),"raw_recorder":self.raw_ok_by_session[sid] if sid else False,"database_writer":self.db_ok_by_session[sid] if sid else False}
    def current_quotes(self,session_id):
        if session_id!=self.active_session_id: return {}
        return {ticker:self.books.quote(ticker) for ticker in self.books.books}
