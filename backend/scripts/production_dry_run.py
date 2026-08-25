"""Read-only production smoke run. Requires credentials in the ignored root .env."""
import io, json, sqlite3, sys, time, uuid, zipfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
from app.config import settings
from app.main import app

TARGET = "Shanghai Shenhua vs Shandong Taishan"
OBSERVE_SECONDS = 30
POST_RECONNECT_SECONDS = 15

def wait_until(fn, timeout, label):
    deadline=time.time()+timeout
    while time.time()<deadline:
        value=fn()
        if value: return value
        time.sleep(.25)
    raise RuntimeError(f"Timed out waiting for {label}")

def main():
    if settings.mock_mode: raise SystemExit("Refusing: set MOCK_MODE=false")
    if settings.trading_enabled: raise SystemExit("Refusing: TRADING_ENABLED must be false")
    if not settings.kalshi_api_key_id or not settings.kalshi_private_key_path: raise SystemExit("Blocked: configure KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH in .env")
    if not Path(settings.kalshi_private_key_path).expanduser().is_file(): raise SystemExit("Blocked: configured private key file does not exist")
    with TestClient(app) as c:
        matches=c.get("/api/matches").json(); match=next((x for x in matches if x["title"]==TARGET),None)
        if not match: match=next(iter(matches),None)
        if not match: raise RuntimeError("No open CSL match returned by production REST")
        created=c.post("/api/sessions",json={"event_ticker":match["event_ticker"],"notes":"automated production read-only dry run"}); created.raise_for_status(); sid=created.json()["session_id"]
        state=lambda: c.get(f"/api/sessions/{sid}/state").json()
        wait_until(lambda: state()["preflight"]["kalshi_api_auth"],20,"AUTH_OK")
        wait_until(lambda: state()["preflight"]["orderbook_snapshot"],20,"orderbook snapshot")
        time.sleep(OBSERVE_SECONDS)
        before=state(); initial_resync=before["health"]["resync_complete_ts_ns"] or 0; group=str(uuid.uuid4()); test_id=str(uuid.uuid4()); now=time.time()*1000
        event={"event_id":test_id,"event_group_id":group,"event_type":"TEST_EVENT","team":"HOME","device_wall_ts_ms":now,"device_perf_ts_ms":time.perf_counter()*1000,"score_at_click":before["score"],"detail":{"reason":"PRODUCTION_READ_ONLY_DRY_RUN"}}
        c.post(f"/api/sessions/{sid}/events",json=event).raise_for_status()
        c.post(f"/api/sessions/{sid}/events",json={**event,"event_id":str(uuid.uuid4()),"event_type":"EVENT_VOIDED","target_event_id":test_id,"device_wall_ts_ms":time.time()*1000,"device_perf_ts_ms":time.perf_counter()*1000,"detail":{"reason":"TEST_EVENT"}}).raise_for_status()
        c.post("/api/admin/controlled-reconnect").raise_for_status()
        wait_until(lambda: state()["health"]["reconnect_count"]>=1,20,"controlled reconnect")
        wait_until(lambda: (state()["health"]["resync_complete_ts_ns"] or 0)>initial_resync,20,"orderbook resync")
        time.sleep(POST_RECONNECT_SECONDS)
        replay=c.get(f"/api/sessions/{sid}/replay",params={"event_id":test_id}); replay.raise_for_status()
        c.post(f"/api/sessions/{sid}/end").raise_for_status(); final=state(); exported=c.get(f"/api/sessions/{sid}/export"); exported.raise_for_status()
        export_dir=settings.data_dir/"exports"; export_dir.mkdir(parents=True,exist_ok=True); export_path=export_dir/f"production-read-only-{sid}.zip"; export_path.write_bytes(exported.content)
        raw_path=settings.data_dir/"raw"/f"match_{match['event_ticker']}"/sid/"kalshi_ws.ndjson"
        db_path=Path(settings.database_url.removeprefix("sqlite+aiosqlite:///")); db=sqlite3.connect(db_path)
        counts={name:db.execute(f"select count(*) from {table} where session_id=?",(sid,)).fetchone()[0] for name,table in {"raw_rows":"raw_messages","quote_rows":"quotes","trade_rows":"trades","orderbook_rows":"orderbook_events","system_rows":"system_events","human_events":"human_events"}.items()}; db.close()
        with zipfile.ZipFile(io.BytesIO(exported.content)) as z: export_files=sorted(z.namelist())
        report={"result":"PASS" if final["preflight"]["critical_ok"] else "PARTIAL","session_id":sid,"target_match":match["title"],"event_ticker":match["event_ticker"],"scheduled_start":match["scheduled_start"],"duration_seconds":OBSERVE_SECONDS+POST_RECONNECT_SECONDS,"health":final["health"],"recorder":final["recorder"],"db_counts":counts,"replay_quotes":len(replay.json()["quotes"]),"raw_file":str(raw_path),"raw_file_size":raw_path.stat().st_size if raw_path.exists() else 0,"export_path":str(export_path),"export_files":export_files,"security":{"mock_mode":False,"trading_enabled":False,"order_calls":0}}
        print(json.dumps(report,indent=2,default=str))

if __name__=="__main__": main()
