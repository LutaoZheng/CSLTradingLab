# CSL Trading Lab

[中文说明](README.zh-CN.md)

CSL Trading Lab is a research and measurement platform for testing whether an on-site human observer at a Chinese Super League match can detect real-world events early enough to obtain a measurable lead over Kalshi market repricing.

It is **not an automated trading bot**. The current production mode is strictly:

```text
MOCK_MODE=false
TRADING_ENABLED=false
READ ONLY / RECORDER ONLY
Order calls = 0
```

Measurement integrity comes first. Trading infrastructure will only be considered if live experiments demonstrate a stable, executable edge after network latency and slippage. No trading edge has been proven yet.

## Current status — 2026-08-26

| Capability | Status |
| --- | --- |
| Infrastructure validation | PASS |
| Production deployment | PASS |
| Kalshi production read path | PASS |
| Human Event recording and Goal workflow | PASS |
| Latency instrumentation and clock calibration | PASS |
| Raw WS, order-book and quote recording | PASS |
| Sequence integrity test | PASS |
| 90-minute soak test | PASS |
| ZIP export | PASS |
| Trading disabled | PASS |

The next stage is the live stadium experiment. The infrastructure is ready to test whether an edge exists; it does not establish that one exists.

## Architecture

The latency-critical receive loop timestamps Kalshi messages immediately, updates the in-memory book, and hands data to independent consumers. It does not wait for SQLite commits, file flushes, or frontend rendering.

```text
Kalshi production WS
        ↓ local_recv_ts_ns
in-memory order book
        ↓
independent async queues
  ├── append-only raw NDJSON
  ├── SQLite writer
  └── coalesced browser updates

Phone Human Event → FastAPI → append-only SQLite + human NDJSON
```

Score is currently manual and isolated from the market recorder. Focus Mode subscribes only to the selected match's `ticker`, `trade`, and `orderbook_delta` channels. Low-frequency discovery can add newly listed GAME, BTTS, TOTAL, or SPREAD markets without restarting the recorder.

### AWS production

```text
AWS EC2 — us-east-2 / Ohio
Ubuntu Server 26.04 LTS
t3.small — 2 vCPU / 2 GB RAM

Internet
   ↓
Nginx
   ├── /        → Next.js standalone :3000
   ├── /api/*   → FastAPI :8000
   └── /ws      → FastAPI WebSocket :8000/ws
```

Services are `csl-backend.service`, `csl-frontend.service`, and `nginx`.

Frontend production networking is same-origin. With no explicit development override, API requests use `/api/*`; browser WebSocket resolution is automatic:

```text
HTTP  → ws://current-host/ws
HTTPS → wss://current-host/ws
```

Moving from IP to domain to HTTPS/WSS does not require another frontend networking redesign. Local development may still explicitly set `NEXT_PUBLIC_API_URL=http://localhost:8000` and `NEXT_PUBLIC_WS_URL=ws://localhost:8000`.

## Latency instrumentation

`POST /api/latency/ping` is a small, side-effect-free endpoint. It does not create or modify Sessions, write Human Events, call Kalshi, access credentials, or trigger trading.

`RUN LATENCY TEST` performs 20 sequential pings and retains the raw samples. It reports RTT last/p50/p95/p99, estimated one-way latency, clock offset, and jitter. Calibrations are stored separately from business events and exported as `clock_calibrations.json`.

Human Events preserve the original clocks and persistence milestones:

```text
device_wall_ts_ms
device_perf_ts_ms
pointerdown_perf_ts_ms
server_request_entry_ts_ns
server_receive_ts_ns
db_commit_complete_ts_ns
human_raw_fsync_complete_ts_ns
calibration_id
```

Replay defaults to `reference=server` and also supports `reference=device` and `reference=calibrated`. The strongest core comparison is:

```text
AWS Human Event receive
→ AWS-received Kalshi market reaction
```

Both timestamps use the same server wall clock. Raw device timestamps and calibration samples are retained rather than replaced by derived latency values.

## Recorded data semantics

- **Raw Kalshi WS:** every received payload with local receive timestamp, append-only and unsampled.
- **Order book:** snapshots, deltas, sequence, market, side, price-level quantity/depth changes, reconnect and resync evidence. A delta is an aggregate price-level depth change, not an individual user's order. With a snapshot and continuous deltas, the visible book can be reconstructed when the stream is complete.
- **Quotes:** top-of-book YES bid/ask and NO bid/ask reconstructed from the book, with RAW/DERIVED provenance. Quote timing remains distinct from order-book timing.
- **Trades:** observed exchange trades using the fields provided by the production payload.
- **Human Events:** append-only events including `DANGER`, `SHOT`, `BALL_IN_NET`, `GOAL_ASSESSMENT`, `GOAL_CONFIRMED`, `GOAL_CANCELLED`, `PENALTY_EVENT`, `PENALTY_ASSESSMENT`, `PENALTY_CONFIRMED`, `PENALTY_CANCELLED`, `RED_CARD_EVENT`, `RED_CARD_ASSESSMENT`, `RED_CARD_CONFIRMED`, `RED_CARD_CANCELLED`, `VAR_CHECK`, and `EVENT_VOIDED`. Relationships use `event_group_id`, `parent_event_id`, and `target_event_id`; mistaps append a void event instead of deleting history.

Session ZIP export includes `session.json`, `manifest.json`, `clock_calibrations.json`, `timeline.csv`, `human_events.csv`, `quotes.csv`, `trades.csv`, `orderbook_events.csv`, and the Session's raw Kalshi/Human Event NDJSON files.

## Production validation — 2026-08-26

The backend, frontend, and Nginx services were active. Production validation confirmed:

- DANGER and SHOT reached the DATA timeline.
- `BALL_IN_NET → assessment / VAR_CHECK → GOAL_CONFIRMED` remained append-only and visible.
- Queue Drops = 0 and DB Failures = 0 in the observed runs.
- A production latency sample showed typical estimated phone-to-Ohio one-way latency around **145–160 ms**, with meaningful tail latency and jitter on both Wi-Fi and 5G. This is an observation, not an SLA or formal benchmark.
- AWS request entry to persistence was generally on the order of 10–20 ms in the observed samples.
- Device and server wall clocks can differ materially; raw wall-clock subtraction is not treated as calibrated latency.

A roughly 90-minute production TEST soak recorded:

```text
Raw Kalshi WS       234
Orderbook Events    201
Quotes              227
Trades              0
Human Events         0
Queue Drops           0
DB Write Failures     0
Orderbook sequence    1 → 201 (201 unique; 0 missing observed)
```

The recorder ran through STOP, ZIP export succeeded, raw WS/BOOK/QUOTE data were present, and trading stayed disabled. This is evidence from one soak test, not a guarantee of production stability.

## Recent engineering fixes

- The App Router DATA page at `/match/[id]/data` was restored after the broad `data/` ignore rule accidentally excluded `frontend/app/match/[id]/data/`. Production builds now contain the route.
- Bare HTTP is a non-secure browser context, so direct `crypto.randomUUID()` use caused Human Event clicks to throw before optimistic UI or POST, leaving DATA empty and breaking Goal follow-up. A unified helper now prefers `crypto.randomUUID()`, falls back to `crypto.getRandomValues()`, then to local browser-safe UUID generation. These UUIDs identify records; they are not authentication or security tokens. Human Event and Goal workflows were regression-tested over the fallback path.

## Local Native Mode

Native macOS development uses Python, SQLite, Node.js, and pnpm; Docker and PostgreSQL are not required.

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab"
cp .env.example .env

cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

In a second terminal:

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab/frontend"
pnpm install
pnpm dev --hostname 0.0.0.0
```

Open `http://localhost:3000`. For a phone on the same LAN, bind as above, configure the public frontend API/WS variables to the Mac LAN address, restart Next.js, and open `http://<MAC_LAN_IP>:3000`. `ipconfig getifaddr en0` usually returns the Wi-Fi address.

Runtime data is local and ignored by Git:

```text
data/csl_trading_lab.db
data/raw/match_<EVENT_TICKER>/<SESSION_ID>/kalshi_ws.ndjson
data/raw/match_<EVENT_TICKER>/<SESSION_ID>/human_events.ndjson
```

Run verification with:

```bash
cd backend && source .venv/bin/activate && python -m pytest -q
cd ../frontend && pnpm test:regression && pnpm exec tsc --noEmit && pnpm build
```

Docker files remain optional deployment artifacts only.

### Mock validation

With `MOCK_MODE=true`, the same UI and recorder architecture can simulate quotes, order-book deltas, disconnect/resync, and newly discovered markets without mixing mock data into a production Session. Useful checks include Human Event append-only behavior, GOAL/VAR follow-up, DATA/Replay, STOP, and ZIP export. The development-only helpers are:

```bash
curl -X POST http://localhost:8000/api/mock/disconnect
curl -X POST http://localhost:8000/api/mock/new_market
```

## Production read-only configuration

Credentials belong only in the untracked server `.env` and private-key file; never put them in frontend variables, source, fixtures, logs, exports, or Git.

```dotenv
MOCK_MODE=false
TRADING_ENABLED=false
CSL_SERIES_TICKERS=<current CSL series allowlist>
KALSHI_API_KEY_ID=<server-side key id>
KALSHI_PRIVATE_KEY_PATH=<absolute server-side PEM path>
```

The backend startup guard rejects `TRADING_ENABLED=true`. The current engine exposes no order methods.

After manually verifying the current CSL allowlist, server-side key permissions, market identity, disk space, network, and applicable Kalshi/API terms, a controlled read-only smoke run is available:

```bash
cd backend
source .venv/bin/activate
python scripts/production_dry_run.py
```

It exercises the production read path, reconnect/resync and export. It does not call order or portfolio endpoints.

## Live stadium experiment

The experiment will measure:

```text
real-world event
→ human pointerdown / click
→ AWS Human Event receive
→ first ANY order-book change
→ first liquidity/depth reaction
→ first top-of-book or bid/ask move
→ first trade
→ major repricing
```

Definitions such as “major repricing” are intentionally not hard-coded before observing real data. Raw depth is preserved because liquidity withdrawal may precede a price move.

Decision rule:

```text
Live data
   ↓
Stable measurable lead?
   ├── NO → stop or change research direction
   └── YES
        ↓
Executable after latency/slippage?
   ├── NO → do not trade
   └── YES
        ↓
Security hardening → domain → HTTPS/WSS → authentication
→ trading engine → risk controls → paper shadow execution
→ only then consider real money
```

## Security limitations

The current bare-HTTP research deployment is **not suitable for real-money trading**. Before any trading phase it requires HTTPS/WSS, server-side authentication, API authorization, WebSocket authentication, origin validation, CSRF protection, rate limiting, separate recorder/trading credentials, position and market-risk limits, a kill switch, idempotent client order IDs, and an append-only order audit log.

## Official references

- [Kalshi WebSocket quick start](https://docs.kalshi.com/getting_started/quick_start_websockets)
- [Orderbook updates and dynamic subscriptions](https://docs.kalshi.com/websockets/orderbook-updates)
- [Get Event](https://docs.kalshi.com/api-reference/events/get-event)
- [Orderbook response semantics](https://docs.kalshi.com/getting_started/orderbook_responses)
