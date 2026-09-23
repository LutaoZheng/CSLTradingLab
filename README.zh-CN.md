# CSL Trading Lab / 中超现场低延迟实验室

[English](README.md)

CSL Trading Lab 是一个研究与测量平台，用于检验：人在中超比赛现场观察真实事件，是否能够早于 Kalshi 市场重定价，并形成可测量的时间优势。

它**不是自动交易机器人**。当前 Production 严格保持：

```text
MOCK_MODE=false
TRADING_ENABLED=false
READ ONLY / RECORDER ONLY
Order calls = 0
```

第一优先级是 measurement integrity。只有真实实验确认存在稳定优势，而且扣除网络延迟与滑点后仍可执行，才会考虑 trading infrastructure。目前尚未证明存在交易 edge。

## 当前状态 — 2026-08-26

| 能力 | 状态 |
| --- | --- |
| 基础设施验证 | PASS |
| Production 部署 | PASS |
| Kalshi production 只读链路 | PASS |
| Human Event 记录与 Goal workflow | PASS |
| Latency instrumentation 与时钟校准 | PASS |
| Raw WS、订单簿与 Quote 记录 | PASS |
| Sequence integrity test | PASS |
| 90 分钟 soak test | PASS |
| ZIP export | PASS |
| Trading disabled | PASS |

下一阶段是现场比赛实验。当前只证明测量基础设施已经能够检验 edge 是否存在，不代表 edge 已经存在。

## 架构

低延迟接收循环在收到 Kalshi 消息时立即打时间戳、更新内存订单簿，再交给彼此独立的消费者。它不等待 SQLite commit、文件 flush 或前端渲染。

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

比分目前为手动源，并与 Market Recorder 隔离。默认关闭比赛 discovery 和启动时的 focus 恢复。管理员必须先在版本化配置中加入精确 event/market ticker，再明确激活比赛，Focus Mode 才订阅 `ticker`、`trade` 和 `orderbook_delta`。旧的低频 discovery 仍可通过 `AUTO_DISCOVERY_ENABLED=true` 启用，但默认不用。

### AWS Production

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

服务为 `csl-backend.service`、`csl-frontend.service` 和 `nginx`。

Production 前端使用 same-origin networking。未设置开发覆盖变量时，API 请求使用 `/api/*`；浏览器 WebSocket 自动解析：

```text
HTTP  → ws://current-host/ws
HTTPS → wss://current-host/ws
```

以后从 IP 迁移到域名和 HTTPS/WSS，不需要再次修改前端 networking 架构。本地开发仍可显式设置 `NEXT_PUBLIC_API_URL=http://localhost:8000` 和 `NEXT_PUBLIC_WS_URL=ws://localhost:8000`。

## Latency Instrumentation

`POST /api/latency/ping` 是一个极小且无副作用的 endpoint。它不创建或修改 Session、不写 Human Event、不调用 Kalshi、不读取 credentials，也不触发 trading。

`RUN LATENCY TEST` 连续执行 20 次 ping，保留所有 raw samples，并显示 RTT last/p50/p95/p99、estimated one-way、clock offset 和 jitter。Calibration 与业务事件分离保存，并导出为 `clock_calibrations.json`。

Human Event 保留原始时钟和持久化里程碑：

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

Replay 默认使用 `reference=server`，也支持 `reference=device` 和 `reference=calibrated`。研究中最可靠的核心比较是：

```text
AWS Human Event receive
→ AWS-received Kalshi market reaction
```

两者使用同一服务器时钟。系统保留原始设备时间和 calibration samples，不会只保存推导后的 latency。

## Recorder 数据语义

- **Raw Kalshi WS：**每条收到的原始 payload 和本地接收时间戳，append-only、不采样。
- **Order Book：**snapshot、delta、sequence、market、side、价格层级的 quantity/depth 变化，以及 reconnect/resync 证据。Orderbook delta 是聚合 price-level depth change，不代表某个具体用户的独立订单。数据完整时，可以用 snapshot 加连续 delta 重建可见订单簿。
- **Quotes：**从订单簿重建的 YES bid/ask 与 NO bid/ask，并保留 RAW/DERIVED provenance。Quote 时间与 Orderbook 时间不会混为同一数据源。
- **Trades：**按 Production payload 实际字段记录观察到的成交。
- **Human Events：**append-only 保存 `DANGER`、`SHOT`、`BALL_IN_NET`、`GOAL_ASSESSMENT`、`GOAL_CONFIRMED`、`GOAL_CANCELLED`、`PENALTY_EVENT`、`PENALTY_ASSESSMENT`、`PENALTY_CONFIRMED`、`PENALTY_CANCELLED`、`RED_CARD_EVENT`、`RED_CARD_ASSESSMENT`、`RED_CARD_CONFIRMED`、`RED_CARD_CANCELLED`、`VAR_CHECK` 和 `EVENT_VOIDED`。事件通过 `event_group_id`、`parent_event_id`、`target_event_id` 关联；误触通过追加 void event 表达，不删除历史。

Session ZIP 包含 `session.json`、`manifest.json`、`clock_calibrations.json`、`timeline.csv`、`human_events.csv`、`quotes.csv`、`trades.csv`、`orderbook_events.csv`，以及该 Session 的 Kalshi/Human Event raw NDJSON。

## Production 验证 — 2026-08-26

backend、frontend 和 Nginx 服务均已正常运行。已经真实验证：

- DANGER 与 SHOT 能正常进入 DATA timeline。
- `BALL_IN_NET → assessment / VAR_CHECK → GOAL_CONFIRMED` 能 append-only 记录并显示。
- 观察到的运行中 Queue Drops = 0、DB Failures = 0。
- Production latency 样本中，手机到 Ohio 的 typical estimated one-way 约为 **145–160 ms**；Wi-Fi 与 5G 都有明显 tail latency/jitter。这是一次观察结果，不是 SLA 或正式 benchmark。
- 观察样本中 AWS request entry 到 persistence 通常约为 10–20 ms 量级。
- 设备和服务器 wall clock 可能存在明显偏移，因此不能把两个 raw wall-clock 直接相减当作校准后 latency。

一次约 90 分钟的 Production TEST soak 得到：

```text
Raw Kalshi WS       234
Orderbook Events    201
Quotes              227
Trades              0
Human Events         0
Queue Drops           0
DB Write Failures     0
Orderbook sequence    1 → 201（201 unique；观察到 0 missing）
```

Recorder 持续运行到 STOP，ZIP export 成功，Raw WS/BOOK/QUOTE 数据存在，Trading 始终关闭。该结果是一次 soak test 的证据，不代表 Production 稳定性保证。

## 近期工程修复

- App Router 的 DATA 页面 `/match/[id]/data` 曾被过宽的 `data/` ignore 规则误伤，因为它排除了 `frontend/app/match/[id]/data/`。该规则已修复，Production build 现在包含 DATA route。
- 裸 HTTP 属于 non-secure browser context，直接调用 `crypto.randomUUID()` 会在 optimistic UI 和 POST 之前抛异常，导致 DATA 没有 Human Event、Goal follow-up 失效。统一 UUID helper 现在依次使用 `crypto.randomUUID()`、`crypto.getRandomValues()` 和本地 browser-safe fallback。UUID 只用于 record identity，不是 authentication/security token；fallback 路径下的 Human Event 与 Goal workflow 已完成 regression test。

## macOS Native Mode

本地默认使用 Python、SQLite、Node.js 和 pnpm；不需要 Docker 或 PostgreSQL。

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab"
cp .env.example .env

cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

第二个终端：

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab/frontend"
pnpm install
pnpm dev --hostname 0.0.0.0
```

Mac 打开 `http://localhost:3000`。手机通过同一局域网访问时，保持上述 bind，使用 Mac LAN IP 配置公开的 frontend API/WS 变量，重启 Next.js，然后打开 `http://<MAC_LAN_IP>:3000`。通常可用 `ipconfig getifaddr en0` 查看 Wi-Fi IP。

运行时数据位于项目内且被 Git ignore：

```text
data/csl_trading_lab.db
data/raw/match_<EVENT_TICKER>/<SESSION_ID>/kalshi_ws.ndjson
data/raw/match_<EVENT_TICKER>/<SESSION_ID>/human_events.ndjson
```

验证命令：

```bash
cd backend && source .venv/bin/activate && python -m pytest -q
cd ../frontend && pnpm test:regression && pnpm exec tsc --noEmit && pnpm build
```

Docker 文件仅作为可选部署方式保留。

### Mock 验收

设置 `MOCK_MODE=true` 后，可以通过同一套 UI 与 Recorder 架构模拟 quote、orderbook delta、disconnect/resync 和动态新增 market，同时不会把 Mock 数据混入 Production Session。应重点检查 Human Event append-only、GOAL/VAR follow-up、DATA/Replay、STOP 和 ZIP export。开发专用 helper：

```bash
curl -X POST http://localhost:8000/api/mock/disconnect
curl -X POST http://localhost:8000/api/mock/new_market
```

## Production 只读配置

Credentials 只能放在服务器未跟踪的 `.env` 和私钥文件中，不得进入 frontend variables、源码、fixture、日志、export 或 Git。

```dotenv
MOCK_MODE=false
TRADING_ENABLED=false
CSL_SERIES_TICKERS=<current CSL series allowlist>
KALSHI_API_KEY_ID=<server-side key id>
KALSHI_PRIVATE_KEY_PATH=<absolute server-side PEM path>
```

Backend startup guard 会拒绝 `TRADING_ENABLED=true`。当前 engine 不暴露 order method。

在人工确认 CSL allowlist、服务器端私钥权限、比赛/market identity、磁盘、网络以及适用的 Kalshi/API 条款后，可以执行受控的 production read-only smoke run：

```bash
cd backend
source .venv/bin/activate
python scripts/production_dry_run.py
```

它验证 Production 只读链路、reconnect/resync 和 export，不调用 order 或 portfolio endpoint。

## 现场比赛实验

现场实验将测量：

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

在观察真实数据前，系统不会硬编码 “major repricing” 等定义。Raw depth 必须保留，因为 liquidity withdrawal 可能先于价格变化。

决策原则：

```text
Live data
   ↓
Stable measurable lead?
   ├── NO → 停止或改变研究方向
   └── YES
        ↓
Executable after latency/slippage?
   ├── NO → 不交易
   └── YES
        ↓
安全加固 → 域名 → HTTPS/WSS → authentication
→ trading engine → risk controls → paper shadow execution
→ 最后才考虑真钱
```

## 安全限制

## Phase 3 登录与重庆操作端

系统现在只有一个共享基础账号。基础登录只能访问 `/live/chongqing` 的授权比赛事件界面。原 Dashboard、数据、导出、比分、重连、Session 管理 API、OpenAPI 文档和行情 WebSocket 都要求独立且短时有效的管理员二次验证；权限由 FastAPI 服务端执行，不依赖隐藏按钮。

在服务器上交互式生成基础密码和管理员二次验证密码的 scrypt hash（终端不会显示密码）：

```bash
cd backend
.venv/bin/python scripts/generate_password_hash.py
```

仅在服务器未跟踪的 `.env` 中配置 hash 和精确授权的比赛 ticker：

```dotenv
PUBLIC_ORIGIN=https://csltradinglab.duckdns.org
AUTH_USERNAME=<共享用户名>
AUTH_PASSWORD_HASH=<scrypt hash>
ADMIN_PASSWORD_HASH=<不同的管理员二次验证密码 hash>
AUTO_DISCOVERY_ENABLED=false
MATCH_CONFIG_PATH=./config/matches.v1.json
TRADING_ENABLED=false
NEXT_PUBLIC_API_URL=
NEXT_PUBLIC_WS_URL=
```

Production frontend build 必须显式保持两个 public base 变量为空，使 HTTPS 下的 API/WS 使用 same-origin。Session token 是服务端保存、可撤销的 opaque token；Cookie 使用 Secure、HttpOnly、SameSite=Strict，所有修改请求还要求匹配的 CSRF cookie/header 和精确 Origin。

补传会保留原始 event ID 和时间戳；超过 `REALTIME_SIGNAL_MAX_AGE_MS` 的事件仍用于研究，但保存为 `realtime_eligible=false`，未来 execution 必须对此 fail closed。

比赛定义位于 schema version 1 的 `config/matches.v1.json`。默认文件特意保持为空，因此进程重启后没有 Active Match，也不会自动建立任何 Kalshi 比赛订阅。新增比赛时复制 `config/matches.v1.example.json` 的结构，人工核对精确 event ticker、market ticker、双方、当地开赛时间与时区、事件方向、结算规则及允许按钮，然后由管理员明确激活。精确 event 或任一配置 market 不存在时，激活直接失败，不会匹配相似名称的其他比赛。

当前裸 HTTP 研究部署**不适合真钱交易**。任何交易阶段开始前，都必须增加 HTTPS/WSS、server-side authentication、API authorization、WebSocket authentication、origin validation、CSRF protection、rate limiting、Recorder/Trading credentials 隔离、position/market risk limits、kill switch、幂等 client order ID 和 append-only order audit log。

## 官方参考

- [Kalshi WebSocket quick start](https://docs.kalshi.com/getting_started/quick_start_websockets)
- [Orderbook updates and dynamic subscriptions](https://docs.kalshi.com/websockets/orderbook-updates)
- [Get Event](https://docs.kalshi.com/api-reference/events/get-event)
- [Orderbook response semantics](https://docs.kalshi.com/getting_started/orderbook_responses)
