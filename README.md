# CSL Trading Lab / 中超现场低延迟实验室

CSL Trading Lab 是一个**研究用途、无交易能力**的中超现场事件记录与 Kalshi 单场行情采集系统。它研究：现场观察者点击真实事件后，Kalshi 可执行订单簿多久开始撤单、暂停或重定价？

> 本项目不下单，不含 BUY/SELL、fair value、概率模型、q、Kelly、bankroll、仓位、PnL 或自动策略。

## 推荐方式：macOS Native Mode

第一场实验默认直接使用 Mac 上的 Python、SQLite、Node.js 和 pnpm。**不需要 Docker，也不需要 PostgreSQL。**

### 0. 从零安装系统工具

安装 Xcode Command Line Tools：

```bash
xcode-select --install
```

建议用 Homebrew 安装 Python 3.12、Node.js 和 pnpm：

```bash
brew install python@3.12 node pnpm
python3 --version
node --version
pnpm --version
```

后端需要 Python 3.11 或更高版本。

### 1. 配置环境

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab"
cp .env.example .env
```

默认配置是：

```dotenv
MOCK_MODE=true
DATABASE_URL=sqlite+aiosqlite:///./data/csl_trading_lab.db
DATA_DIR=./data
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000
```

相对路径始终按项目根目录解析，因此无论从根目录还是 `backend/` 启动，数据库和 raw log 都写入同一个 `data/`。

### 2. 启动 Backend

从全新终端执行：

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab/backend"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

`0.0.0.0` 允许同一局域网中的手机连接。退出后再次启动只需：

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab/backend"
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 3. 启动 Frontend

打开第二个终端：

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab/frontend"
pnpm install
pnpm dev --hostname 0.0.0.0
```

Mac 浏览器访问 `http://localhost:3000`。

## 手机通过局域网访问

1. 确保 Mac 和手机连接同一个可信 Wi-Fi。
2. 查找 Mac 的 Wi-Fi IP：

```bash
ipconfig getifaddr en0
```

如果 Mac 使用有线网或该命令无输出，可运行：

```bash
networksetup -listallhardwareports
ipconfig getifaddr en1
```

3. 假设 IP 是 `192.168.1.20`，修改项目根目录 `.env`：

```dotenv
NEXT_PUBLIC_API_URL=http://192.168.1.20:8000
NEXT_PUBLIC_WS_URL=ws://192.168.1.20:8000
```

4. `frontend/next.config.ts` 只从根目录 `.env` 读取这两个公开字段，不会把 Kalshi credential 注入浏览器。前端环境变量在构建/启动时读取；停止并重新运行 `pnpm dev --hostname 0.0.0.0`。
5. 手机打开 `http://192.168.1.20:3000`。
6. macOS 防火墙弹窗中允许 Python/Node 接收入站连接。若无法访问，先在手机浏览器测试 `http://192.168.1.20:8000/api/matches`。

公网部署必须使用 HTTPS/WSS；不要把 Kalshi 私钥放入 frontend 或暴露给浏览器。

## Architecture / 架构

```text
Kalshi WS ── timestamp_ns ── in-memory orderbook ── unbounded queues
                                                    ├─ append-only NDJSON
                                                    ├─ async SQLite writer
                                                    └─ coalesced UI (75 ms)
Kalshi score adapter (currently MANUAL) ────────────────┘
Phone event clock ──────── append-only Human Events ────┘
```

WS receive loop不等待数据库 commit、文件 flush 或浏览器。raw writer、DB writer 和 UI broadcaster 是独立消费者。UI 可以合并帧；Recorder 不采样。订单簿保存 snapshot 和每个 delta，sequence gap、断线、重连及 resync 都显式记录。

## 目录结构与运行时数据

```text
backend/
  app/
    main.py          FastAPI、preflight、events、replay、export
    kalshi.py        CSL discovery、Focus WS、Mock、Score Adapter
    recorder.py      raw/DB/UI queues、orderbook reconstruction
    models.py        append-only schema
  requirements.txt  Native Mode Python dependencies
  tests/
frontend/
  app/page.tsx
  app/match/[id]/page.tsx
  app/replay/[id]/page.tsx
data/
  csl_trading_lab.db
  raw/match_<EVENT_TICKER>/<SESSION_ID>/kalshi_ws.ndjson
docker-compose.yml   optional deployment only
```

整个 `data/`、SQLite `*.db`、WAL/SHM、raw logs、`.env`、virtualenv、Node modules 均已加入 `.gitignore`。

## Native Mock 验收

保持 `MOCK_MODE=true`，启动 backend/frontend 后：

1. 首页看到唯一的 Mock CSL 比赛并进入 Focus Mode。
2. Preflight 显示 WS、orderbook、raw recorder、database writer 正常。
3. 查看实时 ask，点击 Home Goal → 明显有效，或进入 VAR/Cancelled 路径。
4. `UNDO / MISTAP` 会追加 `EVENT_VOIDED`，不会删除目标事件。
5. 模拟断线与 resync：

```bash
curl -X POST http://localhost:8000/api/mock/disconnect
```

6. 模拟动态新增 BTTS market：

```bash
curl -X POST http://localhost:8000/api/mock/new_market
```

7. 在 Replay 选择 human event，检查 T−10s 到 T+30s 的真实事件驱动 quote。
8. 导出 ZIP，确认包含 `events.csv`、`quotes.csv`、`trades.csv`、`orderbook_deltas.csv`、`market_metadata.json`、`session.json` 和 `raw_kalshi_ws.ndjson`。

自动测试：

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab/backend"
source .venv/bin/activate
PYTHONPATH=. pytest -q
```

生产前端构建：

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab/frontend"
pnpm install
pnpm build
```

## 连接真实 Kalshi：只读 dry run

修改 `.env`：

```dotenv
MOCK_MODE=false
TRADING_ENABLED=false
CSL_SERIES_TICKERS=<当前中超 series ticker，多个用逗号分隔>
KALSHI_API_KEY_ID=<API key id>
KALSHI_PRIVATE_KEY_PATH=/绝对路径/kalshi-private-key.pem
```

保留以下官方 endpoint，除非 Kalshi 文档更新：

```dotenv
KALSHI_REST_URL=https://external-api.kalshi.com/trade-api/v2
KALSHI_WS_URL=wss://external-api-ws.kalshi.com/trade-api/ws/v2
```

本项目没有订单 endpoint。dry run 前仍需人工确认：

- 当前 CSL series/event ticker、主客队和开球时间。
- RSA key 文件权限和 API key 有效性。
- `ticker`、`trade`、`orderbook_delta` subscription acknowledgements/SID。
- 初始 `orderbook_snapshot`、增量重建、断线后的 snapshot/resync。
- live derivative market 能在 2–5 秒 discovery 周期内加入现有 WS。
- 手机与 Mac 的 NTP 时钟偏差、现场网络、磁盘空间、电源和禁用休眠。
- 所在地适用的 Kalshi/API 条款。

配置完成后，可执行受控的只读 production smoke run。它会等待真实 snapshot/行情、追加并立即 VOID 一个 `TEST_EVENT`、主动关闭一次本地 WS 验证重连/resync，并导出 Session；不会调用任何订单或 portfolio endpoint：

```bash
cd "/Users/lutaozheng/Documents/CSL Trading Lab/backend"
source .venv/bin/activate
python scripts/production_dry_run.py
```

应用启动时如果发现 `TRADING_ENABLED=true` 会直接拒绝启动。

## Kalshi integration status

- REST：`GET /events`、`GET /events/{event_ticker}`；orderbook REST endpoint 可用于初始化/恢复。
- WS：`ticker`、`trade`、`orderbook_delta`；后者先发 `orderbook_snapshot`。支持同一 socket 动态增加 subscription，无需重启 Recorder。
- 当前 schema 使用 fixed-point dollar/count 字段，并保存 payload 提供的 `ts_ms`；缺失字段保持 null，不虚构。
- `CSL_SERIES_TICKERS` 是显式 allowlist；未配置时 Live Mode 不扫描其他联赛。
- Kalshi 公开 Trade API 没有已确认的足球 score/clock 契约，因此当前 `score_source=MANUAL`、`clock=null`，不运行虚假本地倒计时。

## SQLite 与可选 PostgreSQL/Docker

Native Mode 只安装 `requirements.txt`，其中没有 `asyncpg` 或 PostgreSQL 依赖。SQLAlchemy 模型使用 SQLite 兼容类型；大整数主键在 SQLite 下映射为 `INTEGER` 以保持自增。

Docker 仅作为未来 optional deployment 保留：

```bash
docker compose up --build
```

Docker backend 显式安装 `.[postgres]` optional dependency，并由 Compose 覆盖 `DATABASE_URL` 为 PostgreSQL。它不影响 Native Mode。

## Data semantics and limitations

- Human event 与状态转换全部 append-only，共享 `event_group_id`；undo 追加 `EVENT_VOIDED`。
- 手机在请求前记录 wall clock 与 monotonic `performance.now()`；后端时间只作为额外证据。
- Replay 不插值，返回实际收到的 event-driven updates。
- 不硬编码“Market Reaction”定义，保留 full depth 供赛后研究。
- SQLite WAL 可降低读写互相阻塞，但 raw NDJSON 仍是独立冗余记录。OS/filesystem crash 仍可能损失尚未刷盘的尾部数据，比赛前应完成断电与磁盘容量评估。

## Official references

- [Kalshi WebSocket quick start](https://docs.kalshi.com/getting_started/quick_start_websockets)
- [Orderbook updates and dynamic subscriptions](https://docs.kalshi.com/websockets/orderbook-updates)
- [Get Event](https://docs.kalshi.com/api-reference/events/get-event)
- [Orderbook response semantics](https://docs.kalshi.com/getting_started/orderbook_responses)
