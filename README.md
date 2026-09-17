# Webhook 可靠投递服务

基于 **Python + FastAPI + PostgreSQL** 的 webhook 投递系统，Docker Compose 一键启动。

核心特性：

- **幂等事件接入**：生产者以 `source` + `event_id` 提交事件；重复提交返回原事件（HTTP 200），不产生新投递。
- **同一订阅严格有序**：每个订阅是一个有序队列，前一事件**成功或进入死信之前**，后一事件不会被发送。
- **数据库租约**：工作进程通过原子 `UPDATE` 抢占订阅租约；租约过期（如进程崩溃）后其他 worker 可接管，续租/写库均带 fencing 校验。
- **时间戳 HMAC 签名**：每次投递携带 `X-Webhook-Timestamp` 与 `X-Webhook-Signature: t=...,v1=...`（Stripe 风格）。
- **指数退避重试**：失败后按 `base * 2^(n-1)` 退避（带抖动、可封顶），**6 次失败后进入死信队列**。
- **投递历史 + 死信重放 API**：每次尝试都有记录；重放会**新建投递链**（新 `chain_id`、尝试次数清零），旧记录完整保留。
- **本地回调接收器**：内置可模拟成功/失败/抖动的接收器，用于本地开发与演示。

## 架构

```
                ┌────────────┐   POST /events    ┌──────────────┐
  生产者 ──────▶│  api (8000) │──────────────────▶│  PostgreSQL  │
                │  FastAPI    │◀── 管理/查询 API ──│  events      │
                └────────────┘                    │  subscriptions│
                                                  │  deliveries  │
  worker ×2 ─── 租约抢占/续租 ────────────────────▶│  attempts    │
    │                                           └──────────────┘
    │  POST + HMAC 签名（指数退避，6 次后死信）
    ▼
  订阅方回调地址（本地演示用 receiver:9000）
```

| 服务       | 说明                                   | 端口 |
| ---------- | -------------------------------------- | ---- |
| `api`      | 事件接入 + 订阅/投递/死信管理 API      | 8000 |
| `worker`   | 投递工作进程（compose 默认 2 副本）    | —    |
| `receiver` | 本地回调接收器（验签 + 故障模拟）      | 9000 |
| `db`       | PostgreSQL 16                          | 5432 |

## 快速开始

```bash
docker compose up --build
```

启动后可访问：

- API 文档（Swagger UI）：http://localhost:8000/docs
- 健康检查：http://localhost:8000/healthz 、http://localhost:9000/healthz
- 接收器已收记录：http://localhost:9000/received

一键演示（创建订阅 → 发事件 → 模拟失败 → 死信 → 重放）：

```bash
./scripts/demo.sh
```

### 手动走一遍

```bash
# 1. 创建订阅（secret 仅在创建时完整返回；本地演示直接用接收器的默认密钥）
curl -X POST http://localhost:8000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"source": "shop",
       "target_url": "http://receiver:9000/callback",
       "secret": "whsec_dev_receiver_secret"}'

# 2. 提交事件（重复提交同一 source+event_id 返回 200 且不新增投递）
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"source": "shop", "event_id": "order-1001",
       "type": "order.created", "payload": {"total": 99.5}}'

# 3. 查看投递历史
curl http://localhost:8000/deliveries
curl http://localhost:8000/deliveries/<delivery_id>   # 含每次尝试记录

# 4. 查看接收器收到的 webhook
curl http://localhost:9000/received
```

模拟失败与死信重放：

```bash
# 指向 receiver 的 /callback/fail（永远返回 500）
curl -X POST http://localhost:8000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"source": "payments",
       "target_url": "http://receiver:9000/callback/fail",
       "secret": "whsec_dev_receiver_secret"}'
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"source": "payments", "event_id": "pay-1", "payload": {}}'

# 约 63 秒（1+2+4+8+16+32s 退避）后进入死信
curl http://localhost:8000/dead-letters

# 重放：新建投递链（新 chain_id、attempts=0），旧死信记录保留
curl -X POST http://localhost:8000/dead-letters/<delivery_id>/replay
```

## API 一览

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| `POST` | `/events` | 提交事件；重复 `(source, event_id)` 返回 200 + 原事件，不新增投递 |
| `GET`  | `/events` | 事件列表（可按 `source` 过滤） |
| `POST` | `/subscriptions` | 创建订阅（`source`、`target_url`、可选 `secret`/`event_types`） |
| `GET`  | `/subscriptions` | 订阅列表（密钥脱敏显示） |
| `GET`  | `/subscriptions/{id}` | 订阅详情（含当前租约持有者） |
| `DELETE` | `/subscriptions/{id}` | 软删除：停用并释放租约，历史保留 |
| `GET`  | `/deliveries` | 投递历史（`subscription_id`/`event_id`/`status`/`chain_id` 过滤） |
| `GET`  | `/deliveries/{id}` | 投递详情：每次尝试记录 + 由它重放出的投递 |
| `GET`  | `/dead-letters` | 死信列表 |
| `POST` | `/dead-letters/{id}/replay` | 死信重放：新建投递链，旧记录保留 |
| `GET`  | `/healthz` | 健康检查 |

接收器（开发辅助）：

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| `POST` | `/callback`、`/callback/ok` | 验签后返回 200 |
| `POST` | `/callback/fail` | 永远 500（驱动死信） |
| `POST` | `/callback/flaky` | 每个投递先失败 2 次再成功（演示退避重试） |
| `GET`  | `/received` | 已收到的 webhook 列表 |
| `DELETE` | `/received` | 清空记录 |

## 签名算法

每次投递的请求头：

```
X-Webhook-Delivery-Id: <delivery uuid>
X-Webhook-Event-Type:  <event type>
X-Webhook-Timestamp:   <unix 秒>
X-Webhook-Signature:   t=<unix 秒>,v1=<HMAC_SHA256(secret, "{t}.{raw_body}") 的 hex>
```

接收方验证（Python 示例）：

```python
import hashlib, hmac, time

def verify(secret: str, headers, body: bytes, tolerance: int = 300) -> bool:
    sig = dict(p.split("=", 1) for p in headers["X-Webhook-Signature"].split(","))
    if abs(time.time() - int(sig["t"])) > tolerance:
        return False                                    # 防重放
    expected = hmac.new(secret.encode(),
                        sig["t"].encode() + b"." + body,
                        hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig["v1"])
```

## 可靠性语义

- **幂等接入**：`events(source, event_id)` 唯一约束 + `ON CONFLICT DO NOTHING`；投递与事件同事务创建，重复提交不会产生新投递。
- **严格顺序**：每个订阅的投递按全局递增 `seq` 排序，worker 只处理队首（`status='pending'` 且到期）。队首等待重试时，后续事件不会被发送；队首成功或转为死信后，才处理下一个。
- **租约与接管**：worker 以原子 `UPDATE ... WHERE lease_expires_at <= now()` 抢占订阅租约，处理每个事件前续租；写结果前再次校验租约归属（fencing）。worker 崩溃后租约到期（默认 30s），其他 worker 接管并从队首继续。
- **退避策略**：第 `n` 次失败后延迟 `min(base * 2^(n-1), max)` + 抖动，默认 `1,2,4,8,16s`；第 6 次失败后进入死信。
- **死信重放**：重放创建全新投递（新 `chain_id`、`attempts=0`、`replayed_from` 指向原投递），追加到订阅队列尾部，不破坏顺序语义；原死信及其 6 次尝试记录完整保留。
- **至少一次投递**：worker 在「HTTP 成功但写库前崩溃」等极端情况下可能重复投递，接收方应以 `X-Webhook-Delivery-Id` / 事件 `id` 做幂等处理。

## 配置

通过环境变量配置（本地开发可复制 `.env.example` 为 `.env`）：

| 变量 | 默认值 | 说明 |
| ---- | ------ | ---- |
| `DATABASE_URL` | `postgresql+asyncpg://webhook:webhook@localhost:5432/webhook` | PostgreSQL 连接串 |
| `MAX_ATTEMPTS` | `6` | 进入死信前的最大尝试次数 |
| `BACKOFF_BASE_SECONDS` | `1` | 退避基数（`base * 2^(n-1)`） |
| `BACKOFF_MAX_SECONDS` | `300` | 退避上限 |
| `BACKOFF_JITTER_RATIO` | `0.1` | 抖动比例 |
| `HTTP_TIMEOUT_SECONDS` | `10` | 回调请求超时 |
| `LEASE_TTL_SECONDS` | `30` | 租约时长（超时后可被接管） |
| `POLL_INTERVAL_SECONDS` | `0.5` | worker 空闲轮询间隔 |
| `WORKER_ID` | 主机名 | worker 标识（租约归属） |
| `RECEIVER_SECRET` | `whsec_dev_receiver_secret` | 接收器验签密钥 |
| `RECEIVER_BEHAVIOR` | `ok` | 接收器默认行为（`ok`/`fail`/`flaky`） |
| `RECEIVER_FLAKY_FAILS` | `2` | flaky 模式下每个投递先失败的次数 |

## 本地开发

```bash
cd app
pip install -r requirements-dev.txt

# 测试会自动拉起一个内嵌 PostgreSQL（pgserver），无需外部依赖；
# 也可指定已有数据库：WEBHOOK_TEST_DATABASE_URL=postgresql+asyncpg://user:pass@host/db
python -m pytest
```

单独启动各进程（需先备好 PostgreSQL 并设置 `DATABASE_URL`）：

```bash
uvicorn app.main:app --port 8000      # API
python -m app.worker                  # 投递 worker
uvicorn app.receiver:app --port 9000  # 本地回调接收器
```

## 项目结构

```
├── docker-compose.yml      # db + api + worker×2 + receiver
├── scripts/demo.sh         # 端到端演示脚本
└── app/
    ├── Dockerfile          # 单一镜像，compose 以不同 command 启动三种角色
    ├── requirements.txt
    ├── app/
    │   ├── main.py         # FastAPI：事件接入 / 订阅 / 投递历史 / 死信重放
    │   ├── engine.py       # 投递引擎：租约、严格顺序、退避、死信
    │   ├── worker.py       # worker 进程入口
    │   ├── receiver.py     # 本地回调接收器（验签 + 故障模拟）
    │   ├── models.py       # SQLAlchemy 模型
    │   ├── security.py     # 时间戳 HMAC 签名/验证
    │   ├── config.py       # 环境变量配置
    │   └── db.py           # 异步引擎/会话
    └── tests/              # pytest（真实 PostgreSQL 上运行）
```
