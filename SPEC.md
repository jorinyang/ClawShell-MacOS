# ClawShell 2.0 — 接口规范 / 数据模型 / 协议定义

> 版本: v1.0
> 状态: 进行中

---

## 1. MCP over WebSocket 协议

### 1.1 连接握手

```
Client                          Cloud Hub
  |                                  |
  |────── WSS CONNECT ──────────────>|
  |                                  |
  |<───── 101 Switching Protocols ───|
  |                                  |
  |────── auth frame ──────────────>|
  |  { "type": "auth", "token": "..." } |
  |                                  |
  |<───── auth_ok ──────────────────|
  |  { "type": "auth_ok" }
```

### 1.2 MCP Request/Response

```json
// Request
{
  "type": "mcp_request",
  "id": "uuid-v4",
  "method": "vault_list | skill_list | kanban_*",
  "params": {}
}

// Response
{
  "type": "mcp_response",
  "id": "uuid-v4",
  "result": {}
}
```

### 1.3 路由前缀

| 前缀 | 目标服务 | 示例 |
|------|---------|------|
| `vault_*` | Cloud Hub VaultHandler | `vault_list`, `vault_upload` |
| `skill_*` | Skill Registry :8445 | `skill_list`, `skill_publish` |
| `kanban_*` | Kanban MCP :8446 | `kanban_task_create` |

---

## 2. REST API (Cloud Hub :8080)

### 2.1 认证

```
POST /api/v1/auth/token
Body:   { "username": "...", "password": "..." }
Return: { "token": "jwt-token", "expires_in": 3600 }
```

### 2.2 状态

```
GET /status
Return: { "connected": 3, "version": "2.0.0" }

GET /health
Return: { "status": "ok" }
```

### 2.3 OSS Sync

```
POST /sync/push
Body: { "category": "vault", "path": "...", "content": "base64" }

GET  /sync/pull/:since
Return: { "changes": [...] }
```

---

## 3. 数据模型

### 3.1 pending_operations.jsonl

```jsonl
{"category": "memory", "action": "add", "data": {...}, "timestamp": "2026-05-11T..."}
{"category": "kanban", "action": "move", "data": {"task_id": "...", "target_column": "doing"}, "timestamp": "..."}
```

### 3.2 conflict_log.jsonl

```jsonl
{"timestamp": "...", "category": "kanban", "entity_id": "task-abc123", "local_version": 3, "cloud_version": 5, "resolution": "cloud_wins"}
```

### 3.3 Skill Registry DB (SQLite)

```sql
CREATE TABLE skills (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    version     TEXT NOT NULL,
    cloud_only  INTEGER DEFAULT 0,
    adapter_required TEXT,
    local_template  TEXT,
    parameters  TEXT,       -- JSON array
    published_by TEXT,
    published_at TEXT,
    updated_at  TEXT
);
CREATE UNIQUE INDEX idx_name_version ON skills(name, version);
```

### 3.4 Kanban Board (JSON)

```json
{
  "board_id": "default",
  "columns": [
    { "id": "todo",  "name": "待办",   "wip_limit": 10 },
    { "id": "doing", "name": "进行中",  "wip_limit": 3  },
    { "id": "done",  "name": "已完成",  "wip_limit": null }
  ],
  "tasks": [
    {
      "id": "task-abc123",
      "title": "实现 Phase 2",
      "column": "doing",
      "assignee": "yang",
      "version": 1,
      "created_at": "...",
      "updated_at": "..."
    }
  ],
  "version": 5
}
```

---

## 4. 环境变量

### 4.1 Cloud Hub

| 变量 | 说明 | 示例 |
|------|------|------|
| `JWT_SECRET` | JWT 签名密钥 | `openssl rand -hex 32` |
| `OSS_ACCESS_KEY_ID` | OSS AccessKey | — |
| `OSS_ACCESS_KEY_SECRET` | OSS AccessSecret | — |
| `OSS_BUCKET` | Bucket 名 | `clawshell-vault` |
| `OSS_ENDPOINT` | OSS endpoint | `oss-cn-hongkong.aliyuncs.com` |
| `OSS_VAULT_PREFIX` | vault 前缀 | `vault/` |

### 4.2 Edge Gateway

| 变量 | 说明 |
|------|------|
| `CLOUD_URL` | Cloud Hub WSS URL |
| `JWT_TOKEN` | 认证 Token |

---

## 5. 端口映射

| 服务 | 内部端口 | 外部映射 |
|------|---------|---------|
| cloud-hub (WS) | 8443 | 8443 |
| cloud-hub (REST) | 8080 | 8080 |
| cloud-hub (MCP) | 8081 | 127.0.0.1（本地） |
| cloud-hub (HTTP) | 8082 | 127.0.0.1（本地） |
| skill-registry | 8445 | 8445 |
| kanban-mcp | 8446 | 8446 |
| nginx | 443/80 | 443/80 |

---

## 6. Hermes 云端大脑架构（v2.2+）

### 6.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           阿里云 ECS                                     │
│                                                                         │
│   Port 8080：WS Server（Edge 连接）←── ClawShell-MacOS-Local 连接      │
│   Port 8081：MCP Server（主通道）  ←── Hermes 主用                     │
│   Port 8082：HTTP API（备用通道）  ←── Hermes 降级                     │
│                                                                         │
│   ┌─────────────────────────┐    ┌──────────────────────────────────┐  │
│   │   CloudHub（协调层）     │◄───│   Hermes Agent（云端大脑）        │  │
│   │                         │    │                                  │  │
│   │  EventStore（OSS）      │    │  cloud_hub_connect skill          │  │
│   │  PubSubManager          │    │  cloud_brain skills              │  │
│   │  StateAggregator        │    │    ├── insight_analyzer          │  │
│   │  9 Domains              │    │    ├── review_generator          │  │
│   │  MCP Server（8081）     │    │    ├── strategy_optimizer        │  │
│   │  HTTP Server（8082）     │    │    └── deep_thinker             │  │
│   └─────────────────────────┘    └──────────────┬───────────────────┘  │
│                                                 │ 双通道写回              │
│                                                 │ MCP(8081) / HTTP(8082)  │
│                                                 └──────────────────────┘  │
│                                                                         │
│                          MiniMax API（快速层）                           │
│                          DashScope Qwen-Max（深度层）                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 6.2 双通道握手协议

**消息格式（Hermes → CloudHub）**：
```json
{
  "message_id": "uuid-v4",
  "seq": 1001,
  "channel": "mcp",
  "type": "insight_add | review_publish | strategy_update | deep_think_result",
  "payload": {...},
  "timestamp": 1718000000,
  "retry_count": 0
}
```

**ACK 格式（CloudHub → Hermes）**：
```json
{
  "message_id": "uuid-v4",
  "ack_seq": 1001,
  "status": "ok | error | duplicate",
  "channel": "mcp",
  "detail": ""
}
```

**降级策略**：
1. Hermes 发送消息 → 主通道 MCP（Port 8081）
2. 3s 无 ACK → 降级备用通道 HTTP（Port 8082）
3. HTTP 重试 2 次仍失败 → 存入本地 DLQ（`/opt/clawshell/data/dlq/`）
4. DLQ 每 5 分钟自动重试

**去重**：CloudHub 按 `message_id` 去重，重复消息返回 `status: "duplicate"`

### 6.3 Hermes Skill 触发机制

| 触发条件 | 调用的 Skill | LLM 模型 |
|---------|------------|---------|
| `error.*` 事件 | insight_analyzer | MiniMax（快速） |
| `task.done` 事件 | review_generator | Qwen-Max（深度） |
| `node.offline` 事件 | insight_analyzer | MiniMax（快速） |
| `0 8 * * *`（每日） | review_generator | Qwen-Max（深度） |
| `0 * * * *`（每小时） | insight_analyzer | MiniMax（快速） |
| 手动触发 | deep_thinker | Qwen-Max（深度） |

### 6.4 Hermes → CloudHub 消息类型

| type | 目标 Domain | 说明 |
|------|------------|------|
| `insight_add` | InsightDomain | 洞察结论写入 |
| `review_publish` | ReviewDomain | 复盘报告写入 |
| `strategy_update` | AdaptiveDomain | 策略建议更新 |
| `deep_think_result` | DeepThinkEngine | 深度思考结果写入 |

### 6.5 环境变量

| 变量 | 说明 |
|------|------|
| `CLOUDHUB_WS_URL` | CloudHub WS 地址（`ws://localhost:8080`） |
| `CLOUDHUB_MCP_URL` | CloudHub MCP 地址（`http://localhost:8081`） |
| `CLOUDHUB_HTTP_URL` | CloudHub HTTP 备用（`http://localhost:8082`） |
| `MINIMAX_API_KEY` | MiniMax API Key（快速层） |
| `DASHSCOPE_API_KEY` | DashScope API Key（深度层） |
| `LLM_PROVIDER_FAST` | 快速层 Provider（`minimax`） |
| `LLM_PROVIDER_DEEP` | 深度层 Provider（`dashscope`） |
| `DLQ_PATH` | DLQ 目录（`/opt/clawshell/data/dlq`） |
