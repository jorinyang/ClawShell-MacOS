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
| skill-registry | 8445 | 8445 |
| kanban-mcp | 8446 | 8446 |
| nginx | 443/80 | 443/80 |
