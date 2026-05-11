# ClawShell 2.0 — 云端协同架构规划

> 状态：规划中
> 版本：v2.0-draft
> 更新：2026-05-11

---

## 1. 愿景与设计原则

### 1.1 目标

将 ClawShell 从本地单一智能体外骨骼，升级为**云端协同的多智能体操作系统**。云端承载可复用、可多端共享的能力（记忆中枢、任务看板、技能编排、工作流）；端侧保留与本地 Agent 架构深度耦合的执行能力。两端通过统一的 MCP over WebSocket 协议通信。

### 1.2 设计原则

| 原则 | 说明 |
|------|------|
| **本地优先，云端可选** | 端侧完全独立运行，云端为"多端共享"的增强层 |
| **协议先行，传输无关** | 核心交互用 MCP 协议，底层可以是 WebSocket/gRPC/HTTP |
| **数据不变，状态可变** | 记忆/技能/工作流定义不可变，状态通过版本化变更记录 |
| **冲突可仲裁** | 多端并发操作通过 CRDT + 云端权威时间戳解决 |
| **渐进式迁移** | 从 MemPalace 云化开始，逐步迁移其他能力，不做一次性重写 |

### 1.3 名词定义

| 术语 | 定义 |
|------|------|
| ClawShell Cloud | 云端部署单元，包含所有可多端共享的服务 |
| ClawShell Local | 端侧安装单元，包含 auto-discovery、sync engine、本地适配层 |
| Edge Gateway | 端侧的 MCP-over-WebSocket 客户端，负责与云端 MCP server 通信 |
| Cloud Hub | 云端的 MCP-over-WebSocket 服务端，统一接收来自所有端侧 Gateway 的连接 |
| Sync Engine | 数据同步层，处理增量同步、冲突检测与仲裁 |
| Platform Adapter | 端侧针对每个已安装 Agent（Hermes/OpenClaw/Wukong）的接口适配器 |

---

## 2. 架构全景图

```
                          ┌──────────────────────────────────────────────┐
                          │              ClawShell Cloud                 │
                          │              (Aliyun ECS)                   │
  ┌──────────────┐        │  ┌────────────┐  ┌────────────┐            │
  │  端侧 Mac    │        │  │ MemPalace  │  │  Skill     │            │
  │  ClawShell   │◄──WS──►│  │ MCP Server │  │  Registry  │            │
  │  Local       │        │  │ (共享记忆)  │  │  (技能中枢) │            │
  └──────────────┘        │  ├────────────┤  ├────────────┤            │
                          │  │  Kanban    │  │  Workflow  │            │
  ┌──────────────┐        │  │  MCP Svr   │  │  Engine    │            │
  │  端侧 车机   │◄──WS──►│  │ (任务看板) │  │  (工作流)  │            │
  │  ClawShell   │        │  └────────────┘  └────────────┘            │
  │  Local       │        │         │                │                 │
  └──────────────┘        │         └───────┬────────┘                 │
                          │                 ▼                          │
                          │         ┌──────────────┐                   │
                          │         │  Cloud Hub   │                   │
                          │         │  MCP Router  │ ← WebSocket 443   │
                          │         │  (认证/路由) │                   │
                          │         └──────────────┘                   │
                          └──────────────────┬─────────────────────────┘
                                             │ WS / gRPC
                          ┌──────────────────┴─────────────────────────┐
                          │              ClawShell Local               │
                          │  ┌────────────┐  ┌────────────────────┐    │
                          │  │  Edge      │  │  Platform Adapter  │    │
                          │  │  Gateway   │──│  Hermes           │    │
                          │  │  (WS客户端) │  │  OpenClaw         │    │
                          │  └─────┬──────┘  │  Wukong           │    │
                          │        │         └────────────────────┘    │
                          │        │                                 │
                          │  ┌─────▼──────┐  ┌────────────────────┐    │
                          │  │  Sync      │  │  Local Cache       │    │
                          │  │  Engine    │──│  (离线可用)        │    │
                          │  └────────────┘  └────────────────────┘    │
                          └─────────────────────────────────────────────┘
```

---

## 3. 目录结构

```
Desktop/ClawShell/
├── ClawShell Cloud/                    # 云端部署单元
│   ├── README.md
│   ├── docker-compose.yml              # 一键部署（ECS）
│   ├── .env.example                    # 环境变量模板
│   ├── ansible/                        # 阿里云 ECS 非 Docker 部署
│   │   ├── inventory.yml
│   │   ├── playbook.yml
│   │   └── roles/
│   │       ├── mempalace/
│   │       ├── skill-registry/
│   │       ├── kanban-mcp/
│   │       └── nginx/
│   ├── cloud-hub/                      # Cloud Hub MCP Router
│   │   ├── src/
│   │   │   ├── __init__.py
│   │   │   ├── hub.py                  # WebSocket MCP Hub
│   │   │   ├── auth.py                 # JWT 认证
│   │   │   ├── router.py               # 请求路由
│   │   │   └── registry.py             # 连接端注册表
│   │   ├── requirements.txt
│   │   └── config.yaml
│   ├── mempalace-cloud/                # 云端记忆中枢
│   │   ├── Dockerfile
│   │   ├── config.yaml
│   │   └── init-scripts/
│   ├── skill-registry/                  # 技能注册表
│   │   ├── registry.db                  # SQLite 或 PostgreSQL
│   │   ├── skills/                      # 云端技能骨架
│   │   │   ├── prompt-optimzer/
│   │   │   ├── daily-report/
│   │   │   └── ...
│   │   └── SKILL.md
│   ├── kanban-mcp/                      # 任务看板 MCP Server
│   │   ├── src/
│   │   │   ├── kanban.py
│   │   │   └── sync.py
│   │   └── config.yaml
│   ├── nginx/
│   │   ├── nginx.conf
│   │   └── ssl/                         # SSL 证书
│   └── scripts/
│       ├── deploy.sh                    # 一键部署脚本
│       └── migrate-from-local.sh        # 从本地迁移数据
│
├── ClawShell Local/                    # 端侧安装单元
│   ├── README.md
│   ├── install.sh                       # 主安装脚本
│   ├── config/
│   │   ├── config.yaml                 # 主配置
│   │   ├── platforms/                  # 平台发现配置
│   │   │   ├── hermes.yaml
│   │   │   ├── openclaw.yaml
│   │   │   └── wukong.yaml
│   │   └── ecosystem.yaml              # 生态组件配置
│   ├── scripts/
│   │   ├── discover.sh                  # 平台自动发现
│   │   ├── install-ecosystem.sh        # 生态组件安装
│   │   ├── setup-edge-gateway.sh       # Edge Gateway 安装
│   │   └── sync-now.sh                 # 手动触发同步
│   ├── edge-gateway/                    # Edge Gateway
│   │   ├── __init__.py
│   │   ├── gateway.py                   # WS 客户端主程序
│   │   ├── sync_engine.py               # 同步引擎
│   │   ├── local_cache.py               # 本地缓存
│   │   ├── adapters/                    # 平台适配器
│   │   │   ├── __init__.py
│   │   │   ├── hermes_adapter.py
│   │   │   ├── openclaw_adapter.py
│   │   │   └── wukong_adapter.py
│   │   ├── protocol.py                  # MCP over WS 实现
│   │   ├── config.yaml
│   │   └── requirements.txt
│   ├── platform-detectors/              # 平台检测模块
│   │   ├── detect_hermes.sh
│   │   ├── detect_openclaw.sh
│   │   └── detect_wukong.sh
│   └── templates/                      # 配置模板
│       ├── hermes-mcp-remote.yaml
│       ├── openclaw-mcp-remote.yaml
│       └── sync-conflict-policy.yaml
│
├── SPEC.md                            # 本文档（架构规格）
├── CHANGELOG.md
└── CONTRIBUTING.md
```

---

## 4. 云端组件详细设计

### 4.1 Cloud Hub（核心）

**职责**：
- 接收所有端侧 Gateway 的 WebSocket 连接
- JWT 认证 + 端侧注册
- 将请求路由到对应的 MCP Server（记忆/技能/看板）
- 广播：技能更新、看板变更推送到所有已连接的端侧

**技术选型**：`Python 3.11 + asyncio + websockets + FastAPI（路由）`

**接口**：

```
WebSocket: wss://your-cloud.com/hub
  → 认证：首帧发送 JWT token
  → 心跳：每 30s ping/pong
  → 消息：MCP JSON-RPC 2.0

REST: https://your-cloud.com/api/v1/
  → POST /auth/token          # 获取 JWT
  → GET  /status              # 云端状态
  → POST /sync/push           # 离线数据上传
  → GET  /sync/pull/:since    # 增量拉取
```

**多租户**：单用户架构（ClawShell 定位），JWT 中嵌入 `user_id`，所有数据按 `user_id` 隔离。不需要多租户 SaaS 层。

### 4.2 MemPalace Cloud（记忆中枢）

**职责**：全量共享记忆的云端存储与检索

**部署方式**：
- Docker 容器，持久化存储用阿里云 RDS（PostgreSQL）或 ECS 本地 SSD
- 启动脚本自动初始化 ChromaDB collections

**配置**：
```yaml
# mempalace-cloud/config.yaml
storage:
  type: chromadb
  persist_directory: /data/palace
  distance_function: cosine

auth:
  jwt_secret: ${JWT_SECRET}
  allowed_users:
    - ${USER_ID}

sync:
  enabled: true
  push_endpoint: https://your-cloud.com/api/v1/sync/push
  pull_endpoint: https://your-cloud.com/api/v1/sync/pull
```

### 4.3 Skill Registry（技能注册表）

**职责**：管理技能骨架的云端注册与版本控制

**数据结构**：
```yaml
skill:
  id: prompt-optimizer-v1
  name: Prompt Optimizer
  description: 自动优化 LLM Prompt
  version: 1.0.0
  cloud_only: false          # true=纯云端执行, false=端侧执行
  adapter_required:          # 需要的端侧适配器
    - hermes
  local_template: |          # 端侧执行模板（可选）
    ...
  parameters:
    - name: input
      type: string
      required: true
  published_at: 2026-05-01
  published_by: yangyang
```

**存储**：SQLite（单用户）或 PostgreSQL（多端写入扩展）

### 4.4 Kanban MCP Server（任务看板）

**职责**：多端共享的 WIP-limit 看板

**数据结构**：
```json
{
  "board_id": "default",
  "columns": [
    {"id": "todo", "name": "待办", "wip_limit": 10},
    {"id": "doing", "name": "进行中", "wip_limit": 3},
    {"id": "done", "name": "已完成", "wip_limit": null}
  ],
  "tasks": [
    {
      "id": "task-001",
      "title": "完成设计文档",
      "column": "doing",
      "assignee": "edge-mac",
      "created_at": "2026-05-10T12:00:00Z",
      "updated_at": "2026-05-11T08:30:00Z",
      "version": 5
    }
  ]
}
```

**冲突解决**：使用 Semantic Versioning（version 字段），每次更新 +1。冲突时：云端权威，端侧回退并提示用户。

### 4.5 阿里云 ECS 部署方案

**推荐规格**：
- **轻量应用服务器**（适合个人/入门）：2 vCPU / 4 GB / 80 GB SSD / 每月 30 Mbps 峰值，月约 ¥60
- **ECS 通用型**（适合团队）：2 vCPU / 8 GB / 40 GB ESSD，月约 ¥150

**部署方式选择**：

| 方式 | 适用场景 | 复杂度 |
|------|---------|--------|
| Docker Compose | 有 Docker 环境 | ★☆☆ |
| Ansible 非 Docker | 纯systemd服务 | ★★★ |
| 手动安装 | 调试阶段 | ★★★★★ |

**Docker Compose 架构**：
```yaml
services:
  cloud-hub:
    build: ./cloud-hub
    ports: ["8443:8443"]
    environment:
      - JWT_SECRET=${JWT_SECRET}
      - USER_ID=${USER_ID}
    volumes:
      - ./cloud-hub/data:/data
    restart: unless-stopped

  mempalace:
    image: mempalace/mcp-server:latest
    environment:
      - PALACE_PATH=/data/palace
    volumes:
      - ./mempalace-data:/data/palace
    restart: unless-stopped

  skill-registry:
    build: ./skill-registry
    volumes:
      - ./skill-registry/data:/data
    restart: unless-stopped

  kanban:
    build: ./kanban-mcp
    volumes:
      - ./kanban-data:/data
    restart: unless-stopped

  nginx:
    image: nginx:alpine
    ports: ["443:443", "80:80"]
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf
      - ./nginx/ssl:/etc/nginx/ssl
    depends_on:
      - cloud-hub
      - mempalace
      - skill-registry
      - kanban
    restart: unless-stopped
```

---

## 5. 端侧组件详细设计

### 5.1 平台自动发现（Platform Auto-Discovery）

**目标**：不依赖用户手动配置，自动检测已安装的 Agent 平台

**检测策略**：

```
Hermes 检测：
  1. 检查 ~/Library/LaunchAgents/ai.hermes.gateway.plist
  2. 检查 ~/.hermes/ 目录是否存在
  3. 检查 PATH 中是否有 hermes 命令
  4. 检查运行中的进程：ps aux | grep hermes
  → 满足任一即视为已安装

OpenClaw 检测：
  1. 检查 ~/.openclaw/ 目录是否存在
  2. 检查 /usr/local/bin/openclaw 或 ~/.openclaw/openclaw
  3. 检查 openclaw CLI：openclaw --version
  → 满足任一即视为已安装

Wukong 检测：
  1. 检查 /Applications/悟空.app 或 ~/Applications/悟空.app
  2. 检查 ~/.wukong/ 配置目录
  3. 检查 PATH 中是否有 wukong 命令
  → 满足任一即视为已安装
```

**配置输出**：
```yaml
# config/discovered-platforms.yaml
platforms:
  hermes:
    detected: true
    version: "1.x.x"
    config_path: "/Users/yangyang/.hermes"
    launchctl_name: "ai.hermes.gateway"
    mcp_config_path: "/Users/yangyang/.hermes/config/mcp.yaml"
    status: running

  openclaw:
    detected: true
    version: "0.9.x"
    root_path: "/Users/yangyang/.openclaw"
    cli_path: "/usr/local/bin/openclaw"
    status: installed

  wukong:
    detected: false
```

### 5.2 Edge Gateway

**职责**：
- 维护与 Cloud Hub 的 WebSocket 长连接
- 代理端侧 MCP 请求到云端（记忆/技能/看板）
- 本地缓存（离线可用）
- 增量同步（只同步 diff，不全量拉取）

**同步策略**：

```
上线同步：
  1. Edge Gateway 连接到 Cloud Hub
  2. 发送本地 last_sync_timestamp
  3. Cloud Hub 返回 delta（自该 timestamp 以来所有变更）
  4. Edge 合并到本地缓存

离线操作：
  1. 端侧操作写入本地队列（pending_operations.jsonl）
  2. 用户感知无延迟（乐观更新）
  3. 上线后，将 pending_operations 按顺序重放

冲突仲裁：
  - 看板操作：云端权威（last-write-wins with version check）
  - 记忆写入：CRDT merge（MemPalace 的 vector DB 天然支持并发）
  - 技能更新：版本号比对，提示用户手动选择
```

**离线缓存数据结构**：
```
~/.clawshell-local/
├── cache/
│   ├── memory/              # 记忆缓存（ChromaDB，轻量版）
│   ├── skills/             # 技能骨架本地副本
│   └── kanban/             # 看板状态本地快照
├── sync/
│   ├── pending_operations.jsonl   # 待同步操作队列
│   ├── last_sync.txt              # 最后同步时间戳
│   └── conflict_log.jsonl         # 冲突记录
└── state/
    └── connection.json           # 连接状态
```

### 5.3 平台适配器（Platform Adapter）

**每个已检测到的平台，都生成一个 MCP adapter**，将云端 MCP 工具映射到本地 Agent 的执行能力：

```
Hermes Adapter：
  → 将云端 "task.create" 映射为 Hermes cron job create
  → 将云端 "memory.search" 映射为 Hermes MCP mempalace_* 调用
  → 将云端 "skill.invoke" 映射为 Hermes skill run

OpenClaw Adapter：
  → 将云端技能调用路由到 OpenClaw skill runner
  → 将云端文件操作映射为 OpenClaw filesystem skill

Wukong Adapter：
  → 将云端对话路由到悟空的对话接口
  → 将云端任务映射为悟空的任务系统
```

### 5.4 生态组件安装

**install-ecosystem.sh** 脚本负责下载安装以下组件：

| 组件 | 安装方式 | 配置路径 |
|------|---------|---------|
| MemPalace | `pip3 install mempalace` | `~/.mempalace/` |
| Memos | Docker 或 `brew install memos` | `~/.memos/` |
| n8n | Docker 或 npm | `~/.n8n/` |
| ChromaDB（本地缓存） | `pip3 install chromadb` | `~/.clawshell-local/cache/chroma` |

**安装选择菜单**（交互式）：
```
ClawShell Local 生态组件安装
============================
[1] MemPalace (记忆中枢)        — 已安装 v0.9.x
[2] Memos (轻量笔记)           — 未安装
[3] n8n (工作流自动化)         — 未安装
[4] ChromaDB (本地向量缓存)     — 已安装 v0.5.x
[5] 全部安装 (推荐)
[6] 自定义选择
[0] 跳过

请选择 [1-6, 0]:
```

---

## 6. MCP over WebSocket 协议设计

### 6.1 为什么不用原生 MCP stdio

MCP 原生使用 stdio（标准输入/输出），适用于本地进程通信。云端协同需要跨网络，因此需要将 MCP 封装到 WebSocket 传输层。

### 6.2 协议封装

```json
// 客户端 → 服务端（认证握手）
{
  "type": "auth",
  "token": "eyJhbGciOiJIUzI1NiJ9..."
}

// 客户端 → 服务端（MCP 请求）
{
  "type": "mcp_request",
  "id": "req-uuid-123",
  "method": "tools/call",
  "params": {
    "name": "mempalace_search",
    "arguments": {"query": "ClawShell", "limit": 5}
  }
}

// 服务端 → 客户端（MCP 响应）
{
  "type": "mcp_response",
  "id": "req-uuid-123",
  "result": { ... }
}

// 服务端 → 客户端（推送/通知）
{
  "type": "mcp_push",
  "method": "notifications/tools_changed",
  "params": {}
}
```

### 6.3 Cloud Hub 路由规则

```
客户端请求 → Cloud Hub
  → 检查 token 有效性
  → 解析 method name（如 mempalace_search）
  → 路由到对应 MCP Server：
      mempalace_*  → MemPalace Cloud MCP Server
      skill_*      → Skill Registry MCP Server
      kanban_*     → Kanban MCP Server
      ~other       → 本地适配器（Edge-side only）
  → 响应返回给客户端
```

---

## 7. 安全设计

### 7.1 认证

```
端侧首次配置：
  1. 用户在 Cloud 控制台生成 USER_ID + JWT_SECRET
  2. 将凭证写入 ClawShell Local 配置
  3. Edge Gateway 启动时用 USER_ID:JWT_SECRET 换取 token

Token 刷新：
  - Access Token 有效期 1 小时
  - Refresh Token 有效期 7 天
  - Edge Gateway 自动刷新
```

### 7.2 传输安全

- **TLS 1.3**（Cloud Hub 强制，nginx 配置 `ssl_protocols TLSv1.3`）
- **WSS**（WebSocket over TLS，端口 443）
- **内网通信**：同一 VPC 内服务间用内网 IP，不走公网

### 7.3 数据安全

- 凭证不落盘：JWT secret 只在启动时通过环境变量注入
- 敏感数据（API keys）存在记忆中的 drawer 时加密存储
- 端侧缓存加密：`~/.clawshell-local/cache` 使用 macOS Keychain 管理的对称密钥

---

## 8. 实施计划

### Phase 0：项目脚手架（1天）

**目标**：建立目录结构、Git 初始化、CI/CD 基础

```
[Day 1]
✓ Desktop/ClawShell/ 目录结构创建
✓ Git 仓库初始化
✓ README.md 编写
✓ CI: GitHub Actions（测试 + lint）
✓ 制定 coding style guide
```

### Phase 1：云端基础 + MemPalace 云化（3天）

**目标**：Cloud Hub 骨架 + MemPalace 云端部署

```
[Day 2-3] Cloud Hub 骨架
  ✓ WebSocket server（Python asyncio + websockets）
  ✓ JWT 认证层
  ✓ 请求路由基础
  ✓ Docker Compose 本地开发环境
  ✓ nginx 反向代理配置

[Day 4] MemPalace Cloud 部署
  ✓ MemPalace MCP Server Docker 镜像
  ✓ 持久化存储配置（Aliyun SSD）
  ✓ 与 Cloud Hub 集成
  ✓ https + 域名配置（可选，用 IP 先跑）
  ✓ 本地测试：Edge → Cloud Hub → MemPalace 联通
```

### Phase 2：端侧基础框架（2天）

**目标**：Edge Gateway 骨架 + 平台发现

```
[Day 5] 平台自动发现
  ✓ detect_hermes.sh / detect_openclaw.sh / detect_wukong.sh
  ✓ discover.sh 主脚本
  ✓ discovered-platforms.yaml 输出格式

[Day 6] Edge Gateway 骨架
  ✓ WebSocket 客户端（MCP over WS）
  ✓ 本地缓存目录结构
  ✓ 配置加载（config.yaml）
  ✓ 主程序入口（gateway.py）
```

### Phase 3：同步引擎（3天）

**目标**：完整的数据同步能力

```
[Day 7] 同步引擎核心
  ✓ pending_operations 队列读写
  ✓ delta pull（按 timestamp 增量拉取）
  ✓ delta push（上传本地变更）

[Day 8] 冲突处理
  ✓ 看板冲突仲裁（云端权威 + version check）
  ✓ 冲突日志（conflict_log.jsonl）
  ✓ 用户冲突通知机制

[Day 9] 离线支持
  ✓ 本地 ChromaDB 缓存（记忆）
  ✓ 离线操作队列持久化
  ✓ 重连后自动同步
```

### Phase 4：生态集成 + 安装脚本（2天）

**目标**：用户可用的安装体验

```
[Day 10] 生态组件安装
  ✓ install-ecosystem.sh（MemPalace/Memos/n8n/ChromaDB）
  ✓ 交互式菜单
  ✓ 依赖检测（Docker/pip3/brew）

[Day 11] 完整安装脚本
  ✓ install.sh（主入口，引导用户完成全部配置）
  ✓ setup-edge-gateway.sh
  ✓ 配置文件模板（templates/）
  ✓ 阿里云 ECS 部署脚本（deploy.sh）
```

### Phase 5：技能注册表 + 看板 MCP（3天）

**目标**：完整的云端服务能力

```
[Day 12-13] Skill Registry
  ✓ MCP Server 实现
  ✓ SQLite/PostgreSQL 存储
  ✓ 技能 CRUD API
  ✓ 技能发布/版本管理

[Day 14] Kanban MCP Server
  ✓ WIP-limit 看板逻辑
  ✓ MCP 工具定义
  ✓ 与 Cloud Hub 路由集成
```

### Phase 6：生产部署 + 文档（2天）

**目标**：可对外发布的完整系统

```
[Day 15] 阿里云 ECS 部署验证
  ✓ Docker Compose ECS 部署验证
  ✓ Ansible 非 Docker 部署验证
  ✓ 数据迁移脚本（migrate-from-local.sh）

[Day 16] 文档 + 演示
  ✓ 完整 README.md
  ✓ 架构文档（SPEC.md）
  ✓ 使用教程
  ✓ 演示视频
```

**总工期：约 16 个工作日**

---

## 9. 风险与缓解

| 风险 | 概率 | 影响 | 缓解方案 |
|------|------|------|---------|
| MCP over WebSocket 延迟影响体感 | 中 | 中 | 本地缓存优先，异步同步 |
| 多端并发写同一任务导致冲突 | 低 | 中 | CRDT + 云端权威 + 用户提示 |
| 阿里云 ECS 安全组配置错误 | 中 | 高 | 部署脚本中内嵌安全组配置 |
| MemPalace 云端性能不足 | 低 | 中 | 先用轻量应用服务器测试，不够再升级 |
| Edge Gateway 自动发现误判平台 | 低 | 低 | 提供手动覆盖选项 |
| 长期积累数据量超出 ECS 存储 | 中 | 中 | 设计时预留 OSS 扩展接口 |

---

## 10. 依赖关系图

```
Phase 1（Cloud Hub + MemPalace Cloud）
    │
    ├──► Phase 2（Edge Gateway） ←─ 需要 Phase 1 的协议规范
    │         │
    │         └──► Phase 3（Sync Engine） ←─ 需要 Edge Gateway
    │                   │
    │                   └──► Phase 4（安装脚本） ←─ 需要所有组件
    │
    ├──► Phase 5（Skill + Kanban）←─ 独立于 Phase 2-4
    │
    └──► Phase 6（部署 + 文档）←─ 需要 Phase 1-5
```

---

## 11. 验收标准

### Phase 1 完成标准
- [ ] `docker-compose up` 可在本地启动 Cloud Hub + MemPalace Cloud
- [ ] Edge Gateway（手动配置）可连接 Cloud Hub 并调用 `mempalace_search`
- [ ] JWT 认证生效，无 token 拒绝访问

### Phase 2 完成标准
- [ ] `discover.sh` 在干净 macOS 系统上能检测到已安装的 Hermes
- [ ] Edge Gateway 启动无报错，日志正常

### Phase 3 完成标准
- [ ] 断网后操作记录到 `pending_operations.jsonl`
- [ ] 恢复网络后自动同步，数据不丢失
- [ ] 冲突产生时生成 `conflict_log.jsonl`

### Phase 4 完成标准
- [ ] `./install.sh` 在 10 分钟内完成端侧完整安装
- [ ] 安装过程无手动干预（自动化安装）

### Phase 5 完成标准
- [ ] 技能可从云端发布，端侧自动可见
- [ ] 看板状态在多端保持一致

### 最终验收
- [ ] 在阿里云 ECS（轻量应用服务器）上 `./deploy.sh` 完成全量部署
- [ ] 端侧 Mac 和车机（两个端侧）可同时连接云端，记忆/看板/技能三端共享
