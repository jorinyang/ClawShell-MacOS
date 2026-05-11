# ClawShell 2.0

> 云端/本地分离的多智能体协同操作系统
> 适用于类 OpenClaw 架构的增强型外骨骼功能插件

**架构版本**: v2.0
**仓库**: [jorinyang/ClawShell-MacOS](https://github.com/jorinyang/ClawShell-MacOS)

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Cloud Hub (Aliyun ECS)                 │
│                                                             │
│   :8443  WebSocket MCP Router ←─ JWT Auth ─← Edge Gateway  │
│   :8080  REST API (token/sync/status)                      │
│                                                             │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│   │ Skill        │  │ Kanban       │  │  OSS Vault   │    │
│   │ Registry     │  │ MCP Server   │  │  (HongKong)  │    │
│   │ :8445 WS     │  │ :8446 WS    │  │              │    │
│   └──────────────┘  └──────────────┘  └──────────────┘    │
│                              ▲                              │
│                      nginx :443 (TLS)                       │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ WSS + JWT
                              │
┌──────────────────────────────┴──────────────────────────────┐
│                     Local Device (macOS)                    │
│                                                             │
│   Edge Gateway ←─ discover.sh ─► Hermes / OpenClaw / 悟空   │
│        │                                                   │
│   Sync Engine (pending_operations.jsonl)                    │
│        │                                                   │
│   ┌────┴────┐  ┌─────────────┐  ┌──────────────────┐    │
│   │ MemPalace│  │ Memos Local │  │ ChromaDB Cache   │    │
│   │ (vector)│  │ (notes)     │  │                  │    │
│   └─────────┘  └─────────────┘  └──────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **自感知** | 自动发现 Hermes / OpenClaw / 悟空平台环境 |
| **自适应** | 云端 OSS 香港节点，本地缓存，离线优先 |
| **自组织** | pending_operations 队列 + delta pull/push |
| **云端协同** | MCP over WebSocket，JWT 认证 |
| **冲突处理** | 云端权威策略 + conflict_log.jsonl |

---

## 快速开始

### 云端部署（Day 1）

```bash
# 克隆
git clone https://github.com/jorinyang/ClawShell-MacOS.git
cd ClawShell-MacOS/ClawShell\ Cloud

# 配置
cp .env.example .env
# 编辑 .env: JWT_SECRET, OSS_ACCESS_KEY_ID/_SECRET

# 启动
docker compose up -d
```

### 端侧安装（Day 1）

```bash
cd ../ClawShell\ Local
chmod +x install.sh scripts/*.sh
./install.sh
```

---

## Phase 进度

| Phase | 内容 | 状态 |
|-------|------|------|
| Phase 0 | Git 仓库 + PLANNING.md | ✅ |
| Phase 1 | Cloud Hub + OSS Vault 同步 | ✅ |
| Phase 2 | Edge Gateway + 平台发现 | ✅ |
| Phase 3 | Sync Engine + 冲突处理 | ✅ |
| Phase 4 | 生态安装脚本 + 模板 | ✅ |
| Phase 5 | Skill + Kanban MCP WS Server | ✅ |
| Phase 6 | 文档 + Ansible + 迁移脚本 | 🔲 进行中 |

---

## 目录结构

```
ClawShell/
├── PLANNING.md          # 架构规范 (钱学森工程控制论)
├── IMPLEMENTATION.md     # 实施计划详解
├── README.md            # 本文件
│
├── ClawShell Cloud/     # 云端部署
│   ├── cloud-hub/       # MCP WebSocket 路由中枢
│   ├── skill-registry/  # 技能注册表 MCP Server
│   ├── kanban-mcp/      # 看板 MCP Server
│   ├── vault-oss/       # OSS 双向同步脚本
│   ├── nginx/           # 反向代理 + TLS
│   ├── scripts/         # 部署脚本
│   └── docker-compose.yml
│
└── ClawShell Local/     # 端侧
    ├── edge-gateway/    # Edge Gateway 核心
    ├── scripts/         # discover.sh / install.sh
    └── config/          # 配置模板
```

---

## 文档

- [PLANNING.md](PLANNING.md) — 完整架构规范
- [IMPLEMENTATION.md](IMPLEMENTATION.md) — 分 Phase 实施任务
- [SPEC.md](SPEC.md) — 接口规范 / 数据模型 / 协议定义

---

## 安全说明

- **凭证管理**: AccessKey 仅存于本地 `.env`，不提交到 Git
- **JWT**: 所有 WebSocket 连接需通过 Cloud Hub 认证
- **TLS**: nginx 反向代理强制 TLS 1.3
- **凭证轮换**: 阿里云 RAM 控制台建议定期轮换 AccessKey
