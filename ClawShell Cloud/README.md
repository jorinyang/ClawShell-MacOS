# ClawShell Cloud

云端部署单元，包含所有可多端共享的服务。

## 快速开始

### 前置要求
- Docker + Docker Compose
- 阿里云 ECS（推荐 2C4G 最小规格）

### 部署

```bash
# 1. 配置环境变量
cp .env.example .env
# 编辑 .env，填入 JWT_SECRET 和 USER_ID

# 2. 一键部署（本地开发）
docker compose up

# 3. 阿里云 ECS 部署
./scripts/deploy.sh <ecs_ip> <ssh_key_path>
```

## 组件

| 组件 | 端口 | 说明 |
|------|------|------|
| Cloud Hub | 8443/443 | MCP WebSocket 路由中枢 |
| MemPalace | 8444 | 共享记忆云端存储 |
| Skill Registry | 8445 | 技能骨架注册表 |
| Kanban | 8446 | WIP-limit 任务看板 |

## 安全

- TLS 1.3 强制
- JWT 认证
- WebSocket over WSS
