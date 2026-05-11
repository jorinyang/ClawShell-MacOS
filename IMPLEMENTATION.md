# ClawShell 2.0 — 实施计划详解

> 版本：v1.0
> 更新：2026-05-11
> 仓库：https://github.com/jorinyang/ClawShell-MacOS

---

## Phase 0：项目脚手架 ✅ 已完成

**Day 1**
- [x] Desktop/ClawShell/ 目录结构创建
- [x] Git 仓库初始化 + 推送到 GitHub
- [x] PLANNING.md 编写
- [ ] CI: GitHub Actions（测试 + lint）
- [ ] 制定 coding style guide

---

## Phase 1：云端基础 + Obsidian Vault OSS 同步（3天）

**Day 2-3：Cloud Hub 骨架**

#### 1.1 WebSocket Server（cloud-hub/src/hub.py）
- [ ] `websockets` 库导入与基础服务器搭建
- [ ] `asyncio` 异步连接管理（连接表、ping/pong）
- [ ] MCP JSON-RPC 2.0 消息解析
- [ ] 多端连接并发处理
- [ ] 日志系统配置

#### 1.2 JWT 认证层（cloud-hub/src/auth.py）
- [ ] JWT token 生成（HS256，1h有效期）
- [ ] JWT token 验证中间件
- [ ] 首次连接认证握手流程（auth帧）
- [ ] Refresh token 生成与刷新接口
- [ ] 错误响应封装（401/403）

#### 1.3 请求路由（cloud-hub/src/router.py）
- [ ] method name 解析（vault_* / skill_* / kanban_*）
- [ ] 路由到对应 MCP Server
- [ ] 响应格式统一封装
- [ ] 路由表配置（config.yaml）

#### 1.4 Docker Compose 本地开发环境
- [ ] cloud-hub Dockerfile 完善（Python 3.11 + requirements.txt）
- [ ] docker-compose.yml 端口映射（8443:8443）
- [ ] 本地开发 `docker-compose up` 验证
- [ ] 健康检查配置

#### 1.5 nginx 反向代理配置
- [ ] nginx.conf WebSocket 支持（upgrade header）
- [ ] TLS 终止配置（ssl_certificate 占位）
- [ ] upstream 指向 cloud-hub:8443
- [ ] 80→443 重定向

**Day 4：Obsidian Vault OSS 同步**

#### 1.6 OSS Bucket 创建与配置
- [ ] 阿里云 OSS 控制台创建 Bucket（命名如 `clawshell-vault`）
- [ ] 创建 AccessKey（RAM 用户，限制 OSS 权限）
- [ ] 配置 `rclone` 或 `s3fs` 挂载工具
- [ ] vault-oss/config.yaml 配置文件（endpoint / bucket / access_key / secret_key）

#### 1.7 双向同步脚本（vault-oss/sync.sh）
- [ ] `watchmedo` 监听本地 vault 目录变更
- [ ] 变更文件实时上传到 OSS
- [ ] OSS → 本地拉取脚本（`rclone sync oss:bucket/vault ~/Obsidian/Vault`）
- [ ] 冲突处理（本地优先，上传前检查 md5）

#### 1.8 Cloud Hub Vault 路由（可选）
- [ ] vault_list / vault_upload / vault_download 工具注册
- [ ] JWT 认证 + OSS API 调用
- [ ] 端到端验证：Edge → Cloud Hub → OSS

---

## Phase 2：端侧基础框架（2天）

**Day 5：平台自动发现**

#### 2.1 平台检测脚本
- [ ] `platform-detectors/detect_hermes.sh`
  - 检查 `~/Library/LaunchAgents/ai.hermes.gateway.plist`
  - 检查 `~/.hermes/` 目录
  - 检查 `hermes` 命令可用性
- [ ] `platform-detectors/detect_openclaw.sh`
  - 检查 `~/.openclaw/` 目录
  - 检查 `openclaw` CLI
- [ ] `platform-detectors/detect_wukong.sh`
  - 检查 `悟空.app`
  - 检查 `~/.wukong/` 配置目录

#### 2.2 discover.sh 主脚本
- [ ] 遍历所有 detector 并收集结果
- [ ] `discovered-platforms.yaml` 输出格式
- [ ] JSON 输出模式（可选）
- [ ] 错误处理与日志

#### 2.3 discovered-platforms.yaml 格式定义
- [ ] 字段：detected / version / config_path / launchctl_name / status
- [ ] 模板文件（templates/discovered-platforms.yaml.example）

**Day 6：Edge Gateway 骨架**

#### 2.4 WebSocket 客户端（edge-gateway/src/gateway.py）
- [ ] `websockets` 客户端连接 cloud_url
- [ ] JWT token 认证帧发送
- [ ] 重连逻辑（指数退避）
- [ ] 心跳机制（ping/pong）
- [ ] 消息收发循环

#### 2.5 MCP over WS 协议实现（edge-gateway/src/protocol.py）
- [ ] 认证帧封装
- [ ] MCP request/response 封装
- [ ] 推送消息处理（notifications/tools_changed）
- [ ] 请求ID管理（uuid）

#### 2.6 本地缓存目录结构
- [ ] `~/.clawshell-local/cache/memory/`（ChromaDB）
- [ ] `~/.clawshell-local/cache/skills/`
- [ ] `~/.clawshell-local/cache/kanban/`
- [ ] `~/.clawshell-local/sync/`
- [ ] `~/.clawshell-local/logs/`

#### 2.7 配置加载（edge-gateway/config.yaml）
- [ ] cloud_url / jwt_token / sync_interval
- [ ] 平台适配器开关
- [ ] 日志级别
- [ ] 配置加密存储（可选）

#### 2.8 主程序入口
- [ ] 参数解析（`--config` / `--daemon`）
- [ ] 配置加载与校验
- [ ] 信号处理（SIGTERM / SIGINT）
- [ ] 守护进程模式

---

## Phase 3：同步引擎（3天）

**Day 7：同步引擎核心**

#### 3.1 pending_operations 队列（edge-gateway/src/sync_engine.py）
- [ ] `pending_operations.jsonl` 读写
- [ ] 操作类型：memory_add / memory_search / kanban_update / skill_invoke
- [ ] 操作ID（uuid）+ 时间戳
- [ ] 队列文件锁（避免并发写冲突）

#### 3.2 Delta Pull
- [ ] 计算本地 last_sync_timestamp
- [ ] 请求 `GET /sync/pull/:since`
- [ ] 增量数据合并到本地缓存
- [ ] last_sync.txt 更新

#### 3.3 Delta Push
- [ ] 读取 pending_operations.jsonl
- [ ] 批量 POST `/sync/push`
- [ ] 成功后的操作标记（删除或备份到 processed_operations.jsonl）
- [ ] 失败重试（指数退避）

**Day 8：冲突处理**

#### 3.4 看板冲突仲裁
- [ ] version 字段比对（每次更新+1）
- [ ] 云端权威策略：本地版本 < 云端版本 → 放弃本地
- [ ] 冲突检测：本地版本 == 云端版本 → 提示用户
- [ ] 自动合并：仅删除操作可自动合并

#### 3.5 冲突日志（conflict_log.jsonl）
- [ ] 冲突记录格式：timestamp / entity_type / entity_id / local_version / cloud_version / resolution
- [ ] 日志轮转（超过1000条时归档）
- [ ] 用户查看命令（`clawshell local conflicts`）

#### 3.6 用户冲突通知机制
- [ ] 冲突产生时写入 `~/.clawshell-local/state/conflicts_pending.jsonl`
- [ ] Edge Gateway 启动时检查并提醒
- [ ] 交互式解决（keep_local / keep_cloud / merge）

**Day 9：离线支持**

#### 3.7 本地 ChromaDB 缓存
- [ ] `chromadb` 客户端初始化
- [ ] collection 命名空间（cloud_id 前缀避免冲突）
- [ ] 离线写入（写入本地 collection）
- [ ] 上线后与云端数据合并

#### 3.8 离线操作队列持久化
- [ ] pending_operations.jsonl 文件存在性保证
- [ ] 应用关闭时队列完整flush
- [ ] 应用启动时队列完整性检查

#### 3.9 重连后自动同步
- [ ] 网络恢复检测（ping cloud_url）
- [ ] 自动触发 sync
- [ ] 进度通知（写入 state/sync_status.json）
- [ ] 同步完成状态上报

---

## Phase 4：生态集成 + 安装脚本（2天）

**Day 10：生态组件安装**

#### 4.1 install-ecosystem.sh
- [ ] MemPalace（本地向量记忆）保留
- [ ] 依赖检测（Docker / pip3 / brew）
- [ ] 各组件安装函数
- [ ] 安装后验证（健康检查）
- [ ] 卸载函数

#### 4.2 MemPalace（本地向量记忆）
- [ ] `pip3 install mempalace`
- [ ] `mempalace init` 初始化
- [ ] 配置 `~/.mempalace/config.yaml`
- [ ] 启动服务（本地 MCP Server，端口 8444）

#### 4.3 Memos Local（本地结构化笔记）
- [ ] Docker 方式检测
- [ ] `docker run` 启动 memos
- [ ] 端口配置（默认 5230）
- [ ] 初始化账号引导

#### 4.4 n8n 安装
- [ ] Docker 方式检测
- [ ] `docker run` 启动 n8n
- [ ] 工作流导入模板

#### 4.5 ChromaDB 本地缓存
- [ ] `pip3 install chromadb`
- [ ] 持久化目录 `~/.clawshell-local/cache/chroma`
- [ ] 版本验证

**Day 11：完整安装脚本**

#### 4.6 install.sh 主入口
- [ ] 前置检查（Python 3.9+ / git / curl）
- [ ] 目录创建（~/.clawshell-local/）
- [ ] discover.sh 调用
- [ ] install-ecosystem.sh 调用
- [ ] setup-edge-gateway.sh 调用
- [ ] 配置引导（cloud_url / JWT token）
- [ ] 启动 Edge Gateway

#### 4.7 setup-edge-gateway.sh
- [ ] 配置文件生成（从 templates/）
- [ ] cloud.json 写入
- [ ] JWT token 安全存储
- [ ] Edge Gateway 服务注册（launchd）
- [ ] 启动验证

#### 4.8 配置文件模板（templates/）
- [ ] cloud.json.example
- [ ] hermes-mcp-remote.yaml.example
- [ ] openclaw-mcp-remote.yaml.example
- [ ] sync-conflict-policy.yaml.example

#### 4.9 阿里云 ECS 部署脚本
- [ ] `deploy.sh` 接受参数（ecs_ip / ssh_key_path）
- [ ] SSH 连接验证
- [ ] Docker + docker-compose 安装
- [ ] `.env` 文件生成
- [ ] `docker-compose up -d` 执行
- [ ] 健康检查

---

## Phase 5：技能注册表 + 看板 MCP（3天）

**Day 12-13：Skill Registry**

#### 5.1 MCP Server 实现（skill-registry/src/server.py）
- [ ] MCP tools/list 工具注册
- [ ] MCP tools/call 工具调用
- [ ] 技能骨架定义（name / version / description / parameters）
- [ ] Python skill runner 调用

#### 5.2 数据存储
- [ ] SQLite 数据库（skill_registry.db）
- [ ] 表结构：skills（id / name / version / description / cloud_only / adapter_required / local_template / parameters / published_at / published_by）
- [ ] 索引优化（name + version 唯一索引）

#### 5.3 技能 CRUD API
- [ ] skill_publish（发布技能）
- [ ] skill_update（更新技能）
- [ ] skill_list（列出技能）
- [ ] skill_get（获取技能详情）
- [ ] skill_delete（删除技能）

#### 5.4 技能发布/版本管理
- [ ] 版本号规范（SemVer）
- [ ] 版本历史（skill_versions 表）
- [ ] 云端技能同步到端侧

**Day 14：Kanban MCP Server**

#### 5.5 WIP-limit 看板逻辑（kanban-mcp/src/kanban.py）
- [ ] 看板数据结构（board_id / columns / tasks）
- [ ] WIP limit 检查（column.wip_limit）
- [ ] 任务移动验证（从 todo → doing 时检查 limit）
- [ ] 任务创建/更新/删除

#### 5.6 MCP 工具定义
- [ ] kanban_list（列出所有看板）
- [ ] kanban_task_create（创建任务）
- [ ] kanban_task_move（移动任务）
- [ ] kanban_task_update（更新任务）
- [ ] kanban_task_delete（删除任务）

#### 5.7 与 Cloud Hub 路由集成
- [ ] cloud-hub 路由规则更新（kanban_* → kanban-mcp）
- [ ] WebSocket 连接池
- [ ] 请求超时处理

---

## Phase 6：生产部署 + 文档（2天）

**Day 15：阿里云 ECS 部署验证**

#### 6.1 Docker Compose ECS 部署验证
- [ ] 阿里云轻量应用服务器购买 + 网络配置
- [ ] SSH 密钥配置
- [ ] `deploy.sh` 执行验证
- [ ] 端口访问验证（安全组开放 80/443/8443）
- [ ] Docker 服务健康检查

#### 6.2 Ansible 非 Docker 部署验证
- [ ] inventory.yml 配置
- [ ] playbook.yml 任务定义
- [ ] 各 role 测试（cloud-hub / skill-registry / kanban / nginx）
- [ ] systemd 服务注册验证

#### 6.3 数据迁移脚本（migrate-from-local.sh）
- [ ] 从本地配置导出（discovered-platforms.yaml）
- [ ] 从本地 vault 导出到 OSS（rclone copy 本地vault oss:bucket/vault）
- [ ] 迁移前备份
- [ ] 迁移后校验

**Day 16：文档 + 演示**

#### 6.4 完整 README.md
- [ ] 项目介绍（中英文）
- [ ] 架构图（ASCII/PNG）
- [ ] 快速开始（5分钟上手）
- [ ] 文档目录链接

#### 6.5 架构文档（SPEC.md）
- [ ] 完整接口定义
- [ ] 数据模型
- [ ] 协议规范（MCP over WS）
- [ ] 安全性说明

#### 6.6 使用教程
- [ ] 云端部署教程
- [ ] 端侧安装教程
- [ ] 平台适配器配置
- [ ] 同步与冲突处理
- [ ] 常见问题 FAQ

#### 6.7 演示视频（可选）
- [ ] 录制部署流程
- [ ] 录制端侧安装流程
- [ ] 录制多端同步演示

---

## 验收总览

| Phase | 完成标准 | 状态 |
|-------|---------|------|
| Phase 0 | Git仓库建立 / PLANNING.md | ✅ |
| Phase 1 | docker-compose up 本地启动 Cloud Hub + OSS Bucket 创建 | 🔲 |
| Phase 2 | discover.sh 检测 Hermes / Edge Gateway 启动无报错 | 🔲 |
| Phase 3 | 断网后操作记录到 pending_operations.jsonl | 🔲 |
| Phase 4 | install.sh 10分钟内完成端侧安装 | 🔲 |
| Phase 5 | 技能云端发布端侧可见 / 看板多端一致 | 🔲 |
| Phase 6 | ECS部署验证 / 文档完整 | 🔲 |

---

## 依赖关系

```
Phase 1（Cloud Hub + Obsidian Vault OSS）
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
