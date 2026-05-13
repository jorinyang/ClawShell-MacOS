---
name: cloud-hub-connect
description: Hermes CloudHub 连接 Skill — 订阅 CloudHub 事件流，双通道写回洞察
version: 1.0.0
author: ClawShell
trigger:
  events:
    - "node.registered"
    - "node.offline"
    - "task.done"
    - "task.failed"
    - "error.*"
    - "insight.*"
  cron:
    - "0 8 * * *"    # 每日 08:00 复盘
    - "0 * * * *"    # 每小时洞察摘要
---

# CloudHub Connect — Hermes 云端大脑入口

## 功能描述

Hermes 订阅 CloudHub 事件流，触发 LLM 分析，结果通过双通道写回 CloudHub。

## 架构

```
CloudHub（ECS:8080/8081/8082）
    │ WS 订阅（Port 8080）
    ├────────────────────────► Hermes
    │                             │
    │                      LLM 分析（MiniMax / Qwen-Max）
    │                             │
    │                        双通道写回
    │                        ├─ MCP（Port 8081，主）
    │                        └─ HTTP（Port 8082，备）
    │                             │
    │                        CloudHub 广播给 Edge
    │
    └─ MCP Server（8081）/ HTTP（8082）◄─ Hermes 写回
```

## 消息类型

| type | 来源 | 触发 Skill |
|------|------|-----------|
| `insight_add` | Hermes → CloudHub | Hermes 写洞察结论 |
| `review_publish` | Hermes → CloudHub | Hermes 写复盘报告 |
| `strategy_update` | Hermes → CloudHub | Hermes 更新策略 |
| `deep_think_result` | Hermes → CloudHub | Hermes 写推理结果 |

## 双通道配置

```yaml
channels:
  primary:
    protocol: mcp
    url: http://localhost:8081
    timeout: 3.0
  backup:
    protocol: http
    url: http://localhost:8082/cloudbrain/write
    timeout: 3.0
  fallback:
    protocol: dlq
    path: /opt/clawshell/data/dlq
    retry_interval: 300  # 5分钟重试
```

## 环境变量

```bash
CLOUDHUB_WS_URL=wss://localhost:8080
CLOUDHUB_MCP_URL=http://localhost:8081
CLOUDHUB_HTTP_URL=http://localhost:8082
MINIMAX_API_KEY=<key>
DASHSCOPE_API_KEY=<key>
LLM_PROVIDER_FAST=minimax
LLM_PROVIDER_DEEP=dashscope
```
