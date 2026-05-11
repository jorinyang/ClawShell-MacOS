# ClawShell v2.0 — 综合迭代规划

> 基于 ClawShell-Windows (v1.0) 和 ClawShell-MacOS (v1.3) 的全方位融合
> 版本：v2.0-draft
> 更新：2026-05-12

---

## 1. 为什么是融合而不是重写

| | Windows 版优势 | MacOS 版优势 |
|--|--------------|-------------|
| **架构** | 四层 Layer1-4 完整，自感知→自适应→自组织→集群 | 云端分离，事件驱动，Event Sourcing |
| **通信** | EventBus 内存 pub/sub + 条件引擎 | PubSub Manager + WebSocket 广播 |
| **持久化** | 多层（Genome/MemOS/Obsidian） | 单一 OSS EventStore |
| **知识管理** | Genome 知识传承体系，版本化技能 | MemoryDomain → OSS，无版本化 |
| **工作流** | N8N 外部编排（耦合重） | 内置 WorkflowEngine（需补 Saga） |
| **自适应** | ConditionEngine DSL + StrategySwitcher | Platform Adapters（仅接口适配） |
| **集群协作** | TrustManager + EcologyMatcher + SwarmDiscovery | PubSub 广播（无信任/生态评估） |
| **Hermes 集成** | HermesBridge 双脑协同（成熟） | HermesAdapter（仅接口映射） |
| **自修复** | SelfHealing + SelfRepair（凌晨 cron 驱动） | 无 |
| **部署** | 单机 install.sh | Docker Compose（云端）+ Shell（端侧） |

**结论**：MacOS 的架构更现代，Windows 的能力更丰富。融合策略：
- 以 MacOS 云端分离架构为骨架
- 将 Windows 经过验证的核心模块以适配/扩展方式嵌入

---

## 2. 融合架构全景

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           ClawShell v2.0 Cloud                               │
│                                                                              │
│   Edge Devices ── WSS+JWT ── Cloud Hub (Event-Driven MCP Router)             │
│                                     │                                        │
│   ┌────────────────────────────────┼────────────────────────────────────┐   │
│   │                                │                                        │   │
│   │  EventStore (OSS)             │  StateAggregator                    │   │
│   │  ┌─────────────────────────┐  │                                     │   │
│   │  │ Seq Gen + Replay Queue  │  │  node/task/workflow/skill 状态      │   │
│   │  └─────────────────────────┘  │                                     │   │
│   │                                │                                        │   │
│   │  ┌──────────────────────────────────────────────────────────────┐   │   │
│   │  │                  Domain Handlers                              │   │   │
│   │  │  Kanban │ Skill │ Node │ Memory │ Workflow │ Genome        │   │   │
│   │  └──────────────────────────────────────────────────────────────┘   │   │
│   │                                │                                        │   │
│   │  ┌────────────────────────────┴────────────────────────────────┐   │   │
│   │  │              Adaptive Engine (from Windows Layer2)            │   │   │
│   │  │  ConditionEngine │ StrategySwitcher │ SelfHealing            │   │   │
│   │  └──────────────────────────────────────────────────────────────┘   │   │
│   │                                │                                        │   │
│   │  ┌────────────────────────────┴────────────────────────────────┐   │   │
│   │  │              Swarm Coordination (from Windows Layer4)        │   │   │
│   │  │  TrustManager │ EcologyMatcher │ SwarmDiscovery              │   │   │
│   │  └──────────────────────────────────────────────────────────────┘   │   │
│   └────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                          ClawShell v2.0 Edge                                │
│                                                                              │
│   Edge Gateway (WS Client)                                                   │
│        │                                                                     │
│        ├── Sync Engine (pending_ops + Event Replay Queue)                    │
│        ├── Platform Adapter Manager                                           │
│        │     ├── HermesAdapter (from MacOS)                                  │
│        │     ├── OpenClawAdapter                                             │
│        │     └── WukongAdapter                                               │
│        │                                                                     │
│        ├── EventBus Local (from Windows) ← 轻量版，本地事件处理              │
│        │     ├── ConditionEngine (本地规则)                                   │
│        │     ├── DeadLetterQueue                                             │
│        │     └── EventTracer                                                 │
│        │                                                                     │
│        └── Local Cache (ChromaDB + Memos + pending_operations.jsonl)         │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 新增模块清单

### 3.1 Cloud Hub 新增

| 模块 | 来源 | 说明 |
|------|------|------|
| `GenomeDomain` | Windows genome/ | 知识传承体系：skill versioning, heritage, pattern miner |
| `AdaptiveDomain` | Windows layer2/ | ConditionEngine + StrategySwitcher + SelfHealing |
| `SwarmDomain` | Windows layer4/ | TrustManager + EcologyMatcher + SwarmDiscovery |
| `KnowledgeGraph` | Windows genome/ | 语义搜索 + 关系引擎 |
| `IQTraining` | Windows genome/iq_training/ | 能力训练框架 |

### 3.2 Edge Gateway 新增

| 模块 | 来源 | 说明 |
|------|------|------|
| `EventBus Local` | Windows eventbus/ | 轻量本地事件总线 |
| `EdgeSelfHealing` | Windows layer2/ | 边缘侧自修复 |
| `LocalConditionEngine` | Windows condition_engine | 本地条件规则 |

### 3.3 新增 Domain 职责

**GenomeDomain**（新增）
- `genome_import()` — 导入外部知识到云端
- `genome_version()` — 技能版本化管理
- `genome_heritage()` — 知识传承（从历史版本继承）
- `genome_pattern_mine()` — 从行为日志中挖掘模式
- `genome_semantic_search()` — 知识图谱语义搜索

**AdaptiveDomain**（新增）
- `rule_evaluate()` — 条件引擎评估
- `strategy_switch()` — 策略切换
- `system_heal()` — 系统自愈
- `health_diagnosis()` — 健康诊断

**SwarmDomain**（新增）
- `trust_evaluate()` — 节点信任评估
- `ecology_match()` — 生态位匹配
- `swarm_discover()` — 集群发现

---

## 4. 目录结构（v2.0）

```
Desktop/ClawShell/
├── ClawShell Cloud/
│   ├── cloud-hub/
│   │   └── src/
│   │       ├── hub.py                      # CloudHub（事件驱动中枢）
│   │       ├── auth.py                     # JWT 认证
│   │       ├── domains/
│   │       │   ├── __init__.py
│   │       │   ├── kanban.py              # ✅ 已有
│   │       │   ├── skill.py               # ✅ 已有
│   │       │   ├── node.py                # ✅ 已有
│   │       │   ├── memory.py              # ✅ 已有
│   │       │   ├── workflow.py            # ✅ 已有
│   │       │   ├── genome.py              # 🆕 新增（from Windows）
│   │       │   ├── adaptive.py            # 🆕 新增（from Windows layer2）
│   │       │   └── swarm.py               # 🆕 新增（from Windows layer4）
│   │       ├── event_store/               # ✅ 已有
│   │       ├── pubsub/                     # ✅ 已有
│   │       ├── state/                      # ✅ 已有
│   │       ├── storage/                    # ✅ 已有
│   │       └── adaptive/                   # 🆕 新增
│   │           ├── condition_engine.py     # 条件引擎
│   │           ├── strategy_switcher.py    # 策略切换
│   │           └── self_healing.py        # 自愈系统
│   ├── skill-registry/
│   ├── kanban-mcp/
│   └── vault-oss/
│
├── ClawShell Local/
│   ├── edge-gateway/
│   │   └── src/
│   │       ├── gateway.py                  # ✅ 已有
│   │       ├── sync_engine.py              # ✅ 已有
│   │       ├── adapters/                   # ✅ 已有
│   │       │   ├── base.py
│   │       │   ├── hermes_adapter.py
│   │       │   ├── openclaw_adapter.py
│   │       │   └── wukong_adapter.py
│   │       ├── eventbus/                   # 🆕 轻量本地事件总线
│   │       │   ├── core.py
│   │       │   ├── schema.py
│   │       │   ├── subscriber.py
│   │       │   ├── publisher.py
│   │       │   ├── dead_letter_queue.py
│   │       │   └── condition_engine.py
│   │       └── local_cache/                # 🆕 本地缓存层
│   └── scripts/
│
├── docs/
│   ├── ARCHITECTURE.md                    # 🆕 综合架构文档
│   └── MIGRATION.md                       # 🆕 从v1.3迁移指南
│
├── PLANNING.md
├── SYNTHESIS.md                          # 本文档
└── README.md
```

---

## 5. 实施计划

### Phase A：Cloud Hub 核心增强（A1-A3）

**A1: GenomeDomain**（知识传承体系）
- 从 Windows `lib/core/genome/` 提取核心模块
- `GenomeManager` → `GenomeDomain`
- `KnowledgeGraph` → `KnowledgeGraphDomain`
- 保留 skill versioning + heritage 逻辑，去除与 Hermes 强耦合部分

**A2: AdaptiveDomain**（自适应引擎）
- ConditionEngine → `AdaptiveDomain.rule_evaluate()`
- StrategySwitcher → `AdaptiveDomain.strategy_switch()`
- SelfHealing → `AdaptiveDomain.system_heal()`

**A3: SwarmDomain**（集群协调）
- TrustManager + EcologyMatcher + SwarmDiscovery → `SwarmDomain`
- 支持多端注册、信任评估、生态位匹配

### Phase B：Edge Gateway 轻量化移植（B1-B2）

**B1: Local EventBus**
- 提取 Windows EventBus 核心逻辑
- 去掉 N8N/Memos/Obsidian 直接依赖（由 Adapter 层代理）
- 加 DeadLetterQueue + EventTracer
- 与 SyncEngine 集成：本地事件 → 同步队列

**B2: Edge SelfHealing**
- 从 `lib/layer2/self_healing.py` 提取核心逻辑
- 适配边缘侧（无 cron 守护，依赖 SyncEngine 心跳）

### Phase C：Hub 原生能力增强（C1）

**C1: WorkflowEngine Saga 补偿**
- 在 MacOS 已有 `workflow.py` 基础上
- 补全 `compensation` 字段的自动执行逻辑
- 补全 `saga_step` 类型的原子事务支持

### Phase D：文档与部署（D1-D3）

**D1: 综合 ARCHITECTURE.md**
- 将 Windows `ARCHITECTURE.md` 的架构哲学和全景图融入
- 更新 MacOS 的架构文档

**D2: v2.0 部署脚本**
- 合并 Windows install.sh 的智能检测逻辑
- 保留 MacOS Docker Compose 部署

**D3: Git commit + tag v2.0**

---

## 6. 优先级与依赖

```
A1 (GenomeDomain)
    ↓
A2 (AdaptiveDomain)      A3 (SwarmDomain)
    ↓                         ↓
C1 (Workflow Saga) ─────→ D1 (文档)
    ↓
B1 (Local EventBus)
    ↓
B2 (Edge SelfHealing)
    ↓
D2 (部署脚本)
    ↓
D3 (v2.0 tag)
```

---

## 7. 关键设计决策

### 决策 1：Windows 模块的移植方式
不直接复制文件，而是**提取核心逻辑重写**：
- 去除与悟空/Hermes EventBus 的直接耦合
- 保留条件引擎 DSL、策略切换、信任评估的核心算法
- 适配 MacOS 的 `EventStore` 持久化模型

### 决策 2：Domain 数量控制
当前 MacOS 有 5 个 Domain（Kanban/Skill/Node/Memory/Workflow）。新增 3 个（Genome/Adaptive/Swarm），总数 8 个，属于合理范围。

### 决策 3：Edge EventBus 的定位
Edge 的 EventBus 是**轻量版**，不复制 Windows 的全部 9 个 eventbus 文件。只保留：
- Core pub/sub（50-100 行）
- ConditionEngine（本地规则）
- DeadLetterQueue（异常处理）
- EventTracer（调试）

### 决策 4：Saga 补偿 vs N8N
不引入 N8N（Windows 的耦合问题）。Saga 补偿逻辑直接在 WorkflowEngine 内实现，用 `compensation` 字段声明式定义补偿步骤。

---

## 8. 验收标准

- [ ] CloudHub 导入新增 Domain 无循环依赖
- [ ] GenomeDomain.genome_import → EventStore 持久化
- [ ] AdaptiveDomain.rule_evaluate → JSON 条件规则求值
- [ ] SwarmDomain.trust_evaluate → 节点信任分计算
- [ ] Edge Local EventBus → SyncEngine 集成（本地事件同步到云端）
- [ ] WorkflowEngine saga 补偿自动执行
- [ ] 所有新增模块单元测试通过
- [ ] Docker Compose 部署成功
- [ ] Git tag v2.0 推送完成
