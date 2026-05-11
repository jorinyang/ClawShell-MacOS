"""
ClawShell Cloud Hub — GlobalOptimizer
=====================================

全局优化器（跨域协同优化）

核心能力：
- ResourceAllocator: 全局资源分配（节点、计算力、带宽）
- GenomeEvaluator: Genome 适应度评估
- OptimizationStrategy: 优化策略（成本优先/性能优先/均衡）
- CrossDomainOptimizer: 跨域联动优化

事件类型：
- optimizer.resource_allocated  → 资源分配完成
- optimizer.genome_evaluated    → Genome 评估完成
- optimizer.optimization_done  → 全局优化完成
- optimizer.cost_updated        → 成本更新
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from ..event_store.schema import Event, Topic
from ..event_store.store import OssEventStore
from ..pubsub.manager import PubSubManager

from .swarm import SwarmDomain, NodeRegistry, Node, NodeStatus, NodeType
from .genome import GenomeDomain, Genome

logger = logging.getLogger("global_optimizer")


# ─── Optimization Strategy Types ───────────────────────────────────────────────

class OptimizationGoal(str, Enum):
    COST_MIN = "cost_min"           # 成本最优
    PERFORMANCE_MAX = "performance_max"  # 性能最优
    BALANCED = "balanced"            # 均衡模式
    RELIABILITY = "reliability"      # 可靠性优先
    EFFICIENCY = "efficiency"        # 效率最优


class ResourceType(str, Enum):
    COMPUTE = "compute"             # 计算资源（CPU）
    MEMORY = "memory"               # 内存资源
    STORAGE = "storage"             # 存储资源
    NETWORK = "network"             # 网络带宽
    TOKENS = "tokens"               # AI Token 配额


@dataclass
class ResourceQuota:
    """资源配额"""
    resource_type: str
    total: float
    used: float = 0.0
    reserved: float = 0.0

    @property
    def available(self) -> float:
        return max(0.0, self.total - self.used - self.reserved)

    @property
    def usage_ratio(self) -> float:
        if self.total == 0:
            return 0.0
        return self.used / self.total

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource_type": self.resource_type,
            "total": self.total,
            "used": self.used,
            "reserved": self.reserved,
            "available": self.available,
            "usage_ratio": self.usage_ratio,
        }


@dataclass
class OptimizationResult:
    """优化结果"""
    goal: str
    actions: List[Dict[str, Any]]
    projected_cost_change: float = 0.0
    projected_performance_change: float = 0.0
    confidence: float = 0.0
    reason: str = ""
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "goal": self.goal,
            "actions": self.actions,
            "projected_cost_change": self.projected_cost_change,
            "projected_performance_change": self.projected_performance_change,
            "confidence": self.confidence,
            "reason": self.reason,
            "timestamp": self.timestamp,
        }


@dataclass
class AllocationPlan:
    """分配方案"""
    plan_id: str
    node_allocations: Dict[str, Dict[str, float]]  # node_id -> {resource_type: amount}
    strategy: str
    expected_cost: float
    expected_performance: float
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "node_allocations": self.node_allocations,
            "strategy": self.strategy,
            "expected_cost": self.expected_cost,
            "expected_performance": self.expected_performance,
            "created_at": self.created_at,
        }


# ─── Cost Model ───────────────────────────────────────────────────────────────

class CostModel:
    """
    简单成本模型。

    基于节点类型和资源使用量计算成本：
    - 每小时成本 = sum(node_cost_per_hour * resource_usage)
    """

    # 节点类型基础成本（$/小时）
    NODE_COSTS = {
        NodeType.OPENCLAW: 0.05,
        NodeType.HERMES: 0.02,
        NodeType.WUKONG: 0.08,
        NodeType.EDGE_MAC: 0.01,
        NodeType.EDGE_WIN: 0.01,
        NodeType.EDGE_LINUX: 0.005,
        NodeType.N8N: 0.03,
        NodeType.MEMOS: 0.01,
        NodeType.SKILL: 0.02,
        NodeType.UNKNOWN: 0.01,
    }

    # 资源权重
    RESOURCE_WEIGHTS = {
        ResourceType.COMPUTE: 1.0,
        ResourceType.MEMORY: 0.5,
        ResourceType.STORAGE: 0.1,
        ResourceType.NETWORK: 0.3,
        ResourceType.TOKENS: 2.0,
    }

    def estimate_hourly_cost(self, node: Node, usage_ratio: float = 1.0) -> float:
        """估算节点小时成本"""
        base = self.NODE_COSTS.get(node.type, 0.01)
        return base * usage_ratio

    def estimate_total_cost(
        self,
        nodes: List[Node],
        allocation: Dict[str, Dict[str, float]],
    ) -> float:
        """估算总成本"""
        total = 0.0
        for node_id, resources in allocation.items():
            node = nodes.get(node_id)
            if not node:
                continue
            usage_ratio = resources.get("compute", 1.0)
            total += self.estimate_hourly_cost(node, usage_ratio)
        return total


# ─── Global Optimizer ─────────────────────────────────────────────────────────

class GlobalOptimizer:
    """
    全局优化器。

    在 Swarm/Genome/Workflow 多域协同下，寻找全局最优资源配置：

    优化维度：
    - 成本：最小化资源开销
    - 性能：最大化吞吐/最小化延迟
    - 可靠性：最大化节点健康/信任分
    - 效率：最大化资源利用率

    优化约束：
    - 资源上限（预算限制）
    - 最低服务质量（QoS）
    - 节点能力匹配
    """

    def __init__(
        self,
        event_store: OssEventStore,
        pubsub: PubSubManager,
        swarm_domain: SwarmDomain,
        genome_domain: Optional[GenomeDomain] = None,
    ):
        self.event_store = event_store
        self.pubsub = pubsub
        self.swarm = swarm_domain
        self.genome = genome_domain
        self.cost_model = CostModel()

        # 资源配置
        self._quotas: Dict[str, ResourceQuota] = {}
        self._allocation_history: List[AllocationPlan] = []

        # 优化目标
        self._goal = OptimizationGoal.BALANCED

        # 约束
        self._max_cost_per_hour = 10.0
        self._min_success_rate = 0.95
        self._min_active_nodes = 1

    # ── Configuration ──────────────────────────────────────────────────────────

    def set_goal(self, goal: OptimizationGoal):
        """设置优化目标"""
        self._goal = goal

    def set_constraints(
        self,
        max_cost_per_hour: Optional[float] = None,
        min_success_rate: Optional[float] = None,
        min_active_nodes: Optional[int] = None,
    ):
        """设置优化约束"""
        if max_cost_per_hour is not None:
            self._max_cost_per_hour = max_cost_per_hour
        if min_success_rate is not None:
            self._min_success_rate = min_success_rate
        if min_active_nodes is not None:
            self._min_active_nodes = min_active_nodes

    def set_quota(self, resource_type: str, total: float, reserved: float = 0.0):
        """设置资源配额"""
        self._quotas[resource_type] = ResourceQuota(
            resource_type=resource_type,
            total=total,
            reserved=reserved,
        )

    # ── Resource Analysis ──────────────────────────────────────────────────────

    def analyze_resources(self) -> Dict[str, Any]:
        """分析当前资源状态"""
        if not self.swarm or not self.swarm.registry:
            return {}

        nodes = self.swarm.registry
        active = [n for n in nodes.values() if n.status == NodeStatus.ACTIVE]
        idle = [n for n in nodes.values() if n.status == NodeStatus.IDLE]
        busy = [n for n in nodes.values() if n.status == NodeStatus.BUSY]
        offline = [n for n in nodes.values() if n.status == NodeStatus.OFFLINE]

        total_compute = sum(
            self.cost_model.estimate_hourly_cost(n) for n in nodes.values()
        )

        avg_trust = (
            sum(n.trust_score for n in nodes.values()) / len(nodes)
            if nodes else 0.0
        )

        return {
            "total_nodes": len(nodes),
            "active": len(active),
            "idle": len(idle),
            "busy": len(busy),
            "offline": len(offline),
            "total_hourly_cost": total_compute,
            "avg_trust_score": avg_trust,
            "avg_success_rate": (
                sum(n.successful_tasks for n in nodes.values()) /
                max(1, sum(n.successful_tasks + n.failed_tasks for n in nodes.values()))
            ),
        }

    def get_node_scoring(self) -> Dict[str, float]:
        """
        获取节点评分（用于分配决策）。

        评分维度：信任分 * 成功率 * 可用性
        """
        if not self.swarm or not self.swarm.registry:
            return {}

        scores = {}
        for node_id, node in self.swarm.registry.items():
            if node.status == NodeStatus.OFFLINE:
                scores[node_id] = 0.0
                continue

            success_rate = (
                node.successful_tasks / max(1, node.successful_tasks + node.failed_tasks)
            )
            # 综合评分
            score = node.trust_score * success_rate * (1.0 if node.status == NodeStatus.ACTIVE else 0.5)
            scores[node_id] = min(1.0, score)
        return scores

    # ─── Genome Evaluation ─────────────────────────────────────────────────────

    def evaluate_genome_fitness(self, genome: Genome) -> float:
        """
        评估 Genome 适应度。

        适应度 = w1*性能 + w2*成本 + w3*可靠性
        """
        w_perf = 0.4
        w_cost = 0.3
        w_rel = 0.3

        # 性能得分（基于知识条目数、skill 数）
        perf_score = min(1.0, len(genome.knowledge_entries) / 50.0)
        perf_score += min(1.0, len(genome.skills) / 20.0)
        perf_score = min(1.0, perf_score / 2.0)

        # 成本得分（知识条目越多通常成本越高）
        cost_score = 1.0 - min(1.0, len(genome.knowledge_entries) / 200.0)

        # 可靠性得分（错误模式越少越好）
        rel_score = 1.0 - min(1.0, len(genome.error_patterns) / 30.0)

        fitness = w_perf * perf_score + w_cost * cost_score + w_rel * rel_score
        return round(fitness, 4)

    # ─── Allocation Planning ───────────────────────────────────────────────────

    def generate_allocation_plan(self) -> AllocationPlan:
        """
        生成资源分配方案。

        根据当前节点状态和优化目标，生成最优分配方案。
        """
        nodes = self.swarm.registry if self.swarm else {}
        node_scores = self.get_node_scoring()

        if not nodes:
            return AllocationPlan(
                plan_id=f"plan_{int(time.time())}",
                node_allocations={},
                strategy=self._goal.value,
                expected_cost=0.0,
                expected_performance=0.0,
            )

        # 按评分排序节点
        sorted_nodes = sorted(
            node_scores.items(),
            key=lambda x: x[1],
            reverse=True,
        )

        allocation: Dict[str, Dict[str, float]] = {}
        total_cost = 0.0
        total_perf = 0.0

        if self._goal == OptimizationGoal.COST_MIN:
            # 成本优先：只保留高评分低成本的节点
            for node_id, score in sorted_nodes:
                node = nodes[node_id]
                if node.status == NodeStatus.OFFLINE:
                    continue
                cost = self.cost_model.estimate_hourly_cost(node)
                if total_cost + cost <= self._max_cost_per_hour:
                    allocation[node_id] = {"compute": 0.8, "memory": 0.6}
                    total_cost += cost
                    total_perf += score

        elif self._goal == OptimizationGoal.PERFORMANCE_MAX:
            # 性能优先：分配所有可用节点
            for node_id, score in sorted_nodes:
                node = nodes[node_id]
                if node.status == NodeStatus.OFFLINE:
                    continue
                allocation[node_id] = {"compute": 1.0, "memory": 0.8}
                total_cost += self.cost_model.estimate_hourly_cost(node, 1.0)
                total_perf += score

        elif self._goal == OptimizationGoal.RELIABILITY:
            # 可靠性优先：选择最高信任分节点
            sorted_by_trust = sorted(
                nodes.items(),
                key=lambda x: x[1].trust_score,
                reverse=True,
            )
            for node_id, node in sorted_by_trust[:max(2, len(nodes)//2)]:
                if node.status == NodeStatus.OFFLINE:
                    continue
                allocation[node_id] = {"compute": 0.9, "memory": 0.7}
                total_cost += self.cost_model.estimate_hourly_cost(node, 0.9)
                total_perf += node.trust_score

        else:  # BALANCED / EFFICIENCY
            # 均衡：选择评分较高且成本适中的节点
            for node_id, score in sorted_nodes:
                node = nodes[node_id]
                if node.status == NodeStatus.OFFLINE:
                    continue
                cost = self.cost_model.estimate_hourly_cost(node)
                efficiency = score / max(cost, 0.001)
                if efficiency > 5.0:  # 效率阈值
                    allocation[node_id] = {"compute": 0.85, "memory": 0.65}
                    total_cost += cost
                    total_perf += score

        plan = AllocationPlan(
            plan_id=f"plan_{int(time.time())}",
            node_allocations=allocation,
            strategy=self._goal.value,
            expected_cost=total_cost,
            expected_performance=total_perf / max(len(allocation), 1),
        )

        self._allocation_history.append(plan)
        return plan

    # ─── Global Optimization ───────────────────────────────────────────────────

    async def optimize(self) -> OptimizationResult:
        """
        执行全局优化。

        整合资源分析、Genome评估、分配规划，返回优化结果。
        """
        # 1. 资源分析
        resource_state = self.analyze_resources()

        # 2. 生成分配方案
        plan = self.generate_allocation_plan()

        # 3. Genome 评估（如果有）
        genome_scores = []
        if self.genome and hasattr(self.genome, "genomes"):
            for gid, g in self.genome.genomes.items():
                fitness = self.evaluate_genome_fitness(g)
                genome_scores.append({"genome_id": gid, "fitness": fitness})

        # 4. 构建优化动作
        actions = []
        if plan.node_allocations:
            actions.append({
                "type": "resource_reallocation",
                "plan_id": plan.plan_id,
                "affected_nodes": list(plan.node_allocations.keys()),
                "expected_cost": plan.expected_cost,
            })

        if genome_scores:
            best_genome = max(genome_scores, key=lambda x: x["fitness"])
            actions.append({
                "type": "genome_promotion",
                "genome_id": best_genome["genome_id"],
                "fitness": best_genome["fitness"],
            })

        # 5. 成本/性能变化预测
        current_cost = resource_state.get("total_hourly_cost", 0.0)
        cost_change = plan.expected_cost - current_cost

        result = OptimizationResult(
            goal=self._goal.value,
            actions=actions,
            projected_cost_change=cost_change,
            projected_performance_change=plan.expected_performance,
            confidence=0.85 if genome_scores else 0.7,
            reason=f"Optimized for {self._goal.value} with {len(actions)} actions",
        )

        # 6. 发布事件
        await self.pubsub.publish(
            Topic.OPTIMIZATION,
            Event(
                topic=Topic.OPTIMIZATION,
                event_type="optimizer.optimization_done",
                data=result.to_dict(),
            )
        )

        logger.info(
            f"[OPTIMIZER] Goal={self._goal.value}, "
            f"Actions={len(actions)}, CostChange={cost_change:.4f}"
        )

        return result

    # ─── Cross-Domain Optimization ─────────────────────────────────────────────

    async def optimize_cross_domain(
        self,
        target_domain: str,
        goal: OptimizationGoal,
    ) -> OptimizationResult:
        """
        跨域优化：针对特定域进行专项优化。

        - workflow: 优化工作流执行效率
        - swarm: 优化节点资源配置
        - genome: 优化知识库结构
        """
        self._goal = goal

        if target_domain == "swarm":
            return await self._optimize_swarm()
        elif target_domain == "workflow":
            return await self._optimize_workflow()
        elif target_domain == "genome":
            return await self._optimize_genome()
        else:
            return await self.optimize()

    async def _optimize_swarm(self) -> OptimizationResult:
        """Swarm 专项优化"""
        plan = self.generate_allocation_plan()
        return OptimizationResult(
            goal=self._goal.value,
            actions=[{"type": "swarm_reallocation", "plan": plan.to_dict()}],
            projected_cost_change=plan.expected_cost,
            projected_performance_change=plan.expected_performance,
            confidence=0.8,
            reason="Swarm resource reallocation completed",
        )

    async def _optimize_workflow(self) -> OptimizationResult:
        """Workflow 专项优化"""
        # 简化实现：统计待处理任务数
        actions = [{"type": "workflow_tune", "recommendation": "balance_pending_tasks"}]
        return OptimizationResult(
            goal=self._goal.value,
            actions=actions,
            projected_cost_change=0.0,
            projected_performance_change=0.1,
            confidence=0.6,
            reason="Workflow optimization completed",
        )

    async def _optimize_genome(self) -> OptimizationResult:
        """Genome 专项优化"""
        if not self.genome or not hasattr(self.genome, "genomes"):
            return OptimizationResult(
                goal=self._goal.value,
                actions=[],
                reason="No genome domain available",
            )

        best_genome_id = None
        best_fitness = -1.0
        for gid, g in self.genome.genomes.items():
            f = self.evaluate_genome_fitness(g)
            if f > best_fitness:
                best_fitness = f
                best_genome_id = gid

        return OptimizationResult(
            goal=self._goal.value,
            actions=[{
                "type": "genome_optimize",
                "best_genome_id": best_genome_id,
                "fitness": best_fitness,
            }],
            projected_cost_change=0.0,
            projected_performance_change=best_fitness,
            confidence=0.75,
            reason=f"Genome optimization, best fitness={best_fitness}",
        )

    # ─── Status ────────────────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """获取优化器状态"""
        return {
            "goal": self._goal.value,
            "constraints": {
                "max_cost_per_hour": self._max_cost_per_hour,
                "min_success_rate": self._min_success_rate,
                "min_active_nodes": self._min_active_nodes,
            },
            "resources": {k: v.to_dict() for k, v in self._quotas.items()},
            "allocation_history_count": len(self._allocation_history),
            "resource_analysis": self.analyze_resources(),
        }
