#!/usr/bin/env python3
"""CloudHub 验收测试 — Mock OssStore + 完整 Domain 测试"""
import asyncio, json, sys
sys.path.insert(0, '/Users/yangyang/Desktop/ClawShell/ClawShell Cloud/cloud-hub')

# OssEventStore API (confirmed):
#   append(topic, source, payload)      -> Event
#   replay_by_seq(since_seq, limit=1000) -> List[Event]
#   load(key)   -> json string (empty string = miss)
#   save(key, json_string)

class InMemoryStore:
    """完整 Mock — 对齐 OssEventStore API"""
    def __init__(self):
        self._events = []
        self._vault = {}
        self._files = {}
        self._seq = 0

    async def initialize(self):
        pass

    async def append(self, event):
        # OssEventStore.append(event: Event) — 单参数
        self._seq += 1
        event.seq = self._seq
        self._events.append(event)
        return event

    async def replay_by_seq(self, since_seq=0, limit=1000):
        evts = [e for e in self._events if e.seq >= since_seq]
        if limit:
            evts = evts[-limit:]
        return evts

    async def vault_upload(self, key: str, content: str):
        self._vault[key] = content

    async def vault_download(self, key: str) -> str:
        return self._vault.get(key, "")

    def vault_list(self, prefix=''):
        return [k for k in self._vault if k.startswith(prefix)]

    async def load(self, key: str) -> str:
        return self._files.get(key, "")

    async def save(self, key: str, data: str):
        self._files[key] = data


class InMemoryPubSub:
    def __init__(self):
        self._subscribers = {}

    async def subscribe(self, topics, callback):
        if isinstance(topics, str):
            topics = [topics]
        for t in topics:
            self._subscribers.setdefault(t, []).append(callback)

    async def publish(self, event):
        # sync mock — PubSubManager.publish 实际是 async 的
        pass

    async def broadcast(self, msg):
        pass
        pass


results = []
def check(name, ok, detail=''):
    status = "PASS" if ok else "FAIL"
    results.append((status, name, detail))
    print(f"[{status}] {name}" + (f" - {detail}" if detail else ""))


async def main():
    import src.hub as hub_module

    # 手动构造 hub 实例
    hub_instance = object.__new__(hub_module.CloudHub)
    hub_instance.store = InMemoryStore()
    hub_instance.pubsub = InMemoryPubSub()
    hub_instance.seq_gen = hub_module.SequenceGenerator(hub_instance.store)
    hub_instance.event_store = hub_module.OssEventStore(hub_instance.store, hub_instance.seq_gen)
    hub_instance.state = hub_module.StateAggregator()

    # 所有 Domain
    from src.domains import (
        MemoryDomain, KanbanDomain, SkillDomain, NodeDomain,
        WorkflowDomain, GenomeDomain, AdaptiveDomain, SwarmDomain,
    )
    hub_instance.node_domain = NodeDomain(hub_instance.store)
    hub_instance.kanban_domain = KanbanDomain(hub_instance.store)
    hub_instance.skill_domain = SkillDomain(hub_instance.store)
    hub_instance.memory_domain = MemoryDomain(hub_instance.store)
    hub_instance.workflow_domain = WorkflowDomain(hub_instance.store, hub_instance.pubsub)
    hub_instance.genome_domain = GenomeDomain(hub_instance.store, hub_instance.pubsub)
    hub_instance.adaptive_domain = AdaptiveDomain(hub_instance.store, hub_instance.pubsub)
    hub_instance.swarm_domain = SwarmDomain(hub_instance.store, hub_instance.pubsub)

    print("Hub ready. Running tests...\n")

    # ── Genome Domain ─────────────────────────────────────────────────────────
    r = await hub_instance.genome_domain.genome_get({"agent_type": "shared"})
    check("Genome: genome_get", r.get("success"))

    r = await hub_instance.genome_domain.knowledge_add({"key": "test_key", "value": "test_value", "category": "test"})
    check("Genome: knowledge_add", r.get("success"))

    r = await hub_instance.genome_domain.knowledge_query({"query": "test"})
    check("Genome: knowledge_query fuzzy", r.get("success"), f"found={r.get('count', 0)}")

    r = await hub_instance.genome_domain.error_pattern_add({
        "error_type": "connection_timeout", "description": "conn failed",
        "solution": "retry with backoff", "tags": ["network"]
    })
    check("Genome: error_pattern_add", r.get("success"))

    r = await hub_instance.genome_domain.error_resolve({"error_type": "connection_timeout"})
    check("Genome: error_resolve", r.get("success") and r.get("solution") is not None)

    r = await hub_instance.genome_domain.skill_update({"skill_name": "coding", "performance": 0.9})
    check("Genome: skill_update", r.get("success"))

    r = await hub_instance.genome_domain.evolve({"changes": ["added genome"]})
    check("Genome: evolve", r.get("success"), f"v{r.get('new_version')}")

    r = await hub_instance.genome_domain.genome_stats({"agent_type": "shared"})
    check("Genome: genome_stats", r.get("success"), f"v{r.get('stats',{}).get('version')}")

    r = await hub_instance.genome_domain.heritage({"heritage_type": "restart", "notes": "test"})
    check("Genome: heritage", r.get("success"))

    # ── Adaptive Domain ───────────────────────────────────────────────────────
    r = await hub_instance.adaptive_domain.health_check({
        "metrics": {"cpu_usage": 95, "memory_usage": 90, "error_rate": 0.1}
    })
    check("Adaptive: health_check unhealthy", r.get("healthy") == False, f"score={r.get('score'):.2f}")

    r = await hub_instance.adaptive_domain.health_check({
        "metrics": {"cpu_usage": 30, "memory_usage": 40, "error_rate": 0.001}
    })
    check("Adaptive: health_check healthy", r.get("healthy") == True, f"score={r.get('score'):.2f}")

    r = await hub_instance.adaptive_domain.rule_evaluate({
        "condition": {"type": "threshold", "target_metric": "cpu_usage", "comparison": ">", "threshold": 50.0},
        "context": {"cpu_usage": 80}
    })
    check("Adaptive: rule_evaluate satisfied", r.get("satisfied") == True, f"score={r.get('score'):.2f}")

    r = await hub_instance.adaptive_domain.rule_evaluate({
        "condition": {"type": "threshold", "target_metric": "cpu_usage", "comparison": ">", "threshold": 50.0},
        "context": {"cpu_usage": 30}
    })
    check("Adaptive: rule_evaluate unsatisfied", r.get("satisfied") == False)

    r = await hub_instance.adaptive_domain.strategy_switch({"mode": "manual", "strategy_name": "economy"})
    check("Adaptive: strategy_switch manual", r.get("success"))

    r = await hub_instance.adaptive_domain.system_heal({"metrics": {"cpu_usage": 95, "error_rate": 0.1}})
    check("Adaptive: system_heal", r.get("success"), f"action={r.get('action')}")

    r = await hub_instance.adaptive_domain.get_current_strategy({})
    check("Adaptive: get_current_strategy", r.get("success"), r.get("strategy"))

    # ── Swarm Domain ──────────────────────────────────────────────────────────
    r = await hub_instance.swarm_domain.node_register({
        "name": "test-node", "type": "hermes",
        "endpoint": "ws://localhost:9000",
        "capabilities": ["coding", "planning"],
    })
    node_id = r.get("node", {}).get("id", "")
    check("Swarm: node_register", r.get("success"), f"id={node_id[:20]}")

    r = await hub_instance.swarm_domain.node_heartbeat({"node_id": node_id, "status": "active"})
    check("Swarm: node_heartbeat", r.get("success"))

    r = await hub_instance.swarm_domain.list_nodes({"active_only": False})
    check("Swarm: list_nodes", r.get("success"), f"count={r.get('count')}")

    r = await hub_instance.swarm_domain.trust_evaluate({"node_id": node_id, "event_type": "success"})
    check("Swarm: trust_evaluate success", r.get("success"), f"score={r.get('trust_score', 0):.3f}")

    r = await hub_instance.swarm_domain.ecology_match({"roles": ["coordinator", "executor", "observer"]})
    check("Swarm: ecology_match", r.get("success"), f"unassigned={r.get('unassigned',[])}")

    r = await hub_instance.swarm_domain.node_unregister({"node_id": node_id})
    check("Swarm: node_unregister", r.get("success"))

    # ── Workflow Domain ────────────────────────────────────────────────────────
    r = await hub_instance.workflow_domain.workflow_define({
        "workflow_id": "wf1",
        "title": "Test Workflow",
        "version": "1.0.0",
        "steps": [
            {"step_id": "step1", "step_type": "skill", "params": {"skill": "test_skill"}, "name": "Step 1"},
            {"step_id": "step2", "step_type": "skill", "params": {"skill": "test_skill2"}, "name": "Step 2"},
        ]
    })
    check("Workflow: define", r.get("success"), r.get("error", ""))

    r = await hub_instance.workflow_domain.workflow_execute({"workflow_id": "wf1", "params": {}})
    exec_id = r.get("execution_id", "")
    check("Workflow: execute", r.get("success"), f"exec_id={exec_id[:16]}")

    # Cancel BEFORE waiting — execution completes fast (0.05s), so cancel must race it
    r = await hub_instance.workflow_domain.workflow_cancel({"execution_id": exec_id})
    check("Workflow: cancel", r.get("success"), r.get("status", r.get("error", "")))

    # ── Summary ────────────────────────────────────────────────────────────────
    passed = sum(1 for s, _, _ in results if s == "PASS")
    failed = sum(1 for s, _, _ in results if s == "FAIL")
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{passed+failed} passed")
    if failed:
        print("\nFAILURES:")
        for s, name, detail in results:
            if s == "FAIL":
                print(f"  FAIL: {name} - {detail}")
    else:
        print("All tests passed!")

asyncio.run(main())
