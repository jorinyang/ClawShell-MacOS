"""
ClawShell Cloud Hub — Review Domain (自动复盘模块)
监听 task.done 事件 → 提取任务上下文 → 生成结构化复盘 → 存入 OSS
"""
import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from ..event_store.schema import Event, Topic
from ..pubsub.manager import PubSubManager
from ..storage import OssStore

logger = logging.getLogger("cloud-hub.domain.review")

REVIEWS_PREFIX = "reviews/"
TASKS_PREFIX = "kanban/tasks/"


# ─── Review Schema ────────────────────────────────────────────────────────────

@dataclass
class ReviewResult:
    """结构化复盘结果"""
    review_id   : str = ""
    task_id     : str = ""
    node_id     : str = ""
    timestamp   : str = ""

    # 复盘维度
    summary     : str = ""      # 任务摘要
    outcome     : str = ""      # 执行结果（success/failed/partial）
    duration    : float = 0.0   # 耗时（秒）

    # 结构化分析
    what_went_well : List[str] = field(default_factory=list)
    what_went_poorly: List[str] = field(default_factory=list)
    root_causes    : List[str] = field(default_factory=list)
    improvements   : List[str] = field(default_factory=list)

    # 元数据
    tags         : List[str] = field(default_factory=list)
    llm_generated: bool = False
    error        : str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "ReviewResult":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ─── Review Domain ───────────────────────────────────────────────────────────

class ReviewDomain:
    """
    自动复盘域。

    工作流程：
    1. 订阅 task.done 事件（通过 PubSubManager）
    2. 提取任务上下文（从 OSS 加载 kanban task 数据）
    3. 生成结构化复盘（LLM 生成 + fallback 规则引擎）
    4. 存储复盘结果到 OSS

    LLM fallback: 当 LLM 服务不可用时，使用启发式规则生成复盘。
    """

    def __init__(self, store: OssStore, pubsub: Optional[PubSubManager] = None):
        self.store = store
        self.pubsub = pubsub
        self._listener_task: Optional[asyncio.Task] = None
        self._running = False

        # LLM 配置
        self._llm_endpoint = os.environ.get("LLM_ENDPOINT", "")
        self._llm_model = os.environ.get("LLM_MODEL", "gpt-4")

        # 启发式复盘用的关键词规则
        self._success_keywords = ["完成", "成功", "done", "ok", "✓", "completed"]
        self._failure_keywords = ["失败", "错误", "error", "failed", "✗"]

    # ─── Public API ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """启动事件监听（后台协程）"""
        if self._running:
            return
        self._running = True
        self._listener_task = asyncio.create_task(self._event_listener())
        logger.info("ReviewDomain started — listening for task.done events")

    async def stop(self) -> None:
        """停止事件监听"""
        self._running = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        logger.info("ReviewDomain stopped")

    async def review_generate(self, params: dict) -> dict:
        """
        review_generate — 手动触发复盘生成（支持指定 task_id）
        params: {task_id, node_id, result}
        """
        task_id = params.get("task_id", "")
        node_id = params.get("node_id", "")
        result = params.get("result", "")

        if not task_id:
            return {"success": False, "error": "task_id required"}

        # 加载任务上下文
        context = await self._load_task_context(task_id)
        context["result"] = result
        context["node_id"] = node_id

        # 生成复盘
        review = await self._generate_review(task_id, node_id, context)

        # 存储
        await self._save_review(review)

        return {"success": True, "review": review.to_dict()}

    async def review_get(self, params: dict) -> dict:
        """review_get — 获取复盘结果"""
        task_id = params.get("task_id", "")
        if not task_id:
            return {"success": False, "error": "task_id required"}

        raw = await self.store.load(f"{REVIEWS_PREFIX}{task_id}.json")
        if raw is None:
            return {"success": False, "error": f"review for task {task_id} not found"}

        try:
            data = json.loads(raw)
            review = ReviewResult.from_dict(data)
            return {"success": True, "review": review.to_dict()}
        except Exception as e:
            return {"success": False, "error": f"parse error: {e}"}

    async def review_list(self, params: dict) -> dict:
        """review_list — 列出最近复盘"""
        try:
            keys = await self.store.list_all(REVIEWS_PREFIX)
            reviews = []
            for k in keys:
                if not k.endswith(".json"):
                    continue
                raw = await self.store.load(k)
                if raw:
                    try:
                        reviews.append(json.loads(raw))
                    except Exception:
                        continue
            reviews.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
            limit = params.get("limit", 20)
            return {"success": True, "reviews": reviews[:limit], "total": len(reviews)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ─── Event Listener ────────────────────────────────────────────────────────

    async def _event_listener(self) -> None:
        """
        后台协程：轮询新事件，匹配 task.done 则触发自动复盘。
        通过 PubSubManager 的内部状态检测新事件。
        """
        logger.info("ReviewDomain event listener started")

        # 记录已处理的 event_id，避免重复
        processed_ids: set = set()
        check_interval = 1.0  # 秒

        while self._running:
            try:
                # 从事件存储中获取最近的 task.done 事件
                events = await self._fetch_recent_task_done_events()

                for event in events:
                    if event.event_id in processed_ids:
                        continue
                    processed_ids.add(event.event_id)

                    # 避免集合无限增长
                    if len(processed_ids) > 1000:
                        processed_ids = set(list(processed_ids)[-500:])

                    await self._on_task_done(event)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"ReviewDomain listener error: {e}")

            await asyncio.sleep(check_interval)

        logger.info("ReviewDomain event listener stopped")

    async def _fetch_recent_task_done_events(self) -> List[Event]:
        """
        获取最近的 task.done 事件。
        策略：从 OSS 事件存储目录中列出最近的事件文件，
        过滤出 task.done 类型。
        """
        try:
            # 事件存储在 event_store/ 目录
            event_keys = await self.store.list_all("event_store/")
            task_done_events = []

            for key in event_keys[-100:]:  # 只检查最近100个
                if not key.endswith(".json"):
                    continue
                raw = await self.store.load(key)
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                    if data.get("topic") == Topic.TASK_DONE:
                        event = Event.from_dict(data)
                        task_done_events.append(event)
                except Exception:
                    continue

            return task_done_events
        except Exception as e:
            logger.debug(f"_fetch_recent_task_done_events: {e}")
            return []

    async def _on_task_done(self, event: Event) -> None:
        """处理单个 task.done 事件"""
        payload = event.payload
        task_id = payload.get("task_id", "")
        node_id = payload.get("node_id", "")
        result = payload.get("result", "")

        if not task_id:
            return

        logger.info(f"ReviewDomain: processing task.done for task_id={task_id}")

        # 加载完整上下文
        context = await self._load_task_context(task_id)
        context["result"] = result
        context["node_id"] = node_id
        context["event_timestamp"] = event.timestamp

        # 生成复盘
        review = await self._generate_review(task_id, node_id, context)

        # 存储
        await self._save_review(review)

        logger.info(f"ReviewDomain: review generated for task_id={task_id}, "
                    f"review_id={review.review_id}")

    # ─── Task Context ──────────────────────────────────────────────────────────

    async def _load_task_context(self, task_id: str) -> Dict[str, Any]:
        """从 OSS 加载任务的完整上下文"""
        context: Dict[str, Any] = {"task_id": task_id}

        # 加载 kanban task 数据
        task_raw = await self.store.load(f"{TASKS_PREFIX}{task_id}.json")
        if task_raw:
            try:
                task_data = json.loads(task_raw)
                context["task"] = task_data
                context["title"] = task_data.get("title", "")
                context["description"] = task_data.get("description", "")
                context["status"] = task_data.get("status", "")
                context["priority"] = task_data.get("priority", "")
                context["board_id"] = task_data.get("board_id", "")
                context["created_at"] = task_data.get("created_at", "")
                context["updated_at"] = task_data.get("updated_at", "")
            except Exception as e:
                logger.warning(f"Failed to parse task {task_id}: {e}")

        # 加载工作流执行上下文（如果有关联）
        wf_exec_key = f"workflows/executions/"
        try:
            exec_keys = await self.store.list_all(wf_exec_key)
            for ek in exec_keys:
                raw = await self.store.load(ek)
                if raw:
                    try:
                        exec_data = json.loads(raw)
                        if exec_data.get("trigger_params", {}).get("task_id") == task_id:
                            context["workflow_execution"] = exec_data
                    except Exception:
                        continue
        except Exception:
            pass

        return context

    # ─── Review Generation ────────────────────────────────────────────────────

    async def _generate_review(
        self, task_id: str, node_id: str, context: Dict[str, Any]
    ) -> ReviewResult:
        """
        生成结构化复盘。
        优先使用 LLM；LLM 失败时 fallback 到启发式规则引擎。
        """
        review = ReviewResult(
            review_id=f"rev_{uuid.uuid4().hex[:16]}",
            task_id=task_id,
            node_id=node_id,
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime()),
        )

        # 提取耗时
        created_at = context.get("created_at", "")
        updated_at = context.get("updated_at", context.get("event_timestamp", ""))
        if created_at and updated_at:
            try:
                fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
                t_start = time.mktime(time.strptime(created_at[:26] + "Z", fmt))
                t_end = time.mktime(time.strptime(updated_at[:26] + "Z", fmt))
                review.duration = round(t_end - t_start, 2)
            except Exception:
                pass

        # 提取结果标签
        result = context.get("result", "")
        review.outcome = self._classify_outcome(result)

        # LLM 生成
        if self._llm_endpoint:
            try:
                llm_review = await self._llm_generate_review(task_id, context)
                if llm_review:
                    review.summary = llm_review.get("summary", review.summary)
                    review.what_went_well = llm_review.get("what_went_well", [])
                    review.what_went_poorly = llm_review.get("what_went_poorly", [])
                    review.root_causes = llm_review.get("root_causes", [])
                    review.improvements = llm_review.get("improvements", [])
                    review.tags = llm_review.get("tags", [])
                    review.llm_generated = True
                    logger.info(f"ReviewDomain: LLM review generated for {task_id}")
                    return review
            except Exception as e:
                logger.warning(f"ReviewDomain: LLM generation failed, using fallback: {e}")
                review.error = str(e)

        # Fallback: 启发式规则引擎
        self._fallback_generate_review(review, context, result)
        return review

    async def _llm_generate_review(
        self, task_id: str, context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """调用 LLM API 生成复盘"""
        import aiohttp

        prompt = self._build_review_prompt(context)

        payload = {
            "model": self._llm_model,
            "messages": [
                {"role": "system", "content": (
                    "你是一个专业的任务复盘助手。请根据以下任务上下文，"
                    "生成结构化复盘报告，以 JSON 格式返回，包含字段："
                    "summary（摘要）, what_went_well（做好的地方，数组）, "
                    "what_went_poorly（需改进的地方，数组）, "
                    "root_causes（根本原因分析，数组）, "
                    "improvements（改进建议，数组）, tags（标签，数组）。"
                    "只返回 JSON，不要其他内容。"
                )},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.3,
            "max_tokens": 800,
        }

        headers = {"Content-Type": "application/json"}
        timeout = aiohttp.ClientTimeout(total=15)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                self._llm_endpoint, json=payload, headers=headers
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(f"LLM API error {resp.status}: {text}")
                    return None

                result = await resp.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")

                # 尝试解析 JSON
                try:
                    # 提取 ```json ... ``` 块
                    import re
                    json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", content, re.DOTALL)
                    if json_match:
                        content = json_match.group(1)
                    else:
                        # 尝试直接解析
                        json_match = re.search(r"(\{.*\})", content, re.DOTALL)
                        if json_match:
                            content = json_match.group(1)

                    return json.loads(content)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse LLM JSON response: {e}, content={content[:200]}")
                    return None

    def _build_review_prompt(self, context: Dict[str, Any]) -> str:
        """构建 LLM 复盘 prompt"""
        task = context.get("task", {})
        title = task.get("title", context.get("title", ""))
        description = task.get("description", context.get("description", ""))
        result = context.get("result", "")
        node_id = context.get("node_id", "")
        duration = context.get("duration", 0)

        prompt = f"""任务复盘请求

任务标题：{title}
任务描述：{description}
执行节点：{node_id}
执行结果：{result}
任务耗时：{duration}秒

请分析以上任务执行情况，生成结构化复盘报告。"""
        return prompt

    def _fallback_generate_review(
        self,
        review: ReviewResult,
        context: Dict[str, Any],
        result: str,
    ) -> None:
        """
        Fallback 启发式复盘生成（当 LLM 不可用时）。
        基于关键词和任务元数据生成结构化复盘。
        """
        task = context.get("task", {})
        title = task.get("title", context.get("title", ""))
        outcome = review.outcome

        # 摘要
        if outcome == "success":
            review.summary = f"任务「{title}」已成功完成。执行节点：{context.get('node_id', 'unknown')}。"
        elif outcome == "failed":
            review.summary = f"任务「{title}」执行失败。结果：{result[:100]}"
        else:
            review.summary = f"任务「{title}」执行完成，结果：{result[:100]}"

        # 基于 outcome 分类
        if outcome == "success":
            review.what_went_well = [
                "任务按预期完成",
                f"执行节点 {context.get('node_id', '')} 正常响应",
            ]
            if review.duration > 0:
                review.what_went_well.append(f"耗时 {review.duration} 秒")
            review.what_went_poorly = ["可进一步优化执行效率"]
            review.improvements = ["可添加更多执行细节记录", "建议记录中间步骤指标"]
            review.tags = ["success", "completed"]
        elif outcome == "failed":
            review.what_went_poorly = [
                f"执行失败：{result[:80]}",
                "需要分析失败原因",
            ]
            review.what_went_well = ["节点检测到异常并报告"]
            review.root_causes = [
                "可能原因：输入参数异常",
                "可能原因：执行环境问题",
                "可能原因：依赖服务不可用",
            ]
            review.improvements = [
                "建议增加重试机制",
                "建议添加更详细的错误日志",
                "建议增加前置条件检查",
            ]
            review.tags = ["failed", "needs-analysis"]
        else:
            review.what_went_well = ["任务执行完毕"]
            review.what_went_poorly = ["结果不明确，需要人工确认"]
            review.improvements = ["建议明确成功/失败标准"]
            review.tags = ["partial", "needs-review"]

        # 从标题/描述推断标签
        title_lower = title.lower()
        if "bug" in title_lower or "修复" in title:
            review.tags.append("bug-fix")
        if "部署" in title or "deploy" in title_lower:
            review.tags.append("deployment")
        if "测试" in title or "test" in title_lower:
            review.tags.append("testing")

    def _classify_outcome(self, result: str) -> str:
        """根据 result 字符串分类执行结果"""
        if not result:
            return "unknown"

        result_lower = result.lower()

        # 显式失败
        for kw in self._failure_keywords:
            if kw in result_lower:
                return "failed"

        # 显式成功
        for kw in self._success_keywords:
            if kw in result_lower:
                return "success"

        # 隐式判断
        if "error" in result_lower or "exception" in result_lower:
            return "failed"

        return "partial"

    # ─── Storage ──────────────────────────────────────────────────────────────

    async def _save_review(self, review: ReviewResult) -> None:
        """保存复盘结果到 OSS"""
        key = f"{REVIEWS_PREFIX}{review.task_id}.json"
        await self.store.save(key, json.dumps(review.to_dict(), ensure_ascii=False))
        logger.debug(f"Review saved: {key}")

    # ─── Utility ─────────────────────────────────────────────────────────────

    def _now_ts(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime())
