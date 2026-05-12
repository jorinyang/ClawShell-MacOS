"""PluginDomain — YAML plugin system (inspired by ClawShell-Deep PluginManager)

内置插件（与 Deep 保持一致）：
  - n8n: N8N 工作流自动化
  - memos: MemOS 语义记忆
  - comfyui: ComfyUI 图像生成
  - ollama: Ollama 本地模型
  - openclaw_skills: OpenClaw 技能库

每个插件由 plugin.yaml 描述：
  name: Plugin Name
  domain: skill|tool|api|model|service
  provider: publisher-name
  endpoint: http://... (optional)
  health_check:
    type: http|tcp|exec
    endpoint: http://...
"""
import asyncio
import importlib.util
import subprocess
from pathlib import Path
from typing import Any, Optional
import yaml
from loguru import logger

# ── 兼容导入 ────────────────────────────────────────────────────────────────
try:
    from shared.models import Plugin, PluginRegistry, HealthStatus, CapabilityDomain
except ImportError:
    from ..shared.models import Plugin, PluginRegistry, HealthStatus, CapabilityDomain


BUILTIN_PLUGINS = {
    "n8n": {
        "name": "N8N",
        "domain": CapabilityDomain.SERVICE,
        "provider": "n8n",
        "endpoint": "http://localhost:5678",
        "health_check": {"type": "http", "endpoint": "http://localhost:5678"},
    },
    "memos": {
        "name": "MemOS",
        "domain": CapabilityDomain.SERVICE,
        "provider": "memos",
        "endpoint": "https://api.memos.cloud/v1",
        "health_check": {"type": "http", "endpoint": "https://api.memos.cloud/v1/health"},
    },
    "comfyui": {
        "name": "ComfyUI",
        "domain": CapabilityDomain.TOOL,
        "provider": "comfyui",
        "endpoint": "http://localhost:8188",
        "health_check": {"type": "http", "endpoint": "http://localhost:8188/system_stats"},
    },
    "ollama": {
        "name": "Ollama",
        "domain": CapabilityDomain.MODEL,
        "provider": "ollama",
        "endpoint": "http://localhost:11434",
        "health_check": {"type": "http", "endpoint": "http://localhost:11434/api/tags"},
    },
    "openclaw_skills": {
        "name": "OpenClaw Skills",
        "domain": CapabilityDomain.SKILL,
        "provider": "openclaw",
        "endpoint": None,
        "health_check": None,
    },
}


class PluginDomain:
    """Plugin registry with YAML discovery and health checking.

    插件目录结构（与 Deep 兼容）：
        plugins/
          n8n/
            plugin.yaml
          memos/
            plugin.yaml
    """

    def __init__(
        self,
        plugins_dir: str | Path = "plugins",
        node_id: str = "hub-01",
        health_check_interval: int = 60,
    ):
        self.node_id = node_id
        self.plugins_dir = Path(plugins_dir)
        self.health_check_interval = health_check_interval
        self._plugins: dict[str, Plugin] = {}
        self._registry = PluginRegistry(node_id=node_id)
        self._health_tasks: dict[str, asyncio.Task] = {}
        logger.info(f"PluginDomain initialized (dir={self.plugins_dir})")

    # ── Discovery ──────────────────────────────────────────────────────────────

    async def discover(self) -> list[Plugin]:
        """Discover plugins from BUILTIN list + plugins_dir YAML files."""
        discovered: list[Plugin] = []

        # Built-in plugins
        for pid, info in BUILTIN_PLUGINS.items():
            p = Plugin(
                plugin_id=pid,
                name=info["name"],
                domain=info["domain"],
                provider=info["provider"],
                endpoint=info.get("endpoint"),
                enabled=True,
                health_status=HealthStatus.UNKNOWN,
            )
            discovered.append(p)
            self._plugins[pid] = p

        # External YAML plugins
        if self.plugins_dir.exists():
            for entry in self.plugins_dir.iterdir():
                if not entry.is_dir():
                    continue
                yaml_path = entry / "plugin.yaml"
                if not yaml_path.exists():
                    continue
                try:
                    cfg = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
                    if not cfg:
                        continue
                    p = Plugin(
                        plugin_id=entry.name,
                        name=cfg.get("name", entry.name),
                        domain=CapabilityDomain(cfg.get("domain", "tool")),
                        provider=cfg.get("provider", "custom"),
                        endpoint=cfg.get("endpoint"),
                        enabled=cfg.get("enabled", True),
                        health_status=HealthStatus.UNKNOWN,
                    )
                    discovered.append(p)
                    self._plugins[p.plugin_id] = p
                    logger.debug(f"Loaded plugin: {p.plugin_id}")
                except Exception:
                    logger.exception(f"Plugin load error: {entry}")

        self._registry.plugins = list(self._plugins.values())
        logger.info(f"PluginDomain discovered {len(discovered)} plugins")
        return discovered

    # ── Health Check ────────────────────────────────────────────────────────────

    async def health_check(self) -> dict[str, HealthStatus]:
        """Run health checks for all enabled plugins."""
        results: dict[str, HealthStatus] = {}
        for pid, plugin in self._plugins.items():
            if not plugin.enabled:
                results[pid] = HealthStatus.UNKNOWN
                continue
            status = await self._check_plugin(plugin)
            plugin.health_status = status
            results[pid] = status
        return results

    async def _check_plugin(self, plugin: Plugin) -> HealthStatus:
        """Check a single plugin's health."""
        info = BUILTIN_PLUGINS.get(plugin.plugin_id, {})
        hc_cfg = info.get("health_check")

        if hc_cfg is None:
            return HealthStatus.HEALTHY  # 无需检查，默认健康

        htype = hc_cfg.get("type", "http")
        endpoint = hc_cfg.get("endpoint", plugin.endpoint)

        if not endpoint:
            return HealthStatus.UNKNOWN

        try:
            if htype == "http":
                return await self._http_health_check(endpoint)
            elif htype == "tcp":
                return await self._tcp_health_check(endpoint)
        except Exception:
            logger.warning(f"Health check failed for {plugin.plugin_id}: {plugin.endpoint}")
        return HealthStatus.UNKNOWN

    async def _http_health_check(self, url: str) -> HealthStatus:
        try:
            import aiohttp
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=3)) as r:
                    if r.status < 500:
                        return HealthStatus.HEALTHY
                    return HealthStatus.DEGRADED
        except asyncio.TimeoutError:
            return HealthStatus.DEGRADED
        except Exception:
            return HealthStatus.CRITICAL

    async def _tcp_health_check(self, addr: str) -> HealthStatus:
        """Check TCP connectivity. addr format: host:port"""
        try:
            host, port_str = addr.rsplit(":", 1)
            port = int(port_str)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=3
            )
            writer.close()
            await writer.wait_closed()
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.CRITICAL

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self):
        """Start periodic health check background task."""
        await self.discover()
        logger.info("PluginDomain started")

    async def stop(self):
        """Stop all health check tasks."""
        for task in self._health_tasks.values():
            task.cancel()
        self._health_tasks.clear()
        logger.info("PluginDomain stopped")

    # ── Sync wrappers ───────────────────────────────────────────────────────────

    def sync_discover(self) -> list[Plugin]:
        import inspect
        return list(self._plugins.values())

    def sync_health_check(self) -> dict[str, HealthStatus]:
        results = {}
        for pid, plugin in self._plugins.items():
            results[pid] = plugin.health_status
        return results
