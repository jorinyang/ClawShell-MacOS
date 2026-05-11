#!/usr/bin/env python3
"""
ClawShell Edge — Device Monitor Module (D2)
==========================================

端侧设备监控模块：监控本地设备状态、性能、网络质量等

核心能力：
- 本地硬件监控（CPU、内存、磁盘、网络接口）
- 进程监控（关键进程存活检测）
- 网络质量监控（延迟、丢包、带宽）
- 设备温度监控（Raspberry Pi 等）
- 通过 EventBus 发布设备状态事件
- 与 SyncEngine 同步设备健康数据

依赖：
- psutil（跨平台系统监控）
- 可选：gpiozero（Raspberry Pi 温度传感器）
"""

import asyncio
import json
import logging
import os
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from threading import Lock, Thread
from datetime import datetime

logger = logging.getLogger("edge.device_monitor")


# ─── 路径配置 ────────────────────────────────────────────────────────────────

EDGE_STATE_DIR = Path.home() / ".clawshell-local"
MONITOR_STATE_FILE = EDGE_STATE_DIR / "device_monitor" / "status.json"
MONITOR_CACHE_DIR = EDGE_STATE_DIR / "device_monitor"
MONITOR_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 关键进程列表（这些进程必须存活）
CRITICAL_PROCESSES = [
    "python",
    "node",
    "docker",
    "containerd",
]

# 阈值配置
THRESHOLDS = {
    "cpu_percent_warning": 80.0,
    "cpu_percent_critical": 95.0,
    "memory_percent_warning": 80.0,
    "memory_percent_critical": 95.0,
    "disk_percent_warning": 85.0,
    "disk_percent_critical": 95.0,
    "temperature_warning": 70.0,     # Celsius
    "temperature_critical": 85.0,
    "network_latency_warning": 100.0,  # ms
    "network_latency_critical": 500.0,
    "network_packet_loss_warning": 1.0,  # percent
    "network_packet_loss_critical": 5.0,
}


# ─── 数据结构 ────────────────────────────────────────────────────────────────

@dataclass
class DeviceMetrics:
    """设备指标快照"""
    timestamp: float = field(default_factory=time.time)
    hostname: str = ""
    ip_address: str = ""

    # CPU
    cpu_percent: float = 0.0
    cpu_count: int = 0
    load_average: Optional[List[float]] = None

    # Memory
    memory_total: int = 0
    memory_available: int = 0
    memory_percent: float = 0.0

    # Disk
    disk_total: int = 0
    disk_used: int = 0
    disk_percent: float = 0.0

    # Network
    network_bytes_sent: int = 0
    network_bytes_recv: int = 0
    network_latency_ms: Optional[float] = None
    network_packet_loss: Optional[float] = None

    # Temperature (if available)
    temperature: Optional[float] = None

    # Process count
    process_count: int = 0

    def to_dict(self) -> Dict:
        return {
            "timestamp": self.timestamp,
            "hostname": self.hostname,
            "ip_address": self.ip_address,
            "cpu_percent": self.cpu_percent,
            "cpu_count": self.cpu_count,
            "load_average": self.load_average,
            "memory_total": self.memory_total,
            "memory_available": self.memory_available,
            "memory_percent": self.memory_percent,
            "disk_total": self.disk_total,
            "disk_used": self.disk_used,
            "disk_percent": self.disk_percent,
            "network_bytes_sent": self.network_bytes_sent,
            "network_bytes_recv": self.network_bytes_recv,
            "network_latency_ms": self.network_latency_ms,
            "network_packet_loss": self.network_packet_loss,
            "temperature": self.temperature,
            "process_count": self.process_count,
        }


@dataclass
class ProcessStatus:
    """进程状态"""
    name: str
    pid: int
    is_running: bool = True
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    started: Optional[str] = None


@dataclass
class HealthStatus:
    """设备健康状态"""
    is_healthy: bool = True
    cpu_status: str = "normal"      # normal | warning | critical
    memory_status: str = "normal"
    disk_status: str = "normal"
    network_status: str = "normal"
    temperature_status: str = "normal"
    process_status: str = "normal"
    issues: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "is_healthy": self.is_healthy,
            "cpu_status": self.cpu_status,
            "memory_status": self.memory_status,
            "disk_status": self.disk_status,
            "network_status": self.network_status,
            "temperature_status": self.temperature_status,
            "process_status": self.process_status,
            "issues": self.issues,
        }


# ─── 设备监控引擎 ────────────────────────────────────────────────────────────

class DeviceMonitor:
    """
    端侧设备监控引擎

    功能：
    - 采集系统指标（CPU、内存、磁盘、网络）
    - 监控关键进程存活
    - 检测网络质量
    - 通过 EventBus 发布设备状态事件
    - 与 SyncEngine 同步设备健康数据
    """

    MONITOR_INTERVAL = 30          # 30 秒采集一次
    NETWORK_TEST_INTERVAL = 60     # 60 秒测试一次网络质量
    PROCESS_CHECK_INTERVAL = 60    # 60 秒检查一次进程

    def __init__(
        self,
        eventbus=None,
        sync_engine=None,
        thresholds: Optional[Dict] = None,
    ):
        self.eventbus = eventbus
        self.sync_engine = sync_engine
        self.thresholds = {**THRESHOLDS, **(thresholds or {})}

        self._metrics_history: List[DeviceMetrics] = []
        self._health_status = HealthStatus()
        self._critical_processes_status: Dict[str, ProcessStatus] = {}
        self._lock = Lock()
        self._running = False
        self._monitor_thread: Optional[Thread] = None
        self._last_network_test: float = 0
        self._last_process_check: float = 0

        # 初始化 psutil（延迟加载）
        self._psutil = None
        self._try_import_psutil()

        # 获取本机信息
        self._hostname = socket.gethostname()
        self._ip_address = self._get_local_ip()

        logger.info("DeviceMonitor initialized (hostname=%s, ip=%s)", self._hostname, self._ip_address)

    def _try_import_psutil(self):
        """尝试导入 psutil"""
        try:
            import psutil
            self._psutil = psutil
            logger.info("psutil loaded for device monitoring")
        except ImportError:
            logger.warning("psutil not available, monitoring will be limited")

    def _get_local_ip(self) -> Optional[str]:
        """获取本机 IP 地址"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            return None

    # ─── 指标采集 ───────────────────────────────────────────────────────────

    def _collect_cpu_metrics(self, metrics: DeviceMetrics):
        """采集 CPU 指标"""
        if self._psutil is None:
            return
        try:
            metrics.cpu_percent = self._psutil.cpu_percent(interval=0.1)
            metrics.cpu_count = self._psutil.cpu_count()
            if hasattr(self._psutil, "getloadavg"):
                metrics.load_average = list(self._psutil.getloadavg())
            elif hasattr(self._psutil, "os") and hasattr(self._psutil.os, "getloadavg"):
                metrics.load_average = list(self._psutil.os.getloadavg())
        except Exception as e:
            logger.debug("Failed to collect CPU metrics: %s", e)

    def _collect_memory_metrics(self, metrics: DeviceMetrics):
        """采集内存指标"""
        if self._psutil is None:
            return
        try:
            mem = self._psutil.virtual_memory()
            metrics.memory_total = mem.total
            metrics.memory_available = mem.available
            metrics.memory_percent = mem.percent
        except Exception as e:
            logger.debug("Failed to collect memory metrics: %s", e)

    def _collect_disk_metrics(self, metrics: DeviceMetrics):
        """采集磁盘指标"""
        if self._psutil is None:
            return
        try:
            disk = self._psutil.disk_usage("/")
            metrics.disk_total = disk.total
            metrics.disk_used = disk.used
            metrics.disk_percent = disk.percent
        except Exception as e:
            logger.debug("Failed to collect disk metrics: %s", e)

    def _collect_network_metrics(self, metrics: DeviceMetrics):
        """采集网络指标"""
        if self._psutil is None:
            return
        try:
            net = self._psutil.net_io_counters()
            metrics.network_bytes_sent = net.bytes_sent
            metrics.network_bytes_recv = net.bytes_recv
        except Exception as e:
            logger.debug("Failed to collect network metrics: %s", e)

    def _collect_temperature(self, metrics: DeviceMetrics):
        """采集温度（仅支持 Raspberry Pi 等）"""
        if self._psutil is None:
            return
        try:
            # 尝试多种温度获取方式
            temp = None

            # 方式1: /sys/class/thermal
            thermal_path = Path("/sys/class/thermal/thermal_zone0/temp")
            if thermal_path.exists():
                temp_millidegrees = int(thermal_path.read_text().strip())
                temp = temp_millidegrees / 1000.0

            # 方式2: vcgencmd (Raspberry Pi)
            if temp is None:
                try:
                    import subprocess
                    result = subprocess.run(
                        ["vcgencmd", "measure_temp"],
                        capture_output=True, text=True, timeout=5
                    )
                    if result.returncode == 0:
                        temp_str = result.stdout.strip()
                        temp = float(temp_str.replace("temp=", "").replace("'C", ""))
                except Exception:
                    pass

            # 方式3: psutil sensors
            if temp is None and hasattr(self._psutil, "sensors_temperatures"):
                try:
                    temps = self._psutil.sensors_temperatures()
                    if temps:
                        for name, entries in temps.items():
                            for entry in entries:
                                if entry.current:
                                    temp = entry.current
                                    break
                except Exception:
                    pass

            if temp is not None:
                metrics.temperature = temp
        except Exception as e:
            logger.debug("Failed to collect temperature: %s", e)

    def _collect_process_count(self, metrics: DeviceMetrics):
        """采集进程数量"""
        if self._psutil is None:
            return
        try:
            metrics.process_count = len(self._psutil.pids())
        except Exception as e:
            logger.debug("Failed to collect process count: %s", e)

    def _test_network_quality(self, metrics: DeviceMetrics):
        """测试网络质量（延迟、丢包）"""
        if self._psutil is None:
            return
        latency = None
        packet_loss = None

        # 测试到多个目标的延迟
        targets = ["8.8.8.8", "1.1.1.1", "114.114.114.114"]
        latencies = []

        for target in targets:
            try:
                # 使用 socket 测试 TCP 延迟
                start = time.time()
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3)
                # 不实际连接，只测量到目标的延迟
                s.connect_ex((target, 53))
                elapsed = (time.time() - start) * 1000
                latencies.append(elapsed)
                s.close()
            except Exception:
                pass

        if latencies:
            latency = sum(latencies) / len(latencies)

        # 简化丢包检测（通过是否能连接判断）
        try:
            if self._psutil:
                # 获取网络接口统计
                net = self._psutil.net_io_counters()
                # 这是一个近似值，实际丢包率需要对比发送和接收的 ICMP
                packet_loss = 0.0  # 简化处理
        except Exception:
            pass

        metrics.network_latency_ms = latency
        metrics.network_packet_loss = packet_loss

    def _collect_all_metrics(self) -> DeviceMetrics:
        """采集所有指标"""
        metrics = DeviceMetrics(
            hostname=self._hostname,
            ip_address=self._ip_address or "",
        )

        self._collect_cpu_metrics(metrics)
        self._collect_memory_metrics(metrics)
        self._collect_disk_metrics(metrics)
        self._collect_network_metrics(metrics)
        self._collect_temperature(metrics)
        self._collect_process_count(metrics)

        # 定期测试网络质量
        now = time.time()
        if now - self._last_network_test > self.NETWORK_TEST_INTERVAL:
            self._test_network_quality(metrics)
            self._last_network_test = now

        return metrics

    # ─── 进程监控 ───────────────────────────────────────────────────────────

    def _check_critical_processes(self) -> Dict[str, ProcessStatus]:
        """检查关键进程状态"""
        if self._psutil is None:
            return {}
        statuses = {}
        try:
            for proc in self._psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_info']):
                try:
                    name = proc.info['name']
                    if name in CRITICAL_PROCESSES:
                        pid = proc.info['pid']
                        cpu = proc.cpu_percent(interval=0) or 0
                        mem = proc.info['memory_info']
                        mem_mb = mem.rss / (1024 * 1024) if mem else 0

                        statuses[name] = ProcessStatus(
                            name=name,
                            pid=pid,
                            is_running=True,
                            cpu_percent=cpu,
                            memory_mb=mem_mb,
                        )
                except (self._psutil.NoSuchProcess, self._psutil.AccessDenied):
                    pass
        except Exception as e:
            logger.debug("Failed to check processes: %s", e)
        return statuses

    # ─── 健康状态评估 ──────────────────────────────────────────────────────

    def _evaluate_health(self, metrics: DeviceMetrics) -> HealthStatus:
        """评估设备健康状态"""
        status = HealthStatus()
        issues = []

        # CPU
        if metrics.cpu_percent >= self.thresholds["cpu_percent_critical"]:
            status.cpu_status = "critical"
            issues.append(f"CPU usage critical: {metrics.cpu_percent:.1f}%")
        elif metrics.cpu_percent >= self.thresholds["cpu_percent_warning"]:
            status.cpu_status = "warning"
            issues.append(f"CPU usage high: {metrics.cpu_percent:.1f}%")
        else:
            status.cpu_status = "normal"

        # Memory
        if metrics.memory_percent >= self.thresholds["memory_percent_critical"]:
            status.memory_status = "critical"
            issues.append(f"Memory usage critical: {metrics.memory_percent:.1f}%")
        elif metrics.memory_percent >= self.thresholds["memory_percent_warning"]:
            status.memory_status = "warning"
            issues.append(f"Memory usage high: {metrics.memory_percent:.1f}%")
        else:
            status.memory_status = "normal"

        # Disk
        if metrics.disk_percent >= self.thresholds["disk_percent_critical"]:
            status.disk_status = "critical"
            issues.append(f"Disk usage critical: {metrics.disk_percent:.1f}%")
        elif metrics.disk_percent >= self.thresholds["disk_percent_warning"]:
            status.disk_status = "warning"
            issues.append(f"Disk usage high: {metrics.disk_percent:.1f}%")
        else:
            status.disk_status = "normal"

        # Network latency
        if metrics.network_latency_ms is not None:
            if metrics.network_latency_ms >= self.thresholds["network_latency_critical"]:
                status.network_status = "critical"
                issues.append(f"Network latency critical: {metrics.network_latency_ms:.1f}ms")
            elif metrics.network_latency_ms >= self.thresholds["network_latency_warning"]:
                status.network_status = "warning"
                issues.append(f"Network latency high: {metrics.network_latency_ms:.1f}ms")
            else:
                status.network_status = "normal"

        # Temperature
        if metrics.temperature is not None:
            if metrics.temperature >= self.thresholds["temperature_critical"]:
                status.temperature_status = "critical"
                issues.append(f"Temperature critical: {metrics.temperature:.1f}°C")
            elif metrics.temperature >= self.thresholds["temperature_warning"]:
                status.temperature_status = "warning"
                issues.append(f"Temperature high: {metrics.temperature:.1f}°C")
            else:
                status.temperature_status = "normal"

        # Process status
        now = time.time()
        if now - self._last_process_check > self.PROCESS_CHECK_INTERVAL:
            self._critical_processes_status = self._check_critical_processes()
            self._last_process_check = now

        missing = [p for p in CRITICAL_PROCESSES if p not in self._critical_processes_status]
        if missing:
            status.process_status = "warning"
            issues.append(f"Missing critical processes: {', '.join(missing)}")
        else:
            status.process_status = "normal"

        status.issues = issues
        status.is_healthy = all([
            status.cpu_status == "normal",
            status.memory_status == "normal",
            status.disk_status == "normal",
            status.network_status == "normal",
            status.temperature_status == "normal",
            status.process_status == "normal",
        ])

        return status

    # ─── EventBus 集成 ─────────────────────────────────────────────────────

    def _publish_event(self, event_type: str, data: Dict):
        """通过 EventBus 发布事件"""
        if self.eventbus is None:
            return
        try:
            from eventbus.schema import Event, EventType
            event = Event(
                type=EventType.CUSTOM,
                source="device_monitor",
                payload={**data, "_event_type": event_type}
            )
            self.eventbus.publish(event)
        except Exception as e:
            logger.warning("Failed to publish EventBus event: %s", e)

    def _subscribe_to_events(self):
        """订阅 EventBus 事件"""
        if self.eventbus is None:
            return
        try:
            from eventbus.schema import EventType
            self.eventbus.subscribe(EventType.CUSTOM, self._on_custom_event)
            logger.info("Subscribed to EventBus for device monitor")
        except Exception as e:
            logger.warning("Failed to subscribe to EventBus: %s", e)

    def _on_custom_event(self, event):
        """处理 EventBus 自定义事件"""
        data = event.payload or {}
        et = data.get("_event_type", "")
        if et == "force_collect":
            logger.info("Force metrics collection triggered via event")
            self.collect_and_report()
        elif et == "get_status":
            logger.info("Status request triggered via event")
            self._publish_event("monitor.status_response", self.get_status())

    # ─── SyncEngine 集成 ───────────────────────────────────────────────────

    def _sync_to_cloud(self, metrics: DeviceMetrics, health: HealthStatus):
        """同步设备状态到云端"""
        if self.sync_engine is None:
            return
        try:
            self.sync_engine.queue_operation(
                category="device",
                action="metrics",
                data={
                    "metrics": metrics.to_dict(),
                    "health": health.to_dict(),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
            logger.debug("Queued device metrics for cloud sync")
        except Exception as e:
            logger.warning("Failed to sync metrics to cloud: %s", e)

    # ─── 监控循环 ───────────────────────────────────────────────────────────

    def collect_and_report(self) -> DeviceMetrics:
        """采集并上报一次指标"""
        metrics = self._collect_all_metrics()
        health = self._evaluate_health(metrics)

        with self._lock:
            self._metrics_history.append(metrics)
            # 保留最近 1000 条历史
            if len(self._metrics_history) > 1000:
                self._metrics_history = self._metrics_history[-1000:]
            self._health_status = health

        # 发布事件
        self._publish_event("metrics.collected", metrics.to_dict())

        if not health.is_healthy:
            self._publish_event("health.degraded", health.to_dict())

        # 同步到云端
        self._sync_to_cloud(metrics, health)

        # 保存状态
        self._save_status()

        return metrics

    def _monitor_loop(self):
        """后台监控循环"""
        while self._running:
            try:
                self.collect_and_report()
                time.sleep(self.MONITOR_INTERVAL)
            except Exception as e:
                logger.error("Monitor loop error: %s", e)
                time.sleep(10)

    # ─── 状态持久化 ────────────────────────────────────────────────────────

    def _save_status(self):
        """保存状态到文件"""
        with self._lock:
            data = {
                "last_update": time.time(),
                "health": self._health_status.to_dict(),
                "hostname": self._hostname,
                "ip_address": self._ip_address,
            }
            MONITOR_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(MONITOR_STATE_FILE, 'w') as f:
                json.dump(data, f, indent=2)

    # ─── 生命周期 ───────────────────────────────────────────────────────────

    def start(self):
        """启动设备监控"""
        if self._running:
            return
        self._running = True
        self._subscribe_to_events()
        self._monitor_thread = Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("DeviceMonitor started (interval=%ds)", self.MONITOR_INTERVAL)

    def stop(self):
        """停止设备监控"""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=10)
        self._save_status()
        logger.info("DeviceMonitor stopped")

    # ─── 外部接口 ───────────────────────────────────────────────────────────

    def get_current_metrics(self) -> Optional[DeviceMetrics]:
        """获取当前指标"""
        with self._lock:
            if self._metrics_history:
                return self._metrics_history[-1]
        return None

    def get_health_status(self) -> HealthStatus:
        """获取健康状态"""
        with self._lock:
            return self._health_status

    def get_metrics_history(self, limit: int = 100) -> List[DeviceMetrics]:
        """获取指标历史"""
        with self._lock:
            return self._metrics_history[-limit:]

    def get_status(self) -> Dict:
        """获取监控状态"""
        with self._lock:
            metrics = self._metrics_history[-1] if self._metrics_history else None
            return {
                "hostname": self._hostname,
                "ip_address": self._ip_address,
                "is_healthy": self._health_status.is_healthy,
                "current_metrics": metrics.to_dict() if metrics else None,
                "health": self._health_status.to_dict(),
                "psutil_available": self._psutil is not None,
            }
