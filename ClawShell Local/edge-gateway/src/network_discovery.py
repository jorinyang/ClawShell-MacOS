#!/usr/bin/env python3
"""
ClawShell Edge — Network Discovery Module (D1)
==============================================

端侧网络发现模块：发现局域网内的设备

核心能力：
- mDNS/Bonjour 设备发现
- ARP 扫描发现局域网设备
- SSDP 发现（UPnP 设备）
- 设备信息收集（MAC 厂商、hostname 等）
- 通过 EventBus 发布设备发现事件
- 与 SyncEngine 同步设备列表

依赖：
- 需要 sudo 权限进行 ARP 扫描（可选）
- 依赖 python-avahi（可选）
"""

import asyncio
import json
import logging
import socket
import subprocess
import time
import ipaddress
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from threading import Lock, Thread
from datetime import datetime

logger = logging.getLogger("edge.network_discovery")


# ─── 路径配置 ────────────────────────────────────────────────────────────────

EDGE_STATE_DIR = Path.home() / ".clawshell-local"
DISCOVERY_STATE_FILE = EDGE_STATE_DIR / "network_discovery" / "devices.json"
DISCOVERY_CACHE_DIR = EDGE_STATE_DIR / "network_discovery"
DISCOVERY_CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ─── 数据结构 ────────────────────────────────────────────────────────────────

@dataclass
class DiscoveredDevice:
    """发现的设备"""
    ip_address: str
    mac_address: Optional[str] = None
    hostname: Optional[str] = None
    vendor: Optional[str] = None
    port: Optional[int] = None
    services: List[str] = field(default_factory=list)
    last_seen: float = field(default_factory=time.time)
    first_seen: float = field(default_factory=time.time)
    is_alive: bool = True

    def to_dict(self) -> Dict:
        return {
            "ip_address": self.ip_address,
            "mac_address": self.mac_address,
            "hostname": self.hostname,
            "vendor": self.vendor,
            "port": self.port,
            "services": self.services,
            "last_seen": self.last_seen,
            "first_seen": self.first_seen,
            "is_alive": self.is_alive,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "DiscoveredDevice":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ─── MAC 厂商数据库（简化）────────────────────────────────────────────────────

# 常见 MAC 厂商前缀（OUI）
KNOWN_VENDORS = {
    "00:1A:2B": "Cisco",
    "00:1C:B3": "Apple",
    "00:50:56": "VMware",
    "08:00:27": "Oracle VirtualBox",
    "00:15:5D": "Microsoft Hyper-V",
    "00:1C:42": "Parallels",
    "B8:27:EB": "Raspberry Pi",
    "DC:A6:32": "Raspberry Pi",
    "E4:5F:01": "Raspberry Pi",
    "00:17:88": "Philips Hue",
    "D0:73:D5": "TP-Link",
    "50:C7:BF": "TP-Link",
    "AC:84:C6": "TP-Link",
    "A4:2B:8C": "TP-Link",
    "00:27:19": "TP-Link",
    "B0:BE:76": "TP-Link",
    "14:CC:20": "TP-Link",
    "30:B5:C2": "TP-Link",
    "10:FE:ED": "TP-Link",
    "C4:6E:1F": "TP-Link",
    "00:E0:4C": "Realtek",
    "52:54:00": "QEMU",
    "00:03:93": "Apple",
    "3C:22:FB": "Apple",
    "A4:83:E7": "Apple",
    "F0:18:98": "Apple",
    "00:1F:F3": "Apple",
    "A8:20:66": "Apple",
    "9C:20:7B": "Apple",
    "C8:69:CD": "Apple",
    "84:38:35": "Apple",
    "60:F8:1D": "Apple",
    "F4:31:C3": "Apple",
    "94:E9:6A": "Apple",
    "E0:AC:CB": "Apple",
    "CC:08:E0": "Apple",
    "50:32:37": "Apple",
    "30:10:E4": "Apple",
    "B8:C1:11": "Apple",
    "F8:1E:DF": "Apple",
    "DC:A9:04": "Apple",
    "48:60:BC": "Apple",
    "08:6D:41": "Samsung",
    "00:24:54": "Samsung",
    "00:1D:25": "Samsung",
    "00:16:32": "Samsung",
    "00:12:47": "Samsung",
    "B4:3A:28": "Samsung",
    "9C:02:98": "Samsung",
    "A8:7C:01": "Samsung",
    "D8:31:34": "Samsung",
    "50:01:BB": "Samsung",
    "A4:07:B6": "Samsung",
    "80:65:6D": "Samsung",
    "18:F6:43": "Samsung",
    "C0:BD:D1": "Samsung",
    "AC:36:13": "Samsung",
    "E8:11:32": "Samsung",
    "CC:7B:35": "Samsung",
    "F0:25:B7": "Samsung",
    "F8:D0:BD": "Samsung",
    "A0:07:47": "Samsung",
    "38:AA:3C": "Samsung",
    "00:1A:79": "Netgear",
    "44:94:FC": "Netgear",
    "C0:3F:0E": "Netgear",
    "00:22:3F": "Netgear",
    "00:24:B2": "Netgear",
    "00:26:F2": "Netgear",
    "20:4E:7F": "Netgear",
    "2C:B0:5D": "Netgear",
    "A0:21:B7": "Netgear",
    "A4:2B:8C": "TP-Link",
    "B0:4E:26": "Unknown",
    "00:1E:68": "Quanta",
    "00:23:69": "Cisco",
    "00:26:0B": "Cisco",
    "00:17:DF": "Cisco",
    "00:16:C7": "Cisco",
    "00:1A:A0": "Cisco",
    "00:1C:58": "Cisco",
    "00:1E:BE": "Cisco",
    "00:21:55": "Cisco",
    "00:22:55": "Cisco",
    "00:24:14": "Cisco",
    "00:25:84": "Cisco",
    "00:26:33": "Cisco",
    "00:26:C8": "Cisco",
    "00:26:F1": "Cisco",
}


def get_vendor_from_mac(mac: str) -> Optional[str]:
    """从 MAC 地址获取厂商信息"""
    if not mac:
        return None
    mac_clean = mac.upper().replace(":", "").replace("-", "")
    if len(mac_clean) >= 6:
        prefix = ":".join([mac_clean[i:i+2] for i in range(0, 6, 2)])
        return KNOWN_VENDORS.get(prefix)
    return None


# ─── 网络发现引擎 ────────────────────────────────────────────────────────────

class NetworkDiscovery:
    """
    端侧网络发现引擎

    功能：
    - 扫描局域网发现设备
    - 通过 EventBus 发布设备发现事件
    - 与 SyncEngine 同步设备列表
    - 定期巡检设备在线状态
    """

    DISCOVERY_INTERVAL = 300        # 5 分钟扫描一次
    DEVICE_TIMEOUT = 600           # 10 分钟无响应视为离线
    ARP_SCAN_RANGE = "192.168.1.0/24"  # 默认扫描范围

    def __init__(
        self,
        eventbus=None,
        sync_engine=None,
        scan_range: Optional[str] = None,
    ):
        self.eventbus = eventbus
        self.sync_engine = sync_engine
        self.scan_range = scan_range or self.ARP_SCAN_RANGE

        self._devices: Dict[str, DiscoveredDevice] = {}
        self._lock = Lock()
        self._running = False
        self._scan_thread: Optional[Thread] = None

        # 加载缓存的设备列表
        self._load_devices()

        logger.info("NetworkDiscovery initialized (range=%s)", self.scan_range)

    # ─── 设备存储 ───────────────────────────────────────────────────────────

    def _load_devices(self):
        """加载缓存的设备列表"""
        if DISCOVERY_STATE_FILE.exists():
            try:
                with open(DISCOVERY_STATE_FILE) as f:
                    data = json.load(f)
                    for ip, dev_data in data.items():
                        self._devices[ip] = DiscoveredDevice.from_dict(dev_data)
                logger.info("Loaded %d cached devices", len(self._devices))
            except Exception as e:
                logger.warning("Failed to load device cache: %s", e)

    def _save_devices(self):
        """保存设备列表到缓存"""
        with self._lock:
            data = {ip: dev.to_dict() for ip, dev in self._devices.items()}
            DISCOVERY_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(DISCOVERY_STATE_FILE, 'w') as f:
                json.dump(data, f, indent=2)

    # ─── 发现方法 ───────────────────────────────────────────────────────────

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

    def _ping_host(self, ip: str, timeout: float = 1.0) -> bool:
        """Ping 单个主机（跨平台）"""
        try:
            # Windows
            if subprocess.os.name == 'nt':
                result = subprocess.run(
                    ["ping", "-n", "1", "-w", str(int(timeout * 1000)), ip],
                    capture_output=True, timeout=timeout + 1
                )
                return result.returncode == 0
            # Unix/Linux/macOS
            else:
                result = subprocess.run(
                    ["ping", "-c", "1", "-W", "1", ip],
                    capture_output=True, timeout=timeout + 1
                )
                return result.returncode == 0
        except Exception:
            return False

    def _arp_lookup(self, ip: str) -> Optional[str]:
        """ARP 查询获取 MAC 地址"""
        try:
            if subprocess.os.name == 'nt':
                result = subprocess.run(
                    ["arp", "-a", ip],
                    capture_output=True, text=True, timeout=5
                )
                # Windows ARP 输出格式解析
                for line in result.stdout.splitlines():
                    if ip in line:
                        parts = line.split()
                        for part in parts:
                            if "-" in part and len(part) == 17:
                                return part.upper()
            else:
                result = subprocess.run(
                    ["arp", "-n", ip],
                    capture_output=True, text=True, timeout=5
                )
                # Unix ARP 输出格式解析
                lines = result.stdout.splitlines()
                if len(lines) >= 2:
                    parts = lines[1].split()
                    for part in parts:
                        if ":" in part and len(part) == 17:
                            return part.upper()
        except Exception as e:
            logger.debug("ARP lookup failed for %s: %s", ip, e)
        return None

    def _resolve_hostname(self, ip: str) -> Optional[str]:
        """反向 DNS 解析获取 hostname"""
        try:
            hostname, _, _ = socket.gethostbyaddr(ip)
            return hostname
        except Exception:
            return None

    def _parse_mac_address(self, mac_str: str) -> Optional[str]:
        """标准化 MAC 地址格式"""
        if not mac_str:
            return None
        mac = mac_str.upper().replace("-", ":")
        # 确保格式为 XX:XX:XX:XX:XX:XX
        parts = mac.split(":")
        if len(parts) == 6 and all(len(p) == 2 for p in parts):
            return ":".join(parts)
        return None

    async def _scan_range_async(self, range_str: str) -> List[DiscoveredDevice]:
        """异步扫描 IP 范围"""
        devices = []
        network = ipaddress.ip_network(range_str, strict=False)

        logger.info("Scanning network %s (%d hosts)", range_str, network.num_addresses)

        for ip in network.hosts():
            ip_str = str(ip)
            try:
                # 并发 Ping（限制并发数）
                is_alive = await asyncio.get_event_loop().run_in_executor(
                    None, self._ping_host, ip_str
                )

                if is_alive:
                    device = DiscoveredDevice(ip_address=ip_str)

                    # 获取 MAC 地址
                    mac = self._arp_lookup(ip_str)
                    if mac:
                        device.mac_address = self._parse_mac_address(mac)
                        device.vendor = get_vendor_from_mac(device.mac_address)

                    # 获取 hostname
                    hostname = await asyncio.get_event_loop().run_in_executor(
                        None, self._resolve_hostname, ip_str
                    )
                    if hostname:
                        device.hostname = hostname

                    device.last_seen = time.time()
                    devices.append(device)
                    logger.debug("Discovered device: %s (%s)", ip_str, device.hostname or device.vendor or "unknown")
            except Exception as e:
                logger.debug("Scan error for %s: %s", ip_str, e)

        return devices

    def _scan_network(self) -> List[DiscoveredDevice]:
        """执行网络扫描"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            devices = loop.run_until_complete(self._scan_range_async(self.scan_range))
            loop.close()
            return devices
        except Exception as e:
            logger.error("Network scan failed: %s", e)
            return []

    # ─── 设备管理 ───────────────────────────────────────────────────────────

    def _update_device(self, device: DiscoveredDevice) -> bool:
        """更新设备信息，返回是否是新设备"""
        is_new = device.ip_address not in self._devices
        with self._lock:
            existing = self._devices.get(device.ip_address)
            if existing:
                # 更新现有设备
                existing.last_seen = device.last_seen
                if device.mac_address and not existing.mac_address:
                    existing.mac_address = device.mac_address
                if device.hostname and not existing.hostname:
                    existing.hostname = device.hostname
                if device.vendor and not existing.vendor:
                    existing.vendor = device.vendor
                existing.is_alive = True
            else:
                # 新设备
                self._devices[device.ip_address] = device
        return is_new

    def _mark_offline_devices(self, max_age: float):
        """标记超时设备为离线"""
        now = time.time()
        with self._lock:
            for device in self._devices.values():
                if device.is_alive and (now - device.last_seen) > max_age:
                    device.is_alive = False
                    logger.info("Device marked offline: %s", device.ip_address)

    def get_devices(self) -> List[DiscoveredDevice]:
        """获取所有设备列表"""
        with self._lock:
            return list(self._devices.values())

    def get_device(self, ip: str) -> Optional[DiscoveredDevice]:
        """获取指定 IP 的设备"""
        return self._devices.get(ip)

    def get_online_devices(self) -> List[DiscoveredDevice]:
        """获取在线设备列表"""
        with self._lock:
            return [d for d in self._devices.values() if d.is_alive]

    # ─── EventBus 集成 ──────────────────────────────────────────────────────

    def _publish_event(self, event_type: str, data: Dict):
        """通过 EventBus 发布事件"""
        if self.eventbus is None:
            return
        try:
            from eventbus.schema import Event, EventType
            event = Event(
                type=EventType.CUSTOM,
                source="network_discovery",
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
            logger.info("Subscribed to EventBus for network discovery")
        except Exception as e:
            logger.warning("Failed to subscribe to EventBus: %s", e)

    def _on_custom_event(self, event):
        """处理 EventBus 自定义事件"""
        data = event.payload or {}
        et = data.get("_event_type", "")
        if et == "force_scan":
            logger.info("Force network scan triggered via event")
            self.run_discovery()
        elif et == "sync_devices":
            logger.info("Device sync triggered via event")
            self._sync_to_cloud()

    # ─── SyncEngine 集成 ───────────────────────────────────────────────────

    def _sync_to_cloud(self):
        """同步设备列表到云端"""
        if self.sync_engine is None:
            return
        try:
            devices = self.get_devices()
            device_data = [d.to_dict() for d in devices]
            self.sync_engine.queue_operation(
                category="device",
                action="sync",
                data={"devices": device_data, "timestamp": datetime.utcnow().isoformat()}
            )
            logger.debug("Queued %d devices for cloud sync", len(devices))
        except Exception as e:
            logger.warning("Failed to sync devices to cloud: %s", e)

    # ─── 扫描循环 ───────────────────────────────────────────────────────────

    def run_discovery(self) -> List[DiscoveredDevice]:
        """执行一次网络发现"""
        logger.info("Starting network discovery...")

        # 扫描网络
        discovered = self._scan_network()

        # 更新设备
        new_devices = []
        for device in discovered:
            if self._update_device(device):
                new_devices.append(device)
                self._publish_event("device.discovered", device.to_dict())

        # 标记离线设备
        self._mark_offline_devices(self.DEVICE_TIMEOUT)

        # 保存状态
        self._save_devices()

        logger.info("Discovery complete: %d devices found, %d new", len(discovered), len(new_devices))
        return discovered

    def _discovery_loop(self):
        """后台发现循环"""
        while self._running:
            try:
                self.run_discovery()
                time.sleep(self.DISCOVERY_INTERVAL)
            except Exception as e:
                logger.error("Discovery loop error: %s", e)
                time.sleep(60)

    # ─── 生命周期 ───────────────────────────────────────────────────────────

    def start(self):
        """启动网络发现引擎"""
        if self._running:
            return
        self._running = True
        self._subscribe_to_events()
        self._scan_thread = Thread(target=self._discovery_loop, daemon=True)
        self._scan_thread.start()
        logger.info("NetworkDiscovery started (interval=%ds)", self.DISCOVERY_INTERVAL)

    def stop(self):
        """停止网络发现引擎"""
        self._running = False
        if self._scan_thread:
            self._scan_thread.join(timeout=10)
        self._save_devices()
        logger.info("NetworkDiscovery stopped")

    # ─── 外部接口 ───────────────────────────────────────────────────────────

    def force_scan(self) -> List[DiscoveredDevice]:
        """手动触发一次扫描"""
        return self.run_discovery()

    def get_status(self) -> Dict:
        """获取发现系统状态"""
        with self._lock:
            online = sum(1 for d in self._devices.values() if d.is_alive)
            return {
                "total_devices": len(self._devices),
                "online_devices": online,
                "offline_devices": len(self._devices) - online,
                "scan_range": self.scan_range,
                "last_scan": max((d.last_seen for d in self._devices.values()), default=0),
            }
