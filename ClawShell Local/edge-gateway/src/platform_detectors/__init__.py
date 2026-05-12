"""
ClawShell Edge Gateway — Platform Detectors Module
平台检测器模块，负责检测本地已安装的 ClawShell 兼容平台。

检测平台：
- OpenClaw: ~/.openclaw/ 目录
- Hermes: ~/.hermes/ 目录 + hermes 命令
- Wukong: 悟空.app + ~/.wukong/ 目录
- System: macOS 系统信息
"""
import logging
from typing import List, Dict, Any

from .base import PlatformDetector, DetectionResult
from .openclaw_detector import OpenClawDetector
from .hermes_detector import HermesDetector
from .wukong_detector import WukongDetector
from .system_detector import SystemDetector
from .detector_manager import DetectorManager

logger = logging.getLogger("platform_detectors")

__all__ = [
    "PlatformDetector",
    "DetectionResult",
    "OpenClawDetector",
    "HermesDetector",
    "WukongDetector",
    "SystemDetector",
    "DetectorManager",
]


def get_all_detectors() -> List[PlatformDetector]:
    """获取所有平台检测器实例"""
    return [
        OpenClawDetector(),
        HermesDetector(),
        WukongDetector(),
        SystemDetector(),
    ]


async def detect_all_platforms() -> Dict[str, DetectionResult]:
    """检测所有平台，返回结果字典"""
    manager = DetectorManager()
    await manager.initialize()
    return manager.get_all_results()
