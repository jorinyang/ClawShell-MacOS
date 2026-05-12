"""
ClawShell Edge Gateway — Detector Manager
管理所有平台检测器，提供统一的检测接口。

功能：
- 批量检测所有平台
- 并发检测加速
- 缓存检测结果
- 事件通知
"""
import asyncio
import logging
from typing import Any, Dict, List, Optional

from .base import PlatformDetector, DetectionResult
from .openclaw_detector import OpenClawDetector
from .hermes_detector import HermesDetector
from .wukong_detector import WukongDetector
from .system_detector import SystemDetector

logger = logging.getLogger("detector.manager")


class DetectorManager:
    """
    平台检测器管理器。
    
    负责初始化和协调所有检测器，提供统一的检测结果查询接口。
    采用 macOS async patterns。
    """

    def __init__(self):
        self._detectors: Dict[str, PlatformDetector] = {}
        self._results: Dict[str, DetectionResult] = {}
        self._initialized = False
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """初始化所有检测器"""
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            # 注册检测器
            self._detectors = {
                "openclaw": OpenClawDetector(),
                "hermes": HermesDetector(),
                "wukong": WukongDetector(),
                "system": SystemDetector(),
            }

            # 执行初始检测
            await self._run_all_detections()
            
            self._initialized = True
            logger.info(f"DetectorManager 初始化完成，检测到 {len(self.get_available_platforms())} 个可用平台")

    async def _run_all_detections(self) -> None:
        """并发运行所有检测"""
        tasks = []
        for name, detector in self._detectors.items():
            tasks.append(self._detect_and_store(name, detector))
        
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _detect_and_store(self, name: str, detector: PlatformDetector) -> None:
        """执行单个检测并存储结果"""
        try:
            result = await detector.detect()
            self._results[name] = result
            status = "available" if result.available else "unavailable"
            logger.debug(f"{name} 检测完成: {status}")
        except Exception as e:
            logger.exception(f"{name} 检测失败: {e}")
            self._results[name] = DetectionResult(
                platform=name,
                available=False,
                error=str(e),
            )

    async def refresh(self) -> Dict[str, DetectionResult]:
        """
        刷新所有检测结果。
        
        重新执行所有平台检测，返回最新结果。
        """
        await self._run_all_detections()
        return self._results

    async def detect_platform(self, platform_name: str) -> Optional[DetectionResult]:
        """
        检测指定平台。
        
        platform_name: openclaw, hermes, wukong, system
        返回检测结果，如果平台不存在返回 None
        """
        if platform_name not in self._detectors:
            logger.warning(f"未知平台: {platform_name}")
            return None

        detector = self._detectors[platform_name]
        result = await detector.detect()
        self._results[platform_name] = result
        return result

    def get_result(self, platform_name: str) -> Optional[DetectionResult]:
        """获取指定平台的检测结果（缓存）"""
        return self._results.get(platform_name)

    def get_all_results(self) -> Dict[str, DetectionResult]:
        """获取所有检测结果"""
        return self._results.copy()

    def get_available_platforms(self) -> List[str]:
        """获取所有可用平台列表"""
        return [
            name for name, result in self._results.items()
            if result.available
        ]

    def is_platform_available(self, platform_name: str) -> bool:
        """检查指定平台是否可用"""
        result = self._results.get(platform_name)
        return result.available if result else False

    def get_platform_info(self, platform_name: str) -> Optional[Dict[str, Any]]:
        """获取平台详细信息"""
        result = self._results.get(platform_name)
        if result:
            return result.to_dict()
        return None

    def get_summary(self) -> Dict[str, Any]:
        """获取检测摘要"""
        available = self.get_available_platforms()
        return {
            "total_platforms": len(self._detectors),
            "available_platforms": len(available),
            "available": available,
            "unavailable": [
                name for name in self._detectors.keys()
                if name not in available
            ],
            "results": {
                name: result.to_dict()
                for name, result in self._results.items()
            },
        }

    async def detect_and_wait_for(
        self,
        platform_name: str,
        timeout: float = 60.0,
        check_interval: float = 5.0
    ) -> Optional[DetectionResult]:
        """
        检测平台并等待其就绪。
        
        用于等待某个平台变为可用状态（如等待 LaunchAgent 启动）。
        """
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            result = await self.detect_platform(platform_name)
            if result and result.available:
                return result
            
            await asyncio.sleep(check_interval)
        
        logger.warning(f"等待 {platform_name} 就绪超时 ({timeout}s)")
        return None

    def __repr__(self) -> str:
        available = self.get_available_platforms()
        return f"<DetectorManager available={available}>"


# 全局单例
_global_manager: Optional[DetectorManager] = None


async def get_detector_manager() -> DetectorManager:
    """获取全局检测器管理器单例"""
    global _global_manager
    if _global_manager is None:
        _global_manager = DetectorManager()
        await _global_manager.initialize()
    return _global_manager


async def quick_detect() -> Dict[str, DetectionResult]:
    """
    快速检测所有平台。
    
    便捷函数，用于快速获取检测结果。
    """
    manager = await get_detector_manager()
    return manager.get_all_results()
