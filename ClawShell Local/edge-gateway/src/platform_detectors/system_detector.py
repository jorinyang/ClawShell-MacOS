"""
ClawShell Edge Gateway — System Detector
检测 macOS 系统信息，用于平台适配和功能开关。

检测内容：
- macOS 版本和架构
- Python 版本
- 已安装的 ClawShell 组件
- 网络状态
- 硬件信息
"""
import asyncio
import logging
import os
import platform
import subprocess
from typing import Any, Dict

from .base import PlatformDetector, DetectionResult

logger = logging.getLogger("detector.system")


class SystemDetector(PlatformDetector):
    """
    macOS 系统信息检测器。
    
    收集系统级信息，用于功能适配和条件判断。
    """

    def __init__(self):
        super().__init__()
        self.platform_name = "system"

    async def detect(self) -> DetectionResult:
        """
        检测系统信息。
        
        检测内容：
        1. macOS 版本
        2. 硬件架构
        3. Python 版本
        4. ClawShell 组件状态
        5. 磁盘空间
        6. 内存信息
        """
        details = {}
        version = None
        available = True  # 系统始终可用
        error = None

        # 1. macOS 版本
        try:
            r = self._run_sync(["sw_vers"])
            if r.get("success"):
                details["sw_vers"] = {}
                for line in r.get("stdout", "").split("\n"):
                    if ":" in line:
                        key, value = line.split(":", 1)
                        details["sw_vers"][key.strip()] = value.strip()
                
                # 组合版本号
                product_version = details["sw_vers"].get("ProductVersion", "")
                build_version = details["sw_vers"].get("BuildVersion", "")
                version = f"{product_version} ({build_version})" if build_version else product_version
        except Exception as e:
            logger.debug(f"Failed to get sw_vers: {e}")

        # 2. 硬件架构
        details["architecture"] = platform.machine()
        details["processor"] = platform.processor()
        
        # 架构兼容性
        arch = details["architecture"]
        details["is_arm64"] = arch == "arm64"
        details["is_x86_64"] = arch == "x86_64"
        details["is_rosetta"] = self._check_rosetta() if details["is_arm64"] else False

        # 3. Python 版本
        details["python_version"] = platform.python_version()
        details["python_executable"] = sys.executable if "sys" in dir() else "/usr/bin/python3"

        # 4. ClawShell 组件检查
        details["components"] = self._detect_clawshell_components()

        # 5. 磁盘空间
        details["disk"] = self._get_disk_info()

        # 6. 内存信息
        details["memory"] = self._get_memory_info()

        # 7. 网络状态
        details["network"] = self._get_network_info()

        logger.info(f"System info: macOS={version}, arch={details['architecture']}")

        return DetectionResult(
            platform=self.platform_name,
            available=available,
            version=version,
            details=details,
            error=None,
        )

    def _check_rosetta(self) -> bool:
        """检查是否通过 Rosetta 2 运行"""
        try:
            # 检查 /usr/libexec/rosetta 的存在
            r = self._run_sync(["file", "/usr/libexec/rosetta"])
            return r.get("success")
        except Exception:
            return False

    def _detect_clawshell_components(self) -> Dict[str, Any]:
        """检测 ClawShell 组件"""
        components = {}

        # ~/.clawshell
        clawshell_dir = os.path.expanduser("~/.clawshell")
        components["clawshell_dir"] = clawshell_dir
        components["clawshell_installed"] = os.path.isdir(clawshell_dir)
        
        if components["clawshell_installed"]:
            # 检查 bin 目录
            bin_dir = os.path.join(clawshell_dir, "bin")
            components["bin_dir"] = bin_dir
            components["clawshell_bin"] = os.path.isfile(os.path.join(bin_dir, "clawshell"))
            
            # 检查 lib 目录
            lib_dir = os.path.join(clawshell_dir, "lib")
            components["lib_dir"] = lib_dir
            components["lib_exists"] = os.path.isdir(lib_dir)

        # ~/.clawshell-local
        clawshell_local_dir = os.path.expanduser("~/.clawshell-local")
        components["clawshell_local_dir"] = clawshell_local_dir
        components["clawshell_local_installed"] = os.path.isdir(clawshell_local_dir)

        # ~/.openclaw
        openclaw_dir = os.path.expanduser("~/.openclaw")
        components["openclaw_dir"] = openclaw_dir
        components["openclaw_installed"] = os.path.isdir(openclaw_dir)

        # ~/.hermes
        hermes_dir = os.path.expanduser("~/.hermes")
        components["hermes_dir"] = hermes_dir
        components["hermes_installed"] = os.path.isdir(hermes_dir)

        # ~/.wukong
        wukong_dir = os.path.expanduser("~/.wukong")
        components["wukong_dir"] = wukong_dir
        components["wukong_installed"] = os.path.isdir(wukong_dir)

        return components

    def _get_disk_info(self) -> Dict[str, Any]:
        """获取磁盘信息"""
        try:
            r = self._run_sync(["df", "-h", "/"])
            if r.get("success"):
                lines = r.get("stdout", "").split("\n")
                if len(lines) >= 2:
                    parts = lines[1].split()
                    if len(parts) >= 4:
                        return {
                            "total": parts[1] if len(parts) > 1 else "unknown",
                            "used": parts[2] if len(parts) > 2 else "unknown",
                            "available": parts[3] if len(parts) > 3 else "unknown",
                            "use_percent": parts[4] if len(parts) > 4 else "unknown",
                        }
        except Exception as e:
            logger.debug(f"Failed to get disk info: {e}")
        return {}

    def _get_memory_info(self) -> Dict[str, Any]:
        """获取内存信息"""
        try:
            r = self._run_sync(["sysctl", "hw.memsize"])
            if r.get("success"):
                mem_size = int(r.get("stdout", "").split(":")[1].strip())
                return {
                    "total_gb": round(mem_size / (1024**3), 2),
                    "total_bytes": mem_size,
                }
        except Exception as e:
            logger.debug(f"Failed to get memory info: {e}")
        return {}

    def _get_network_info(self) -> Dict[str, Any]:
        """获取网络信息"""
        network = {}

        # 检查互联网连接
        try:
            r = self._run_sync(["ping", "-c", "1", "-W", "2", "8.8.8.8"])
            network["internet_reachable"] = r.get("success")
        except Exception:
            network["internet_reachable"] = False

        # 检查代理设置
        try:
            r = self._run_sync(["networksetup", "-getwebproxy", "Wi-Fi"])
            network["proxy_configured"] = not ("Off" in r.get("stdout", "") or "No" in r.get("stdout", ""))
        except Exception:
            network["proxy_configured"] = False

        return network

    def is_clawshell_fully_installed(self) -> bool:
        """检查 ClawShell 是否完整安装"""
        components = self._detect_clawshell_components()
        return (
            components.get("clawshell_installed", False) and
            components.get("clawshell_local_installed", False)
        )

    def get_supported_platforms(self) -> list:
        """获取支持的平台列表"""
        components = self._detect_clawshell_components()
        platforms = []
        if components.get("openclaw_installed"):
            platforms.append("openclaw")
        if components.get("hermes_installed"):
            platforms.append("hermes")
        if components.get("wukong_installed"):
            platforms.append("wukong")
        return platforms
