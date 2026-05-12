"""ClawShell 2.1 — Shared config (YAML + ENV, 3-level, inspired by ClawShell-Deep)"""
import os
from pathlib import Path
from typing import Optional
import yaml


class HubConfig:
    node_id: str = "hub-01"
    host: str = "0.0.0.0"
    port: int = 8080
    wss_port: int = 8443
    jwt_secret: str = "dev-secret-change-in-prod"
    oss_bucket: str = "clawshell-vault"
    oss_endpoint: str = "oss-cn-hongkong.aliyuncs.com"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "HubConfig":
        c = cls()
        c.node_id = os.environ.get("CLAWSHELL_NODE_ID", c.node_id)
        c.host = os.environ.get("CLAWSHELL_HOST", c.host)
        c.port = int(os.environ.get("CLAWSHELL_PORT", c.port))
        c.wss_port = int(os.environ.get("CLAWSHELL_WSS_PORT", c.wss_port))
        c.jwt_secret = os.environ.get("JWT_SECRET", c.jwt_secret)
        c.oss_bucket = os.environ.get("OSS_BUCKET", c.oss_bucket)
        c.oss_endpoint = os.environ.get("OSS_ENDPOINT", c.oss_endpoint)
        c.log_level = os.environ.get("LOG_LEVEL", c.log_level)
        return c

    @classmethod
    def from_yaml(cls, path: Path | str) -> "HubConfig":
        p = Path(path)
        if not p.exists():
            return cls.from_env()
        with open(p, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        c = cls()
        for k, v in data.items():
            if hasattr(c, k):
                setattr(c, k, v)
        return cls.from_env()  # env always overrides yaml


def get_hub_config() -> HubConfig:
    cfg_path = os.environ.get("CLAWSHELL_CONFIG", "")
    if cfg_path:
        return HubConfig.from_yaml(cfg_path)
    return HubConfig.from_env()
