"""
ClawShell Cloud Hub — 存储层
统一封装所有 OSS 存储操作（boto3 S3 兼容）
各 domain handler 通过 OssStore 读写数据
"""
import json
import logging
from typing import Any, Optional, List, Dict
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("oss-store")


class OssStore:
    """
    OSS 统一存储接口。
    通过 boto3 S3 兼容 API 操作阿里云 OSS。
    所有 domain 数据最终都存在这里。
    """

    def __init__(self, config: dict):
        self.cfg = config
        self.client = boto3.client(
            "s3",
            endpoint_url=f"https://{config.get('endpoint', '')}",
            aws_access_key_id=config.get("access_key", ""),
            aws_secret_access_key=config.get("secret_key", ""),
            region_name="auto",
        )
        self.bucket = config.get("bucket", "")
        logger.info(f"OssStore initialized: bucket={self.bucket}")

    # ─── Generic Key-Value ────────────────────────────────────────────────

    async def save(self, key_path: str, content: str) -> dict:
        """save: 通用存储（JSON/文本），key_path 如 'memory/snapshots/xxx.json'"""
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key_path,
                Body=content.encode("utf-8"),
                ContentType="application/json",
            )
            return {"success": True, "key": key_path}
        except ClientError as e:
            logger.error(f"OSS save error [{key_path}]: {e}")
            raise Exception(f"OSS save error: {e}")

    async def load(self, key_path: str) -> Optional[str]:
        """load: 读取内容，不存在返回 None"""
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=key_path)
            body = resp["Body"].read()
            return body.decode("utf-8")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "NoSuchBucket"):
                return None
            logger.error(f"OSS load error [{key_path}]: {e}")
            raise Exception(f"OSS load error: {e}")

    async def delete(self, key_path: str) -> dict:
        """delete: 删除单个对象"""
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key_path)
            return {"success": True, "key": key_path}
        except ClientError as e:
            logger.error(f"OSS delete error [{key_path}]: {e}")
            raise Exception(f"OSS delete error: {e}")

    async def list_all(self, prefix: str = "") -> List[str]:
        """
        list_all: 列出前缀下所有对象的 key。
        适用于 knowledge/docs/, memory/snapshots/ 等目录遍历。
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                MaxKeys=1000,
            )
            contents = response.get("Contents", [])
            return [obj["Key"] for obj in contents]
        except ClientError as e:
            logger.error(f"OSS list error [{prefix}]: {e}")
            raise Exception(f"OSS list error: {e}")

    # ─── Vault ────────────────────────────────────────────────────────────

    async def vault_upload(self, key_path: str, content: str) -> dict:
        """vault_upload: 上传文件到 vault 分区"""
        p = self.cfg.get("vault_prefix", "vault/")
        full_key = f"{p}{key_path}".lstrip("/")
        return await self.save(full_key, content)

    async def vault_download(self, key_path: str) -> Optional[str]:
        """vault_download: 从 vault 下载文件"""
        p = self.cfg.get("vault_prefix", "vault/")
        full_key = f"{p}{key_path}".lstrip("/")
        return await self.load(full_key)

    async def vault_list(self, prefix: str = "", limit: int = 100) -> dict:
        """vault_list: 列出 vault 路径下的对象"""
        p = self.cfg.get("vault_prefix", "vault/")
        full_prefix = f"{p}{prefix}".lstrip("/")
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=full_prefix,
                MaxKeys=limit,
            )
            contents = response.get("Contents", [])
            vp_len = len(p)
            return {
                "objects": [
                    {
                        "key": obj["Key"][vp_len:],
                        "size": obj["Size"],
                        "modified": obj["LastModified"].isoformat(),
                    }
                    for obj in contents
                ],
                "is_truncated": response.get("IsTruncated", False),
            }
        except ClientError as e:
            raise Exception(f"OSS vault_list error: {e}")

    async def vault_delete(self, key_path: str) -> dict:
        """vault_delete: 删除 vault 路径下的对象"""
        p = self.cfg.get("vault_prefix", "vault/")
        full_key = f"{p}{key_path}".lstrip("/")
        try:
            self.client.delete_object(Bucket=self.bucket, Key=full_key)
            return {"success": True, "key": key_path}
        except ClientError as e:
            raise Exception(f"OSS vault_delete error: {e}")
