"""
Request Router — routes MCP requests to appropriate backends
"""
import json
import logging
from typing import Dict, Callable, Awaitable, Optional
import asyncio

logger = logging.getLogger("cloud-hub.router")

# Type for MCP handler functions
MCPHandler = Callable[[dict, str], Awaitable[dict]]


class Router:
    """
    Routes incoming MCP requests to the correct handler.

    Method naming convention:
      vault_*  → OSS Vault (boto3)
      skill_*  → Skill Registry MCP
      kanban_* → Kanban MCP
    """

    def __init__(self):
        self._handlers: Dict[str, MCPHandler] = {}

    def register_handler(self, method_prefix: str, handler: MCPHandler):
        """Register a handler for methods starting with prefix."""
        self._handlers[method_prefix] = handler
        logger.info(f"Router: registered handler for '{method_prefix}*'")

    def register_default(self, handler: MCPHandler):
        """Register a fallback handler for unmatched methods."""
        self._handlers["__default__"] = handler

    async def route(self, method: str, params: dict, request_id: str) -> dict:
        """
        Route an MCP request to the appropriate handler.
        Returns an MCP-formatted response dict.
        """
        # Find handler by prefix
        handler: Optional[MCPHandler] = None
        for prefix, h in self._handlers.items():
            if prefix != "__default__" and method.startswith(prefix):
                handler = h
                break

        if handler is None:
            handler = self._handlers.get("__default__")
            if handler is None:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": f"Method not found: {method}"}
                }

        try:
            result = await handler(method, params, request_id)
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result
            }
        except Exception as e:
            logger.error(f"Handler error for {method}: {e}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": str(e)}
            }
