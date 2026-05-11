"""
Client Registry — tracks connected Edge Gateway clients
"""
import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional, List
from websockets.server import WebSocketServerProtocol

logger = logging.getLogger("cloud-hub.registry")


@dataclass
class ClientInfo:
    client_id: str
    user_id: str
    ws: WebSocketServerProtocol
    connected_at: float
    last_active: float
    platform: Optional[str] = None
    version: Optional[str] = None
    metadata: Dict = field(default_factory=dict)


class ClientRegistry:
    """Thread-safe registry of connected clients."""

    def __init__(self):
        self._clients: Dict[str, ClientInfo] = {}
        self._lock = asyncio.Lock()

    async def register(self, ws: WebSocketServerProtocol, user_id: str, platform: str = None, version: str = None) -> str:
        """Register a new client, returns client_id."""
        async with self._lock:
            client_id = str(uuid.uuid4())
            import time
            self._clients[client_id] = ClientInfo(
                client_id=client_id,
                user_id=user_id,
                ws=ws,
                connected_at=time.time(),
                last_active=time.time(),
                platform=platform,
                version=version,
            )
            logger.info(f"Registered client {client_id} (user={user_id}, platform={platform})")
            return client_id

    async def unregister(self, client_id: str):
        """Unregister a client."""
        async with self._lock:
            if client_id in self._clients:
                del self._clients[client_id]
                logger.info(f"Unregistered client {client_id}")

    async def update_activity(self, client_id: str):
        """Update last_active timestamp."""
        async with self._lock:
            if client_id in self._clients:
                import time
                self._clients[client_id].last_active = time.time()

    async def get_client(self, client_id: str) -> Optional[ClientInfo]:
        return self._clients.get(client_id)

    async def get_all_clients(self) -> List[ClientInfo]:
        async with self._lock:
            return list(self._clients.values())

    async def get_clients_by_user(self, user_id: str) -> List[ClientInfo]:
        async with self._lock:
            return [c for c in self._clients.values() if c.user_id == user_id]

    async def client_count(self) -> int:
        async with self._lock:
            return len(self._clients)

    async def broadcast(self, message: dict, exclude_client_id: str = None):
        """Broadcast a message to all registered clients."""
        async with self._lock:
            clients = list(self._clients.items())

        for client_id, client_info in clients:
            if client_id == exclude_client_id:
                continue
            try:
                import json
                await client_info.ws.send(json.dumps(message))
            except Exception as e:
                logger.warning(f"Failed to send to client {client_id}: {e}")
