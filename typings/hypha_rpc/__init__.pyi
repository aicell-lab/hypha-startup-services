from typing import Any

from .rpc import RemoteException, RemoteService
from .utils.schema import schema_function

class ServerConfig(dict[str, Any]):
    server_url: str
    token: str
    client_id: str | None

async def connect_to_server(config: ServerConfig | dict[str, Any]) -> RemoteService: ...

__all__ = [
    "RemoteException",
    "RemoteService",
    "connect_to_server",
    "schema_function",
]
