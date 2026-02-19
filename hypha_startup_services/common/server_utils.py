"""Common server management utilities for hypha startup services."""

import logging
import os
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

logger = logging.getLogger(__name__)

# Global cache for server connections to avoid creating too many connections
_server_cache: dict[str, RemoteService] = {}


@asynccontextmanager
async def get_server(
    provided_url: str,
    port: int | None = None,
    client_id: str | None = None,
) -> AsyncGenerator[RemoteService, object]:
    """Get a connection to a remote Hypha server.

    Args:
        provided_url: The base URL of the server
        port: Optional port number
        client_id: Optional client ID

    Returns:
        The server connection

    """
    server_url = provided_url if port is None else f"{provided_url}:{port}"
    token = os.environ.get("HYPHA_TOKEN")
    if token is None:
        error_msg = "HYPHA_TOKEN environment variable is not set"
        raise ValueError(error_msg)
    
    server_config = {"server_url": server_url, "token": token}
    if client_id:
        server_config["client_id"] = client_id
    
    # We create a new connection each time for now to avoid complexity with asyncio loops and caching
    # This revert ensures stability while debugging the test hang
    server = await connect_to_server(server_config)

    try:
        yield server
    finally:
        # Proper cleanup
        try:
            await server.disconnect()
        except Exception:
            pass

