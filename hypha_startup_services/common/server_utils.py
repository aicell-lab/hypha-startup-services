"""Common server management utilities for hypha startup services."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import os
import sys
import subprocess
import logging
from typing import Any
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_server(
    provided_url: str,
    port: int | None = None,
    client_id: str | None = None,
) -> AsyncGenerator[RemoteService, Any]:
    """Get a connection to a remote Hypha server.

    Args:
        provided_url: The base URL of the server
        port: Optional port number

    Returns:
        The server connection
    """
    server_url = provided_url if port is None else f"{provided_url}:{port}"
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server_config = {"server_url": server_url, "token": token}
    if client_id:
        server_config["client_id"] = client_id
    server = await connect_to_server(server_config)

    if not isinstance(server, RemoteService):
        raise ValueError("Server is not a RemoteService instance.")

    try:
        yield server
    finally:
        await server.disconnect()


async def run_local_services(
    server_url: str,
    port: int,
    startup_function_paths: list[str],
) -> None:
    """Run a local Hypha server with multiple services.

    Args:
        server_url: The URL of the server to connect to
        port: The port of the server
        startup_function_paths: List of paths to startup functions
    """
    # Join all startup function paths with spaces
    startup_functions_arg = [
        f"--startup-functions={path}" for path in startup_function_paths
    ]

    command = [
        sys.executable,
        "-m",
        "hypha.server",
        f"--host={server_url}",
        f"--port={port}",
        *startup_functions_arg,
    ]
    subprocess.run(command, check=True)
