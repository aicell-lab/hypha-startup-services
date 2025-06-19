"""Common server management utilities for hypha startup services."""

import os
import sys
import subprocess
import logging
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

logger = logging.getLogger(__name__)


async def get_server(
    provided_url: str,
    port: int | None = None,
) -> RemoteService:
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
    server = await connect_to_server({"server_url": server_url, "token": token})

    if not isinstance(server, RemoteService):
        raise ValueError("Server is not a RemoteService instance.")

    return server


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
    startup_functions_arg = " ".join(startup_function_paths)

    command = [
        sys.executable,
        "-m",
        "hypha.server",
        f"--host={server_url}",
        f"--port={port}",
        f"--startup-functions={startup_functions_arg}",
    ]
    subprocess.run(command, check=True)
