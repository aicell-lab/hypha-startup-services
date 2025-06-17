"""Common server management utilities for hypha startup services."""

import os
import sys
import subprocess
import asyncio
from typing import Callable, Any
from websockets.exceptions import InvalidURI
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService, RemoteException
from .constants import DEFAULT_LOCAL_EXISTING_HOST


async def register_to_existing_server(
    provided_url: str,
    service_id: str,
    register_function: Callable[[RemoteService, str], Any],
    port: int | None = None,
) -> None:
    """Register a service to an existing Hypha server.

    Args:
        provided_url: The base URL of the server
        port: Optional port number
        service_id: The ID for the service
        register_function: Function to register the specific service
    """
    server_url = provided_url if port is None else f"{provided_url}:{port}"
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server = await connect_to_server({"server_url": server_url, "token": token})

    if not isinstance(server, RemoteService):
        raise ValueError("Server is not a RemoteService instance.")

    await register_function(server, service_id)


async def run_local_server(
    server_url: str,
    port: int,
    service_id_existing_server: str,
    startup_function_path: str,
    register_function: Callable[[RemoteService, str], Any],
) -> None:
    """Run a local Hypha server with the specified service.

    Args:
        server_url: The URL of the server to connect to
        port: The port of the server
        service_id_existing_server: The ID of the service
        startup_function_path: Path to the startup function for the service
        register_function: Function to register the specific service
    """
    try:
        await register_to_existing_server(
            DEFAULT_LOCAL_EXISTING_HOST,
            port=port,
            service_id=service_id_existing_server,
            register_function=register_function,
        )
    except (ConnectionRefusedError, InvalidURI, RemoteException) as e:
        print(f"Failed to connect to the server at {server_url}:{port}. Error: {e}")
        command = [
            sys.executable,
            "-m",
            "hypha.server",
            f"--host={server_url}",
            f"--port={port}",
            f"--startup-functions={startup_function_path}",
        ]
        subprocess.run(command, check=True)


def connect_to_remote(
    server_url: str,
    port: int | None,
    service_id: str,
    register_function: Callable[[RemoteService, str], Any],
) -> None:
    """Connect to remote server and register service."""
    loop = asyncio.get_event_loop()
    loop.create_task(
        register_to_existing_server(server_url, service_id, register_function, port)
    )
    loop.run_forever()


def run_locally(
    server_url: str,
    port: int,
    service_id_existing_server: str,
    startup_function_path: str,
    register_function: Callable[[RemoteService, str], Any],
) -> None:
    """Run the local server with the provided arguments."""
    asyncio.run(
        run_local_server(
            server_url,
            port,
            service_id_existing_server,
            startup_function_path,
            register_function,
        )
    )
