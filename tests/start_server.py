"""Shared utility to start test services for both weaviate and mem0."""

import asyncio
import os
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.weaviate_service.register_service import (
    register_weaviate,
)
from hypha_startup_services.mem0_service.register_service import (
    register_mem0_service,
)

SERVER_URL = "https://hypha.aicell.io"


async def start_services():
    """Start both weaviate-test and mem0-test services."""
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"

    server = await connect_to_server(
        {
            "server_url": SERVER_URL,
            "token": token,
        }
    )

    if not isinstance(server, RemoteService):
        raise TypeError("connect_to_server did not return a RemoteService instance")

    # Register both services
    print("Registering weaviate-test service...")
    await register_weaviate(server, "weaviate-test")

    print("Registering mem0-test service...")
    await register_mem0_service(server, "mem0-test")

    print("Both services registered. Starting server...")
    await server.serve()


if __name__ == "__main__":
    asyncio.run(start_services())
