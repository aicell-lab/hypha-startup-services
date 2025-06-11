"""Shared utility to start test services for weaviate, mem0, and bioimage."""

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
from hypha_startup_services.bioimage_service.register_service import (
    register_bioimage_service,
)

SERVER_URL = "https://hypha.aicell.io"


async def start_services():
    """Start weaviate-test, mem0-test, and bioimage-test services."""
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

    # Register all services
    print("Registering weaviate-test service...")
    await register_weaviate(server, "weaviate-test")

    print("Registering mem0-test service...")
    await register_mem0_service(server, "mem0-test")

    print("Registering bioimage-test service...")
    await register_bioimage_service(server, "bioimage-test")

    print("All services successfully registered!")
    await server.serve()


if __name__ == "__main__":
    asyncio.run(start_services())
