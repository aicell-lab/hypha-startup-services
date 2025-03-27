"""Common test fixtures for weaviate tests."""

import os
import pytest_asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_startup_services.service_codecs import register_weaviate_codecs

load_dotenv()


async def get_server(server_url: str):
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server = await connect_to_server(
        {
            "server_url": server_url,
            "token": token,
        }
    )
    register_weaviate_codecs(server)

    return server


@pytest_asyncio.fixture
async def weaviate_service():
    server = await get_server("https://hypha.aicell.io")
    service = await server.get_service("weaviate")
    yield service
    # Cleanup after tests
    try:
        await service.collections.delete("Movie")
    except ValueError:  # Collection doesn't exist
        pass
    await server.disconnect()
