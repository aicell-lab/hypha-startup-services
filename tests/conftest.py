"""Common test fixtures for weaviate tests."""

import os
import pytest_asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_startup_services.register_weaviate_service import register_weaviate

load_dotenv()


async def get_server(server_url: str):
    token = os.environ.get("PERSONAL_TOKEN")
    assert token is not None, "PERSONAL_TOKEN environment variable is not set"
    server = await connect_to_server(
        {
            "server_url": server_url,
            "token": token,
        }
    )
    await register_weaviate(server, "weaviate-test")

    return server


@pytest_asyncio.fixture
async def weaviate_service():
    """Fixture for connecting to the weaviate service.

    Use --service-id command-line option to override the default service ID.
    """
    server = await get_server("https://hypha.aicell.io")
    service = await server.get_service("weaviate-test")
    yield service
    # Cleanup after tests
    try:
        await service.collections.delete("Movie")
    except ValueError:  # Collection doesn't exist
        pass
    await server.disconnect()
