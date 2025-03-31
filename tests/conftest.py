"""Common test fixtures for weaviate tests."""

import os
import pytest_asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server

load_dotenv()


def pytest_addoption(parser):
    """Add command-line options for tests."""
    parser.addoption(
        "--service-id",
        default="weaviate",
        help="Service ID to use for connecting to Hypha service (default: weaviate)",
    )


async def get_server(server_url: str):
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server = await connect_to_server(
        {
            "server_url": server_url,
            "token": token,
        }
    )

    return server


@pytest_asyncio.fixture
async def weaviate_service(request):
    """Fixture for connecting to the weaviate service.

    Use --service-id command-line option to override the default service ID.
    """
    service_id = request.config.getoption("--service-id")
    server = await get_server("https://hypha.aicell.io")
    service = await server.get_service(service_id)
    yield service
    # Cleanup after tests
    try:
        await service.collections.delete("Movie")
    except ValueError:  # Collection doesn't exist
        pass
    await server.disconnect()
