"""Common test fixtures for weaviate tests."""

import os
import pytest
import pytest_asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_startup_services.start_weaviate_service import register_weaviate

load_dotenv()


# Add command line options for testing
def pytest_addoption(parser):
    parser.addoption(
        "--token",
        action="store",
        default=os.environ.get("HYPHA_TOKEN"),
        help="Token for connecting to the Hypha server",
    )
    parser.addoption(
        "--service-id",
        action="store",
        default="weaviate-test",
        help="Service ID to use for the Weaviate service",
    )


@pytest.fixture
def token(request):
    return request.config.getoption("--token")


@pytest.fixture
def service_id(request):
    return request.config.getoption("--service-id")


async def get_server(server_url: str):
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
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
