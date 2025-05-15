"""Common test fixtures for weaviate tests."""

import os
import pytest_asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_startup_services.register_weaviate_service import register_weaviate

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
    parser.addoption(
        "--server-url",
        action="store",
        default="https://hypha.aicell.io",
        help="URL for the Hypha server",
    )


async def get_server(request):
    """Get a server connection using command line options."""
    token = request.config.getoption("--token")
    service_id = request.config.getoption("--service-id")
    server_url = request.config.getoption("--server-url")

    assert (
        token is not None
    ), "Token is not provided. Set HYPHA_TOKEN environment variable or use --token option"

    server = await connect_to_server(
        {
            "server_url": server_url,
            "token": token,
        }
    )
    await register_weaviate(server, service_id)
    print("LOOK HERE!!!!!!!")
    print(server.config)

    return server


@pytest_asyncio.fixture
async def weaviate_service(request):
    """Fixture for connecting to the weaviate service.

    Use --service-id command-line option to override the default service ID.
    Use --token command-line option to override the default token.
    Use --server-url command-line option to override the default server URL.
    """
    server = await get_server(request)
    service_id = request.config.getoption("--service-id")
    service = await server.get_service(service_id)
    yield service
    # Cleanup after tests
    try:
        await service.collections.delete("Movie")
    except ValueError:  # Collection doesn't exist
        pass
    await server.disconnect()
