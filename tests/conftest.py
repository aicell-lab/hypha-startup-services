"""Common test fixtures for weaviate tests."""

import os
import pytest_asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_startup_services.service_codecs import register_weaviate_codecs

load_dotenv()

SERVER_URL = "https://hypha.aicell.io"
APP_ID = "TestApp"


async def get_user_server():
    token = os.environ.get("PERSONAL_TOKEN")
    assert token is not None, "PERSONAL_TOKEN environment variable is not set"
    server = await connect_to_server(
        {
            "server_url": SERVER_URL,
            "token": token,
        }
    )

    register_weaviate_codecs(server)

    return server


@pytest_asyncio.fixture
async def weaviate_service():
    """Fixture for connecting to the weaviate service.

    Use --service-id command-line option to override the default service ID.
    """
    server = await get_user_server()
    service = await server.get_service("aria-agents/weaviate-test")
    yield service
    # Cleanup after tests
    try:
        # Try to delete test applications first
        try:
            await service.applications.delete(
                collection_name="Movie", application_id=APP_ID
            )
        except Exception:
            pass

        # Then delete the collection
        await service.collections.delete("Movie")
    except ValueError:  # Collection doesn't exist
        pass
    await server.disconnect()
