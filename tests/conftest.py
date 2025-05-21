"""Common test fixtures for weaviate tests."""

import os
import pytest_asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_startup_services.service_codecs import register_weaviate_codecs

load_dotenv()

SERVER_URL = "https://hypha.aicell.io"
APP_ID = "TestApp"

# User workspace IDs
USER1_WS = "ws-user-google-oauth2|104255278140940970953"  # Admin user
USER2_WS = "ws-user-google-oauth2|101844867326318340275"  # Regular user
USER3_WS = "ws-user-google-oauth2|101564907182096510974"  # Regular user


async def get_user_server(token_env="PERSONAL_TOKEN"):
    token = os.environ.get(token_env)
    assert token is not None, f"{token_env} environment variable is not set"
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


@pytest_asyncio.fixture
async def weaviate_service2():
    """Fixture for connecting to the weaviate service with a second personal token.

    This represents a different user accessing the same service.
    """
    server = await get_user_server("PERSONAL_TOKEN2")
    service = await server.get_service("aria-agents/weaviate-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service3():
    """Fixture for connecting to the weaviate service with a third personal token.

    This represents a third user accessing the same service.
    """
    server = await get_user_server("PERSONAL_TOKEN3")
    service = await server.get_service("aria-agents/weaviate-test")
    yield service
    await server.disconnect()
