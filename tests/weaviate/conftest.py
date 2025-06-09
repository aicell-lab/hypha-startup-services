"""Common test fixtures for weaviate tests."""

import pytest_asyncio
from hypha_rpc.rpc import RemoteService, RemoteException
from hypha_startup_services.weaviate_service.service_codecs import (
    register_weaviate_codecs,
)
from tests.conftest import get_user_server, APP_ID


async def cleanup_weaviate_service(service: RemoteService):
    # Cleanup after tests
    try:
        # Try to delete test applications first
        try:
            await service.applications.delete(
                collection_name="Movie", application_id=APP_ID
            )
        except RemoteException as e:
            print("Error deleting test application:", e)

        # Then delete the collection
        await service.collections.delete("Movie")
    except ValueError:  # Collection doesn't exist
        pass


@pytest_asyncio.fixture
async def weaviate_service():
    """Fixture for connecting to the weaviate service.

    Use --service-id command-line option to override the default service ID.
    """
    server = await get_user_server("PERSONAL_TOKEN")
    register_weaviate_codecs(server)
    service = await server.get_service("aria-agents/weaviate-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service2():
    """Fixture for connecting to the weaviate service with a second personal token.

    This represents a different user accessing the same service.
    """
    server = await get_user_server("PERSONAL_TOKEN2")
    register_weaviate_codecs(server)
    service = await server.get_service("aria-agents/weaviate-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service3():
    """Fixture for connecting to the weaviate service with a third personal token.

    This represents a third user accessing the same service.
    """
    server = await get_user_server("PERSONAL_TOKEN3")
    register_weaviate_codecs(server)
    service = await server.get_service("aria-agents/weaviate-test")
    yield service
    await server.disconnect()
