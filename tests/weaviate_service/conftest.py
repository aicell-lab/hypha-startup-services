"""Common test fixtures for weaviate tests."""

import pytest_asyncio
from hypha_rpc.rpc import RemoteService, RemoteException
from hypha_startup_services.weaviate_service.service_codecs import (
    register_weaviate_codecs,
)
from tests.weaviate_service.utils import APP_ID
from tests.conftest import get_user_server


async def cleanup_weaviate_service(service: RemoteService):
    """Cleanup after weaviate tests."""
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


def setup_weaviate_server(server: RemoteService):
    """Setup function to register weaviate codecs."""
    register_weaviate_codecs(server)


@pytest_asyncio.fixture
async def weaviate_service():
    """Weaviate service fixture for user 1."""
    server = await get_user_server("PERSONAL_TOKEN")
    register_weaviate_codecs(server)
    service = await server.get_service("aria-agents/weaviate-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service2():
    """Weaviate service fixture for user 2."""
    server = await get_user_server("PERSONAL_TOKEN2")
    register_weaviate_codecs(server)
    service = await server.get_service("aria-agents/weaviate-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service3():
    """Weaviate service fixture for user 3."""
    server = await get_user_server("PERSONAL_TOKEN3")
    register_weaviate_codecs(server)
    service = await server.get_service("aria-agents/weaviate-test")
    yield service
    await server.disconnect()
