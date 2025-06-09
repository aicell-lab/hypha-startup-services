"""Common test fixtures for mem0 tests."""

import pytest_asyncio
from tests.conftest import get_user_server


@pytest_asyncio.fixture
async def mem0_service():
    """Fixture for connecting to the mem0 service.

    Use --service-id command-line option to override the default service ID.
    """
    server = await get_user_server("PERSONAL_TOKEN")
    service = await server.get_service("aria-agents/mem0-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def mem0_service2():
    """Fixture for connecting to the mem0 service with a second personal token.

    This represents a different user accessing the same service.
    """
    server = await get_user_server("PERSONAL_TOKEN2")
    service = await server.get_service("aria-agents/mem0-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def mem0_service3():
    """Fixture for connecting to the mem0 service with a third personal token.

    This represents a third user accessing the same service.
    """
    server = await get_user_server("PERSONAL_TOKEN3")
    service = await server.get_service("aria-agents/mem0-test")
    yield service
    await server.disconnect()
