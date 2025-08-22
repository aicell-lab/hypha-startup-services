"""Common test fixtures for mem0 tests."""

import pytest_asyncio

from tests.conftest import get_user_server
from tests.mem0_service.utils import (
    TEST_AGENT_ID,
    TEST_AGENT_ID2,
)


@pytest_asyncio.fixture
async def mem0_live_service():
    """Mem0 BioImage service fixture for live (admin) environment."""
    server = await get_user_server("HYPHA_TOKEN")
    service = await server.get_service("aria-agents/mem0")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def mem0_service():
    """Mem0 service fixture for user 1."""
    server = await get_user_server("PERSONAL_TOKEN")
    service = await server.get_service("aria-agents/mem0-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def mem0_service2():
    """Mem0 service fixture for user 2."""
    server = await get_user_server("PERSONAL_TOKEN2")
    service = await server.get_service("aria-agents/mem0-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def mem0_service3():
    """Mem0 service fixture for user 3."""
    server = await get_user_server("PERSONAL_TOKEN3")
    service = await server.get_service("aria-agents/mem0-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture(autouse=True)
async def init_agents(mem0_service):
    """Initialize agents for the mem0 service."""
    print("Initializing agents for mem0 service...")
    await mem0_service.init_agent(
        agent_id=TEST_AGENT_ID,
        description="Test agent for mem0 service",
        metadata={"test": True, "environment": "pytest"},
    )
    await mem0_service.init_agent(
        agent_id=TEST_AGENT_ID2,
        description="Test agent 2 for mem0 service",
        metadata={"test": True, "environment": "pytest"},
    )
