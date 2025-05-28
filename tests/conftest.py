"""Common test fixtures for mem0 tests."""

import os
import pytest_asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

load_dotenv()

SERVER_URL = "https://hypha.aicell.io"
APP_ID = "TestApp"


async def get_user_server(token_env="PERSONAL_TOKEN"):
    token = os.environ.get(token_env)
    assert token is not None, f"{token_env} environment variable is not set"
    server = await connect_to_server(
        {
            "server_url": SERVER_URL,
            "token": token,
        }
    )

    if not isinstance(server, RemoteService):
        raise TypeError("connect_to_server did not return a RemoteService instance")

    return server


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
