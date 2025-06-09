"""Shared test fixtures and utilities across all test suites."""

import os
import pytest_asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService


# Common constants
SERVER_URL = "https://hypha.aicell.io"

# User workspace IDs - shared across test suites
USER1_WS = "ws-user-google-oauth2|104255278140940970953"  # Admin user
USER2_WS = "ws-user-google-oauth2|101844867326318340275"  # Regular user
USER3_WS = "ws-user-google-oauth2|101564907182096510974"  # Regular user

# Token environments for different users
TOKEN_ENVS = ["PERSONAL_TOKEN", "PERSONAL_TOKEN2", "PERSONAL_TOKEN3"]


async def get_user_server(token_env="PERSONAL_TOKEN"):
    """Get a user server connection with the specified token."""
    load_dotenv()
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


def create_service_fixtures(service_name: str, service_id: str, setup_func=None):
    """Create service fixtures for all three users.

    Args:
        service_name: Name for the fixture (e.g., 'weaviate_service')
        service_id: Service ID to connect to (e.g., 'aria-agents/weaviate-test')
        setup_func: Optional function to call on server before getting service

    Returns:
        Dict of fixture functions for pytest registration
    """
    fixtures = {}

    for i, token_env in enumerate(TOKEN_ENVS, 1):
        fixture_name = f"{service_name}" if i == 1 else f"{service_name}{i}"

        @pytest_asyncio.fixture
        async def service_fixture(token_env=token_env):
            server = await get_user_server(token_env)
            if setup_func:
                setup_func(server)
            service = await server.get_service(service_id)
            yield service
            await server.disconnect()

        service_fixture.__name__ = fixture_name
        fixtures[fixture_name] = service_fixture

    return fixtures


# Common test constants
APP_ID = "TestApp"
USER1_APP_ID = "User1App"
USER2_APP_ID = "User2App"
USER3_APP_ID = "User3App"
SHARED_APP_ID = "SharedApp"
