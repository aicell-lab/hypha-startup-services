"""Shared test fixtures and utilities across all test suites."""

import os
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService


# Common constants
SERVER_URL = "https://hypha.aicell.io"

# User workspace IDs - shared across test suites
USER1_WS = "ws-user-google-oauth2|104255278140940970953"  # Admin user
USER2_WS = "ws-user-google-oauth2|101844867326318340275"  # Regular user
USER3_WS = "ws-user-google-oauth2|101564907182096510974"  # Regular user


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


# Common test constants
APP_ID = "TestApp"
USER1_APP_ID = "User1App"
USER2_APP_ID = "User2App"
USER3_APP_ID = "User3App"
SHARED_APP_ID = "SharedApp"
