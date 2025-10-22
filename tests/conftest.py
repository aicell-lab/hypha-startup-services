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

# Token environments for different users
TOKEN_ENVS = ["PERSONAL_TOKEN", "PERSONAL_TOKEN2", "PERSONAL_TOKEN3"]


async def get_user_server(token_env="PERSONAL_TOKEN"):
    """Get a user server connection with the specified token."""
    load_dotenv(override=True)
    token = os.environ.get(token_env)
    assert token is not None, f"{token_env} environment variable is not set"
    server = await connect_to_server(
        {
            "server_url": SERVER_URL,
            "token": token,
        },
    )

    if not isinstance(server, RemoteService):
        raise TypeError("connect_to_server did not return a RemoteService instance")

    return server
