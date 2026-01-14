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
DEFAULT_TOKEN_NAME = TOKEN_ENVS[0]


async def get_user_server(token_env: str = DEFAULT_TOKEN_NAME) -> RemoteService:
    """Get a user server connection with the specified token."""
    load_dotenv(override=True)
    token = os.environ.get(token_env)

    if token is None:
        error_msg = f"{token_env} environment variable is not set"
        raise ValueError(error_msg)

    return await connect_to_server(
        {
            "server_url": SERVER_URL,
            "token": token,
        },
    )
