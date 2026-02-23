"""Validate Weaviate CI secrets/variables against live token identities.

This script is designed for GitHub Actions. It verifies that required
environment variables are present, confirms each token resolves to the
expected workspace, and optionally checks embedding backend reachability
when embedding tests are enabled.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from http.client import HTTPSConnection
from urllib.parse import urlparse

from scripts.hypha_connection import connect_with_fallback

DEFAULT_SERVER_URL = "https://hypha.aicell.io"
SERVER_URL_ENV_VAR = "HYPHA_SERVER_URL"
EMBEDDING_ENV_VAR = "WEAVIATE_TEST_ENABLE_EMBEDDING"
OLLAMA_ENDPOINT = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se"
REQUEST_TIMEOUT_SECONDS = 10
HTTP_SERVER_ERROR_THRESHOLD = 500

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TokenWorkspacePair:
    """Map one token environment variable to one workspace variable."""

    token_env_name: str
    workspace_env_name: str


REQUIRED_TOKEN_WORKSPACE_PAIRS: tuple[TokenWorkspacePair, ...] = (
    TokenWorkspacePair("PERSONAL_TOKEN", "USER1_WS"),
    TokenWorkspacePair("PERSONAL_TOKEN2", "USER2_WS"),
    TokenWorkspacePair("PERSONAL_TOKEN3", "USER3_WS"),
)


def _get_missing_env_names(required_names: list[str]) -> list[str]:
    """Return required names that are missing or empty."""
    return [name for name in required_names if not os.environ.get(name)]


def _embedding_enabled() -> bool:
    """Return whether embedding-dependent tests are enabled."""
    value = os.environ.get(EMBEDDING_ENV_VAR, "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _check_ollama_reachability() -> tuple[bool, str]:
    """Check Ollama endpoint reachability for embedding mode."""
    parsed_endpoint = urlparse(OLLAMA_ENDPOINT)
    if parsed_endpoint.scheme != "https" or not parsed_endpoint.netloc:
        return False, "Embedding backend endpoint must be https."

    connection = HTTPSConnection(
        host=parsed_endpoint.netloc,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    request_path = parsed_endpoint.path or "/"
    try:
        connection.request("GET", request_path)
        response = connection.getresponse()
        status_code = int(response.status)
        if status_code >= HTTP_SERVER_ERROR_THRESHOLD:
            return False, f"Embedding backend responded with HTTP {status_code}."
    except OSError as error:
        return False, f"Embedding backend unreachable: {error!s}"
    else:
        return True, f"Embedding backend reachable (HTTP {status_code})."
    finally:
        connection.close()


async def _resolve_workspace_for_token(token: str) -> str:
    """Resolve workspace ID from a Hypha token by connecting to the server."""
    server_url = os.environ.get(SERVER_URL_ENV_VAR, DEFAULT_SERVER_URL)
    server = await connect_with_fallback(server_url=server_url, token=token)
    try:
        return str(server.config.workspace)
    finally:
        await server.disconnect()


async def _validate_token_workspace_pairs() -> tuple[bool, list[str]]:
    """Validate that each token belongs to the declared workspace."""
    errors: list[str] = []
    for pair in REQUIRED_TOKEN_WORKSPACE_PAIRS:
        token = os.environ[pair.token_env_name]
        expected_workspace = os.environ[pair.workspace_env_name]
        resolved_workspace = await _resolve_workspace_for_token(token)
        if resolved_workspace != expected_workspace:
            error_message = (
                "Workspace mismatch for "
                f"{pair.token_env_name}: expected {expected_workspace}, "
                f"resolved {resolved_workspace}."
            )
            errors.append(error_message)
    return (len(errors) == 0), errors


async def _main() -> int:
    """Run CI preflight checks and return process exit code."""
    required_names = [
        pair.token_env_name for pair in REQUIRED_TOKEN_WORKSPACE_PAIRS
    ] + [pair.workspace_env_name for pair in REQUIRED_TOKEN_WORKSPACE_PAIRS]

    missing_names = _get_missing_env_names(required_names)
    if missing_names:
        logger.error("Missing required CI value(s):")
        for name in missing_names:
            logger.error("- %s", name)
        return 1

    token_workspace_valid, token_workspace_errors = (
        await _validate_token_workspace_pairs()
    )
    if not token_workspace_valid:
        logger.error("Token/workspace validation failed:")
        for error in token_workspace_errors:
            logger.error("- %s", error)
        return 1

    if _embedding_enabled():
        embedding_ok, embedding_message = _check_ollama_reachability()
        logger.info(embedding_message)
        if not embedding_ok:
            return 1

    logger.info("CI preflight checks passed.")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(asyncio.run(_main()))
