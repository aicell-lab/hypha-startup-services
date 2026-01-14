#!/usr/bin/env python3
"""Script to cleanup test artifacts from Hypha.

This script removes all artifacts that contain "Shared__DELIM__test_weaviate_"
in their alias.
"""

import asyncio
import logging
import os
from typing import Any, cast

from hypha_rpc import connect_to_server

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_SERVER_URL = "https://hypha.aicell.io"
ARTIFACT_MANAGER_ID = "public/artifact-manager"
TARGET_ALIAS_PATTERN = "Shared__DELIM__test_weaviate_"


async def cleanup_test_artifacts() -> None:
    """Cleanup test artifacts."""
    logger.info("🧹 Starting test artifact cleanup")

    # Get token from environment
    token = os.environ.get("HYPHA_TOKEN")
    if not token:
        # Try to load formatted token from env file if available, or just fail
        logger.warning(
            "HYPHA_TOKEN not found in environment, "
            "connection might fail if authentication is required",
        )

    connect_kwargs: dict[str, Any] = {
        "name": "cleanup-script",
        "server_url": DEFAULT_SERVER_URL,
        "method_timeout": 30,
    }
    if token:
        connect_kwargs["token"] = token

    # Connect to server
    try:
        server = await connect_to_server(connect_kwargs)
    except Exception:
        logger.exception("Failed to connect to server")
        return

    try:
        # Get the artifact manager
        logger.info("Getting artifact manager...")
        artifact_manager = await server.get_service(ARTIFACT_MANAGER_ID)

        logger.info("Listing artifacts...")
        artifacts = await artifact_manager.list()

        # Filter artifacts
        artifacts_to_delete: list[str] = []
        for artifact in artifacts:
            alias = cast("str | None", artifact.get("alias", ""))
            if alias and TARGET_ALIAS_PATTERN in alias:
                artifacts_to_delete.append(alias)

        logger.info("Found %d artifacts to delete", len(artifacts_to_delete))

        # Delete artifacts
        for alias in artifacts_to_delete:
            logger.info("Deleting artifact: %s", alias)
            try:
                await artifact_manager.delete(
                    artifact_id=alias,
                    recursive=True,
                )
                logger.info("✅ Deleted %s", alias)
            except Exception:
                logger.exception("❌ Failed to delete %s", alias)
                await server.disconnect()
                return

        logger.info("\n✨ Cleanup completed!")

    finally:
        await server.disconnect()


if __name__ == "__main__":
    asyncio.run(cleanup_test_artifacts())
