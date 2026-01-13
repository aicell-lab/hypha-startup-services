"""Cleanup service for Weaviate resources."""

import asyncio
import logging

from hypha_rpc.rpc import RemoteService
from weaviate import WeaviateAsyncClient

from hypha_startup_services.common.constants import COLLECTION_DELIMITER

logger = logging.getLogger(__name__)

CLEANUP_INTERVAL = 60 * 60  # 1 hour


async def start_cleanup_loop(
    server: RemoteService,
    client: WeaviateAsyncClient,
    interval: int = CLEANUP_INTERVAL,
) -> None:
    """Start the cleanup loop."""
    while True:
        try:
            await cleanup_weaviate_resources(server, client)
        except Exception:
            logger.exception("Error in Weaviate cleanup loop")

        await asyncio.sleep(interval)


async def cleanup_weaviate_resources(
    server: RemoteService,
    client: WeaviateAsyncClient,
) -> None:
    """Cleanup orphaned artifacts and collections.

    This function:
    1. Fetches artifacts from the workspace (hypha-agents)
    2. Identifies Weaviate collection artifacts (containing __DELIM__)
    3. Lists actual Weaviate collections
    4. Deletes artifacts that have no corresponding Weaviate collection
    5. Deletes Weaviate collections that have no corresponding artifact
    """
    logger.info("Running Weaviate resource cleanup...")

    artifact_manager = await server.get_service("public/artifact-manager")
    workspace = server.config.workspace

    # 1. List artifacts
    try:
        artifacts = await artifact_manager.list(context={"workspace": workspace})
    except Exception:
        logger.exception("Failed to list artifacts for cleanup")
        return

    # 2. Identify Weaviate collection artifacts
    collection_artifacts = [
        a for a in artifacts if COLLECTION_DELIMITER in a.get("alias", "")
    ]

    collection_artifact_aliases = {
        a.get("alias") for a in collection_artifacts if a.get("alias")
    }

    # 3. List actual Weaviate collections
    # weaviate_collections returns a dict of name -> collection object
    weaviate_collections = await client.collections.list_all(simple=False)
    weaviate_collection_names = set(weaviate_collections.keys())

    # 4. Delete artifacts with no corresponding collection
    for artifact in collection_artifacts:
        alias = artifact.get("alias")
        if alias and alias not in weaviate_collection_names:
            logger.info(
                "Deleting orphaned artifact: %s (No corresponding Weaviate collection)",
                alias,
            )
            try:
                await artifact_manager.delete(artifact_id=alias, recursive=True)
            except Exception:
                logger.exception("Failed to delete orphaned artifact: %s", alias)
                return

    # 5. Delete collections with no corresponding artifact
    for coll_name in weaviate_collection_names:
        # Only consider collections that look like our managed collections
        if (
            COLLECTION_DELIMITER in coll_name
            and coll_name not in collection_artifact_aliases
        ):
            logger.info(
                "Deleting orphaned collection: %s (No corresponding artifact)",
                coll_name,
            )
            try:
                await client.collections.delete(coll_name)
            except Exception:
                logger.exception(
                    "Failed to delete orphaned collection: %s",
                    coll_name,
                )
                return
