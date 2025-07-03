#!/usr/bin/env python3
"""
Export all objects from the shared bioimage application in Weaviate to a JSON file.
"""
import asyncio
import json
import logging
import os
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

COLLECTION_NAME = "bioimage_data"
SHARED_APPLICATION_ID = "eurobioimaging-shared"
DEFAULT_SERVER_URL = "https://hypha.aicell.io"
WEAVIATE_SERVICE_ID = "aria-agents/weaviate-test"
OUTPUT_FILE = "bioimage_data_export.json"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def export_all_objects():
    token = os.environ.get("HYPHA_TOKEN")
    if not token:
        raise RuntimeError("HYPHA_TOKEN environment variable is required")

    logger.info("Connecting to server: %s", DEFAULT_SERVER_URL)
    server: RemoteService = await connect_to_server(  # type: ignore
        {
            "server_url": DEFAULT_SERVER_URL,
            "token": token,
        }
    )
    weaviate_service = await server.get_service(WEAVIATE_SERVICE_ID)

    logger.info(
        "Fetching all objects from collection '%s', application '%s'...",
        COLLECTION_NAME,
        SHARED_APPLICATION_ID,
    )
    all_objects = []
    batch_size = 100
    offset = 0
    while True:
        result = await weaviate_service.query.fetch_objects(
            collection_name=COLLECTION_NAME,
            application_id=SHARED_APPLICATION_ID,
            limit=batch_size,
            offset=offset,
        )
        objects = result.get("objects", [])
        if not objects:
            break
        all_objects.extend(objects)
        logger.info(
            "Fetched %d objects (total so far: %d)", len(objects), len(all_objects)
        )
        if len(objects) < batch_size:
            break
        offset += batch_size

    logger.info(
        "Fetched a total of %d objects. Writing to %s", len(all_objects), OUTPUT_FILE
    )
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_objects, f, indent=2, ensure_ascii=False)
    logger.info("Export complete.")
    await server.disconnect()


if __name__ == "__main__":
    asyncio.run(export_all_objects())
