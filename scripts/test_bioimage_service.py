#!/usr/bin/env python3
"""Test the bioimage service with the new EuroBioImaging data.

This script tests various queries against the bioimage service to ensure
the data is properly loaded and searchable.
"""

import asyncio
import logging
import os

from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_SERVER_URL = "https://hypha.aicell.io"
BIOIMAGE_SERVICE_ID = "weaviate-bioimage"


async def test_bioimage_service():
    """Test various queries against the bioimage service."""
    logger.info("🧪 Testing bioimage service with new data")

    # Get token from environment
    token = os.environ.get("HYPHA_TOKEN")
    if not token:
        raise ValueError("HYPHA_TOKEN environment variable is required")

    # Connect to server
    server: RemoteService = await connect_to_server(
        {  # type: ignore
            "name": "bioimage-tester",
            "server_url": DEFAULT_SERVER_URL,
            "token": token,
            "method_timeout": 30,
        },
    )

    try:
        # Get the bioimage service
        logger.info("Getting bioimage service: %s", BIOIMAGE_SERVICE_ID)
        bioimage_service = await server.get_service(BIOIMAGE_SERVICE_ID)

        # Test 1: Search for nodes
        logger.info("\n🔍 Test 1: Search for nodes")
        try:
            result = await bioimage_service.search_nodes(
                query="microscopy nodes in Netherlands",
                limit=5,
            )
            logger.info("Found %d nodes", len(result.get("nodes", [])))
            for node in result.get("nodes", [])[:3]:  # Show first 3
                logger.info("  - %s (%s)", node.get("name"), node.get("country"))
        except Exception as e:
            logger.error("Error searching nodes: %s", e)

        # Test 2: Search for technologies
        logger.info("\n🔍 Test 2: Search for technologies")
        try:
            result = await bioimage_service.search_technologies(
                query="electron microscopy",
                limit=5,
            )
            logger.info("Found %d technologies", len(result.get("technologies", [])))
            for tech in result.get("technologies", [])[:3]:  # Show first 3
                logger.info("  - %s (%s)", tech.get("name"), tech.get("category"))
        except Exception as e:
            logger.error("Error searching technologies: %s", e)

        # Test 3: Get nodes by country
        logger.info("\n🔍 Test 3: Get nodes by country")
        try:
            result = await bioimage_service.get_nodes_by_country(
                country="NETHERLANDS",
                limit=5,
            )
            logger.info("Found %d nodes in Netherlands", len(result.get("nodes", [])))
            for node in result.get("nodes", [])[:3]:  # Show first 3
                logger.info("  - %s", node.get("name"))
        except Exception as e:
            logger.error("Error getting nodes by country: %s", e)

        # Test 4: Get technologies by category
        logger.info("\n🔍 Test 4: Get technologies by category")
        try:
            result = await bioimage_service.get_technologies_by_category(
                category="Microscopy",
                limit=5,
            )
            logger.info(
                "Found %d microscopy technologies",
                len(result.get("technologies", [])),
            )
            for tech in result.get("technologies", [])[:3]:  # Show first 3
                logger.info("  - %s", tech.get("name"))
        except Exception as e:
            logger.error("Error getting technologies by category: %s", e)

        # Test 5: General search
        logger.info("\n🔍 Test 5: General search")
        try:
            result = await bioimage_service.search(
                query="super resolution microscopy",
                limit=5,
            )
            logger.info("Found %d total results", len(result.get("results", [])))
            for item in result.get("results", [])[:3]:  # Show first 3
                logger.info("  - %s (%s)", item.get("name"), item.get("entity_type"))
        except Exception as e:
            logger.error("Error in general search: %s", e)

        logger.info("\n✅ Testing completed!")

    finally:
        await server.disconnect()


if __name__ == "__main__":
    asyncio.run(test_bioimage_service())
