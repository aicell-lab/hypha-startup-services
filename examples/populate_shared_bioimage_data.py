#!/usr/bin/env python3
"""
Populate the shared bioimage application with EuroBioImaging data.

This script uses the Weaviate service directly to insert nodes and technologies data
into the shared application that was created by the bioimage service.
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
COLLECTION_NAME = "bioimage_data"
SHARED_APPLICATION_ID = "eurobioimaging-shared"
DEFAULT_SERVER_URL = "https://hypha.aicell.io"
WEAVIATE_SERVICE_ID = "aria-agents/weaviate"


async def load_data_files() -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Load nodes and technologies data from JSON files."""
    script_dir = Path(__file__).parent.parent

    # Load nodes data
    nodes_file = (
        script_dir
        / "hypha_startup_services"
        / "mem0_bioimage_service"
        / "assets"
        / "ebi-nodes.json"
    )
    with open(nodes_file, "r", encoding="utf-8") as f:
        nodes_data = json.load(f)

    # Load technologies data
    tech_file = (
        script_dir
        / "hypha_startup_services"
        / "mem0_bioimage_service"
        / "assets"
        / "ebi-tech.json"
    )
    with open(tech_file, "r", encoding="utf-8") as f:
        tech_data = json.load(f)

    logger.info("Loaded %d nodes and %d technologies", len(nodes_data), len(tech_data))
    return nodes_data, tech_data


def prepare_node_objects(nodes_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Prepare node objects for insertion into Weaviate."""
    objects = []

    for node in nodes_data:
        # Extract country name from nested object if it exists
        country = node.get("country", {})
        country_name = (
            country.get("name", "") if isinstance(country, dict) else str(country)
        )

        # Create a comprehensive text field for vector search
        text_parts = [
            f"Bioimaging node: {node.get('name', 'Unknown')}",
            f"in {country_name}",
        ]

        if node.get("description"):
            text_parts.append(f"Description: {node['description']}")

        obj = {
            "entity_type": "node",
            "name": node.get("name", ""),
            "description": node.get("description", ""),
            "country": country_name,  # Use only the name, not the full object
            "ebi_id": node.get("id", ""),  # Changed from entity_id to ebi_id
            "text": ". ".join(text_parts),
        }
        objects.append(obj)

    return objects


def prepare_technology_objects(tech_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Prepare technology objects for insertion into Weaviate."""
    objects = []

    for tech in tech_data:
        # Create a comprehensive text field for vector search
        text_parts = [
            f"Bioimaging technology: {tech.get('name', 'Unknown')}",
        ]

        # Extract category name from nested object if it exists
        category = tech.get("category", {})
        category_name = (
            category.get("name", "") if isinstance(category, dict) else str(category)
        )

        if category_name:
            text_parts.append(f"Category: {category_name}")

        if tech.get("description"):
            text_parts.append(f"Description: {tech['description']}")

        obj = {
            "entity_type": "technology",
            "name": tech.get("name", ""),
            "description": tech.get("description", ""),
            "category": category_name,  # Use only the name, not the full object
            "entity_id": tech.get("id", ""),
            "text": ". ".join(text_parts),
        }
        objects.append(obj)

    return objects


async def get_weaviate_service(server_url: str):
    """Connect to the Weaviate service."""
    logger.info("Connecting to server: %s", server_url)

    # Get token from environment
    token = os.environ.get("HYPHA_TOKEN")
    if not token:
        raise ValueError("HYPHA_TOKEN environment variable is required")

    server: RemoteService = await connect_to_server(  # type: ignore
        {
            "name": "bioimage-data-populator",
            "server_url": server_url,
            "token": token,
            "method_timeout": 60,
        }
    )

    # Get the Weaviate service
    logger.info("Getting Weaviate service: %s", WEAVIATE_SERVICE_ID)
    weaviate_service = await server.get_service(WEAVIATE_SERVICE_ID)
    logger.info("Connected to Weaviate service")

    return server, weaviate_service


async def insert_data_in_batches(
    weaviate_service,
    objects: List[Dict[str, Any]],
    data_type: str,
    batch_size: int = 10,
):
    """Insert data objects into the shared application in smaller batches."""
    logger.info(
        "Inserting %d %s objects in batches of %d...",
        len(objects),
        data_type,
        batch_size,
    )

    # Debug: Check first object for any id fields
    if objects:
        first_obj = objects[0]
        logger.info(
            "DEBUG: First %s object keys: %s", data_type, list(first_obj.keys())
        )
        for key, value in first_obj.items():
            if "id" in key.lower():
                logger.info("DEBUG: Found ID-like field: %s = %s", key, value)
            if isinstance(value, dict) and "id" in value:
                logger.warning("DEBUG: Found nested id in %s: %s", key, value)
        # Assert no 'id' field exists in any object
        for i, obj in enumerate(objects[:5]):  # Check first 5 objects
            assert "id" not in obj, f"Object {i} contains 'id' field: {obj}"
        logger.info("✅ Verified: No 'id' fields found in %s objects", data_type)

    total_results = {"has_errors": False, "successful": 0, "failed": 0, "errors": []}

    # Split objects into batches
    for i in range(0, len(objects), batch_size):
        batch = objects[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(objects) + batch_size - 1) // batch_size

        logger.info(
            "Processing batch %d/%d (%d objects)...",
            batch_num,
            total_batches,
            len(batch),
        )

        try:
            result = await weaviate_service.data.insert_many(
                collection_name=COLLECTION_NAME,
                application_id=SHARED_APPLICATION_ID,
                objects=batch,
            )

            if result.get("has_errors"):
                logger.warning(
                    "Batch %d had errors: %s", batch_num, result.get("errors")
                )
                total_results["has_errors"] = True
                total_results["errors"].extend(result.get("errors", []))
                total_results["failed"] += len(batch)
            else:
                logger.info("✅ Batch %d completed successfully", batch_num)
                total_results["successful"] += len(batch)

        except (TimeoutError, ConnectionError, Exception) as e:
            logger.error("Failed to insert batch %d: %s", batch_num, e)
            total_results["has_errors"] = True
            total_results["failed"] += len(batch)
            total_results["errors"].append(f"Batch {batch_num}: {str(e)}")

    logger.info(
        "Batch insertion completed: %d successful, %d failed",
        total_results["successful"],
        total_results["failed"],
    )
    return total_results


async def main():
    """Main function to populate shared bioimage application with data."""
    logger.info("🚀 Populating shared bioimage application with data")

    server = None
    try:
        # Connect to server
        server, weaviate_service = await get_weaviate_service(DEFAULT_SERVER_URL)

        # Load data
        nodes_data, tech_data = await load_data_files()

        # Prepare objects
        node_objects = prepare_node_objects(nodes_data)
        tech_objects = prepare_technology_objects(tech_data)

        # Insert data in batches
        logger.info("📥 Inserting bioimage data into shared application...")
        nodes_result = await insert_data_in_batches(
            weaviate_service, node_objects, "nodes"
        )
        tech_result = await insert_data_in_batches(
            weaviate_service, tech_objects, "technologies"
        )

        # Summary
        logger.info("✅ Data population completed!")
        logger.info("Application ID: %s", SHARED_APPLICATION_ID)
        logger.info("Collection: %s", COLLECTION_NAME)
        logger.info("Nodes processed: %d", len(node_objects))
        logger.info("Technologies processed: %d", len(tech_objects))

        # Check for errors
        nodes_errors = nodes_result.get("has_errors", False)
        tech_errors = tech_result.get("has_errors", False)

        if nodes_errors or tech_errors:
            logger.warning("⚠️ Some errors occurred during data insertion:")
            if nodes_errors:
                logger.warning(
                    "- Node insertion errors: %s", nodes_result.get("errors", "Unknown")
                )
            if tech_errors:
                logger.warning(
                    "- Technology insertion errors: %s",
                    tech_result.get("errors", "Unknown"),
                )
        else:
            logger.info("🎉 All data inserted successfully!")

    except Exception as e:
        logger.error("Error populating shared application: %s", e)
        raise

    finally:
        if server is not None:
            await server.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
