#!/usr/bin/env python3
"""
Populate the shared bioimage application with EuroBioImaging data.

This script uses the Weaviate service directly to insert nodes and technologies data
into the shared application that was created by the bioimage service.

This is the primary script for populating the remote shared bioimage database.
It handles data preparation, batching, and error recovery.

Usage:
    python examples/populate_shared_bioimage_data.py

Prerequisites:
    - HYPHA_TOKEN environment variable set
    - EuroBioImaging data files present in the assets directory
"""

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants
COLLECTION_NAME = "bioimage_data"
TEST_COLLECTION_NAME = "bioimage_data_test"
SHARED_APPLICATION_ID = "eurobioimaging-shared"
DEFAULT_SERVER_URL = "https://hypha.aicell.io"
WEAVIATE_SERVICE_ID = "aria-agents/weaviate"


async def load_data_files() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Load nodes and technologies data from JSON files."""
    script_dir = Path(__file__).parent.parent

    # Load nodes data
    nodes_file = script_dir / "common" / "assets" / "ebi-nodes.json"
    with open(nodes_file, "r", encoding="utf-8") as f:
        nodes_data = json.load(f)

    # Load technologies data
    tech_file = script_dir / "common" / "assets" / "ebi-tech.json"
    with open(tech_file, "r", encoding="utf-8") as f:
        tech_data = json.load(f)

    logger.info("Loaded %d nodes and %d technologies", len(nodes_data), len(tech_data))
    return nodes_data, tech_data


def prepare_node_objects(nodes_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            "entity_id": node.get("id", ""),  # Changed from entity_id to ebi_id
            "text": ". ".join(text_parts),
        }
        objects.append(obj)

    return objects


def prepare_technology_objects(tech_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


async def get_weaviate_service(server_url: str, service_id: str | None = None):
    """Connect to the Weaviate service."""
    if service_id is None:
        service_id = WEAVIATE_SERVICE_ID

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
    logger.info("Getting Weaviate service: %s", service_id)
    weaviate_service = await server.get_service(service_id)
    logger.info("Connected to Weaviate service")

    return server, weaviate_service


async def ensure_collection_exists(
    weaviate_service,
    collection_name: str,
    ollama_model: str = "mxbai-embed-large:latest",
    ollama_llm_model: str = "qwen2.5:7b",
    ollama_endpoint: str = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se",
) -> None:
    """Ensure the bioimage collection exists with proper configuration."""
    try:
        # Check if collection exists
        exists = await weaviate_service.collections.exists(collection_name)
        if exists:
            logger.info("Collection %s already exists", collection_name)
            return
    except Exception as e:
        logger.warning("Error checking if collection exists: %s", e)

    logger.info("Creating collection %s...", collection_name)

    class_obj = {
        "class": collection_name,
        "description": "EuroBioImaging nodes and technologies data",
        "properties": [
            {
                "name": "application_id",
                "dataType": ["text"],
                "description": "The ID of the application",
            },
            {
                "name": "entity_id",
                "dataType": ["text"],
                "description": "Unique identifier for the entity (node or technology)",
            },
            {
                "name": "entity_type",
                "dataType": ["text"],
                "description": "Type of entity: 'node' or 'technology'",
            },
            {
                "name": "name",
                "dataType": ["text"],
                "description": "Name of the node or technology",
            },
            {
                "name": "text",
                "dataType": ["text"],
                "description": "Searchable text content (chunked if necessary)",
            },
            {
                "name": "description",
                "dataType": ["text"],
                "description": "Full description of the entity",
            },
            {
                "name": "country",
                "dataType": ["text"],
                "description": "Country for nodes",
            },
            {
                "name": "category",
                "dataType": ["text"],
                "description": "Category for technologies",
            },
            {
                "name": "chunk_index",
                "dataType": ["int"],
                "description": "Index of the chunk if text was chunked",
            },
            {
                "name": "total_chunks",
                "dataType": ["int"],
                "description": "Total number of chunks for this entity",
            },
        ],
        "vectorConfig": {
            "text_vector": {
                "vectorizer": {
                    "text2vec-ollama": {
                        "model": ollama_model,
                        "apiEndpoint": ollama_endpoint,
                    }
                },
                "sourceProperties": ["text", "name"],
                "vectorIndexType": "hnsw",
                "vectorIndexConfig": {"distance": "cosine"},
            },
            "description_vector": {
                "vectorizer": {
                    "text2vec-ollama": {
                        "model": ollama_model,
                        "apiEndpoint": ollama_endpoint,
                    }
                },
                "sourceProperties": ["description"],
                "vectorIndexType": "hnsw",
                "vectorIndexConfig": {"distance": "cosine"},
            },
        },
        "moduleConfig": {
            "generative-ollama": {
                "model": ollama_llm_model,
                "apiEndpoint": ollama_endpoint,
            }
        },
    }

    try:
        await weaviate_service.collections.create(class_obj)
        logger.info("✅ Created collection %s successfully", COLLECTION_NAME)
    except Exception as e:
        logger.error("Failed to create collection %s: %s", COLLECTION_NAME, e)
        raise


async def ensure_application_exists(
    weaviate_service, collection_name: str, application_id: str
) -> None:
    """Ensure the shared bioimage application exists."""
    try:
        # Check if application exists
        exists = await weaviate_service.applications.exists(
            collection_name=collection_name,
            application_id=application_id,
        )
        if exists:
            logger.info(
                "Application %s already exists in collection %s",
                application_id,
                collection_name,
            )
            return
    except Exception as e:
        logger.warning("Error checking if application exists: %s", e)

    logger.info(
        "Creating application %s in collection %s...", application_id, collection_name
    )

    try:
        await weaviate_service.applications.create(
            collection_name=collection_name,
            application_id=application_id,
            description="Shared EuroBioImaging nodes and technologies database",
        )
        logger.info("✅ Created application %s successfully", SHARED_APPLICATION_ID)
    except Exception as e:
        logger.error("Failed to create application %s: %s", SHARED_APPLICATION_ID, e)
        raise


async def insert_data_in_batches(
    weaviate_service,
    objects: list[dict[str, Any]],
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
                enable_chunking=True,
                chunk_size=512,
                text_field="text",
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


async def populate_collection(
    weaviate_service,
    collection_name: str,
    application_id: str,
    delete_existing: bool,
    ollama_model: str,
    ollama_llm_model: str,
    ollama_endpoint: str,
) -> None:
    """Populate a specific collection with bioimage data."""
    logger.info("🔧 Ensuring collection %s exists...", collection_name)

    if delete_existing:
        try:
            await weaviate_service.collections.delete(name=collection_name)
            logger.info("Deleted existing collection %s", collection_name)
        except Exception as e:
            logger.warning("Error deleting collection %s: %s", collection_name, e)

    await ensure_collection_exists(
        weaviate_service,
        collection_name=collection_name,
        ollama_model=ollama_model,
        ollama_llm_model=ollama_llm_model,
        ollama_endpoint=ollama_endpoint,
    )

    if delete_existing:
        try:
            await weaviate_service.applications.delete(
                collection_name=collection_name,
                application_id=application_id,
            )
            logger.info(
                "Deleted existing application %s in collection %s",
                application_id,
                collection_name,
            )
        except Exception as e:
            logger.warning("Error deleting application: %s", e)

    # Ensure application exists
    logger.info("🔧 Ensuring application exists in collection %s...", collection_name)
    await ensure_application_exists(weaviate_service, collection_name, application_id)

    # Load data
    logger.info("📊 Loading bioimage data for collection %s...", collection_name)
    nodes_data, tech_data = await load_data_files()

    # Prepare objects
    nodes_objects = prepare_node_objects(nodes_data)
    tech_objects = prepare_technology_objects(tech_data)

    logger.info(
        "Found %d nodes and %d technologies", len(nodes_objects), len(tech_objects)
    )

    # Insert data in batches
    all_objects = nodes_objects + tech_objects

    if all_objects:
        logger.info(
            "💾 Inserting %d objects into collection %s...",
            len(all_objects),
            collection_name,
        )
        await weaviate_service.data.insert_many(
            collection_name=collection_name,
            application_id=application_id,
            objects=all_objects,
        )
        logger.info("✅ Successfully inserted data into collection %s", collection_name)
    else:
        logger.warning("⚠️  No objects to insert into collection %s", collection_name)


async def main():
    """Main function to populate shared bioimage application with data."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Populate shared bioimage application with data"
    )
    parser.add_argument(
        "--ollama-model",
        default="mxbai-embed-large:latest",
        help="Ollama model to use for embeddings (default: mxbai-embed-large:latest)",
    )
    parser.add_argument(
        "--ollama-llm-model",
        default="qwen2.5:7b",
        help="Ollama model to use for generation (default: qwen2.5:7b)",
    )
    parser.add_argument(
        "--ollama-endpoint",
        default="https://hypha-ollama.scilifelab-2-dev.sys.kth.se",
        help="Ollama endpoint URL (default: https://hypha-ollama.scilifelab-2-dev.sys.kth.se)",
    )
    parser.add_argument(
        "--delete-existing",
        action="store_true",
        help="Delete all existing objects in the shared application before populating",
    )

    args = parser.parse_args()

    logger.info("🚀 Populating shared bioimage applications with data")
    logger.info("Using Ollama endpoint: %s", args.ollama_endpoint)
    logger.info("Using embedding model: %s", args.ollama_model)
    logger.info("Using generation model: %s", args.ollama_llm_model)

    test_server = None
    prod_server = None

    try:
        # Create TEST collection (uses external Weaviate URL)
        logger.info("=" * 60)
        logger.info("🧪 SETTING UP TEST COLLECTION: %s", TEST_COLLECTION_NAME)
        logger.info("=" * 60)

        test_server, test_weaviate_service = await get_weaviate_service(
            DEFAULT_SERVER_URL
        )

        await populate_collection(
            weaviate_service=test_weaviate_service,
            collection_name=TEST_COLLECTION_NAME,
            application_id=SHARED_APPLICATION_ID,
            delete_existing=args.delete_existing,
            ollama_model=args.ollama_model,
            ollama_llm_model=args.ollama_llm_model,
            ollama_endpoint=args.ollama_endpoint,
        )

        logger.info("✅ Test collection %s setup complete!", TEST_COLLECTION_NAME)

        # Create PRODUCTION collection (uses internal K8s Weaviate URL)
        logger.info("=" * 60)
        logger.info("🚀 SETTING UP PRODUCTION COLLECTION: %s", COLLECTION_NAME)
        logger.info("=" * 60)

        try:
            prod_server, prod_weaviate_service = await get_weaviate_service(
                DEFAULT_SERVER_URL
            )

            await populate_collection(
                weaviate_service=prod_weaviate_service,
                collection_name=COLLECTION_NAME,
                application_id=SHARED_APPLICATION_ID,
                delete_existing=args.delete_existing,
                ollama_model=args.ollama_model,
                ollama_llm_model=args.ollama_llm_model,
                ollama_endpoint=args.ollama_endpoint,
            )

            logger.info("✅ Production collection %s setup complete!", COLLECTION_NAME)

        except Exception as e:
            logger.error(
                "❌ Failed to setup production collection (this is expected if not in K8s): %s",
                e,
            )
            logger.info(
                "ℹ️  Production collection will be available when deployed to Kubernetes"
            )

        logger.info("=" * 60)
        logger.info("🎉 ALL COLLECTIONS SETUP COMPLETE!")
        logger.info(
            "   • Test collection: %s (external Weaviate)", TEST_COLLECTION_NAME
        )
        logger.info(
            "   • Production collection: %s (internal K8s Weaviate)", COLLECTION_NAME
        )
        logger.info("=" * 60)
    except Exception as e:
        logger.error("❌ Error during data population: %s", e)
        raise

    finally:
        # Disconnect from servers
        if test_server is not None:
            try:
                await test_server.disconnect()
                logger.info("Disconnected from test server")
            except Exception as e:
                logger.warning("Error disconnecting from test server: %s", e)

        if prod_server is not None:
            try:
                await prod_server.disconnect()
                logger.info("Disconnected from production server")
            except Exception as e:
                logger.warning("Error disconnecting from production server: %s", e)


if __name__ == "__main__":
    asyncio.run(main())
