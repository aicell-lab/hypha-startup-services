#!/usr/bin/env python3
"""
Script to build and validate the remote Weaviate-BioImage database.

This script:
1. Creates the weaviate bioimage collection
2. Ingests bioimage data into the collection
3. Validates the database by testing queries
4. Ensures data was properly stored and can be retrieved

Prerequisites:
- Docker running with Weaviate (use: docker compose -f docker/docker-compose.yaml up)
- Ollama running for embeddings

Usage:
    python examples/build_weaviate_bioimage_database.py [--server-url http://localhost:9527]
"""

import argparse
import asyncio
import logging
import os
from typing import cast
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

from hypha_startup_services.mem0_bioimage_service.data_index import load_external_data
from hypha_startup_services.weaviate_service.client import instantiate_and_connect
from hypha_startup_services.weaviate_service.register_service import register_weaviate
from hypha_startup_services.weaviate_service.methods import data_insert_many
from hypha_startup_services.weaviate_bioimage_service.data_processor import (
    process_bioimage_index,
)
from hypha_startup_services.weaviate_bioimage_service.register_service import (
    register_weaviate_bioimage,
)

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_NAME = "weaviate-bioimage-build"
BIOIMAGE_COLLECTION = "bioimage_data"
DEFAULT_APPLICATION_ID = "bioimage_app"


async def get_server(server_url: str) -> RemoteService:
    """Connect to Hypha server and register Weaviate service."""
    token = os.environ.get("HYPHA_TOKEN")
    if not token:
        logger.warning("HYPHA_TOKEN not set, using local connection")

    server = cast(
        RemoteService,
        await connect_to_server(
            {
                "server_url": server_url,
                "token": token,
            }
        ),
    )
    await register_weaviate(server, SERVICE_NAME)
    return server


async def create_bioimage_collection(
    weaviate_service,
    ollama_model: str = "llama3.2",
    ollama_endpoint: str = "http://localhost:11434",
) -> None:
    """Create the bioimage collection in Weaviate."""
    try:
        # Delete existing collection if it exists
        await weaviate_service.collections.delete("bioimage_data")
        logger.info("Deleted existing bioimage_data collection")
    except (ValueError, RuntimeError, ConnectionError) as e:
        logger.info("No existing bioimage_data collection to delete: %s", str(e))

    class_obj = {
        "class": "bioimage_data",
        "multiTenancyConfig": {
            "enabled": True,
        },
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
                "model": ollama_model,
                "apiEndpoint": ollama_endpoint,
            }
        },
    }

    await weaviate_service.collections.create(class_obj)
    logger.info("Created bioimage_data collection successfully")


async def ingest_bioimage_data(
    chunk_size: int = 512,
    chunk_overlap: int = 50,
) -> tuple[int, int, int]:
    """Ingest bioimage data into Weaviate."""
    logger.info("Starting bioimage data ingestion...")

    # Load bioimage data
    logger.info("Loading bioimage index data...")
    bioimage_index = load_external_data()

    # Process data for ingestion
    logger.info(
        "Processing bioimage data with chunk_size=%d, chunk_overlap=%d",
        chunk_size,
        chunk_overlap,
    )
    objects = process_bioimage_index(bioimage_index, chunk_size, chunk_overlap)

    # Get Weaviate client directly
    weaviate_client = await instantiate_and_connect()
    server = await get_server("http://localhost:9527")

    # Insert data
    logger.info("Inserting %d objects into Weaviate...", len(objects))
    result = await data_insert_many(
        client=weaviate_client,
        server=server,
        collection_name=BIOIMAGE_COLLECTION,
        application_id=DEFAULT_APPLICATION_ID,
        objects=objects,
        context={"user_id": "ingestion_script"},
    )

    # Add summary statistics
    nodes = bioimage_index.get_all_nodes()
    technologies = bioimage_index.get_all_technologies()

    logger.info("Ingestion completed successfully!")
    logger.info("Total objects inserted: %d", len(objects))
    logger.info("Total nodes processed: %d", len(nodes))
    logger.info("Total technologies processed: %d", len(technologies))
    logger.info("Insertion result: %s", result)

    return len(objects), len(nodes), len(technologies)


async def validate_weaviate_database(server_url: str):
    """Validate that the weaviate database was built correctly."""
    logger.info("Validating weaviate bioimage database...")

    try:
        # Register the weaviate bioimage service
        server = cast(
            RemoteService, await connect_to_server({"server_url": server_url})
        )

        await register_weaviate_bioimage(server, "weaviate-bioimage-test")
        service = await server.get_service("weaviate-bioimage-test")

        # Test queries with different entity type filters
        test_cases = [
            {
                "query_text": "microscopy facilities",
                "entity_types": None,
                "expected_min": 1,
            },
            {
                "query_text": "imaging nodes in Italy",
                "entity_types": ["node"],
                "expected_min": 1,
            },
            {
                "query_text": "confocal microscopy",
                "entity_types": ["technology"],
                "expected_min": 1,
            },
            {
                "query_text": "electron microscopy",
                "entity_types": ["technology"],
                "expected_min": 1,
            },
            {"query_text": "super-resolution", "entity_types": None, "expected_min": 1},
        ]

        all_tests_passed = True

        for test_case in test_cases:
            try:
                result = await service.query(
                    query_text=test_case["query_text"],
                    entity_types=test_case["entity_types"],
                    limit=5,
                )

                if "results" in result:
                    num_results = len(result["results"])
                    if num_results >= test_case["expected_min"]:
                        logger.info(
                            "✓ Query: '%s' with types %s returned %d results",
                            test_case["query_text"],
                            test_case["entity_types"],
                            num_results,
                        )

                        # Check first result structure
                        if num_results > 0:
                            first_result = result["results"][0]
                            if "name" in first_result and "entity_type" in first_result:
                                logger.info(
                                    "  ✓ Result structure looks good: %s - %s",
                                    first_result.get("entity_type"),
                                    first_result.get("name", "N/A"),
                                )
                            else:
                                logger.warning("  ⚠ Missing expected fields in result")
                    else:
                        logger.error(
                            "✗ Query: '%s' returned only %d results (expected >= %d)",
                            test_case["query_text"],
                            num_results,
                            test_case["expected_min"],
                        )
                        all_tests_passed = False
                else:
                    logger.error(
                        "✗ Query: '%s' returned no results field",
                        test_case["query_text"],
                    )
                    all_tests_passed = False

            except Exception as e:
                logger.error("✗ Query failed: '%s' - %s", test_case["query_text"], e)
                all_tests_passed = False

        # Test get_entity function
        try:
            # Get first result's entity_id and test get_entity
            result = await service.query(query_text="microscopy", limit=1)
            if result.get("results"):
                entity_id = result["results"][0].get("entity_id")
                if entity_id:
                    entity_result = await service.get_entity(entity_id=entity_id)
                    if "objects" in entity_result and len(entity_result["objects"]) > 0:
                        logger.info("✓ get_entity test passed for entity %s", entity_id)
                    else:
                        logger.error(
                            "✗ get_entity returned no objects for entity %s", entity_id
                        )
                        all_tests_passed = False
                else:
                    logger.warning(
                        "⚠ No entity_id found in query result for get_entity test"
                    )
            else:
                logger.warning("⚠ No results available for get_entity test")

        except Exception as e:
            logger.error(f"✗ get_entity test failed: {e}")
            all_tests_passed = False

        return all_tests_passed

    except Exception as e:
        logger.error(f"✗ Database validation failed: {e}")
        return False


async def main():
    """Main function to build and validate the weaviate-bioimage database."""
    parser = argparse.ArgumentParser(
        description="Build and validate weaviate-bioimage database"
    )
    parser.add_argument(
        "--server-url",
        default="http://localhost:9527",
        help="Hypha server URL (default: http://localhost:9527)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=512,
        help="Maximum tokens per chunk (default: 512)",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=50,
        help="Token overlap between chunks (default: 50)",
    )
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip data ingestion and only run validation",
    )

    args = parser.parse_args()

    try:
        # Step 1: Create collection (unless skipped)
        if not args.skip_ingestion:
            server = await get_server(args.server_url)
            weaviate_service = await server.get_service(SERVICE_NAME)
            await create_bioimage_collection(weaviate_service)

            # Step 2: Ingest data
            objects_count, nodes_count, technologies_count = await ingest_bioimage_data(
                chunk_size=args.chunk_size,
                chunk_overlap=args.chunk_overlap,
            )
            logger.info(
                f"Ingestion completed: {objects_count} objects ({nodes_count} nodes, {technologies_count} technologies)"
            )
        else:
            logger.info("Skipping collection creation and data ingestion")

        # Step 3: Validate database
        validation_passed = await validate_weaviate_database(args.server_url)

        # Summary
        logger.info("\n" + "=" * 50)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 50)
        logger.info(
            f"Database validation: {'PASSED' if validation_passed else 'FAILED'}"
        )

        if validation_passed:
            logger.info(
                "✓ All validations passed! Weaviate-bioimage database is ready."
            )
            return 0
        else:
            logger.error("✗ Validation failed. Check logs above.")
            return 1

    except Exception as e:
        logger.error(f"Build/validation failed: {e}")
        return 1


if __name__ == "__main__":
    exit(asyncio.run(main()))
