#!/usr/bin/env python3
"""
Script to create the bioimage collection in Weaviate.

This script creates a Weaviate collection specifically designed for bioimage data
(nodes and technologies from EuroBioImaging).

Prerequisites:
- Docker running with Weaviate (use: docker compose -f docker/docker-compose.yaml up)
- Weaviate service registered with Hypha

Usage:
    python examples/create_bioimage_collection.py
"""

import asyncio
import logging
import os
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService
from typing import cast
from hypha_startup_services.weaviate_service.register_service import register_weaviate

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_NAME = "weaviate-bioimage-collection"


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


async def get_weaviate_service():
    """Get the Weaviate service for collection management."""
    server = await get_server("http://localhost:9527")
    return await server.get_service(SERVICE_NAME)


async def create_bioimage_collection(
    weaviate_service,
    ollama_model: str = "llama3.2",
    ollama_endpoint: str = "http://localhost:11434",
) -> None:
    """Create the bioimage collection in Weaviate.

    Args:
        weaviate_service: Weaviate service instance
        ollama_model: Ollama model to use for vectorization
        ollama_endpoint: Ollama API endpoint
    """
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
                "name": "metadata",
                "dataType": ["object"],
                "description": "Additional metadata",
                "nestedProperties": [
                    {
                        "name": "country",
                        "dataType": ["text"],
                        "description": "Country for nodes",
                    },
                    {
                        "name": "institution",
                        "dataType": ["text"],
                        "description": "Institution for nodes",
                    },
                    {
                        "name": "contact_email",
                        "dataType": ["text"],
                        "description": "Contact email",
                    },
                    {
                        "name": "website",
                        "dataType": ["text"],
                        "description": "Website URL",
                    },
                    {
                        "name": "technologies",
                        "dataType": ["text[]"],
                        "description": "List of technology IDs for nodes",
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


async def main():
    """Main function to create the bioimage collection."""
    try:
        weaviate_service = await get_weaviate_service()
        await create_bioimage_collection(weaviate_service)
        logger.info("Bioimage collection creation completed successfully")
    except (ValueError, RuntimeError, ConnectionError) as e:
        logger.error("Failed to create bioimage collection: %s", str(e))
        raise


if __name__ == "__main__":
    asyncio.run(main())
