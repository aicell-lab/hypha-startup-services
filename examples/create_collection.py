"""Example create collection script."""

import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

from hypha_startup_services.weaviate_service.register_service import register_weaviate

load_dotenv(override=True)
SERVICE_NAME = "weaviate-test"

logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_server(server_url: str) -> AsyncGenerator[RemoteService, Any]:
    """Get Hypha server."""
    token = os.environ.get("HYPHA_TOKEN")
    if token is None:
        error_msg = "HYPHA_TOKEN environment variable is not set"
        raise ValueError(error_msg)

    server: RemoteService = await connect_to_server(
        {
            "server_url": server_url,
            "token": token,
        },
    )
    await register_weaviate(server, SERVICE_NAME)
    try:
        yield server
    finally:
        await server.disconnect()


async def create_document_collection(
    weaviate_service: RemoteService,
    ollama_model: str,
    ollama_endpoint: str,
):
    """Create a document collection in Weaviate.

    Args:
        weaviate_service (RemoteService): The Weaviate service
        ollama_model (str): The Ollama model to use
        ollama_endpoint (str): The Ollama API endpoint

    """
    await weaviate_service.collections.delete("Document")

    class_obj = {
        "class": "Document",
        "multiTenancyConfig": {
            "enabled": True,
        },
        "description": "A Document class",
        "properties": [
            {
                "name": "application_id",
                "dataType": ["text"],
                "description": "The ID of the application",
            },
            {
                "name": "name",
                "dataType": ["text"],
                "description": "The name of the document",
            },
            {
                "name": "content",
                "dataType": ["text"],
                "description": "The content of the document",
            },
            {
                "name": "metadata",
                "dataType": ["object"],
                "description": "The metadata of the document",
                "nestedProperties": [
                    # Journal, URL, publication date, type
                    {
                        "name": "journal",
                        "dataType": ["text"],
                        "description": "The journal of the document",
                    },
                    {
                        "name": "url",
                        "dataType": ["text"],
                        "description": "The URL of the document",
                    },
                    {
                        "name": "publication_date",
                        "dataType": ["date"],
                        "description": "The publication date of the document",
                    },
                    {
                        "name": "type",
                        "dataType": ["text"],
                        "description": "The type of the document",
                    },
                ],
            },
        ],
        "vectorConfig": {
            "title_vector": {
                "vectorizer": {
                    "text2vec-ollama": {
                        "model": ollama_model,
                        "apiEndpoint": ollama_endpoint,
                    },
                },
                "sourceProperties": ["title"],
                "vectorIndexType": "hnsw",  # Added this line
                "vectorIndexConfig": {  # Optional but recommended for completeness
                    "distance": "cosine",
                },
            },
            "description_vector": {
                "vectorizer": {
                    "text2vec-ollama": {
                        "model": ollama_model,
                        "apiEndpoint": ollama_endpoint,
                    },
                },
                "sourceProperties": ["description"],
                "vectorIndexType": "hnsw",  # Added this line
                "vectorIndexConfig": {  # Optional but recommended for completeness
                    "distance": "cosine",
                },
            },
        },
        "moduleConfig": {
            "generative-ollama": {
                "model": ollama_model,
                "apiEndpoint": ollama_endpoint,
            },
        },
    }

    await weaviate_service.collections.create(class_obj)

    print("Done")


async def main():
    async with get_server("https://hypha.aicell.io") as server:
        service = await server.get_service(SERVICE_NAME)
        await create_document_collection(
            service,
            ollama_model="llama3.2",
            ollama_endpoint="https://hypha-ollama.scilifelab-2-dev.sys.kth.se",
        )


if __name__ == "__main__":
    asyncio.run(main())
