import os
import asyncio
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_startup_services.register_weaviate_service import register_weaviate

load_dotenv()


async def get_server(server_url: str):
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server = await connect_to_server(
        {
            "server_url": server_url,
            "token": token,
        }
    )
    await register_weaviate(server, "weaviate-test")

    return server


async def get_weaviate_service():
    """Fixture for connecting to the weaviate service.

    Use --service-id command-line option to override the default service ID.
    """
    server = await get_server("https://hypha.aicell.io")
    return await server.get_service("weaviate-test")


async def create_document_collection(
    weaviate_service,
    ollama_model: str,
    ollama_endpoint: str,
):
    await weaviate_service.collections.delete("Document")

    class_obj = {
        "class": "Document",
        "multiTenancyConfig": {
            "enabled": True,
        },
        "description": "A Document class",
        "properties": [
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
                    }
                },
                "sourceProperties": ["title"],
                "vectorIndexType": "hnsw",  # Added this line
                "vectorIndexConfig": {  # Optional but recommended for completeness
                    "distance": "cosine"
                },
            },
            "description_vector": {
                "vectorizer": {
                    "text2vec-ollama": {
                        "model": ollama_model,
                        "apiEndpoint": ollama_endpoint,
                    }
                },
                "sourceProperties": ["description"],
                "vectorIndexType": "hnsw",  # Added this line
                "vectorIndexConfig": {  # Optional but recommended for completeness
                    "distance": "cosine"
                },
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

    print("Done")


async def main():
    weaviate_service = await get_weaviate_service()
    await create_document_collection(
        weaviate_service,
        ollama_model="llama3.2",
        ollama_endpoint="https://hypha-ollama.scilifelab-2-dev.sys.kth.se",
    )


if __name__ == "__main__":
    asyncio.run(main())
