"""Common utilities for Weaviate tests."""

from hypha_rpc.rpc import RemoteException

APP_ID = "TestApp"
USER1_APP_ID = "User1App"
USER2_APP_ID = "User2App"
USER3_APP_ID = "User3App"
SHARED_APP_ID = "SharedApp"

# Common test objects
MOVIE_COLLECTION_CONFIG = {
    "class": "Movie",
    "description": "A movie class",
    "multiTenancyConfig": {
        "enabled": True,
    },
    "properties": [
        {
            "name": "title",
            "dataType": ["text"],
            "description": "The title of the movie",
        },
        {
            "name": "description",
            "dataType": ["text"],
            "description": "A description of the movie",
        },
        {
            "name": "genre",
            "dataType": ["text"],
            "description": "The genre of the movie",
        },
        {
            "name": "year",
            "dataType": ["int"],
            "description": "The year the movie was released",
        },
        {
            "name": "application_id",
            "dataType": ["text"],
            "description": "The ID of the application",
        },
    ],
}


# Common test helpers
async def create_test_collection(weaviate_service):
    """Create a test collection for Weaviate tests."""
    ollama_endpoint = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se"
    ollama_model = "llama3.2"  # For embeddings - using an available model

    # Try to delete if it exists - ignore errors
    try:
        await weaviate_service.collections.delete("Movie")
    except RemoteException as e:
        print(f"Error deleting collection: {e}")

    class_obj = MOVIE_COLLECTION_CONFIG.copy()
    # Add vector configurations
    class_obj["vectorConfig"] = {
        "title_vector": {
            "vectorizer": {
                "text2vec-ollama": {
                    "model": ollama_model,
                    "apiEndpoint": ollama_endpoint,
                }
            },
            "sourceProperties": ["title"],
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
    }
    class_obj["moduleConfig"] = {
        "generative-ollama": {
            "model": ollama_model,
            "apiEndpoint": ollama_endpoint,
        }
    }

    collection = await weaviate_service.collections.create(class_obj)
    return collection


async def create_test_application(weaviate_service):
    """Create a test application for Weaviate tests."""
    await create_test_collection(weaviate_service)
    # TODO: fix this uncommented code
    # if await weaviate_service.applications.exists(
    #     collection_name="Movie", application_id=APP_ID
    # ):
    #     # If the application already exists, delete it first
    #     await weaviate_service.applications.delete(
    #         collection_name="Movie", application_id=APP_ID
    #     )

    await weaviate_service.applications.create(
        application_id=APP_ID,
        collection_name="Movie",
        description="An application for movie data",
    )
