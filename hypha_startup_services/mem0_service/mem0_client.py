from mem0 import AsyncMemory
from dotenv import load_dotenv


async def get_mem0() -> AsyncMemory:
    """
    Register the memory service with the Hypha server.
    Sets up all service endpoints for collections, data operations, and queries.
    """
    load_dotenv()

    config = {
        "vector_store": {
            "provider": "weaviate",
            "config": {
                "collection_name": "Document",
                "cluster_url": "localhost:8080",
            },
        }
    }

    return await AsyncMemory.from_config(config)
