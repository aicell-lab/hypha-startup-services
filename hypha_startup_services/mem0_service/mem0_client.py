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
        },
        "llm": {
            "provider": "ollama",
            "config": {
                "model": "qwen3:8b",
                # "temperature": 0,
                "max_tokens": 2000,
                "ollama_base_url": "https://hypha-ollama.scilifelab-2-dev.sys.kth.se",  # Ensure this URL is correct
            },
        },
        "embedder": {
            "provider": "ollama",
            "config": {
                "model": "mxbai-embed-large:latest",
                # Alternatively, you can use "snowflake-arctic-embed:latest"
                "ollama_base_url": "https://hypha-ollama.scilifelab-2-dev.sys.kth.se",
            },
        },
    }

    return await AsyncMemory.from_config(config)
