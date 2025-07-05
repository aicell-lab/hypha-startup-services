from mem0 import AsyncMemory
from dotenv import load_dotenv
from .weaviate_patches import apply_all_patches
import logging

logger = logging.getLogger(__name__)


async def get_mem0() -> AsyncMemory:
    """
    Register the memory service with the Hypha server.
    Sets up all service endpoints for collections, data operations, and queries.
    """
    load_dotenv()

    # Apply patches to fix Weaviate metadata and score issues
    patches_applied = apply_all_patches()
    if patches_applied:
        logger.info("Weaviate patches applied successfully")
    else:
        logger.warning("Failed to apply some Weaviate patches")

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
                "model": "qwen2.5:7b",
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
        # Set API version to v1.1 for the latest format
        "version": "v1.1",
    }

    return await AsyncMemory.from_config(config)
