import os
from mem0 import AsyncMemory
from dotenv import load_dotenv

# client = WeaviateAsyncClient(
#     additional_config=AdditionalConfig(
#         timeout_config=(5, 60),  # (connection_timeout_sec, request_timeout_sec)
#         grpc_insecure=True,  # Skip SSL verification for gRPC
#         grpc_insecure_auth=True,  # Allow insecure authentication
#     ),
#     skip_init_checks=True,
# )


async def get_mem0() -> AsyncMemory:
    """
    Register the memory service with the Hypha server.
    Sets up all service endpoints for collections, data operations, and queries.
    """
    load_dotenv()

    grpc_url = "hypha-weaviate-grpc.scilifelab-2-dev.sys.kth.se"
    http_url = "hypha-weaviate.scilifelab-2-dev.sys.kth.se"

    config = {
        "vector_store": {
            "provider": "weaviate",
            "config": {
                "collection_name": "Document",
                "cluster_url": http_url,
                # "auth_client_secret": os.environ["WEAVIATE_ADMIN_KEY"],
            },
        }
    }

    return await AsyncMemory.from_config(config)
