from mem0 import Memory
from dotenv import load_dotenv

# client = WeaviateAsyncClient(
#     additional_config=AdditionalConfig(
#         timeout_config=(5, 60),  # (connection_timeout_sec, request_timeout_sec)
#         grpc_insecure=True,  # Skip SSL verification for gRPC
#         grpc_insecure_auth=True,  # Allow insecure authentication
#     ),
#     skip_init_checks=True,
# )


def get_mem0(http_url: str) -> Memory:
    """
    Register the memory service with the Hypha server.
    Sets up all service endpoints for collections, data operations, and queries.
    """
    load_dotenv()

    http_url = "https://hypha-weaviate.scilifelab-2-dev.sys.kth.se:443"
    # grpc_url = "https://hypha-weaviate-grpc.scilifelab-2-dev.sys.kth.se:443"

    config = {
        "vector_store": {
            "provider": "weaviate",
            "config": {
                "collection_name": "hypha-agents",
                "cluster_url": http_url,
                # Don't include auth_client_secret at all when not needed
                # This avoids the TypeError when concatenating None with a string
            },
        }
    }

    return Memory.from_config(config)
