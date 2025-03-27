from dotenv import load_dotenv
from weaviate.classes.init import AdditionalConfig
from weaviate.connect import ConnectionParams
from weaviate import WeaviateAsyncClient

load_dotenv()


async def instantiate_and_connect(
    http_host: str, is_secure: bool, grpc_host: str, is_grpc_secure: bool
) -> WeaviateAsyncClient:
    """
    Instantiate and connect to Weaviate client.
    """
    client = WeaviateAsyncClient(
        connection_params=ConnectionParams.from_params(
            http_host=http_host,
            http_port=443 if is_secure else 80,
            http_secure=is_secure,
            grpc_host=grpc_host,
            grpc_port=443 if is_grpc_secure else 50051,
            grpc_secure=is_grpc_secure,
        ),
        additional_config=AdditionalConfig(
            timeout_config=(5, 60),  # (connection_timeout_sec, request_timeout_sec)
            grpc_insecure=True,  # Skip SSL verification for gRPC
            grpc_insecure_auth=True,  # Allow insecure authentication
        ),
        skip_init_checks=True,
    )
    await client.connect()
    return client
