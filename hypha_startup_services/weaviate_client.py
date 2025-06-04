from dotenv import load_dotenv
from weaviate.classes.init import AdditionalConfig
from weaviate.connect import ConnectionParams
from weaviate import WeaviateAsyncClient

load_dotenv()


async def instantiate_and_connect() -> WeaviateAsyncClient:
    """
    Instantiate and connect to Weaviate client.
    """
    client = WeaviateAsyncClient(
        connection_params=ConnectionParams.from_params(
            http_host="hypha-weaviate.scilifelab-2-dev.sys.kth.se",
            http_port=443,
            http_secure=True,
            grpc_host="hypha-weaviate-grpc.scilifelab-2-dev.sys.kth.se",
            grpc_port=443,
            grpc_secure=True,
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
