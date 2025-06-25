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
            timeout=(10, 180),  # (connection_timeout_sec, request_timeout_sec)
            trust_env=True,  # Use environment variables for connection params
        ),
        skip_init_checks=True,
    )
    await client.connect()
    return client
