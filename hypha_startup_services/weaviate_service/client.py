"""Instantiate and connect to Weaviate client."""

import os
from dotenv import load_dotenv
from weaviate import WeaviateAsyncClient
from weaviate.classes.init import AdditionalConfig
from weaviate.connect import ConnectionParams

load_dotenv(override=True)


async def instantiate_and_connect() -> WeaviateAsyncClient:
    """Instantiate and connect to Weaviate client."""
    client = WeaviateAsyncClient(
        connection_params=ConnectionParams.from_params(
            http_host=os.environ.get(
                "WEAVIATE_HTTP_HOST",
                "hypha-weaviate.scilifelab-2-dev.sys.kth.se",
            ),
            http_port=int(os.environ.get("WEAVIATE_HTTP_PORT", 443)),
            http_secure=os.environ.get("WEAVIATE_HTTP_SECURE", "true").lower()
            == "true",
            grpc_host=os.environ.get(
                "WEAVIATE_GRPC_HOST",
                "hypha-weaviate-grpc.scilifelab-2-dev.sys.kth.se",
            ),
            grpc_port=int(os.environ.get("WEAVIATE_GRPC_PORT", 443)),
            grpc_secure=os.environ.get("WEAVIATE_GRPC_SECURE", "true").lower()
            == "true",
        ),
        additional_config=AdditionalConfig(
            timeout=(60, 180),  # (connection_timeout_sec, request_timeout_sec)
            trust_env=True,  # Use environment variables for connection params
        ),
        skip_init_checks=True,
    )
    await client.connect()
    return client
