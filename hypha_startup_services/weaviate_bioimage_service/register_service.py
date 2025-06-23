"""Service registration for the Weaviate BioImage service."""

import logging
from functools import partial
from hypha_rpc.rpc import RemoteService
from weaviate import WeaviateAsyncClient

from hypha_startup_services.weaviate_service.client import instantiate_and_connect
from hypha_startup_services.weaviate_bioimage_service.methods import (
    query,
    get_entity,
)

logger = logging.getLogger(__name__)

DEFAULT_SERVICE_ID = "weaviate-bioimage-service"


async def register_weaviate_bioimage(
    server: RemoteService, service_id: str = DEFAULT_SERVICE_ID
) -> None:
    """Register the Weaviate BioImage service with the Hypha server.

    This is the main registration function that follows the standard pattern.
    It creates the Weaviate client internally.

    Args:
        server: RemoteService instance for service registration
        service_id: Unique identifier for the service
    """
    # Create Weaviate client
    weaviate_client = await instantiate_and_connect()

    # Register the service
    await register_weaviate_bioimage_service(
        server=server,
        weaviate_client=weaviate_client,
        service_id=service_id,
    )


async def register_weaviate_bioimage_service(
    server: RemoteService,
    weaviate_client: WeaviateAsyncClient,
    service_id: str = DEFAULT_SERVICE_ID,
) -> None:
    """
    Register the Weaviate BioImage service with the Hypha server.

    Args:
        server: RemoteService instance for service registration
        weaviate_client: Weaviate client instance
        service_id: Unique identifier for the service
    """

    # Register the service
    await server.register_service(
        {
            "name": "Weaviate BioImage Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": True,
            },
            "query": partial(
                query,
                client=weaviate_client,
                server=server,
            ),
            "get_entity": partial(
                get_entity,
                client=weaviate_client,
                server=server,
            ),
        }
    )

    logger.info("Weaviate BioImage service registered with ID: %s", service_id)
