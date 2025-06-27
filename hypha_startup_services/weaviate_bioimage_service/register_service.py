"""Service registration for the Weaviate BioImage service."""

import logging
from hypha_rpc.rpc import RemoteService
from weaviate import WeaviateAsyncClient

from hypha_startup_services.common.constants import DEFAULT_WEAVIATE_BIOIMAGE_SERVICE_ID
from hypha_startup_services.common.utils import create_partial_with_schema
from hypha_startup_services.weaviate_service.client import instantiate_and_connect
from hypha_startup_services.common.data_index import (
    load_external_data,
    get_related_entities,
)
from .methods import (
    query,
    get_entity,
    search,
)

logger = logging.getLogger(__name__)


async def register_weaviate_bioimage(
    server: RemoteService, service_id: str = DEFAULT_WEAVIATE_BIOIMAGE_SERVICE_ID
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
    service_id: str = DEFAULT_WEAVIATE_BIOIMAGE_SERVICE_ID,
) -> None:
    """
    Register the Weaviate BioImage service with the Hypha server.

    Args:
        server: RemoteService instance for service registration
        weaviate_client: Weaviate client instance
        service_id: Unique identifier for the service
    """

    bioimage_index = load_external_data()

    query_func = create_partial_with_schema(
        query, client=weaviate_client, server=server
    )
    get_entity_func = create_partial_with_schema(
        get_entity, client=weaviate_client, server=server
    )
    search_func = create_partial_with_schema(
        search, client=weaviate_client, server=server, bioimage_index=bioimage_index
    )
    get_related_func = create_partial_with_schema(
        get_related_entities, bioimage_index=bioimage_index
    )

    # Register the service
    await server.register_service(
        {
            "name": "Weaviate BioImage Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": True,
            },
            "query": query_func,
            "get_related": get_related_func,
            "get_entity": get_entity_func,
            "search": search_func,
        }
    )

    logger.info(
        "Service %s registered at %s/%s/services/%s:%s",
        service_id,
        server.config.public_base_url,
        server.config.workspace,
        server.config.client_id,
        service_id,
    )
