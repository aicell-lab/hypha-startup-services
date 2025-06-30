"""
Helper functions to register the BioImage service with proper API endpoints.
"""

import logging
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.common.utils import create_partial_with_schema
from hypha_startup_services.common.data_index import (
    load_external_data,
    get_entity_details,
    get_related_entities,
)
from hypha_startup_services.mem0_service.mem0_client import get_mem0
from .methods import search

logger = logging.getLogger(__name__)


async def register_mem0_bioimage_service(
    server: RemoteService, service_id: str = "bioimage"
) -> None:
    """Register the Mem0-BioImage service with entity-agnostic methods.

    Sets up all service endpoints with both traditional exact matching and semantic search.
    The query method supports filtering by entity types through the entity_types parameter.
    """

    bioimage_index = load_external_data()

    memory = await get_mem0()

    # Create partial functions with dependency injection while preserving schemas
    get_entity_details_func = create_partial_with_schema(
        get_entity_details, bioimage_index=bioimage_index
    )
    get_related_entities_func = create_partial_with_schema(
        get_related_entities, bioimage_index=bioimage_index
    )
    search_func = create_partial_with_schema(
        search, memory=memory, bioimage_index=bioimage_index
    )

    await server.register_service(
        {
            "name": "Hypha Mem0-BioImage Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": False,
            },
            "get": get_entity_details_func,
            "get_related": get_related_entities_func,
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
