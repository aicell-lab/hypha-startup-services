"""
Helper functions to register the BioImage service with proper API endpoints.
"""

import logging
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.bioimage_service.data_index import load_external_data
from hypha_startup_services.bioimage_service.methods import (
    create_get_entity_details,
    create_get_related_entities,
    create_query,
)
from hypha_startup_services.mem0_service.mem0_client import get_mem0

logger = logging.getLogger(__name__)


async def register_bioimage_service(
    server: RemoteService, service_id: str = "bioimage"
) -> None:
    """Register the BioImage service with entity-agnostic methods.

    Sets up all service endpoints with both traditional exact matching and semantic search.
    Only entity-agnostic methods are exposed - no node/technology-specific endpoints.
    """

    bioimage_index = load_external_data()

    memory = await get_mem0()

    get_entity_details_func = create_get_entity_details(bioimage_index)
    get_related_entities_func = create_get_related_entities(bioimage_index)
    query_func = create_query(memory, bioimage_index)

    await server.register_service(
        {
            "name": "Hypha BioImage Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": False,
            },
            "get": get_entity_details_func,
            "get_related": get_related_entities_func,
            "query": query_func,
        }
    )

    logger.info(
        "BioImage Service registered at %s/%s/services/%s",
        server.config.public_base_url,
        server.config.workspace,
        service_id,
    )
