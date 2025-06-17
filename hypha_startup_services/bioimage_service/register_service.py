"""
Helper functions to register the BioImage service with proper API endpoints.
"""

from functools import partial
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.bioimage_service.data_index import load_external_data
from hypha_startup_services.bioimage_service.methods import (
    get_entity_details,
    get_related_entities,
    query,
)
from hypha_startup_services.mem0_service.mem0_client import get_mem0


async def register_bioimage_service(
    server: RemoteService, service_id: str = "bioimage"
) -> None:
    """Register the BioImage service with entity-agnostic methods.

    Sets up all service endpoints with both traditional exact matching and semantic search.
    Only entity-agnostic methods are exposed - no node/technology-specific endpoints.
    """

    # Load the bioimage index data for exact matching
    bioimage_index = load_external_data()

    # Get mem0 instance for semantic search
    memory = await get_mem0()

    await server.register_service(
        {
            "name": "Hypha BioImage Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": False,  # No authentication required for read operations
            },
            # Entity-agnostic methods (preferred)
            "get": partial(get_entity_details, bioimage_index=bioimage_index),
            "get_related": partial(get_related_entities, bioimage_index=bioimage_index),
            "query": partial(
                query,
                memory=memory,
                bioimage_index=bioimage_index,
            ),
        }
    )

    print(
        "BioImage Service registered at",
        f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
    )
