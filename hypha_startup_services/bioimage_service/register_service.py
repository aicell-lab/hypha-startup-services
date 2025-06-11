"""
Helper functions to register the BioImage service with proper API endpoints.
"""

from functools import partial
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.bioimage_service.data_index import load_external_data
from hypha_startup_services.bioimage_service.methods import (
    get_nodes_by_technology_id,
    get_technologies_by_node_id,
    get_node_details,
    get_technology_details,
    search_nodes,
    search_technologies,
    get_all_nodes,
    get_all_technologies,
    get_service_statistics,
)


async def register_bioimage_service(
    server: RemoteService, service_id: str = "bioimage"
) -> None:
    """Register the BioImage service with the Hypha server.

    Sets up all service endpoints for exact matching of EBI nodes and technologies.
    """

    # Load the bioimage index data
    bioimage_index = load_external_data()

    await server.register_service(
        {
            "name": "Hypha BioImage Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": False,  # No authentication required for read operations
            },
            # Core relationship methods
            "get_nodes_by_technology_id": partial(
                get_nodes_by_technology_id, bioimage_index=bioimage_index
            ),
            "get_technologies_by_node_id": partial(
                get_technologies_by_node_id, bioimage_index=bioimage_index
            ),
            # Detail methods
            "get_node_details": partial(
                get_node_details, bioimage_index=bioimage_index
            ),
            "get_technology_details": partial(
                get_technology_details, bioimage_index=bioimage_index
            ),
            # Search methods
            "search_nodes": partial(search_nodes, bioimage_index=bioimage_index),
            "search_technologies": partial(
                search_technologies, bioimage_index=bioimage_index
            ),
            # List all methods
            "get_all_nodes": partial(get_all_nodes, bioimage_index=bioimage_index),
            "get_all_technologies": partial(
                get_all_technologies, bioimage_index=bioimage_index
            ),
            # Service info
            "get_statistics": partial(
                get_service_statistics, bioimage_index=bioimage_index
            ),
        }
    )

    print(
        "BioImage Service registered at",
        f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
    )
