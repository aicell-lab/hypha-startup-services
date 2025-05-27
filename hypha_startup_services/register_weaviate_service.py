"""
Helper functions to register the Weaviate service with proper API endpoints.
"""

from functools import partial
from hypha_rpc.rpc import RemoteService
from mem0 import Memory
from hypha_startup_services.mem0 import get_mem0
from hypha_startup_services.mem0_methods import mem0_add


async def register_weaviate(server: RemoteService, service_id: str) -> None:
    """Register the Weaviate service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """
    mem0 = get_mem0(server.config.public_base_url)
    await register_weaviate_service(server, mem0, service_id)

    print(
        "Service registered at",
        f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
    )


async def register_weaviate_service(
    server: RemoteService, mem0: Memory, service_id: str
) -> None:
    """Register the Weaviate service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """

    await server.register_service(
        {
            "name": "Hypha Weaviate Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": True,
            },
            "mem0": {
                "add": partial(mem0_add, memory=mem0),
            },
        }
    )
