"""
Helper functions to register the Weaviate service with proper API endpoints.
"""

from functools import partial
from hypha_rpc.rpc import RemoteService
from mem0 import AsyncMemory
from hypha_startup_services.mem0 import get_mem0
from hypha_startup_services.mem0_methods import mem0_add, mem0_search, init_run


async def register_mem0(server: RemoteService, service_id: str) -> None:
    """Register the Weaviate service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """
    mem0 = await get_mem0()
    await register_mem0_service(server, mem0, service_id)

    print(
        "Service registered at",
        f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
    )


async def register_mem0_service(
    server: RemoteService, mem0: AsyncMemory, service_id: str
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
            "init": partial(init_run, server=server),
            "add": partial(mem0_add, server=server, memory=mem0),
            "search": partial(mem0_search, server=server, memory=mem0),
        }
    )
