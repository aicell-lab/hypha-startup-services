"""
Helper functions to register the Weaviate service with proper API endpoints.
"""

from functools import partial
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.mem0_service.mem0_client import get_mem0
from hypha_startup_services.mem0_service.methods import (
    mem0_add,
    mem0_search,
    mem0_delete_all,
    init_run,
    init_agent,
)
from hypha_startup_services.mem0_service.utils.constants import DEFAULT_SERVICE_ID


async def register_mem0_service(
    server: RemoteService, service_id: str = DEFAULT_SERVICE_ID
) -> None:
    """Register the Weaviate service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """

    mem0 = await get_mem0()

    await server.register_service(
        {
            "name": "Hypha Mem0 Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": True,
            },
            "init_agent": partial(init_agent, server=server),
            "init": partial(init_run, server=server),
            "add": partial(mem0_add, server=server, memory=mem0),
            "search": partial(mem0_search, server=server, memory=mem0),
            "delete_all": partial(mem0_delete_all, server=server, memory=mem0),
        }
    )

    print(
        "Service registered at",
        f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
    )
