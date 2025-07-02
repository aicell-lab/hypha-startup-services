"""
Helper functions to register the Weaviate service with proper API endpoints.
"""

import logging
from functools import partial
from hypha_rpc.rpc import RemoteService
from .mem0_client import get_mem0
from .methods import (
    mem0_add,
    mem0_search,
    mem0_delete_all,
    mem0_get_all,
    init_run,
    init_agent,
)
from .utils.constants import DEFAULT_SERVICE_ID

logger = logging.getLogger(__name__)


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
            "init_agent": init_agent,
            "init": init_run,
            "add": partial(mem0_add, memory=mem0),
            "search": partial(mem0_search, memory=mem0),
            "delete_all": partial(mem0_delete_all, memory=mem0),
            "get_all": partial(mem0_get_all, memory=mem0),
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
