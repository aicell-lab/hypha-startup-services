"""Helper functions to register the Weaviate service with proper API endpoints."""

import asyncio
import logging
from functools import partial

from hypha_rpc.rpc import RemoteService
from weaviate import WeaviateAsyncClient

from hypha_startup_services.common.constants import (
    DEFAULT_WEAVIATE_SERVICE_ID as DEFAULT_SERVICE_ID,
)

from .client import (
    instantiate_and_connect,
)
from .methods import (
    applications_create,
    applications_delete,
    applications_exists,
    applications_get,
    applications_get_artifact,
    applications_set_permissions,
    collections_create,
    collections_delete,
    collections_exists,
    collections_get,
    collections_get_artifact,
    collections_list_all,
    data_delete_by_id,
    data_delete_many,
    data_exists,
    data_insert,
    data_insert_many,
    data_update,
    generate_near_text,
    query_fetch_objects,
    query_hybrid,
    query_near_vector,
)
from .service_codecs import (
    register_weaviate_codecs,
)

logger = logging.getLogger(__name__)

# Set to keep references to background tasks to prevent garbage collection
_background_tasks: set[asyncio.Task[None]] = set()


async def register_weaviate(
    server: RemoteService,
    service_id: str = DEFAULT_SERVICE_ID,
) -> None:
    """Register the Weaviate service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """
    register_weaviate_codecs(server)
    client = await instantiate_and_connect()

    await register_weaviate_service(server, client, service_id)


def get_weaviate_service_def(
    server: RemoteService,
    client: WeaviateAsyncClient,
    service_id: str,
) -> dict:
    """Get the Weaviate service definition dictionary."""
    return {
        "name": "Hypha Weaviate Service",
        "id": service_id,
        "config": {
            "visibility": "public",
            "require_context": True,
        },
        "collections": {
            "create": partial(collections_create, client, server=server),
            "delete": partial(collections_delete, client, server=server),
            "list_all": partial(collections_list_all, client),
            "get": partial(collections_get, client),
            "exists": partial(collections_exists, client, server=server),
            "get_artifact": partial(collections_get_artifact, client, server=server),
        },
        "applications": {
            "create": partial(applications_create, client, server=server),
            "delete": partial(applications_delete, client, server=server),
            "get": partial(applications_get, client, server=server),
            "exists": partial(applications_exists, client, server=server),
            "get_artifact": partial(applications_get_artifact, client, server=server),
            "set_permissions": partial(
                applications_set_permissions,
                client,
                server=server,
            ),
        },
        "data": {
            "insert_many": partial(data_insert_many, client, server=server),
            "insert": partial(data_insert, client, server=server),
            "update": partial(data_update, client, server=server),
            "delete_by_id": partial(data_delete_by_id, client, server=server),
            "delete_many": partial(data_delete_many, client, server=server),
            "exists": partial(data_exists, client, server=server),
        },
        "query": {
            "near_vector": partial(query_near_vector, client, server=server),
            "fetch_objects": partial(query_fetch_objects, client, server=server),
            "hybrid": partial(query_hybrid, client, server=server),
        },
        "generate": {
            "near_text": partial(generate_near_text, client, server=server),
        },
    }


async def register_weaviate_service(
    server: RemoteService,
    client: WeaviateAsyncClient,
    service_id: str,
) -> None:
    """Register the Weaviate service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """
    service_def = get_weaviate_service_def(server, client, service_id)
    await server.register_service(service_def)

    logger.info(
        "Service %s registered at %s/%s/services/%s:%s",
        service_id,
        server.config.public_base_url,
        server.config.workspace,
        server.config.client_id,
        service_id,
    )
