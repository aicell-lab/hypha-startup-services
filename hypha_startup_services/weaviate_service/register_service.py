"""
Helper functions to register the Weaviate service with proper API endpoints.
"""

import logging
from functools import partial
from hypha_rpc.rpc import RemoteService
from weaviate import WeaviateAsyncClient
from hypha_startup_services.weaviate_service.service_codecs import (
    register_weaviate_codecs,
)
from hypha_startup_services.weaviate_service.client import (
    instantiate_and_connect,
)
from hypha_startup_services.weaviate_service.methods import (
    collections_create,
    collections_delete,
    collections_list_all,
    collections_get,
    collections_exists,
    applications_create,
    applications_delete,
    applications_get,
    applications_exists,
    data_insert_many,
    data_insert,
    data_update,
    data_delete_by_id,
    data_delete_many,
    data_exists,
    query_near_vector,
    query_fetch_objects,
    query_hybrid,
    generate_near_text,
)
from hypha_startup_services.weaviate_service.utils.constants import DEFAULT_SERVICE_ID

logger = logging.getLogger(__name__)


async def register_weaviate(
    server: RemoteService, service_id: str = DEFAULT_SERVICE_ID
) -> None:
    """Register the Weaviate service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """
    register_weaviate_codecs(server)
    client = await instantiate_and_connect()

    await register_weaviate_service(server, client, service_id)


async def register_weaviate_service(
    server: RemoteService, client: WeaviateAsyncClient, service_id: str
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
            "collections": {
                "create": partial(collections_create, client, server),
                "delete": partial(collections_delete, client, server),
                "list_all": partial(collections_list_all, client),
                "get": partial(collections_get, client, server),
                "exists": partial(collections_exists, client, server),
            },
            "applications": {
                "create": partial(applications_create, client, server),
                "delete": partial(applications_delete, client, server),
                "get": partial(applications_get, server),
                "exists": partial(applications_exists, server),
            },
            "data": {
                "insert_many": partial(data_insert_many, client, server),
                "insert": partial(data_insert, client, server),
                "update": partial(data_update, client, server),
                "delete_by_id": partial(data_delete_by_id, client, server),
                "delete_many": partial(data_delete_many, client, server),
                "exists": partial(data_exists, client, server),
            },
            "query": {
                "near_vector": partial(query_near_vector, client, server),
                "fetch_objects": partial(query_fetch_objects, client, server),
                "hybrid": partial(query_hybrid, client, server),
            },
            "generate": {
                "near_text": partial(generate_near_text, client, server),
            },
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
