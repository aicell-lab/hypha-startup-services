"""
Helper functions to register the Weaviate service with proper API endpoints.
"""

from functools import partial
from hypha_startup_services.start_weaviate_service import (
    collections_create,
    collections_delete,
    collections_list_all,
    collections_get,
    collections_exists,
    applications_create,
    applications_delete,
    applications_list_all,
    applications_get,
    applications_exists,
    sessions_create,
    sessions_list_all,
    sessions_delete,
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


def register_weaviate_service(server, client, service_id):
    """Register the Weaviate service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """

    return server.register_service(
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
                "list_all": partial(collections_list_all, client, server),
                "get": partial(collections_get, client, server),
                "exists": partial(collections_exists, client),
            },
            "applications": {
                "create": partial(applications_create, client, server),
                "delete": partial(applications_delete, client, server),
                "list_all": partial(applications_list_all, server),
                "get": partial(applications_get, server),
                "exists": partial(applications_exists, server),
            },
            "sessions": {
                "create": partial(sessions_create, server),
                "list_all": partial(sessions_list_all, server),
                "delete": partial(sessions_delete, server),
            },
            "data": {
                "insert_many": partial(data_insert_many, client),
                "insert": partial(data_insert, client),
                "update": partial(data_update, client),
                "delete_by_id": partial(data_delete_by_id, client),
                "delete_many": partial(data_delete_many, client),
                "exists": partial(data_exists, client),
            },
            "query": {
                "near_vector": partial(query_near_vector, client),
                "fetch_objects": partial(query_fetch_objects, client),
                "hybrid": partial(query_hybrid, client),
            },
            "generate": {
                "near_text": partial(generate_near_text, client),
            },
        }
    )
