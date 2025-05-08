"""
Weaviate service implementation for Hypha.

This module provides functionality to interface with Weaviate vector database,
handling collections, data operations, and query functionality with workspace isolation.
"""

from functools import partial
import uuid
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections.classes.internal import QueryReturn, GenerativeReturn
from weaviate.collections.classes.batch import DeleteManyReturn
from hypha_startup_services.weaviate_client import instantiate_and_connect
from hypha_startup_services.service_codecs import register_weaviate_codecs
from hypha_startup_services.collection_utils import (
    full_collection_name,
    config_minus_workspace,
    name_without_workspace,
    is_in_workspace,
    ws_from_context,
    objects_without_workspace,
    acquire_collection,
    stringify_keys,
    collection_to_config_dict,
)


async def collections_exists(
    client: WeaviateAsyncClient, collection_name: str, context: dict[str, Any]
) -> bool:
    """Check if a collection exists in the workspace."""
    collection_name = full_collection_name(collection_name, context)
    return await client.collections.exists(collection_name)


async def collections_create(
    client: WeaviateAsyncClient, settings: dict, context: dict[str, Any]
) -> dict[str, Any]:
    """Create a new collection in the workspace.

    Adds workspace prefix to the collection name before creating it.
    Returns the collection configuration with the workspace prefix removed.
    """
    settings_with_workspace = settings.copy()
    settings_with_workspace["class"] = full_collection_name(
        settings_with_workspace["class"], context
    )

    collection = await client.collections.create_from_dict(
        settings_with_workspace,
    )

    return await collection_to_config_dict(collection)


async def collections_list_all(
    client: WeaviateAsyncClient, context: dict[str, Any]
) -> dict[str, dict]:
    """List all collections in the workspace.

    Returns collections with workspace prefixes removed from their names.
    """
    workspace = ws_from_context(context)
    collections = await client.collections.list_all(simple=False)
    return {
        name_without_workspace(coll_name): config_minus_workspace(coll_obj)
        for coll_name, coll_obj in collections.items()
        if is_in_workspace(coll_name, workspace)
    }


async def collections_get(
    client: WeaviateAsyncClient, name: str, context: dict[str, Any]
) -> dict[str, Any]:
    """Get a collection's configuration by name.

    Returns the collection configuration with the workspace prefix removed.
    """
    collection = acquire_collection(client, name, context)
    return await collection_to_config_dict(collection)


async def collections_delete(
    client: WeaviateAsyncClient, name: str | list[str], context: dict[str, Any]
) -> None:
    """Delete a collection or multiple collections by name.

    Adds workspace prefix to collection names before deletion.
    """
    collection_name = full_collection_name(name, context)
    await client.collections.delete(collection_name)


async def collection_data_insert_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Insert many objects into the collection.

    Forwards all kwargs to collection.data.insert_many().
    """
    collection = acquire_collection(client, collection_name, context)
    response = await collection.data.insert_many(*args, **kwargs)

    return {
        "elapsed_seconds": response.elapsed_seconds,
        "errors": stringify_keys(response.errors),
        "uuids": stringify_keys(response.uuids),
        "has_errors": response.has_errors,
    }


async def collection_data_insert(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> uuid.UUID:
    """Insert an object into the collection.

    Forwards all kwargs to collection.data.insert().
    """
    collection = acquire_collection(client, collection_name, context)
    return await collection.data.insert(*args, **kwargs)


async def collection_query_near_vector(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using a vector.

    Forwards all kwargs to collection.query.near_vector().
    """
    collection = acquire_collection(client, collection_name, context)
    response: QueryReturn = await collection.query.near_vector(*args, **kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def collection_query_fetch_objects(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Query the collection to fetch objects.

    Forwards all kwargs to collection.query.fetch_objects().
    """
    collection = acquire_collection(client, collection_name, context)
    response: QueryReturn = await collection.query.fetch_objects(*args, **kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def collection_query_hybrid(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using hybrid search.

    Forwards all kwargs to collection.query.hybrid().
    """
    collection = acquire_collection(client, collection_name, context)
    response: QueryReturn = await collection.query.hybrid(*args, **kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def collection_generate_near_text(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using near text search.

    Forwards all kwargs to collection.query.near_text().
    """
    collection = acquire_collection(client, collection_name, context)
    response: GenerativeReturn = await collection.generate.near_text(*args, **kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
        "generated": response.generated,
    }


async def collection_data_update(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> None:
    """Update an object in the collection.

    Forwards all kwargs to collection.data.update().
    """
    collection = acquire_collection(client, collection_name, context)
    await collection.data.update(*args, **kwargs)


async def collection_data_delete_by_id(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> bool:
    """Delete an object by ID from the collection.

    Forwards all kwargs to collection.data.delete_by_id().
    """
    collection = acquire_collection(client, collection_name, context)
    await collection.data.delete_by_id(*args, **kwargs)


async def collection_data_delete_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Delete many objects from the collection.

    Forwards all kwargs to collection.data.delete_many().
    """
    collection = acquire_collection(client, collection_name, context)
    response: DeleteManyReturn = await collection.data.delete_many(*args, **kwargs)

    return {
        "failed": response.failed,
        "matches": response.matches,
        "objects": objects_without_workspace(response.objects),
        "successful": response.successful,
    }


async def collection_data_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    *args,
    context: dict[str, Any],
    **kwargs,
) -> bool:
    """Check if an object exists in the collection.

    Forwards all kwargs to collection.data.exists().
    """
    collection = acquire_collection(client, collection_name, context)
    return await collection.data.exists(*args, **kwargs)


async def register_weaviate(server, service_id: str):
    """Register the Weaviate service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """
    register_weaviate_codecs(server)
    weaviate_url = "https://hypha-weaviate.scilifelab-2-dev.sys.kth.se"
    weaviate_grpc_url = "https://hypha-weaviate-grpc.scilifelab-2-dev.sys.kth.se"

    http_host = weaviate_url.replace("https://", "").replace("http://", "")
    grpc_host = weaviate_grpc_url.replace("https://", "").replace("http://", "")
    is_secure = weaviate_url.startswith("https://")
    is_grpc_secure = weaviate_grpc_url.startswith("https://")
    client = await instantiate_and_connect(
        http_host, is_secure, grpc_host, is_grpc_secure
    )

    await server.register_service(
        {
            "name": "Hypha Weaviate Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": True,
            },
            "collections": {
                "create": partial(collections_create, client),
                "delete": partial(collections_delete, client),
                "list_all": partial(collections_list_all, client),
                "get": partial(collections_get, client),
                "exists": partial(collections_exists, client),
            },
            "data": {
                "insert_many": partial(collection_data_insert_many, client),
                "insert": partial(collection_data_insert, client),
                "update": partial(collection_data_update, client),
                "delete_by_id": partial(collection_data_delete_by_id, client),
                "delete_many": partial(collection_data_delete_many, client),
                "exists": partial(collection_data_exists, client),
            },
            "query": {
                "near_vector": partial(collection_query_near_vector, client),
                "fetch_objects": partial(collection_query_fetch_objects, client),
                "hybrid": partial(collection_query_hybrid, client),
            },
            "generate": {
                "near_text": partial(collection_generate_near_text, client),
            },
        }
    )

    print(
        "Service registered at",
        f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
    )
