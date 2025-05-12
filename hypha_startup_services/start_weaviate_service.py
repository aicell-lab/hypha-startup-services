"""
Weaviate service implementation for Hypha.

This module provides functionality to interface with Weaviate vector database,
handling collections, data operations, and query functionality with workspace isolation.
"""

from functools import partial
import uuid
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.classes.tenants import Tenant
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
    application_artifact_name,
    collection_artifact_name,
)
from hypha_startup_services.artifacts import (
    create_artifact,
    list_artifacts,
    get_artifact,
    delete_artifact,
    artifact_exists,
)
from hypha_rpc.rpc import RemoteService


async def collections_exists(
    client: WeaviateAsyncClient, collection_name: str, context: dict[str, Any]
) -> bool:
    """Check if a collection exists in the workspace."""
    collection_name = full_collection_name(collection_name)
    return await client.collections.exists(collection_name)


async def collections_create(
    client: WeaviateAsyncClient,
    server: RemoteService,
    settings: dict,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Create a new collection in the workspace.

    Adds workspace prefix to the collection name before creating it.
    Returns the collection configuration with the workspace prefix removed.
    """
    settings_with_workspace = settings.copy()
    settings_with_workspace["class"] = full_collection_name(
        settings_with_workspace["class"]
    )

    collection = await client.collections.create_from_dict(
        settings_with_workspace,
    )

    await create_artifact(
        server,
        settings_with_workspace["class"],
        settings_with_workspace.get("description", ""),
        ws_from_context(context),
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
    collection = acquire_collection(client, name)
    return await collection_to_config_dict(collection)


async def collections_delete(
    client: WeaviateAsyncClient, name: str | list[str], context: dict[str, Any]
) -> None:
    """Delete a collection or multiple collections by name.

    Adds workspace prefix to collection names before deletion.
    """
    collection_name = full_collection_name(name)
    await client.collections.delete(collection_name)


async def applications_create(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    description: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Create a new application in the workspace.

    Adds workspace prefix to the collection name before creating it.
    Returns the application configuration with the workspace prefix removed.
    """
    tenant_name = ws_from_context(context)

    collection = acquire_collection(client, collection_name)
    existing_tenant = await collection.tenants.get_by_name(tenant_name)
    if existing_tenant is None or not existing_tenant.name == tenant_name:
        await collection.tenants.create(
            tenants=[Tenant(name=tenant_name)],
        )

    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(ws_collection_name, application_id)
    parent_artifact_name = collection_artifact_name(collection_name)
    await create_artifact(
        server,
        artifact_name,
        description,
        tenant_name,
        parent_id=parent_artifact_name,
    )


async def applications_list_all(
    server: RemoteService,
    collection_name: str,
    context: dict[str, Any],
) -> dict[str, dict]:
    tenant_name = ws_from_context(context)
    parent_artifact_name = collection_artifact_name(collection_name)
    artifacts = await list_artifacts(server, parent_artifact_name, tenant_name)

    return {artifact["name"]: artifact for artifact in artifacts}


async def applications_delete(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> None:
    """Delete an application by ID from the collection.

    Forwards all kwargs to collection.applications.delete().
    """
    tenant_name = ws_from_context(context)
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(ws_collection_name, application_id)
    await delete_artifact(
        server,
        artifact_name,
        tenant_name,
    )
    collection = acquire_collection(client, collection_name)
    tenant_collection = collection.with_tenant(tenant_name)
    # Delete all objects in the collection with the given application ID
    response = await tenant_collection.data.delete_many(
        {
            "where": {
                "path": ["application_id"],
                "operator": "Equal",
                "valueString": application_id,
            }
        }
    )

    return {
        "failed": response.failed,
        "matches": response.matches,
        "objects": objects_without_workspace(response.objects),
        "successful": response.successful,
    }


async def applications_get(
    server: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Get an application by ID from the collection.

    Forwards all kwargs to collection.applications.get().
    """
    tenant_name = ws_from_context(context)
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(ws_collection_name, application_id)
    artifact = await get_artifact(server, artifact_name, tenant_name)
    return {
        "name": artifact.name,
        "description": artifact.description,
        "metadata": artifact.metadata,
    }


async def applications_exists(
    server: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> bool:
    """Check if an application exists in the collection.

    Forwards all kwargs to collection.applications.exists().
    """
    tenant_name = ws_from_context(context)
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(ws_collection_name, application_id)
    return await artifact_exists(server, artifact_name, tenant_name)


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
    collection = acquire_collection(client, collection_name)
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
    collection = acquire_collection(client, collection_name)
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
    collection = acquire_collection(client, collection_name)
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
    collection = acquire_collection(client, collection_name)
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
    collection = acquire_collection(client, collection_name)
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
    collection = acquire_collection(client, collection_name)
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
    collection = acquire_collection(client, collection_name)
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
    collection = acquire_collection(client, collection_name)
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
    collection = acquire_collection(client, collection_name)
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
    collection = acquire_collection(client, collection_name)
    return await collection.data.exists(*args, **kwargs)


async def register_weaviate(server: RemoteService, service_id: str):
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
                "create": partial(collections_create, client, server),
                "delete": partial(collections_delete, client, server),
                "list_all": partial(collections_list_all, client),
                "get": partial(collections_get, client),
                "exists": partial(collections_exists, client),
            },
            "applications": {
                "create": partial(applications_create, client, server),
                "delete": partial(applications_delete, client, server),
                "list_all": partial(applications_list_all, server),
                "get": partial(applications_get, server),
                "exists": partial(applications_exists, server),
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
