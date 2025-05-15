"""
Weaviate service implementation for Hypha.

This module provides functionality to interface with Weaviate vector database,
handling collections, data operations, and query functionality with workspace isolation.
"""

import uuid
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections.classes.internal import QueryReturn, GenerativeReturn
from weaviate.collections.classes.batch import DeleteManyReturn
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.utils.collection_utils import (
    full_collection_name,
    acquire_collection,
    objects_without_workspace,
    create_application_filter,
    name_without_workspace,
    and_app_filter,
    add_tenant_if_not_exists,
    get_tenant_collection,
)
from hypha_startup_services.utils.format_utils import (
    collection_to_config_dict,
    config_minus_workspace,
    is_in_workspace,
    id_from_context,
    stringify_keys,
    get_settings_with_workspace,
    add_app_id,
)
from hypha_startup_services.utils.artifact_utils import (
    application_artifact_name,
    assert_has_permission,
    assert_is_admin_id,
    create_collection_artifact,
    delete_collection_artifacts,
    create_application_artifact,
    delete_application_artifact,
)
from hypha_startup_services.artifacts import (
    get_artifact,
    artifact_exists,
)
from hypha_startup_services.utils.constants import SHARED_WORKSPACE


async def delete_application_objects(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    user_id: str,
) -> dict:
    """Delete all objects associated with an application.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Collection name
        application_id: Application ID
        user_id: Tenant workspace

    Returns:
        Response from delete operation
    """
    collection = acquire_collection(client, collection_name)
    tenant_collection = collection.with_tenant(user_id)

    # Delete all objects in the collection with the given application ID
    response = await tenant_collection.data.delete_many(
        where=create_application_filter(application_id)
    )

    return {
        "failed": response.failed,
        "matches": response.matches,
        "objects": objects_without_workspace(response.objects),
        "successful": response.successful,
    }


async def prepare_application_creation(
    client: WeaviateAsyncClient,
    collection_name: str,
    user_id: str,
) -> dict | None:
    """Prepare for application creation by checking collection existence and adding tenant.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection for the application
        user_id: Tenant workspace

    Returns:
        Error dict if preparation fails, None if successful
    """
    # Make sure the collection exists and the user has the tenant
    if not await collections_exists(client, collection_name):
        return {"error": f"Collection '{collection_name}' does not exist."}

    # Add tenant for this user if it doesn't exist
    await add_tenant_if_not_exists(
        client,
        collection_name,
        user_id,
    )

    return None


async def collections_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any] | None = None,
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

    user_id = id_from_context(context)
    assert_is_admin_id(user_id)

    settings_with_workspace = get_settings_with_workspace(settings)

    collection = await client.collections.create_from_dict(
        settings_with_workspace,
    )

    await create_collection_artifact(server, settings_with_workspace)

    return await collection_to_config_dict(collection)


async def collections_list_all(
    client: WeaviateAsyncClient, context: dict[str, Any]
) -> dict[str, dict]:
    """List all collections in the workspace.

    Returns collections with workspace prefixes removed from their names.
    """
    user_id = id_from_context(context)
    assert_is_admin_id(user_id)

    collections = await client.collections.list_all(simple=False)
    return {
        name_without_workspace(coll_name): config_minus_workspace(coll_obj)
        for coll_name, coll_obj in collections.items()
        if is_in_workspace(coll_name, SHARED_WORKSPACE)
    }


async def collections_get(
    client: WeaviateAsyncClient,
    server: RemoteService,
    name: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Get a collection's configuration by name.

    Returns the collection configuration with the workspace prefix removed.
    """
    user_id = id_from_context(context)
    await assert_has_permission(server, user_id, name)

    collection = acquire_collection(client, name)
    return await collection_to_config_dict(collection)


async def collections_delete(
    client: WeaviateAsyncClient,
    server: RemoteService,
    name: str | list[str],
    context: dict[str, Any],
) -> dict | None:
    """Delete a collection or multiple collections by name.

    Adds workspace prefix to collection names before deletion.
    Also deletes the collection artifact from the shared workspace.
    """
    user_id = id_from_context(context)
    await assert_has_permission(server, user_id, name)

    full_names = full_collection_name(name)
    await client.collections.delete(full_names)

    # Delete collection artifacts
    await delete_collection_artifacts(server, full_names)

    return {"success": True}


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
    Creates an application artifact in the user's workspace as a child of the collection artifact.
    Returns the application configuration.
    """
    user_id = id_from_context(context)

    prep_error = await prepare_application_creation(client, collection_name, user_id)
    if prep_error:
        return prep_error

    result = await create_application_artifact(
        server,
        collection_name,
        application_id,
        description,
        user_id,
    )

    return {
        "application_id": application_id,
        "collection_name": collection_name,
        "description": description,
        "owner": user_id,
        "artifact_name": result["artifact_name"],
        "result": result,
    }


async def applications_delete(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> dict:
    """Delete an application by ID from the collection."""
    user_id = id_from_context(context)
    ws_collection_name = full_collection_name(collection_name)

    await delete_application_artifact(
        server, ws_collection_name, application_id, user_id
    )

    result = await delete_application_objects(
        client, collection_name, application_id, user_id
    )

    return result


async def applications_get(
    server: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Get an application by ID from the collection."""
    ws_collection_name = full_collection_name(collection_name)
    user_id = id_from_context(context)
    artifact_name = application_artifact_name(
        ws_collection_name, user_id, application_id
    )

    artifact = await get_artifact(server, artifact_name)
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
    """Check if an application exists in the collection."""
    user_id = id_from_context(context)
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(
        ws_collection_name, user_id, application_id
    )
    return await artifact_exists(server, artifact_name)


async def data_insert_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    objects: list[dict[str, Any]],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Insert many objects into the collection."""
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    app_objects = add_app_id(objects, application_id)

    response = await tenant_collection.data.insert_many(objects=app_objects)

    return {
        "elapsed_seconds": response.elapsed_seconds,
        "errors": stringify_keys(response.errors),
        "uuids": stringify_keys(response.uuids),
        "has_errors": response.has_errors,
    }


async def data_insert(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    properties: dict[str, Any],
    *args,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> uuid.UUID:
    """Insert an object into the collection.

    Forwards all kwargs to collection.data.insert().
    """
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    app_properties = add_app_id(properties, application_id)

    return await tenant_collection.data.insert(app_properties, *args, **kwargs)


async def query_near_vector(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using a vector.

    Forwards all kwargs to collection.query.near_vector().
    Filters results by application_id.
    """
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    kwargs["filters"] = and_app_filter(application_id, kwargs.get("filters"))

    response: QueryReturn = await tenant_collection.query.near_vector(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def query_fetch_objects(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection to fetch objects.

    Forwards all kwargs to collection.query.fetch_objects().
    Filters results by application_id.
    """
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    kwargs["filters"] = and_app_filter(application_id, kwargs.get("filters"))

    response: QueryReturn = await tenant_collection.query.fetch_objects(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def query_hybrid(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using hybrid search.

    Forwards all kwargs to collection.query.hybrid().
    Filters results by application_id.
    """
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    kwargs["filters"] = and_app_filter(application_id, kwargs.get("filters"))

    response: QueryReturn = await tenant_collection.query.hybrid(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def generate_near_text(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using near text search.

    Forwards all kwargs to collection.query.near_text().
    """
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    kwargs["where"] = and_app_filter(application_id, kwargs.get("where"))

    response: GenerativeReturn = await tenant_collection.generate.near_text(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
        "generated": response.generated,
    }


async def data_update(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> None:
    """Update an object in the collection.

    Forwards all kwargs to collection.data.update().
    """
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    await tenant_collection.data.update(**kwargs)


async def data_delete_by_id(
    client: WeaviateAsyncClient,
    collection_name: str,
    uuid_input: uuid.UUID,
    context: dict[str, Any],
) -> bool:
    """Delete an object by ID from the collection.

    Forwards all kwargs to collection.data.delete_by_id().
    """
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    await tenant_collection.data.delete_by_id(uuid=uuid_input)


async def data_delete_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Delete many objects from the collection.

    Forwards all kwargs to collection.data.delete_many().
    """
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    kwargs["where"] = and_app_filter(application_id, kwargs.get("where"))
    response: DeleteManyReturn = await tenant_collection.data.delete_many(**kwargs)

    return {
        "failed": response.failed,
        "matches": response.matches,
        "objects": objects_without_workspace(response.objects),
        "successful": response.successful,
    }


async def data_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    uuid_input: uuid.UUID,
    context: dict[str, Any],
) -> bool:
    """Check if an object exists in the collection.

    Forwards all kwargs to collection.data.exists().
    """
    user_id = id_from_context(context)
    tenant_collection = get_tenant_collection(client, collection_name, user_id)
    return await tenant_collection.data.exists(uuid=uuid_input)
