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
    filter_app_kwargs,
    add_tenant_if_not_exists,
    get_tenant_collection,
)
from hypha_startup_services.utils.format_utils import (
    collection_to_config_dict,
    config_minus_workspace,
    is_in_workspace,
    ws_from_context,
    stringify_keys,
    get_settings_with_workspace,
    add_app_id,
)
from hypha_startup_services.utils.artifact_utils import (
    application_artifact_name,
    is_admin,
    check_collection_delete_permissions,
    create_collection_artifact,
    delete_collection_artifacts,
    create_application_artifact,
    delete_application_artifact,
)
from hypha_startup_services.artifacts import (
    list_artifacts,
    get_artifact,
    artifact_exists,
)


async def delete_application_objects(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    tenant_ws: str,
) -> dict:
    """Delete all objects associated with an application.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Collection name
        application_id: Application ID
        tenant_ws: Tenant workspace

    Returns:
        Response from delete operation
    """
    collection = acquire_collection(client, collection_name)
    tenant_collection = collection.with_tenant(tenant_ws)

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
    tenant_ws: str,
    context: dict[str, Any],
) -> dict | None:
    """Prepare for application creation by checking collection existence and adding tenant.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection for the application
        tenant_ws: Tenant workspace
        context: Request context

    Returns:
        Error dict if preparation fails, None if successful
    """
    # Make sure the collection exists and the user has the tenant
    if not await collections_exists(client, collection_name, context):
        return {"error": f"Collection '{collection_name}' does not exist."}

    # Add tenant for this user if it doesn't exist
    await add_tenant_if_not_exists(
        client,
        collection_name,
        tenant_ws,
    )

    return None


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
    if not await is_admin(server, context):
        return {
            "error": "You do not have permission to create collections in this workspace."
        }

    settings_with_workspace = get_settings_with_workspace(settings)

    collection = await client.collections.create_from_dict(
        settings_with_workspace,
    )

    workspace = ws_from_context(context)
    await create_collection_artifact(
        server,
        settings_with_workspace,
        workspace,
    )

    return await collection_to_config_dict(collection)


async def collections_list_all(
    client: WeaviateAsyncClient, server: RemoteService, context: dict[str, Any]
) -> dict[str, dict]:
    """List all collections in the workspace.

    Returns collections with workspace prefixes removed from their names.
    """
    if not await is_admin(server, context):
        return {
            "error": "You do not have permission to list collections in this workspace."
        }

    workspace = ws_from_context(context)
    collections = await client.collections.list_all(simple=False)
    return {
        name_without_workspace(coll_name): config_minus_workspace(coll_obj)
        for coll_name, coll_obj in collections.items()
        if is_in_workspace(coll_name, workspace)
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
    if not await is_admin(server, context, name):
        return {
            "error": "You do not have permission to get collections in this workspace."
        }

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
    # Check if single name or list of names
    names = [name] if isinstance(name, str) else name

    # Check permissions
    perm_error = await check_collection_delete_permissions(server, names, context)
    if perm_error:
        return perm_error

    # Delete collections and their artifacts
    for coll_name in names:
        # Delete from Weaviate
        full_name = full_collection_name(coll_name)
        await client.collections.delete(full_name)

    # Delete collection artifacts
    await delete_collection_artifacts(server, names)

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
    tenant_ws = ws_from_context(context)

    prep_error = await prepare_application_creation(
        client, collection_name, tenant_ws, context
    )
    if prep_error:
        return prep_error

    ws_collection_name = full_collection_name(collection_name)
    result = await create_application_artifact(
        server, collection_name, application_id, description, tenant_ws
    )

    # Format and return the result
    artifact_name = application_artifact_name(ws_collection_name, application_id)
    return {
        "application_id": application_id,
        "collection_name": collection_name,
        "description": description,
        "owner": tenant_ws,
        "artifact_name": artifact_name,
        "result": result,
    }


async def applications_list_all(
    server: RemoteService,
    collection_name: str,
    context: dict[str, Any],
) -> dict[str, dict]:
    """List all applications in the collection.
    Returns a dictionary of application artifacts with their names as keys.
    """
    # TODO: Implement this function
    tenant_ws = ws_from_context(context)
    artifacts = await list_artifacts(server, tenant_ws)

    # return {artifact["name"]: artifact for artifact in artifacts}


async def applications_delete(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> dict:
    """Delete an application by ID from the collection."""
    tenant_ws = ws_from_context(context)
    ws_collection_name = full_collection_name(collection_name)

    await delete_application_artifact(
        server, ws_collection_name, application_id, tenant_ws
    )

    result = await delete_application_objects(
        client, collection_name, application_id, tenant_ws
    )

    return result


async def applications_get(
    server: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Get an application by ID from the collection."""
    tenant_ws = ws_from_context(context)
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(ws_collection_name, application_id)

    artifact = await get_artifact(server, artifact_name, tenant_ws)
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
    tenant_ws = ws_from_context(context)
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(ws_collection_name, application_id)
    return await artifact_exists(server, artifact_name, tenant_ws)


async def data_insert_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    objects: list[dict[str, Any]],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Insert many objects into the collection."""
    tenant_collection = get_tenant_collection(client, collection_name, context)
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
    tenant_collection = get_tenant_collection(client, collection_name, context)
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
    tenant_collection = get_tenant_collection(client, collection_name, context)
    kwargs = filter_app_kwargs(kwargs, application_id)

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
    tenant_collection = get_tenant_collection(client, collection_name, context)
    kwargs = filter_app_kwargs(kwargs, application_id)  # TODO: use [filter], not where

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
    tenant_collection = get_tenant_collection(client, collection_name, context)
    kwargs = filter_app_kwargs(kwargs, application_id)

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
    tenant_collection = get_tenant_collection(client, collection_name, context)
    kwargs = filter_app_kwargs(kwargs, application_id)

    response: GenerativeReturn = await tenant_collection.generate.near_text(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
        "generated": response.generated,
    }


async def data_update(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
    **kwargs,
) -> None:
    """Update an object in the collection.

    Forwards all kwargs to collection.data.update().
    """
    # TODO: only update if the object exists in the application
    tenant_collection = get_tenant_collection(client, collection_name, context)

    await tenant_collection.data.update(**kwargs)


async def data_delete_by_id(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    uuid_input: uuid.UUID,
    context: dict[str, Any],
) -> bool:
    """Delete an object by ID from the collection.

    Forwards all kwargs to collection.data.delete_by_id().
    """
    # TODO: only delete if the object exists in the application
    tenant_collection = get_tenant_collection(client, collection_name, context)
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
    tenant_collection = get_tenant_collection(client, collection_name, context)
    kwargs = filter_app_kwargs(kwargs, application_id)
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
    application_id: str,
    uuid_input: uuid.UUID,
    context: dict[str, Any],
) -> bool:
    """Check if an object exists in the collection.

    Forwards all kwargs to collection.data.exists().
    """
    tenant_collection = get_tenant_collection(client, collection_name, context)
    # TODO: only check if the object exists in the application
    return await tenant_collection.data.exists(uuid=uuid_input)
