"""
Weaviate service implementation for Hypha.

This module provides functionality to interface with Weaviate vector database,
handling collections, data operations, and query functionality with user isolation.
"""

import uuid as uuid_class
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections.classes.internal import QueryReturn, GenerativeReturn
from weaviate.collections.classes.batch import DeleteManyReturn
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.utils.collection_utils import (
    acquire_collection,
    objects_part_coll_name,
    create_application_filter,
    get_short_name,
    and_app_filter,
    add_tenant_if_not_exists,
    get_tenant_collection,
)
from hypha_startup_services.utils.format_utils import (
    get_full_collection_name,
    get_full_collection_names,
    collection_to_config_dict,
    config_with_short_name,
    ws_from_context,
    stringify_keys,
    get_settings_full_name,
    add_app_id,
)
from hypha_startup_services.utils.artifact_utils import (
    get_application_artifact_name,
    assert_has_collection_permission,
    assert_has_application_permission,
    assert_is_admin_ws,
    create_collection_artifact,
    delete_collection_artifacts,
    create_application_artifact,
    delete_application_artifact,
)
from hypha_startup_services.artifacts import (
    get_artifact,
    artifact_exists,
)


async def prepare_application_creation(
    client: WeaviateAsyncClient,
    collection_name: str,
    user_ws: str,
) -> dict | None:
    """Prepare for application creation by checking collection existence and adding tenant.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection for the application
        user_ws: User workspace

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
        user_ws,
    )

    return None


async def get_permitted_collection(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
):
    """Get a collection with appropriate tenant permissions.

    Verifies that the caller has permission to access the application.
    Returns a collection object with the appropriate tenant configured.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection to access
        application_id: ID of the application being accessed
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information

    Returns:
        Collection object with tenant permissions configured
    """
    caller_ws = ws_from_context(context)
    if user_ws is not None:
        await assert_has_application_permission(
            server, collection_name, application_id, caller_ws, user_ws
        )
        return get_tenant_collection(client, collection_name, user_ws)

    return get_tenant_collection(client, collection_name, caller_ws)


async def collections_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any] | None = None,
) -> bool:
    """Check if a collection exists."""
    collection_name = get_full_collection_name(collection_name)
    return await client.collections.exists(collection_name)


async def collections_create(
    client: WeaviateAsyncClient,
    server: RemoteService,
    settings: dict,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Create a new collection.

    Verifies that the caller has admin permissions.
    Adds workspace prefix to collection name before creating it.
    Creates a collection artifact to track the collection.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for artifact creation
        settings: Collection configuration settings
        context: Context containing caller information

    Returns:
        The collection configuration with the short collection name
    """

    caller_ws = ws_from_context(context)
    assert_is_admin_ws(caller_ws)

    settings_full_name = get_settings_full_name(settings)

    collection = await client.collections.create_from_dict(
        settings_full_name,
    )

    await create_collection_artifact(server, settings_full_name)

    return await collection_to_config_dict(collection)


async def collections_list_all(
    client: WeaviateAsyncClient, context: dict[str, Any]
) -> dict[str, dict]:
    """List all collections in the database.

    Verifies that the caller has admin permissions.
    Retrieves all collections and converts their names to short names (without workspace prefix).

    Args:
        client: WeaviateAsyncClient instance
        context: Context containing caller information

    Returns:
        Dictionary mapping short collection names to their configuration
    """
    caller_ws = ws_from_context(context)
    assert_is_admin_ws(caller_ws)

    collections = await client.collections.list_all(simple=False)
    return {
        get_short_name(coll_name): config_with_short_name(coll_obj)
        for coll_name, coll_obj in collections.items()
    }


async def collections_get(
    client: WeaviateAsyncClient,
    server: RemoteService,
    name: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Get a collection's configuration by name.

    Verifies that the caller has permission to access the collection.
    Converts the full collection name to a short name in the returned config.

    Returns:
        The collection configuration with its short collection name.
    """
    caller_ws = ws_from_context(context)
    await assert_has_collection_permission(server, caller_ws, name)

    collection = acquire_collection(client, name)
    return await collection_to_config_dict(collection)


async def collections_delete(
    client: WeaviateAsyncClient,
    server: RemoteService,
    name: str | list[str],
    context: dict[str, Any],
) -> dict | None:
    """Delete one or multiple collections by name.

    Verifies that the caller has permission to access the collections.
    Adds workspace prefix to collection names before deletion.
    Also deletes the associated collection artifacts.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for artifact deletion
        name: Collection name(s) to delete
        context: Context containing caller information

    Returns:
        Success dictionary or None if operation fails
    """
    caller_ws = ws_from_context(context)

    short_names = [name] if isinstance(name, str) else name
    for coll_name in short_names:
        await assert_has_collection_permission(server, caller_ws, coll_name)

    full_names = get_full_collection_names(short_names)
    await client.collections.delete(full_names)
    await delete_collection_artifacts(server, short_names)


async def applications_create(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    description: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Create a new application.

    Prepares the collection by ensuring it exists and adding the user as a tenant if needed.
    Creates an application artifact as a child of the collection artifact.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for artifact operations
        collection_name: Name of the collection for the application
        application_id: ID for the new application
        description: Description of the application
        context: Context containing user information

    Returns:
        Dictionary with application details and artifact information
    """
    caller_ws = ws_from_context(context)

    prep_error = await prepare_application_creation(client, collection_name, caller_ws)
    if prep_error:
        return prep_error

    result = await create_application_artifact(
        server,
        collection_name,
        application_id,
        description,
        caller_ws,
    )

    return {
        "application_id": application_id,
        "collection_name": collection_name,
        "description": description,
        "owner": caller_ws,
        "artifact_name": result["artifact_name"],
        "result": result,
    }


async def applications_delete(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict:
    """Delete an application by ID from the collection.

    Deletes the application artifact and all associated objects in the collection.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for artifact operations
        collection_name: Name of the collection containing the application
        application_id: ID of the application to delete
        context: Context containing user information

    Returns:
        Dictionary with deletion operation results
    """

    result = await data_delete_many(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
        where=create_application_filter(application_id),
    )

    full_collection_name = get_full_collection_name(collection_name)
    caller_ws = ws_from_context(context)
    await delete_application_artifact(
        server, full_collection_name, application_id, caller_ws
    )

    return result


async def applications_get(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Get application metadata by retrieving its artifact.

    Retrieves the application artifact using the caller's ID and application ID.

    Args:
        server: RemoteService instance for artifact retrieval (Note: This is actually WeaviateAsyncClient, param name is incorrect)
        collection_name: Name of the collection containing the application
        application_id: ID of the application to retrieve
        context: Context containing caller information

    Returns:
        Dictionary with application artifact information
    """
    full_collection_name = get_full_collection_name(collection_name)
    caller_ws = ws_from_context(context)
    artifact_name = get_application_artifact_name(
        full_collection_name, caller_ws, application_id
    )

    return await get_artifact(server, artifact_name)


async def applications_exists(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> bool:
    """Check if an application exists by checking if its artifact exists.

    Args:
        server: RemoteService instance for artifact checking (Note: This is actually WeaviateAsyncClient, param name is incorrect)
        collection_name: Name of the collection to check
        application_id: ID of the application to check
        context: Context containing caller information

    Returns:
        Boolean indicating whether the application exists
    """
    full_collection_name = get_full_collection_name(collection_name)
    caller_ws = ws_from_context(context)
    artifact_name = get_application_artifact_name(
        full_collection_name, caller_ws, application_id
    )
    return await artifact_exists(server, artifact_name)


async def data_insert_many(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    objects: list[dict[str, Any]],
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Insert multiple objects into the collection.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id to each object before insertion.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection to insert into
        application_id: ID of the application the objects belong to
        objects: List of objects to insert
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information

    Returns:
        Dictionary with insertion results including UUIDs and any errors
    """

    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

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
    server: RemoteService,
    collection_name: str,
    application_id: str,
    properties: dict[str, Any],
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> uuid_class.UUID:
    """Insert a single object into the collection.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id to the object before insertion.
    Forwards all kwargs to collection.data.insert().

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection to insert into
        application_id: ID of the application the object belongs to
        properties: Object properties to insert
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to insert()

    Returns:
        UUID of the inserted object
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )
    app_properties = properties.copy()
    app_properties["application_id"] = application_id

    return await tenant_collection.data.insert(app_properties, **kwargs)


async def query_near_vector(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using vector similarity search.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit results to the specified application.
    Forwards all kwargs to collection.query.near_vector().

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection to query
        application_id: ID of the application to filter results by
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to near_vector()

    Returns:
        Dictionary containing objects with shortened collection names
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    kwargs["filters"] = and_app_filter(application_id, kwargs.get("filters"))

    response: QueryReturn = await tenant_collection.query.near_vector(**kwargs)

    return {
        "objects": objects_part_coll_name(response.objects),
    }


async def query_fetch_objects(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str = None,
    user_ws: str | None = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection to fetch objects based on filters.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit results to the specified application.
    Forwards all kwargs to collection.query.fetch_objects().

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection to query
        application_id: ID of the application to filter results by (optional)
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to fetch_objects()

    Returns:
        Dictionary containing objects with shortened collection names
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )
    kwargs["filters"] = and_app_filter(application_id, kwargs.get("filters"))

    response: QueryReturn = await tenant_collection.query.fetch_objects(**kwargs)

    return {
        "objects": objects_part_coll_name(response.objects),
    }


async def query_hybrid(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str = None,
    user_ws: str | None = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using hybrid search (combination of vector and keyword search).

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit results to the specified application.
    Forwards all kwargs to collection.query.hybrid().

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection to query
        application_id: ID of the application to filter results by (optional)
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to hybrid()

    Returns:
        Dictionary containing objects with shortened collection names
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )
    kwargs["filters"] = and_app_filter(application_id, kwargs.get("filters"))

    response: QueryReturn = await tenant_collection.query.hybrid(**kwargs)

    return {
        "objects": objects_part_coll_name(response.objects),
    }


async def generate_near_text(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Generate content based on query text and similar objects in the collection.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit context to the specified application.
    Forwards all kwargs to collection.generate.near_text().

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection to search
        application_id: ID of the application to filter results by
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to near_text()

    Returns:
        Dictionary containing objects with shortened collection names and generated content
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )
    kwargs["filters"] = and_app_filter(application_id, kwargs.get("filters"))

    response: GenerativeReturn = await tenant_collection.generate.near_text(**kwargs)

    return {
        "objects": objects_part_coll_name(response.objects),
        "generated": response.generated,
    }


async def data_update(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> None:
    """Update an object in the collection.

    Gets a tenant-specific collection after verifying permissions.
    Forwards all kwargs to collection.data.update().

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection containing the object
        application_id: ID of the application the object belongs to
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to update() including uuid and properties

    Returns:
        None
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )
    await tenant_collection.data.update(**kwargs)


async def data_delete_by_id(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    uuid: uuid_class.UUID,
    user_ws: str | None = None,
    context: dict[str, Any] = None,
) -> bool:
    """Delete an object by ID from the collection.

    Gets a tenant-specific collection after verifying permissions.
    Calls collection.data.delete_by_id() with the specified UUID.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection containing the object
        application_id: ID of the application the object belongs to
        uuid: UUID of the object to delete
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information

    Returns:
        True if deletion was successful (implicitly, as no error is raised)
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )
    await tenant_collection.data.delete_by_id(uuid=uuid)


async def data_delete_many(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Delete many objects from the collection based on filter criteria.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit deletion to objects in the specified application.
    Forwards all kwargs to collection.data.delete_many().

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection containing the objects
        application_id: ID of the application to filter objects by
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to delete_many() including where filters

    Returns:
        Dictionary with deletion operation results including match counts
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )
    kwargs["where"] = and_app_filter(application_id, kwargs.get("where"))
    response: DeleteManyReturn = await tenant_collection.data.delete_many(**kwargs)

    return {
        "failed": response.failed,
        "matches": response.matches,
        "objects": (
            objects_part_coll_name(response.objects) if response.objects else None
        ),
        "successful": response.successful,
    }


async def data_exists(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    uuid: uuid_class.UUID,
    context: dict[str, Any],
    user_ws: str | None = None,
) -> bool:
    """Check if an object with the specified UUID exists in the collection.

    Gets a tenant-specific collection after verifying permissions.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection to check
        application_id: ID of the application the object belongs to
        uuid: UUID of the object to check
        context: Context containing caller information
        user_ws: Optional user workspace to use as tenant (if different from caller)

    Returns:
        Boolean indicating whether the object exists
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )
    return await tenant_collection.data.exists(uuid=uuid)
