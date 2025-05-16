"""
Weaviate service implementation for Hypha.

This module provides functionality to interface with Weaviate vector database,
handling collections, data operations, and query functionality with user isolation.
"""

import uuid
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections.classes.internal import QueryReturn, GenerativeReturn
from weaviate.collections.classes.batch import DeleteManyReturn
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.utils.collection_utils import (
    get_full_collection_name,
    acquire_collection,
    objects_part_coll_name,
    create_application_filter,
    get_short_name,
    and_app_filter,
    add_tenant_if_not_exists,
    get_tenant_collection,
)
from hypha_startup_services.utils.format_utils import (
    collection_to_config_dict,
    config_with_short_name,
    id_from_context,
    stringify_keys,
    get_settings_full_name,
    add_app_id,
)
from hypha_startup_services.utils.artifact_utils import (
    get_application_artifact_name,
    assert_has_collection_permission,
    assert_has_application_permission,
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
        user_id: User ID

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
        "objects": objects_part_coll_name(response.objects),
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
        user_id: User ID

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

    caller_id = id_from_context(context)
    assert_is_admin_id(caller_id)

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
    caller_id = id_from_context(context)
    assert_is_admin_id(caller_id)

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
    caller_id = id_from_context(context)
    await assert_has_collection_permission(server, caller_id, name)

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
    caller_id = id_from_context(context)
    await assert_has_collection_permission(server, caller_id, name)

    full_names = get_full_collection_name(name)
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
    caller_id = id_from_context(context)

    prep_error = await prepare_application_creation(client, collection_name, caller_id)
    if prep_error:
        return prep_error

    result = await create_application_artifact(
        server,
        collection_name,
        application_id,
        description,
        caller_id,
    )

    return {
        "application_id": application_id,
        "collection_name": collection_name,
        "description": description,
        "owner": caller_id,
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
    full_collection_name = get_full_collection_name(collection_name)
    caller_id = id_from_context(context)

    await delete_application_artifact(
        server, full_collection_name, application_id, caller_id
    )

    result = await delete_application_objects(
        client, collection_name, application_id, caller_id
    )

    return result


async def applications_get(
    server: WeaviateAsyncClient,
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
    caller_id = id_from_context(context)
    artifact_name = get_application_artifact_name(
        full_collection_name, caller_id, application_id
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
    caller_id = id_from_context(context)
    artifact_name = get_application_artifact_name(
        full_collection_name, caller_id, application_id
    )
    return await artifact_exists(server, artifact_name)


async def data_insert_many(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    objects: list[dict[str, Any]],
    user_id: str | None = None,
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
        user_id: Optional user ID to use as tenant (if different from caller)
        context: Context containing caller information

    Returns:
        Dictionary with insertion results including UUIDs and any errors
    """
    caller_id = id_from_context(context)

    if user_id is not None:
        await assert_has_application_permission(
            server, collection_name, application_id, caller_id, user_id
        )
        tenant_collection = get_tenant_collection(client, collection_name, user_id)
    else:
        tenant_collection = get_tenant_collection(client, collection_name, caller_id)

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
    user_id: str | None = None,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> uuid.UUID:
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
        user_id: Optional user ID to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to insert()

    Returns:
        UUID of the inserted object
    """
    caller_id = id_from_context(context)
    if user_id is not None:
        await assert_has_application_permission(
            server, collection_name, application_id, caller_id, user_id
        )
        tenant_collection = get_tenant_collection(client, collection_name, user_id)
    else:
        tenant_collection = get_tenant_collection(client, collection_name, caller_id)
    app_properties = add_app_id(properties, application_id)

    return await tenant_collection.data.insert(app_properties, **kwargs)


async def get_permitted_collection(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    user_id: str | None = None,
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
        user_id: Optional user ID to use as tenant (if different from caller)
        context: Context containing caller information

    Returns:
        Collection object with tenant permissions configured
    """
    caller_id = id_from_context(context)
    if user_id is not None:
        await assert_has_application_permission(
            server, collection_name, application_id, caller_id, user_id
        )
        return get_tenant_collection(client, collection_name, user_id)

    return get_tenant_collection(client, collection_name, caller_id)


async def query_near_vector(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    user_id: str | None = None,
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
        user_id: Optional user ID to use as tenant (if different from caller)
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
        user_id=user_id,
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
    user_id: str | None = None,
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
        user_id: Optional user ID to use as tenant (if different from caller)
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
        user_id=user_id,
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
    user_id: str | None = None,
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
        user_id: Optional user ID to use as tenant (if different from caller)
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
        user_id=user_id,
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
    user_id: str | None = None,
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
        user_id: Optional user ID to use as tenant (if different from caller)
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
        user_id=user_id,
        context=context,
    )
    kwargs["where"] = and_app_filter(application_id, kwargs.get("where"))

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
    user_id: str | None = None,
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
        user_id: Optional user ID to use as tenant (if different from caller)
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
        user_id=user_id,
        context=context,
    )
    await tenant_collection.data.update(**kwargs)


async def data_delete_by_id(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    uuid_input: uuid.UUID,
    user_id: str | None = None,
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
        uuid_input: UUID of the object to delete
        user_id: Optional user ID to use as tenant (if different from caller)
        context: Context containing caller information

    Returns:
        True if deletion was successful (implicitly, as no error is raised)
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_id=user_id,
        context=context,
    )
    await tenant_collection.data.delete_by_id(uuid=uuid_input)


async def data_delete_many(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    user_id: str | None = None,
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
        user_id: Optional user ID to use as tenant (if different from caller)
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
        user_id=user_id,
        context=context,
    )
    kwargs["where"] = and_app_filter(application_id, kwargs.get("where"))
    response: DeleteManyReturn = await tenant_collection.data.delete_many(**kwargs)

    return {
        "failed": response.failed,
        "matches": response.matches,
        "objects": objects_part_coll_name(response.objects),
        "successful": response.successful,
    }


async def data_exists(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    uuid_input: uuid.UUID,
    context: dict[str, Any],
    user_id: str | None = None,
) -> bool:
    """Check if an object with the specified UUID exists in the collection.

    Gets a tenant-specific collection after verifying permissions.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance for permission checking
        collection_name: Name of the collection to check
        application_id: ID of the application the object belongs to
        uuid_input: UUID of the object to check
        context: Context containing caller information
        user_id: Optional user ID to use as tenant (if different from caller)

    Returns:
        Boolean indicating whether the object exists
    """
    tenant_collection = await get_permitted_collection(
        client,
        server,
        collection_name,
        application_id,
        user_id=user_id,
        context=context,
    )
    return await tenant_collection.data.exists(uuid=uuid_input)
