"""
Weaviate service implementation for Hypha.

This module provides functionality to interface with Weaviate vector database,
handling collections, data operations, and query functionality with user isolation.
"""

import logging
import uuid as uuid_class
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections.classes.internal import QueryReturn, GenerativeReturn
from weaviate.collections.classes.batch import DeleteManyReturn
from hypha_startup_services.common.workspace_utils import ws_from_context
from hypha_startup_services.common.chunking import chunk_text
from hypha_startup_services.weaviate_service.utils.collection_utils import (
    acquire_collection,
    objects_part_coll_name,
    get_short_name,
    and_app_filter,
)
from hypha_startup_services.common.utils import (
    get_full_collection_name,
    stringify_keys,
    get_application_artifact_name,
)
from hypha_startup_services.common.permissions import (
    assert_has_collection_permission,
    assert_is_admin_ws,
)
from hypha_startup_services.common.artifacts import (
    get_artifact,
    artifact_exists,
    artifact_edit,
)
from .utils.service_utils import (
    prepare_application_creation,
    get_permitted_collection,
    collection_exists,
    prepare_tenant_collection,
    ws_app_exists,
)
from .utils.collection_utils import (
    add_tenant_if_not_exists,
    is_multitenancy_enabled,
)
from .utils.format_utils import (
    get_full_collection_names,
    collection_to_config_dict,
    config_with_short_name,
    add_app_id,
    get_settings_full_name,
)
from .utils.artifact_utils import (
    create_collection_artifact,
    delete_collection_artifacts,
    create_application_artifact,
    delete_application_artifact,
)

logger = logging.getLogger(__name__)


async def collections_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any] | None = None,  # pylint: disable=W0613
) -> bool:
    """Check if a collection exists by its name.

    Verifies that the caller has admin permissions.
    Converts the collection name to a full name before checking existence.

    Args:
        client: WeaviateAsyncClient instance
        name: Short collection name to check
        context: Context containing caller information

    Returns:
        True if the collection exists, False otherwise
    """
    return await collection_exists(
        client=client,
        collection_name=collection_name,
    ) and await artifact_exists(
        artifact_id=get_full_collection_name(collection_name),
    )


async def collections_create(
    client: WeaviateAsyncClient,
    settings: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a new collection.

    Verifies that the caller has admin permissions.
    Adds workspace prefix to collection name before creating it.
    Creates a collection artifact to track the collection.

    Args:
        client: WeaviateAsyncClient instance
        settings: Collection configuration settings
        context: Context containing caller information

    Returns:
        The collection configuration with the short collection name
    """
    assert (
        context is not None
    ), "Context must be provided to determine the tenant workspace"
    caller_ws = ws_from_context(context)
    assert_is_admin_ws(caller_ws)

    await create_collection_artifact(settings)

    settings_full_name = get_settings_full_name(settings)
    collection = await client.collections.create_from_dict(
        settings_full_name,
    )

    return await collection_to_config_dict(collection)


async def collections_list_all(
    client: WeaviateAsyncClient, context: dict[str, Any] | None = None
) -> dict[str, dict[str, Any]]:
    """List all collections in the database.

    Verifies that the caller has admin permissions.
    Retrieves all collections and converts their names to short names (without workspace prefix).

    Args:
        client: WeaviateAsyncClient instance
        context: Context containing caller information

    Returns:
        Dictionary mapping short collection names to their configuration
    """
    assert (
        context is not None
    ), "Context must be provided to determine the tenant workspace"
    caller_ws = ws_from_context(context)
    assert_is_admin_ws(caller_ws)

    collections = await client.collections.list_all(simple=False)
    return {
        get_short_name(coll_name): config_with_short_name(coll_obj)
        for coll_name, coll_obj in collections.items()
    }


async def collections_get(
    client: WeaviateAsyncClient,
    name: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Get a collection's configuration by name.

    Verifies that the caller has permission to access the collection.
    Converts the full collection name to a short name in the returned config.

    Returns:
        The collection configuration with its short collection name.
    """
    assert (
        context is not None
    ), "Context must be provided to determine the tenant workspace"
    caller_ws = ws_from_context(context)
    await assert_has_collection_permission(caller_ws, name)

    collection = acquire_collection(client, name)
    return await collection_to_config_dict(collection)


async def collections_delete(
    client: WeaviateAsyncClient,
    name: str | list[str],
    context: dict[str, Any] | None = None,
) -> dict | None:
    """Delete one or multiple collections by name.

    Verifies that the caller has permission to access the collections.
    Adds workspace prefix to collection names before deletion.
    Also deletes the associated collection artifacts.

    Args:
        client: WeaviateAsyncClient instance
        name: Collection name(s) to delete
        context: Context containing caller information

    Returns:
        Success dictionary or None if operation fails
    """
    assert (
        context is not None
    ), "Context must be provided to determine the tenant workspace"
    caller_ws = ws_from_context(context)

    short_names = [name] if isinstance(name, str) else name
    for coll_name in short_names:
        await assert_has_collection_permission(caller_ws, coll_name)

    full_names = get_full_collection_names(short_names)
    await client.collections.delete(full_names)
    await delete_collection_artifacts(short_names)


async def collections_get_artifact(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any] | None = None,
) -> str:
    """Get the artifact for a collection.

    Retrieves the collection artifact using the caller's workspace and collection name.

    Args:
        collection_name: Name of the collection to retrieve the artifact for
        context: Context containing caller information

    Returns:
        Dictionary with collection artifact information
    """
    assert await collections_exists(
        client, collection_name, context=context
    ), f"Collection '{collection_name}' does not exist."
    return get_full_collection_name(collection_name)


async def applications_create(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    description: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a new application.

    Prepares the collection by ensuring it exists and adding the user as a tenant if needed.
    Creates an application artifact as a child of the collection artifact.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection for the application
        application_id: ID for the new application
        description: Description of the application
        context: Context containing user information

    Returns:
        Dictionary with application details and artifact information
    """
    assert (
        context is not None
    ), "Context must be provided to determine the tenant workspace"

    caller_ws = ws_from_context(context)
    if user_ws is None:
        user_ws = caller_ws

    await prepare_application_creation(client, collection_name, user_ws)

    result = await create_application_artifact(
        collection_name, application_id, description, user_ws, caller_ws=caller_ws
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
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict:
    """Delete an application by ID from the collection.

    Deletes the application artifact and all associated objects in the collection.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection containing the application
        application_id: ID of the application to delete
        context: Context containing user information

    Returns:
        Dictionary with deletion operation results
    """
    await prepare_tenant_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    if user_ws is None:
        assert (
            context is not None
        ), "Context must be provided to determine the tenant workspace"
        user_ws = ws_from_context(context)

    if await is_multitenancy_enabled(client, collection_name):
        await add_tenant_if_not_exists(
            client,
            collection_name,
            user_ws,
        )

    result = await data_delete_many(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    assert (
        context is not None
    ), "Context must be provided to determine the tenant workspace"

    full_collection_name = get_full_collection_name(collection_name)
    caller_ws = ws_from_context(context)
    await delete_application_artifact(full_collection_name, application_id, caller_ws)

    return result


async def applications_get(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Get application metadata by retrieving its artifact.

    Retrieves the application artifact using the caller's ID and application ID.

    Args:
        collection_name: Name of the collection containing the application
        application_id: ID of the application to retrieve
        context: Context containing caller information

    Returns:
        Dictionary with application artifact information
    """
    artifact_name = await applications_get_artifact(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    return await get_artifact(artifact_name)


async def applications_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> bool:
    """Check if an application exists by checking if its artifact exists.

    Args:
        collection_name: Name of the collection to check
        application_id: ID of the application to check
        context: Context containing caller information

    Returns:
        Boolean indicating whether the application exists
    """
    assert (
        context is not None
    ), "Context must be provided to determine the tenant workspace"

    caller_ws = ws_from_context(context)

    if user_ws is None:
        user_ws = caller_ws

    await get_permitted_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        caller_ws=caller_ws,
    )

    return await ws_app_exists(
        collection_name,
        application_id,
        workspace=user_ws,
    )


async def applications_get_artifact(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> str:
    """Get the artifact for an application.
    Retrieves the application artifact using the caller's ID and application ID.
    Args:
        collection_name: Name of the collection containing the application
        application_id: ID of the application to retrieve
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
    Returns:
        Dictionary with application artifact information
    """
    if user_ws is None:
        assert (
            context is not None
        ), "Context must be provided to determine the tenant workspace"
        user_ws = ws_from_context(context)

    await prepare_tenant_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    full_collection_name = get_full_collection_name(collection_name)
    artifact_name = get_application_artifact_name(
        full_collection_name, user_ws, application_id
    )

    return artifact_name


async def applications_set_permissions(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    permissions: dict[str, Any],
    user_ws: str | None = None,
    merge: bool = True,
    context: dict[str, Any] | None = None,
) -> None:
    """Set permissions for an application.

    Updates the application artifact with the provided permissions.
    Verifies that the caller has permission to access the application.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection containing the application
        application_id: ID of the application to update permissions for
        permissions: Dictionary of permissions to set
        user_ws: Optional user workspace to use as tenant (if different from caller)
        merge: If True, merge with existing permissions; if False, replace entirely
        context: Context containing caller information

    Returns:
        Dictionary with updated application artifact information
    """

    artifact_name = await applications_get_artifact(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    artifact_data = await get_artifact(artifact_name)

    logger.info(
        "Setting permissions for application '%s' in collection '%s' with user workspace '%s'",
        application_id,
        collection_name,
        user_ws or "default",
    )

    # Get current permissions from config
    current_permissions = artifact_data.get("config", {}).get("permissions", {})

    if merge:
        # Merge new permissions with existing ones
        updated_permissions = {
            **current_permissions,
            **permissions,
        }
    else:
        # Replace permissions entirely
        updated_permissions = permissions

    # Use artifact_edit function with config parameter
    await artifact_edit(
        artifact_id=artifact_name, config={"permissions": updated_permissions}
    )


async def data_insert_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    objects: list[dict[str, Any]],
    user_ws: str | None = None,
    enable_chunking: bool = False,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    text_field: str = "text",
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Insert multiple objects into the collection.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id to each object before insertion.
    Optionally chunks text content for better vector search performance.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection to insert into
        application_id: ID of the application the objects belong to
        objects: List of objects to insert
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        enable_chunking: Whether to chunk text content
        chunk_size: Maximum number of tokens per chunk (if chunking enabled)
        chunk_overlap: Number of tokens to overlap between chunks (if chunking enabled)
        text_field: Name of the field containing text to chunk

    Returns:
        Dictionary with insertion results including UUIDs and any errors
    """
    tenant_collection = await prepare_tenant_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    # Process objects with optional chunking
    if enable_chunking:
        chunked_objects = []
        for obj in objects:
            if text_field in obj and obj[text_field]:
                text_content: str = obj[text_field]
                chunks = chunk_text(text_content, chunk_size, chunk_overlap)

                for chunk_idx, chunk in enumerate(chunks):
                    chunked_obj = obj.copy()
                    chunked_obj[text_field] = chunk
                    chunked_obj["chunk_index"] = chunk_idx
                    chunked_obj["total_chunks"] = len(chunks)
                    chunked_objects.append(chunked_obj)
            else:
                chunked_objects.append(obj)

        processed_objects = chunked_objects
    else:
        processed_objects = objects

    app_objects = add_app_id(processed_objects, application_id)

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
    user_ws: str | None = None,
    enable_chunking: bool = False,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    text_field: str = "text",
    context: dict[str, Any] | None = None,
    **kwargs,
) -> uuid_class.UUID:
    """Insert a single object into the collection.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id to the object before insertion.
    Optionally chunks text content for better vector search performance.
    Forwards all kwargs to collection.data.insert().

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection to insert into
        application_id: ID of the application the object belongs to
        properties: Object properties to insert
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        enable_chunking: Whether to chunk text content
        chunk_size: Maximum number of tokens per chunk (if chunking enabled)
        chunk_overlap: Number of tokens to overlap between chunks (if chunking enabled)
        text_field: Name of the field containing text to chunk
        **kwargs: Additional arguments to pass to insert()

    Returns:
        UUID of the inserted object (or first chunk if chunking enabled)
    """
    tenant_collection = await prepare_tenant_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    if enable_chunking and text_field in properties and properties[text_field]:
        # For single insert with chunking, use data_insert_many and return first UUID
        result = await data_insert_many(
            client=client,
            collection_name=collection_name,
            application_id=application_id,
            objects=[properties],
            user_ws=user_ws,
            context=context,
            enable_chunking=True,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            text_field=text_field,
        )
        # Return the first UUID from the chunked inserts
        if result.get("uuids"):
            return list(result["uuids"].values())[0]
        else:
            raise RuntimeError("No UUIDs returned from chunked insert")

    app_properties = properties.copy()
    app_properties["application_id"] = application_id

    return await tenant_collection.data.insert(app_properties, **kwargs)


async def query_near_vector(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using vector similarity search.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit results to the specified application.
    Forwards all kwargs to collection.query.near_vector().

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection to query
        application_id: ID of the application to filter results by
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to near_vector()

    Returns:
        Dictionary containing objects with shortened collection names
    """
    tenant_collection = await prepare_tenant_collection(
        client,
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
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection to fetch objects based on filters.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit results to the specified application.
    Forwards all kwargs to collection.query.fetch_objects().

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection to query
        application_id: ID of the application to filter results by (optional)
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to fetch_objects()

    Returns:
        Dictionary containing objects with shortened collection names
    """
    tenant_collection = await prepare_tenant_collection(
        client,
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
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using hybrid search (combination of vector and keyword search).

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit results to the specified application.
    Forwards all kwargs to collection.query.hybrid().

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection to query
        application_id: ID of the application to filter results by (optional)
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to hybrid()

    Returns:
        Dictionary containing objects with shortened collection names
    """
    tenant_collection = await prepare_tenant_collection(
        client,
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
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> dict[str, Any]:
    """Generate content based on query text and similar objects in the collection.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit context to the specified application.
    Forwards all kwargs to collection.generate.near_text().

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection to search
        application_id: ID of the application to filter results by
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to near_text()

    Returns:
        Dictionary containing objects with shortened collection names and generated content
    """
    tenant_collection = await prepare_tenant_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    kwargs["filters"] = and_app_filter(application_id, kwargs.get("filters"))

    response: GenerativeReturn = await tenant_collection.generate.near_text(**kwargs)

    return {
        "objects": objects_part_coll_name(response.objects),
        "generated": response.generative.text if response.generative else None,
    }


async def data_update(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> None:
    """Update an object in the collection.

    Gets a tenant-specific collection after verifying permissions.
    Forwards all kwargs to collection.data.update().

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection containing the object
        application_id: ID of the application the object belongs to
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to update() including uuid and properties

    Returns:
        None
    """
    tenant_collection = await prepare_tenant_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    await tenant_collection.data.update(**kwargs)


async def data_delete_by_id(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    uuid: uuid_class.UUID,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> None:
    """Delete an object by ID from the collection.

    Gets a tenant-specific collection after verifying permissions.
    Calls collection.data.delete_by_id() with the specified UUID.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection containing the object
        application_id: ID of the application the object belongs to
        uuid: UUID of the object to delete
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information

    Returns:
        True if deletion was successful (implicitly, as no error is raised)
    """
    tenant_collection = await prepare_tenant_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    await tenant_collection.data.delete_by_id(uuid=uuid)


async def data_delete_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> dict[str, Any]:
    """Delete many objects from the collection based on filter criteria.

    Gets a tenant-specific collection after verifying permissions.
    Automatically adds application_id filter to limit deletion to objects in the specified
    application.
    Forwards all kwargs to collection.data.delete_many().

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection containing the objects
        application_id: ID of the application to filter objects by
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information
        **kwargs: Additional arguments to pass to delete_many() including where filters

    Returns:
        Dictionary with deletion operation results including match counts
    """
    tenant_collection = await prepare_tenant_collection(
        client,
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
    collection_name: str,
    application_id: str,
    uuid: uuid_class.UUID,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> bool:
    """Check if an object with the specified UUID exists in the collection.

    Gets a tenant-specific collection after verifying permissions.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection to check
        application_id: ID of the application the object belongs to
        uuid: UUID of the object to check
        context: Context containing caller information
        user_ws: Optional user workspace to use as tenant (if different from caller)

    Returns:
        Boolean indicating whether the object exists
    """
    tenant_collection = await prepare_tenant_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        context=context,
    )

    return await tenant_collection.data.exists(uuid=uuid)
