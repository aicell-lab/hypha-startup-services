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

    Lengthens collection name before creating it.
    Returns the collection configuration with the short collection name.
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
    """List all collections.

    Returns collections with shortened names.
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

    Returns the collection configuration with its short collection name.
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
    """Delete a collection or multiple collections by name.

    Lengthens collection names before deletion.
    Also deletes the collection artifact.
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

    Lengthens the collection name before creating it.
    Creates an application artifact as a child of the collection artifact.
    Returns the application configuration.
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
    """Delete an application by ID from the collection."""
    caller_id = id_from_context(context)
    full_collection_name = get_full_collection_name(collection_name)

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
    """Get an application by ID from the collection."""
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
    """Check if an application exists in the collection."""
    caller_id = id_from_context(context)
    full_collection_name = get_full_collection_name(collection_name)
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
    """Insert many objects into the collection."""
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
    """Insert an object into the collection.

    Forwards all kwargs to collection.data.insert().
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
    """Get a collection with tenant permissions."""
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
    """Query the collection using a vector.

    Forwards all kwargs to collection.query.near_vector().
    Filters results by application_id.
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
    """Query the collection to fetch objects.

    Forwards all kwargs to collection.query.fetch_objects().
    Filters results by application_id.
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
    """Query the collection using hybrid search.

    Forwards all kwargs to collection.query.hybrid().
    Filters results by application_id.
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
    """Query the collection using near text search.

    Forwards all kwargs to collection.query.near_text().
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

    Forwards all kwargs to collection.data.update().
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

    Forwards all kwargs to collection.data.delete_by_id().
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
    """Delete many objects from the collection.

    Forwards all kwargs to collection.data.delete_many().
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
    """Check if an object exists in the collection.

    Forwards all kwargs to collection.data.exists().
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
