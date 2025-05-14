"""
Weaviate service implementation for Hypha.

This module provides functionality to interface with Weaviate vector database,
handling collections, data operations, and query functionality with workspace isolation.
"""

import uuid
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.classes.tenants import Tenant
from weaviate.collections.classes.internal import QueryReturn, GenerativeReturn
from weaviate.collections.classes.batch import DeleteManyReturn
from hypha_rpc.rpc import RemoteService
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
    is_admin_workspace,
    session_artifact_name,
    get_artifact_permissions,
    SHARED_WORKSPACE,
)
from hypha_startup_services.artifacts import (
    create_artifact,
    list_artifacts,
    get_artifact,
    delete_artifact,
    artifact_exists,
)
from hypha_startup_services.register_service import register_weaviate_service


async def is_admin(
    server: RemoteService, context: dict[str, Any], collection_name: str = None
) -> bool:
    """Check if the user has admin permissions for collections.

    Args:
        server: The RemoteService instance
        context: The request context containing workspace info
        collection_name: Optional collection name to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """
    workspace = ws_from_context(context)

    # First check if the user is in the admin workspaces list
    if is_admin_workspace(workspace):
        return True

    # If no specific collection is provided, return False for non-admins
    if collection_name is None:
        return False

    # Check if the user has admin permissions for this specific collection artifact
    collection_artifact = collection_artifact_name(collection_name)

    try:
        artifact = await get_artifact(server, collection_artifact, workspace)
        # Check if current user has admin permissions in this artifact
        # This would depend on how permissions are stored in the artifact
        if "permissions" in artifact and "admin" in artifact["permissions"]:
            if workspace in artifact["permissions"]["admin"]:
                return True
    except Exception:
        pass

    return False


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

    settings_with_workspace = settings.copy()
    original_class_name = settings_with_workspace["class"]
    settings_with_workspace["class"] = full_collection_name(original_class_name)

    # Create the collection in Weaviate
    collection = await client.collections.create_from_dict(
        settings_with_workspace,
    )

    # Create an artifact in the shared workspace for the collection
    # This artifact will be used for permission management
    workspace = ws_from_context(context)
    permissions = get_artifact_permissions(owner=True, admin=True)
    metadata = create_artifact_metadata(
        workspace=workspace,
        description=settings_with_workspace.get("description", ""),
        collection_type="weaviate",
        settings=settings,
    )

    await create_artifact(
        server,
        settings_with_workspace["class"],
        settings_with_workspace.get("description", ""),
        SHARED_WORKSPACE,  # Store collection artifacts in shared workspace
        permissions=permissions,
        metadata=metadata,
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
) -> None:
    """Delete a collection or multiple collections by name.

    Adds workspace prefix to collection names before deletion.
    Also deletes the collection artifact from the shared workspace.
    """
    # Check if single name or list of names
    names = [name] if isinstance(name, str) else name

    # Check permissions for each collection
    for coll_name in names:
        if not await is_admin(server, context, coll_name):
            return {
                "error": f"You do not have permission to delete collection '{coll_name}'."
            }

    # Delete each collection
    for coll_name in names:
        # Delete from Weaviate
        full_name = full_collection_name(coll_name)
        await client.collections.delete(full_name)

        # Delete collection artifact
        await delete_artifact(
            server,
            full_name,
            SHARED_WORKSPACE,
        )


async def add_tenant_if_not_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    tenant_name: str,
) -> None:
    """Add a tenant to the collection if it doesn't already exist."""
    collection = acquire_collection(client, collection_name)
    existing_tenant = await collection.tenants.get_by_name(tenant_name)
    if existing_tenant is None or not existing_tenant.name == tenant_name:
        await collection.tenants.create(
            tenants=[Tenant(name=tenant_name)],
        )


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

    # Make sure the collection exists and the user has the tenant
    if not await collections_exists(client, collection_name, context):
        return {"error": f"Collection '{collection_name}' does not exist."}

    # Add tenant for this user if it doesn't exist
    await add_tenant_if_not_exists(
        client,
        collection_name,
        tenant_ws,
    )

    # Create application artifact
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(ws_collection_name, application_id)
    parent_artifact_name = collection_artifact_name(collection_name)

    # Set up application metadata
    metadata = create_artifact_metadata(
        application_id=application_id,
        collection_name=collection_name,
        workspace=tenant_ws,
    )

    # Set up permissions - owner can write, everyone can read
    permissions = get_artifact_permissions(owner=True)

    result = await create_artifact(
        server=server,
        artifact_name=artifact_name,
        description=description,
        workspace=tenant_ws,  # Application artifacts are in user's workspace
        parent_id=parent_artifact_name,
        permissions=permissions,
        metadata=metadata,
    )

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
    tenant_ws = ws_from_context(context)
    parent_artifact_name = collection_artifact_name(collection_name)
    artifacts = await list_artifacts(server, parent_artifact_name, tenant_ws)

    return {artifact["name"]: artifact for artifact in artifacts}


async def applications_delete(
    client: WeaviateAsyncClient,
    server: RemoteService,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> None:
    """Delete an application by ID from the collection."""
    tenant_ws = ws_from_context(context)
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(ws_collection_name, application_id)
    await delete_artifact(
        server,
        artifact_name,
        tenant_ws,
    )
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


def append_app_session(
    objects: dict[str, Any] | list[dict[str, Any]],
    application_id: str,
    session_id: str | None = None,
) -> list[dict[str, Any]]:
    """Append the application ID and session ID to each object in the list."""
    object_list = objects if isinstance(objects, list) else [objects]
    for obj in object_list:
        obj["application_id"] = application_id
        obj.setdefault("session_id", session_id)
        assert obj.get("session_id") is not None, "Session ID is required"
    return object_list


async def data_insert_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    objects: list[dict[str, Any]],
    session_id: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Insert many objects into the collection."""
    tenant_ws = ws_from_context(context)
    collection = acquire_collection(client, collection_name)
    app_session_objects = append_app_session(objects, application_id, session_id)
    tenant_collection = collection.with_tenant(tenant_ws)
    response = await tenant_collection.data.insert_many(objects=app_session_objects)

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
    session_id: str | None = None,
    context: dict[str, Any] | None = None,
    **kwargs,
) -> uuid.UUID:
    """Insert an object into the collection.

    Forwards all kwargs to collection.data.insert().
    """
    tenant_ws = ws_from_context(context)
    collection = acquire_collection(client, collection_name)
    app_session_properties = append_app_session(properties, application_id, session_id)
    tenant_collection = collection.with_tenant(tenant_ws)
    return await tenant_collection.data.insert(app_session_properties, *args, **kwargs)


async def query_near_vector(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    session_id: str = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using a vector.

    Forwards all kwargs to collection.query.near_vector().
    Filters results by application_id and optionally by session_id.
    """
    tenant_ws = ws_from_context(context)
    collection = acquire_collection(client, collection_name)
    tenant_collection = collection.with_tenant(tenant_ws)

    # Apply filters for application_id and session_id
    kwargs = apply_query_filter(kwargs, application_id, session_id)

    # Execute query with filters
    response: QueryReturn = await tenant_collection.query.near_vector(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def query_fetch_objects(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str = None,
    session_id: str = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection to fetch objects.

    Forwards all kwargs to collection.query.fetch_objects().
    Filters results by application_id and optionally by session_id if provided.
    """
    tenant_ws = ws_from_context(context)
    collection = acquire_collection(client, collection_name)
    tenant_collection = collection.with_tenant(tenant_ws)

    # Apply filters for application_id and session_id if provided
    kwargs = apply_query_filter(kwargs, application_id, session_id)

    response: QueryReturn = await tenant_collection.query.fetch_objects(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def query_hybrid(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str = None,
    *args,
    session_id: str = None,
    context: dict[str, Any] = None,
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using hybrid search.

    Forwards all kwargs to collection.query.hybrid().
    Filters results by application_id and optionally by session_id if provided.
    """
    tenant_ws = ws_from_context(context)
    collection = acquire_collection(client, collection_name)
    tenant_collection = collection.with_tenant(tenant_ws)

    # Apply filters for application_id and session_id if provided
    kwargs = apply_query_filter(kwargs, application_id, session_id)

    response: QueryReturn = await tenant_collection.query.hybrid(*args, **kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def generate_near_text(
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


async def data_update(
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


async def data_delete_by_id(
    client: WeaviateAsyncClient,
    collection_name: str,
    uuid_input: uuid.UUID,
    context: dict[str, Any],
) -> bool:
    """Delete an object by ID from the collection.

    Forwards all kwargs to collection.data.delete_by_id().
    """
    collection = acquire_collection(client, collection_name)
    await collection.data.delete_by_id(uuid=uuid_input)


async def data_delete_many(
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


async def data_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    uuid_input: uuid.UUID,
    context: dict[str, Any],
) -> bool:
    """Check if an object exists in the collection.

    Forwards all kwargs to collection.data.exists().
    """
    collection = acquire_collection(client, collection_name)
    return await collection.data.exists(uuid=uuid_input)


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

    await register_weaviate_service(server, client, service_id)

    print(
        "Service registered at",
        f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
    )


async def sessions_create(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    session_id: str,
    description: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Create a new session for an application.

    Creates a session artifact in the user's workspace as a child of the application artifact.
    Returns the session configuration.
    """
    tenant_ws = ws_from_context(context)

    # Verify the application exists
    ws_collection_name = full_collection_name(collection_name)
    app_artifact_name = application_artifact_name(ws_collection_name, application_id)

    if not await artifact_exists(server, app_artifact_name, tenant_ws):
        return {
            "error": f"Application '{application_id}' does not exist in collection '{collection_name}'."
        }

    # Create session artifact
    session_artifact_name_full = session_artifact_name(
        ws_collection_name, application_id, session_id
    )

    # Set up session metadata
    metadata = create_artifact_metadata(
        application_id=application_id,
        collection_name=collection_name,
        session_id=session_id,
        workspace=tenant_ws,
    )

    # Set up permissions - only owner can read and write by default
    # This restricts session artifacts to only be readable by the session user
    permissions = get_artifact_permissions(owner=True, read_public=False)

    result = await create_artifact(
        server=server,
        artifact_name=session_artifact_name_full,
        description=description,
        workspace=tenant_ws,  # Session artifacts are in user's workspace
        parent_id=app_artifact_name,
        permissions=permissions,
        metadata=metadata,
    )

    return {
        "session_id": session_id,
        "application_id": application_id,
        "collection_name": collection_name,
        "description": description,
        "owner": tenant_ws,
        "artifact_name": session_artifact_name_full,
        "result": result,
    }


async def sessions_list_all(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    context: dict[str, Any],
) -> dict[str, dict]:
    """List all sessions for an application."""
    tenant_ws = ws_from_context(context)

    # Get application artifact name
    ws_collection_name = full_collection_name(collection_name)
    app_artifact_name = application_artifact_name(ws_collection_name, application_id)

    # List all child artifacts (sessions)
    sessions = await list_artifacts(server, app_artifact_name, tenant_ws)

    return {session["name"]: session for session in sessions}


async def sessions_delete(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    session_id: str,
    context: dict[str, Any],
) -> None:
    """Delete a session artifact."""
    tenant_ws = ws_from_context(context)

    # Get session artifact name
    ws_collection_name = full_collection_name(collection_name)
    session_artifact_name_full = session_artifact_name(
        ws_collection_name, application_id, session_id
    )

    # Delete the session artifact
    await delete_artifact(
        server,
        session_artifact_name_full,
        tenant_ws,
    )

    return {"success": True}


# Helper functions to reduce repetition


def create_application_filter(application_id: str) -> dict:
    """Create a filter for application_id."""
    return {
        "path": ["application_id"],
        "operator": "Equal",
        "valueString": application_id,
    }


def create_session_filter(session_id: str) -> dict:
    """Create a filter for session_id."""
    return {
        "path": ["session_id"],
        "operator": "Equal",
        "valueString": session_id,
    }


def build_query_filter(
    application_id: str = None, session_id: str = None
) -> dict | None:
    """Build a query filter for application_id and optionally session_id.

    Args:
        application_id: The application ID to filter by
        session_id: The optional session ID to filter by

    Returns:
        A Weaviate filter object or None if no filters are requested
    """
    if not application_id:
        return None

    app_filter = create_application_filter(application_id)

    if session_id:
        session_filter = create_session_filter(session_id)
        return {
            "operator": "And",
            "operands": [app_filter, session_filter],
        }

    return app_filter


def apply_query_filter(
    kwargs: dict, application_id: str = None, session_id: str = None
) -> dict:
    """Apply application and session filters to query kwargs if needed.

    Args:
        kwargs: The existing query kwargs
        application_id: The application ID to filter by
        session_id: The optional session ID to filter by

    Returns:
        Updated kwargs dict with filters added
    """
    query_filter = build_query_filter(application_id, session_id)
    if query_filter:
        kwargs["where"] = query_filter
    return kwargs


def create_artifact_metadata(
    collection_name: str = None,
    application_id: str = None,
    session_id: str = None,
    description: str = None,
    workspace: str = None,
    **kwargs,
) -> dict:
    """Create standard metadata for artifacts.

    Args:
        collection_name: The collection name
        application_id: The application ID
        session_id: The session ID
        description: The artifact description
        workspace: The creator's workspace
        **kwargs: Additional metadata fields

    Returns:
        A metadata dictionary with standard fields
    """
    metadata = {
        "created_by": workspace,
        "created_at": str(uuid.uuid1()),
    }

    if collection_name:
        metadata["collection_name"] = collection_name

    if application_id:
        metadata["application_id"] = application_id

    if session_id:
        metadata["session_id"] = session_id

    if description:
        metadata["description"] = description

    # Add any additional metadata
    metadata.update(kwargs)

    return metadata
