"""
Weaviate service utility functions.

This module contains helper functions for Weaviate service operations
that need to be shared across different parts of the service.
"""

from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections import CollectionAsync
from hypha_startup_services.common.workspace_utils import ws_from_context
from hypha_startup_services.common.utils import (
    get_full_collection_name,
    get_application_artifact_name,
)
from hypha_startup_services.common.artifacts import (
    artifact_exists,
)
from hypha_startup_services.common.permissions import (
    assert_has_application_permission,
)
from .collection_utils import (
    add_tenant_if_not_exists,
    get_tenant_collection,
    is_multitenancy_enabled,
)


async def prepare_application_creation(
    client: WeaviateAsyncClient,
    collection_name: str,
    user_ws: str,
) -> None:
    """Prepare for application creation by checking collection existence and adding tenant.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection for the application
        user_ws: User workspace

    Returns:
        Error dict if preparation fails, None if successful

    Raises:
        ValueError: If the collection does not exist
    """
    # Make sure the collection exists and the user has the tenant
    if not await collection_exists(client, collection_name):
        raise ValueError(f"Collection '{collection_name}' does not exist.")

    if await is_multitenancy_enabled(client, collection_name):
        await add_tenant_if_not_exists(
            client,
            collection_name,
            user_ws,
        )


async def get_permitted_collection(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    caller_ws: str,
    user_ws: str | None = None,
) -> CollectionAsync:
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
    if user_ws is not None:
        await assert_has_application_permission(
            collection_name, application_id, caller_ws, user_ws
        )
        return await get_tenant_collection(client, collection_name, user_ws)

    return await get_tenant_collection(client, collection_name, caller_ws)


async def collection_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
) -> bool:
    """Check if a collection exists."""
    collection_name = get_full_collection_name(collection_name)
    return await client.collections.exists(collection_name)


async def ws_app_exists(collection_name: str, application_id: str, workspace: str):
    """Check if an application exists for a specific user workspace.

    Args:
        collection_name: Name of the collection to check
        application_id: ID of the application to check
        user_ws: User workspace to check against

    Returns:
        Boolean indicating whether the application exists for the user workspace
    """

    full_collection_name = get_full_collection_name(collection_name)
    artifact_name = get_application_artifact_name(
        full_collection_name, workspace, application_id
    )
    return await artifact_exists(artifact_name)


async def prepare_tenant_collection(
    client: WeaviateAsyncClient,
    collection_name: str,
    application_id: str,
    user_ws: str | None = None,
    context: dict[str, Any] | None = None,
) -> CollectionAsync:
    """Validate that the Weaviate client is properly configured.

    This function checks if the Weaviate client is connected and ready to perform operations.
    It raises an exception if the client is not properly configured.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection to validate
        application_id: ID of the application to validate
        user_ws: Optional user workspace to use as tenant (if different from caller)
        context: Context containing caller information

    Raises:
        Exception: If the Weaviate client is not properly configured
    """
    assert (
        context is not None
    ), "Context must be provided to determine the tenant workspace"

    caller_ws = ws_from_context(context)

    if user_ws is None:
        user_ws = caller_ws

    assert await ws_app_exists(
        collection_name,
        application_id,
        workspace=user_ws,
    ), f"Application {application_id} does not exist in collection {collection_name}"

    tenant_collection = await get_permitted_collection(
        client,
        collection_name,
        application_id,
        user_ws=user_ws,
        caller_ws=caller_ws,
    )

    return tenant_collection
