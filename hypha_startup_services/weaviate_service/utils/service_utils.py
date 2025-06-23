"""
Weaviate service utility functions.

This module contains helper functions for Weaviate service operations
that need to be shared across different parts of the service.
"""

from typing import Any
from weaviate import WeaviateAsyncClient
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.common.workspace_utils import ws_from_context
from hypha_startup_services.weaviate_service.utils.collection_utils import (
    add_tenant_if_not_exists,
    get_tenant_collection,
)
from hypha_startup_services.common.utils import get_full_collection_name
from hypha_startup_services.common.permissions import (
    assert_has_application_permission,
)


async def prepare_application_creation(
    client: WeaviateAsyncClient,
    collection_name: str,
    user_ws: str,
) -> dict[str, str] | None:
    """Prepare for application creation by checking collection existence and adding tenant.

    Args:
        client: WeaviateAsyncClient instance
        collection_name: Name of the collection for the application
        user_ws: User workspace

    Returns:
        Error dict if preparation fails, None if successful
    """
    # Make sure the collection exists and the user has the tenant
    if not await collection_exists(client, collection_name):
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
    caller_ws = ws_from_context(context) if context else ""
    if user_ws is not None:
        await assert_has_application_permission(
            server, collection_name, application_id, caller_ws, user_ws
        )
        return get_tenant_collection(client, collection_name, user_ws)

    return get_tenant_collection(client, collection_name, caller_ws)


async def collection_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
) -> bool:
    """Check if a collection exists."""
    collection_name = get_full_collection_name(collection_name)
    return await client.collections.exists(collection_name)
