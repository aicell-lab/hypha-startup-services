"""Utility functions for managing Weaviate collections."""

from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections import CollectionAsync
from weaviate.classes.tenants import Tenant
from hypha_startup_services.utils.format_utils import (
    full_collection_name,
    name_without_workspace,
    ws_from_context,
)


def acquire_collection(
    client: WeaviateAsyncClient, collection_name: str
) -> CollectionAsync:
    """Acquire a collection from the client."""
    collection_name = full_collection_name(collection_name)
    return client.collections.get(collection_name)


def objects_without_workspace(objects: list[dict]) -> list[dict]:
    """Remove workspace from object IDs."""
    for obj in objects:
        obj.collection = name_without_workspace(obj.collection)
    return objects


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


def get_tenant_collection(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
) -> CollectionAsync:
    """Get the tenant collection from the client."""
    tenant_ws = ws_from_context(context)
    collection = acquire_collection(client, collection_name)
    return collection.with_tenant(tenant_ws)
