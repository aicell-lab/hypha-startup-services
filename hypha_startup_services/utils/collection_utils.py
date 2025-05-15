"""Utility functions for managing Weaviate collections."""

from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections import CollectionAsync
from weaviate.classes.tenants import Tenant
from weaviate.classes.query import Filter
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


def create_application_filter(application_id: str) -> Filter:
    """Create a filter for application_id."""
    return Filter.by_property("application_id").equal(application_id)


def and_app_filter(
    application_id: str,
    current_filter: Filter | None = None,
) -> dict:
    """Add application filter to filters.

    Args:
        application_id: The application ID to filter by
        param_name: The name of the parameter to filter on

    Returns:
        Updated kwargs dict with filters added
    """
    app_filter = create_application_filter(application_id)
    if current_filter is None:
        return app_filter

    return current_filter & app_filter


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
    user_ws: str,
) -> CollectionAsync:
    """Get the tenant collection from the client."""
    collection = acquire_collection(client, collection_name)
    return collection.with_tenant(user_ws)
