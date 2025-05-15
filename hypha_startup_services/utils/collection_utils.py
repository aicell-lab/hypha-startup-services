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


def where_app_kwargs(kwargs: dict, application_id: str) -> dict:
    """Apply application filters to query kwargs if needed.

    Args:
        kwargs: The existing query kwargs
        application_id: The application ID to filter by

    Returns:
        Updated kwargs dict with filters added
    """

    new_kwargs = kwargs.copy()
    if "where" in kwargs:
        new_kwargs["where"] = {
            "operator": "And",
            "operands": [
                kwargs["where"],
                create_application_filter(application_id),
            ],
        }
    else:
        new_kwargs["where"] = create_application_filter(application_id)

    return new_kwargs


def filters_app_kwargs(
    kwargs: dict,
    application_id: str,
) -> dict:
    """Filter query kwargs to include only those relevant to the application ID."""
    new_kwargs = kwargs.copy()
    if "filters" in kwargs:
        new_kwargs["filters"].append(create_application_filter(application_id))
    else:
        new_kwargs["filters"] = [create_application_filter(application_id)]

    return new_kwargs


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
