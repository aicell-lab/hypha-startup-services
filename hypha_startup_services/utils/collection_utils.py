"""Utility functions for managing Weaviate collections."""

from weaviate import WeaviateAsyncClient
from weaviate.collections import CollectionAsync
from weaviate.classes.tenants import Tenant
from weaviate.classes.query import Filter
from hypha_startup_services.utils.format_utils import (
    get_full_collection_name,
    get_short_name,
)


def acquire_collection(
    client: WeaviateAsyncClient, collection_name: str
) -> CollectionAsync:
    """Acquire a collection from the client."""
    collection_name = get_full_collection_name(collection_name)
    return client.collections.get(collection_name)


def objects_part_coll_name(objects: list[dict]) -> list[dict]:
    """Shorten collection names in object IDs."""
    for obj in objects:
        obj.collection = get_short_name(obj.collection)
    return objects


def create_application_filter(application_id: str) -> Filter:
    """Create a filter for application_id."""
    return Filter.by_property("application_id").equal(application_id)


def and_app_filter(
    application_id: str,
    current_filter: Filter | None = None,
) -> dict:
    """Add application filter to existing filter.

    Args:
        application_id: The application ID to filter by
        current_filter: The existing filter to combine with application filter

    Returns:
        Combined filter with application_id condition
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
