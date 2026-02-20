"""Utility functions for managing Weaviate collections."""

import asyncio
import uuid as uuid_class
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, TypedDict, TypeVar, cast

from weaviate import WeaviateAsyncClient
from weaviate.classes.data import DataObject
from weaviate.classes.query import Filter
from weaviate.classes.tenants import Tenant
from weaviate.collections import CollectionAsync
from weaviate.collections.classes.batch import ErrorObject
from weaviate.collections.classes.filters import (
    _Filters,  # type: ignore[reportPrivateUsage]
)
from weaviate.collections.classes.internal import (
    GenerativeObject,
    Object,
    ReferenceInputs,
)
from weaviate.collections.classes.types import WeaviateProperties

from .format_utils import (
    get_full_collection_name,
    get_short_name,
)

if TYPE_CHECKING:
    from weaviate.types import UUID, VECTORS

P = TypeVar("P")
R = TypeVar("R")
COLLECTION_CONFIG_RETRIES = 5
COLLECTION_CONFIG_SLEEP_SECONDS = 1.0


class InsertManyReturn(TypedDict):
    """Return type for data_insert_many method."""

    elapsed_seconds: float
    errors: dict[str, ErrorObject]
    uuids: dict[str, uuid_class.UUID]
    has_errors: bool


def to_data_object(
    obj: dict[str, Any],
) -> DataObject[WeaviateProperties, ReferenceInputs]:
    """Convert a dictionary to a DataObject."""
    props = dict(obj)

    raw_vector = props.pop("vector", None)
    raw_uuid = props.pop("uuid", props.pop("id", None))
    raw_references = props.pop("references", None)

    uuid_value = raw_uuid if raw_uuid is not None else None
    vector_value = raw_vector if raw_vector is not None else None

    return DataObject(
        properties=cast("WeaviateProperties", props),
        uuid=cast("UUID", uuid_value),
        vector=cast("VECTORS", vector_value),
        references=cast("ReferenceInputs", raw_references),
    )


def acquire_collection(
    client: WeaviateAsyncClient,
    collection_name: str,
) -> CollectionAsync:
    """Acquire a collection from the client."""
    collection_name = get_full_collection_name(collection_name)
    return client.collections.get(collection_name)


def objects_part_coll_name(
    objects: Sequence[Object[P, R] | GenerativeObject[P, R]],
) -> Sequence[Object[P, R] | GenerativeObject[P, R]]:
    """Shorten collection names in object IDs."""
    for obj in objects:
        obj.collection = get_short_name(obj.collection)
    return objects


def create_application_filter(application_id: str) -> _Filters:
    """Create a filter for application_id."""
    return Filter.by_property("application_id").equal(application_id)


def and_app_filter(
    application_id: str,
    current_filter: _Filters | None = None,
) -> _Filters:
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


def format_tenant_name(tenant_name: str) -> str:
    """Format tenant name to lowercase and replace spaces with underscores."""
    return tenant_name.lower().replace("|", "_")


def _is_collection_config_transient_error(error_message: str) -> bool:
    """Return True for transient collection config retrieval errors."""
    return (
        "configuration could not be retrieved" in error_message
        or "unexpected status code: 404" in error_message
    )


async def _get_collection_config_with_retry(collection: CollectionAsync) -> object:
    """Get collection config with bounded retries for transient 404 errors."""
    for _ in range(COLLECTION_CONFIG_RETRIES):
        try:
            return await collection.config.get()
        except Exception as error:  # noqa: BLE001
            error_message = str(error).lower()
            if not _is_collection_config_transient_error(error_message):
                raise
            await asyncio.sleep(COLLECTION_CONFIG_SLEEP_SECONDS)

    return await collection.config.get()


async def is_multitenancy_enabled(
    client: WeaviateAsyncClient,
    collection_name: str,
) -> bool:
    """Check if multitenancy is enabled for the collection."""
    collection = acquire_collection(client, collection_name)
    collection_config = await _get_collection_config_with_retry(collection)
    return collection_config.multi_tenancy_config.enabled


async def add_tenant_if_not_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    tenant_name: str,
) -> None:
    """Add a tenant to the collection if it doesn't already exist."""
    collection = acquire_collection(client, collection_name)
    formatted_tenant_name = format_tenant_name(tenant_name)
    existing_tenant = await collection.tenants.get_by_name(formatted_tenant_name)
    if existing_tenant is None or existing_tenant.name != formatted_tenant_name:
        await collection.tenants.create(
            tenants=[Tenant(name=formatted_tenant_name)],
        )


async def get_tenant_collection(
    client: WeaviateAsyncClient,
    collection_name: str,
    tenant_name: str,
) -> CollectionAsync:
    """Get the tenant collection from the client."""
    collection = acquire_collection(client, collection_name)
    if await is_multitenancy_enabled(client, collection_name):
        formatted_tenant_name = format_tenant_name(tenant_name)
        return collection.with_tenant(formatted_tenant_name)

    return collection
