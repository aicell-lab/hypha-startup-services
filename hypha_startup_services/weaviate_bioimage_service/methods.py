"""Service methods for the Weaviate BioImage service."""

import logging
from typing import Any

from weaviate import WeaviateAsyncClient
from weaviate.classes.query import Filter

from hypha_rpc.utils.schema import schema_function
from pydantic import Field

from hypha_startup_services.common.data_index import get_related_entities
from hypha_startup_services.weaviate_service.methods import (
    generate_near_text,
    query_fetch_objects,
    query_hybrid,
    applications_exists,
    applications_create,
)
from hypha_startup_services.common.data_index import (
    BioimageIndex,
    add_related_entities,
)

logger = logging.getLogger(__name__)

# Constants for bioimage collections
BIOIMAGE_COLLECTION = "bioimage_data"
SHARED_APPLICATION_ID = "eurobioimaging-shared"
SHARED_APPLICATION_DESCRIPTION = "Shared EuroBioImaging nodes and technologies database"


async def ensure_shared_application_exists(
    client: WeaviateAsyncClient, context: dict[str, Any] | None = None
) -> None:
    exists = await applications_exists(
        collection_name=BIOIMAGE_COLLECTION,
        application_id=SHARED_APPLICATION_ID,
        context=context,
    )

    if not exists:
        logger.info("Creating shared application: %s", SHARED_APPLICATION_ID)
        await applications_create(
            client=client,
            collection_name=BIOIMAGE_COLLECTION,
            application_id=SHARED_APPLICATION_ID,
            description=SHARED_APPLICATION_DESCRIPTION,
            context=context,
        )
        logger.info("Shared application created successfully")
    else:
        logger.debug("Shared application already exists")


async def query(
    client: WeaviateAsyncClient,
    query_text: str,
    entity_types: list[str] | None = None,
    limit: int = 10,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Query bioimage data using natural language.

    Args:
        client: Weaviate client instance
        query_text: Natural language query
        entity_types: Filter by entity types ('node', 'technology', or both)
        limit: Maximum number of results
        context: Context containing caller information

    Returns:
        Dictionary with query results and generated response
    """
    # Ensure the shared application exists before querying
    await ensure_shared_application_exists(client=client, context=context)

    # Validate entity_types if provided
    if entity_types:
        valid_types = {"node", "technology"}
        invalid_types = set(entity_types) - valid_types
        if invalid_types:
            raise ValueError(
                f"Invalid entity types: {invalid_types}. Must be 'node' or 'technology'"
            )

    where_filter = None
    if entity_types:
        if len(entity_types) == 1:
            where_filter = Filter.by_property("entity_type").equal(entity_types[0])
        else:
            where_filter = Filter.by_property("entity_type").contains_any(entity_types)

    return await generate_near_text(
        client=client,
        collection_name=BIOIMAGE_COLLECTION,
        application_id=SHARED_APPLICATION_ID,
        query=query_text,
        filters=where_filter,
        limit=limit,
        target_vector="text_vector",  # Use available vector field
        single_prompt=(
            f"Based on the bioimage data below, provide a comprehensive answer about the user's query: '{query_text}'. "
            "Focus on nodes (facilities) and technologies that are relevant to this query."
        ),
        grouped_task=f"Summarize the bioimage information to answer the user's question about: '{query_text}'",
        grouped_properties=["text", "entity_type", "name"],
        context=context,
    )


async def search(
    client: WeaviateAsyncClient,
    bioimage_index: BioimageIndex,
    query_text: str,
    entity_types: list[str] | None = None,
    include_related: bool = True,
    limit: int = 10,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Search bioimage data using natural language.

    Args:
        client: Weaviate client instance
        bioimage_index: Bioimage index for related entities
        query_text: Natural language query
        entity_types: Filter by entity types ('node', 'technology', or both)
        limit: Maximum number of results
        context: Context containing caller information

    Returns:
        Dictionary with search results and generated response
    """
    # Ensure the shared application exists before querying
    await ensure_shared_application_exists(client=client, context=context)

    # Validate entity_types if provided
    if entity_types:
        valid_types = {"node", "technology"}
        invalid_types = set(entity_types) - valid_types
        if invalid_types:
            raise ValueError(
                f"Invalid entity types: {invalid_types}. Must be 'node' or 'technology'"
            )

    where_filter = None
    if entity_types:
        if len(entity_types) == 1:
            where_filter = Filter.by_property("entity_type").equal(entity_types[0])
        else:
            where_filter = Filter.by_property("entity_type").contains_any(entity_types)

    semantic_results = await query_hybrid(
        client=client,
        collection_name=BIOIMAGE_COLLECTION,
        application_id=SHARED_APPLICATION_ID,
        query=query_text,
        filters=where_filter,
        limit=limit,
        target_vector="text_vector",
        context=context,
    )

    result_objects = semantic_results["objects"]

    return_objects = (
        add_related_entities(bioimage_index, result_objects)
        if include_related
        else [{"info": result_obj.get("text", "")} for result_obj in result_objects]
    )

    return {
        "objects": return_objects,
    }


async def get_entity(
    client: WeaviateAsyncClient,
    entity_id: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Get a specific entity by ID.

    Args:
        client: Weaviate client instance
        entity_id: ID of the entity to retrieve
        context: Context containing caller information

    Returns:
        Dictionary with entity details
    """
    # Ensure the shared application exists before querying
    await ensure_shared_application_exists(client=client, context=context)

    return await query_fetch_objects(
        client=client,
        collection_name=BIOIMAGE_COLLECTION,
        application_id=SHARED_APPLICATION_ID,
        filters=Filter.by_property("entity_id").equal(entity_id),
        limit=10,
        context=context,
    )


def create_query(client: WeaviateAsyncClient):
    """Create a query function with injected Weaviate client."""

    @schema_function
    async def query_func(
        query_text: str = Field(
            description="Natural language query to search bioimage data"
        ),
        entity_types: list[str] | None = Field(
            default=None,
            description="Filter by entity types: 'node', 'technology', or both. Defaults to both if not specified.",
        ),
        limit: int = Field(
            default=10, description="Maximum number of results to return"
        ),
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Query bioimage data using natural language."""
        return await query(client, query_text, entity_types, limit, context)

    return query_func


def create_get_entity(client: WeaviateAsyncClient):
    """Create a get_entity function with injected Weaviate client."""

    @schema_function
    async def get_entity_func(
        entity_id: str = Field(description="ID of the entity to retrieve"),
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Get a specific entity by ID."""
        return await get_entity(client, entity_id, context)

    return get_entity_func


def create_search(client: WeaviateAsyncClient, bioimage_index: BioimageIndex):
    """Create a search function with injected dependencies."""

    @schema_function
    async def search_func(
        query_text: str = Field(description="Search query for bioimage data"),
        entity_types: list[str] | None = Field(
            default=None,
            description="Filter by entity types: 'node', 'technology', or both. Defaults to both if not specified.",
        ),
        include_related: bool = Field(
            default=True, description="Include related entities in results"
        ),
        limit: int = Field(
            default=10, description="Maximum number of results to return"
        ),
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Search bioimage data with optional related entities."""
        return await search(
            client,
            bioimage_index,
            query_text,
            entity_types,
            include_related,
            limit,
            context,
        )

    return search_func


def create_get_related(bioimage_index: BioimageIndex):
    """Create a get_related function with injected bioimage index."""

    @schema_function
    async def get_related_func(
        entity_id: str = Field(
            description="ID of the entity to find related entities for"
        ),
        context: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Get entities related to the specified entity."""
        return get_related_entities(bioimage_index, entity_id, context)

    return get_related_func
