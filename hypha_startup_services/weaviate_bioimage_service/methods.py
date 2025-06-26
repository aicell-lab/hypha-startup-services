"""Service methods for the Weaviate BioImage service."""

import logging
import os
from typing import Any, Dict, Callable, Coroutine

from weaviate import WeaviateAsyncClient
from weaviate.classes.query import Filter

from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService
from hypha_rpc.utils.schema import schema_function
from pydantic import Field


from hypha_startup_services.weaviate_service.methods import (
    generate_near_text,
    query_fetch_objects,
    query_hybrid,
)
from hypha_startup_services.common.data_index import (
    BioimageIndex,
    create_get_related_entities,
)

logger = logging.getLogger(__name__)

# Constants for bioimage collections
BIOIMAGE_COLLECTION = "bioimage_data"
SHARED_APPLICATION_ID = "eurobioimaging-shared"
SHARED_APPLICATION_DESCRIPTION = "Shared EuroBioImaging nodes and technologies database"


async def ensure_shared_application_exists() -> None:
    """Ensure the shared bioimage application exists."""
    token = os.getenv("HYPHA_TOKEN")

    admin_server: RemoteService = await connect_to_server(
        {  # type: ignore
            "server_url": "https://hypha.aicell.io",
            "token": token,
        }
    )

    weaviate_service = await admin_server.get_service("aria-agents/weaviate")

    exists = await weaviate_service.applications.exists(
        collection_name=BIOIMAGE_COLLECTION,
        application_id=SHARED_APPLICATION_ID,
    )

    if not exists:
        logger.info("Creating shared application: %s", SHARED_APPLICATION_ID)
        await weaviate_service.applications.create(
            collection_name=BIOIMAGE_COLLECTION,
            application_id=SHARED_APPLICATION_ID,
            description=SHARED_APPLICATION_DESCRIPTION,
        )
        logger.info("Shared application created successfully")
    else:
        logger.debug("Shared application already exists")

    await admin_server.disconnect()


def create_query(
    client: WeaviateAsyncClient,
    server: RemoteService,
) -> Callable[..., Coroutine[Any, Any, Dict[str, Any]]]:
    """Create a schema function for querying bioimage data with dependency injection."""

    @schema_function
    async def query(
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
        context: Dict[str, Any] | None = Field(
            default=None, description="Context containing caller information"
        ),
    ) -> Dict[str, Any]:
        """Query bioimage data using natural language.

        Args:
            query_text: Natural language query
            entity_types: Filter by entity types ('node', 'technology', or both)
            limit: Maximum number of results
            context: Context containing caller information

        Returns:
            Dictionary with query results and generated response
        """
        # Ensure the shared application exists before querying
        await ensure_shared_application_exists()

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
                where_filter = Filter.by_property("entity_type").contains_any(
                    entity_types
                )

        return await generate_near_text(
            client=client,
            server=server,
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

    return query


def create_search(
    client: WeaviateAsyncClient, server: RemoteService, bioimage_index: BioimageIndex
) -> Callable[..., Coroutine[Any, Any, Dict[str, Any]]]:
    """Create a schema function for searching bioimage data with dependency injection."""

    @schema_function
    async def search(
        query_text: str = Field(
            description="Natural language query to search bioimage data"
        ),
        entity_types: list[str] | None = Field(
            default=None,
            description="Filter by entity types: 'node', 'technology', or both. Defaults to both if not specified.",
        ),
        include_related: bool = Field(
            default=True,
            description="Whether to include related entities in the search",
        ),
        limit: int = Field(
            default=10, description="Maximum number of results to return"
        ),
        context: Dict[str, Any] | None = Field(
            default=None, description="Context containing caller information"
        ),
    ) -> Dict[str, Any]:
        """Search bioimage data using natural language.

        Args:
            query_text: Natural language query
            entity_types: Filter by entity types ('node', 'technology', or both)
            limit: Maximum number of results
            context: Context containing caller information

        Returns:
            Dictionary with search results and generated response
        """
        # Ensure the shared application exists before querying
        await ensure_shared_application_exists()

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
                where_filter = Filter.by_property("entity_type").contains_any(
                    entity_types
                )

        semantic_results = await query_hybrid(
            client=client,
            server=server,
            collection_name=BIOIMAGE_COLLECTION,
            application_id=SHARED_APPLICATION_ID,
            query=query_text,
            filters=where_filter,
            limit=limit,
            context=context,
        )

        # Step 2: For each result, find related entities using bioimage_index
        enhanced_results = []
        for result_obj in semantic_results["objects"]:
            enhanced_result = {"info": result_obj.get("text", "")}

            if include_related:
                # Extract entity_id and entity_type from flattened metadata structure
                entity_id = result_obj.get("entity_id")
                entity_type = result_obj.get("entity_type")
                relation_type = (
                    "exists_in_nodes"
                    if entity_type == "technology"
                    else "has_technologies"
                )

                if entity_id:
                    try:
                        # Create the related entities function and call it
                        get_related_func = create_get_related_entities(bioimage_index)
                        related_entities = await get_related_func(entity_id=entity_id)
                        related_entities_names = [
                            entity.get("name", entity.get("entity_id", "Unknown"))
                            for entity in related_entities
                        ]
                        enhanced_result[relation_type] = related_entities_names
                    except ValueError as e:
                        logger.warning(
                            "Failed to get related entities for %s: %s", entity_id, e
                        )
                        # Continue without related entities

            enhanced_results.append(enhanced_result)

        return {
            "objects": enhanced_results,
        }

    return search


def create_get_entity(
    client: WeaviateAsyncClient,
    server: RemoteService,
) -> Callable[..., Coroutine[Any, Any, Dict[str, Any]]]:
    """Create a schema function for getting entity by ID with dependency injection."""

    @schema_function
    async def get_entity(
        entity_id: str = Field(description="ID of the entity to retrieve"),
        context: Dict[str, Any] | None = Field(
            default=None, description="Context containing caller information"
        ),
    ) -> Dict[str, Any]:
        """Get a specific entity by ID.

        Args:
            entity_id: ID of the entity to retrieve
            context: Context containing caller information

        Returns:
            Dictionary with entity details
        """
        # Ensure the shared application exists before querying
        await ensure_shared_application_exists()

        return await query_fetch_objects(
            client=client,
            server=server,
            collection_name=BIOIMAGE_COLLECTION,
            application_id=SHARED_APPLICATION_ID,
            filters=Filter.by_property("entity_id").equal(entity_id),
            limit=10,
            context=context,
        )

    return get_entity
