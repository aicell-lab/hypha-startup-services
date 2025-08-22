"""Service methods for the BioImage service."""

import logging
from collections.abc import Callable, Coroutine
from typing import Any

from hypha_rpc.utils.schema import schema_function
from mem0 import AsyncMemory
from pydantic import Field

from hypha_startup_services.common.data_index import (
    BioimageIndex,
    get_entity_details,
    get_related_entities,
)

from .utils import (
    semantic_bioimage_search,
)

logger = logging.getLogger(__name__)


async def search(
    memory: AsyncMemory,
    bioimage_index: BioimageIndex,
    query_text: str,
    entity_types: list[str] | None = None,
    limit: int = 10,
    *,
    include_related: bool = True,
    context: dict[str, Any] | None = None,  # noqa: ARG001
) -> dict[str, Any]:
    """Unified query method that combines semantic search with related entity lookup.

    This replaces the separate semantic_query and
    find_related_entities_semantic methods.

    Args:
        memory: AsyncMemory instance for semantic search
        bioimage_index: Bioimage index for related entities
        query_text: Natural language query
        entity_types: Filter by entity types ('node', 'technology', or both)
        include_related: Whether to include related entities for each result
        limit: Maximum number of results
        context: Context containing caller information

    Returns:
        Dictionary with semantic search results and related entities

    """
    logger.info("Performing unified query for: '%s'", query_text)

    # Validate entity_types if provided
    if entity_types:
        valid_types = {"node", "technology"}
        invalid_types = set(entity_types) - valid_types
        if invalid_types:
            error_msg = (
                f"Invalid entity types: {invalid_types}. Must be 'node' or 'technology'"
            )
            raise ValueError(error_msg)

    # Step 1: Perform semantic search
    semantic_results = await semantic_bioimage_search(
        memory=memory,
        search_query=query_text,
        entity_types=entity_types,
        limit=limit,
    )

    # Step 2: For each result, find related entities using bioimage_index
    enhanced_results = []
    for result in semantic_results["results"]:
        metadata = result.get("metadata", {})
        entity_id = metadata.get("entity_id")
        entity_type = metadata.get("entity_type")
        enhanced_result = {
            "entity_id": entity_id,
            "info": result.get("memory", ""),
            "country": metadata.get("country", ""),
            "entity_type": entity_type,
        }

        if include_related:
            # Extract entity_id and entity_type from flattened metadata structure
            relation_type = (
                "exists_in_nodes" if entity_type == "technology" else "has_technologies"
            )

            if entity_id:
                try:
                    # Call the related entities function directly
                    related_entities = get_related_entities(
                        bioimage_index=bioimage_index,
                        entity_id=entity_id,
                    )
                    related_entities_names = [
                        {
                            "entity_id": entity.get("entity_id", "Unknown"),
                            "name": entity.get(
                                "name",
                                entity.get("entity_id", "Unknown"),
                            ),
                        }
                        for entity in related_entities
                    ]
                    enhanced_result[relation_type] = related_entities_names
                except ValueError as e:
                    logger.warning(
                        "Failed to get related entities for %s: %s",
                        entity_id,
                        e,
                    )
                    # Continue without related entities

        enhanced_results.append(enhanced_result)

    return {
        "results": enhanced_results,
        "total_results": len(enhanced_results),
    }


def create_search(
    memory: AsyncMemory,
    bioimage_index: BioimageIndex,
) -> Callable[[], Coroutine[Any, Any, dict[str, Any]]]:
    """Create a search function with injected dependencies."""
    query_text_field = Field(
        description="Natural language query to search bioimage data",
    )

    entity_types_field = Field(
        default=None,
        description=(
            "Filter by entity types: 'node', 'technology', or both."
            " Defaults to both if not specified."
        ),
    )

    limit_field = Field(
        default=10,
        description="Maximum number of results to return",
    )

    include_related_field = Field(
        default=True,
        description="Whether to include related entities for each result",
    )

    @schema_function
    async def search_func(
        query_text: str = query_text_field,
        entity_types: list[str] | None = entity_types_field,
        limit: int = limit_field,
        context: dict[str, Any] | None = None,
        *,
        include_related: bool = include_related_field,
    ) -> dict[str, Any]:
        """Unified query method: combines semantic search with related entity lookup."""
        return await search(
            memory,
            bioimage_index,
            query_text,
            entity_types,
            limit,
            include_related=include_related,
            context=context,
        )

    return search_func


def create_get_entity_details(
    bioimage_index: BioimageIndex,
) -> Callable[[], Coroutine[Any, Any, dict[str, Any]]]:
    """Create a get_entity_details function with injected bioimage index."""

    @schema_function
    async def get_entity_details_func(
        entity_id: str = Field(description="ID of the entity to retrieve details for"),
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Get detailed information about a specific entity."""
        return await get_entity_details(bioimage_index, entity_id, context)

    return get_entity_details_func


def create_get_related_entities(
    bioimage_index: BioimageIndex,
) -> Callable[[], Coroutine[Any, Any, list[dict[str, Any]]]]:
    """Create a get_related_entities function with injected bioimage index."""

    @schema_function
    async def get_related_entities_func(
        entity_id: str = Field(
            description="ID of the entity to find related entities for",
        ),
        context: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Get entities related to the specified entity."""
        return get_related_entities(bioimage_index, entity_id, context)

    return get_related_entities_func
