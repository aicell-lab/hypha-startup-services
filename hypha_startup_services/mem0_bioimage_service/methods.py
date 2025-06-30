"""Service methods for the BioImage service."""

import logging
from typing import Any
from mem0 import AsyncMemory
from pydantic import Field
from hypha_rpc.utils.schema import schema_function
from hypha_startup_services.common.data_index import (
    BioimageIndex,
    get_related_entities,
)
from .utils import (
    semantic_bioimage_search,
)

logger = logging.getLogger(__name__)


@schema_function(arbitrary_types_allowed=True)
async def search(
    memory: AsyncMemory,
    bioimage_index: BioimageIndex,
    query_text: str = Field(
        description="Natural language query to search bioimage data"
    ),
    entity_types: list[str] | None = Field(
        default=None,
        description="Filter by entity types: 'node', 'technology', or both. Defaults to both if not specified.",
    ),
    include_related: bool = Field(
        default=True,
        description="Whether to include related entities for each result",
    ),
    limit: int = Field(default=10, description="Maximum number of results to return"),
) -> dict[str, Any]:
    """
    Unified query method that combines semantic search with related entity lookup.
    This replaces the separate semantic_query and find_related_entities_semantic methods.

    Args:
        memory: AsyncMemory instance for semantic search
        bioimage_index: Bioimage index for related entities
        query_text: Natural language query
        entity_types: Filter by entity types ('node', 'technology', or both)
        include_related: Whether to include related entities for each result
        limit: Maximum number of results

    Returns:
        Dictionary with semantic search results and related entities
    """
    logger.info("Performing unified query for: '%s'", query_text)

    # Validate entity_types if provided
    if entity_types:
        valid_types = {"node", "technology"}
        invalid_types = set(entity_types) - valid_types
        if invalid_types:
            raise ValueError(
                f"Invalid entity types: {invalid_types}. Must be 'node' or 'technology'"
            )

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
                        bioimage_index=bioimage_index, entity_id=entity_id
                    )
                    related_entities_names = [
                        {
                            "entity_id": entity.get("entity_id", "Unknown"),
                            "name": entity.get(
                                "name", entity.get("entity_id", "Unknown")
                            ),
                        }
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
        "results": enhanced_results,
        "total_results": len(enhanced_results),
    }
