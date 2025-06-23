"""Service methods for the BioImage service."""

import logging
from typing import Any, Callable, Coroutine
from mem0 import AsyncMemory
from pydantic import Field
from hypha_rpc.utils.schema import schema_function
from hypha_startup_services.bioimage_service.data_index import BioimageIndex
from hypha_startup_services.bioimage_service.utils import (
    semantic_bioimage_search,
)

logger = logging.getLogger(__name__)

# Constants for mem0 integration
EBI_AGENT_ID = "ebi_bioimage_assistant"
EBI_WORKSPACE = "ebi_data"


def create_get_entity_details(
    bioimage_index: BioimageIndex,
) -> Callable[..., Coroutine[Any, Any, dict[str, Any]]]:
    """Create a schema function for getting entity details with dependency injection."""

    @schema_function
    async def get_entity_details(
        entity_id: str = Field(
            description="The ID of the entity (node or technology) to retrieve"
        ),
    ) -> dict[str, Any]:
        """
        Get details for a specific entity (node or technology).
        Entity type is inferred if not provided.

        Args:
            entity_id: The ID of the entity to retrieve.

        Returns:
            A dictionary containing the entity details.

        Raises:
            ValueError: If entity is not found.
        """
        entity = bioimage_index.get_node_by_id(entity_id)
        entity_type = "node"

        if not entity:
            entity = bioimage_index.get_technology_by_id(entity_id)
            entity_type = "technology"

            if not entity:
                raise ValueError(f"Entity not found: {entity_id}")

        return {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "entity_details": entity,
        }

    return get_entity_details


def create_get_related_entities(
    bioimage_index: BioimageIndex,
) -> Callable[..., Coroutine[Any, Any, list[dict[str, Any]]]]:
    """Create a schema function for getting related entities with dependency injection."""

    @schema_function
    async def get_related_entities(
        entity_id: str = Field(
            description="The ID of the entity to find relationships for"
        ),
    ) -> list[dict[str, Any]]:
        """
        Get entities related to a specific entity.
        Entity type is inferred if not provided.

        Args:
            entity_id: The ID of the entity to find relationships for.

        Returns:
            A list of related entities.

        Raises:
            ValueError: If entity is not found or no related entities exist.
        """
        if bioimage_index.get_node_by_id(entity_id):
            return bioimage_index.get_technologies_by_node_id(entity_id)

        if bioimage_index.get_technology_by_id(entity_id):
            return bioimage_index.get_nodes_by_technology_id(entity_id)

        raise ValueError(f"Entity not found: {entity_id}")

    return get_related_entities


def create_query(
    memory: AsyncMemory,
    bioimage_index: BioimageIndex,
) -> Callable[..., Coroutine[Any, Any, dict[str, Any]]]:
    """Create a schema function for querying bioimage data with dependency injection."""

    @schema_function
    async def query(
        query_text: str = Field(
            description="Natural language query to search bioimage data"
        ),
        include_related: bool = Field(
            default=True,
            description="Whether to include related entities for each result",
        ),
        limit: int = Field(
            default=10, description="Maximum number of results to return"
        ),
    ) -> dict[str, Any]:
        """
        Unified query method that combines semantic search with related entity lookup.
        This replaces the separate semantic_query and find_related_entities_semantic methods.

        Args:
            query_text: Natural language query
            include_related: Whether to include related entities for each result
            limit: Maximum number of results

        Returns:
            Dictionary with semantic search results and related entities
        """
        logger.info("Performing unified query for: '%s'", query_text)

        # Step 1: Perform semantic search
        semantic_results = await semantic_bioimage_search(
            memory=memory,
            search_query=query_text,
            limit=limit,
        )

        # Step 2: For each result, find related entities using bioimage_index
        enhanced_results = []
        for result in semantic_results["results"]:
            enhanced_result = {"info": result.get("memory", "")}

            if include_related:
                # Extract entity_id and entity_type from flattened metadata structure
                metadata = result.get("metadata", {})
                entity_id = metadata.get("entity_id")
                entity_type = metadata.get("entity_type")
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
                            entity.get("name", entity.get("id", "Unknown"))
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

    return query
