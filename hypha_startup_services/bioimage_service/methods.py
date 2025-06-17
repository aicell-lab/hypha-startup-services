"""Service methods for the BioImage service."""

import logging
from typing import Any
from mem0 import AsyncMemory
from hypha_startup_services.bioimage_service.data_index import BioimageIndex
from hypha_startup_services.bioimage_service.utils import (
    semantic_bioimage_search,
)

logger = logging.getLogger(__name__)

# Constants for mem0 integration
EBI_AGENT_ID = "ebi_bioimage_assistant"
EBI_WORKSPACE = "ebi_data"


# Functions directly referenced in register_service.py
async def get_entity_details(
    bioimage_index: BioimageIndex,
    entity_id: str,
) -> dict[str, Any]:
    """
    Get details for a specific entity (node or technology).
    Entity type is inferred if not provided.

    Args:
        bioimage_index: The bioimage index instance.
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


async def get_related_entities(
    bioimage_index: BioimageIndex,
    entity_id: str,
) -> list[dict[str, Any]]:
    """
    Get entities related to a specific entity.
    Entity type is inferred if not provided.

    Args:
        bioimage_index: The bioimage index instance.
        entity_id: The ID of the entity to find relationships for.

    Returns:
        A list of related entities.

    Raises:
        ValueError: If entity is not found or no related entities exist.
    """
    # First try to find technologies related to this node
    technologies = bioimage_index.get_technologies_by_node_id(entity_id)

    if technologies:
        return technologies

    # If no technologies found, try to find nodes related to this technology
    nodes = bioimage_index.get_nodes_by_technology_id(entity_id)

    if nodes:
        return nodes

    # If neither worked, the entity might not exist or have no relations
    # Check if the entity exists at all
    entity = bioimage_index.get_node_by_id(entity_id)
    if not entity:
        entity = bioimage_index.get_technology_by_id(entity_id)
        if not entity:
            raise ValueError(f"Entity not found: {entity_id}")

    # Entity exists but has no related entities
    return []


async def query(
    memory: AsyncMemory,
    bioimage_index: BioimageIndex,
    query_text: str,
    include_related: bool = True,
    limit: int = 10,
) -> dict[str, Any]:
    """
    Unified query method that combines semantic search with related entity lookup.
    This replaces the separate semantic_query and find_related_entities_semantic methods.

    Args:
        memory: AsyncMemory instance
        bioimage_index: BioimageIndex instance for related entity lookups
        query_text: Natural language query
        entity_types: Filter by entity types (node, technology)
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
        enhanced_result = {"info": result.get("memory", {})}

        if include_related:
            entity_id = result["metadata"].get("entity_id")
            entity_type = result["metadata"].get("entity_type")
            relation_type = (
                "exists_in_nodes" if entity_type == "technology" else "has_technologies"
            )

            if entity_id:
                related_entities = await get_related_entities(
                    entity_id=entity_id,
                    bioimage_index=bioimage_index,
                )
                related_entities_names = [
                    entity.get("name", entity.get("id", "Unknown"))
                    for entity in related_entities
                ]
                enhanced_result[relation_type] = related_entities_names

        enhanced_results.append(enhanced_result)

    return {
        "results": enhanced_results,
        "total_results": len(enhanced_results),
    }
