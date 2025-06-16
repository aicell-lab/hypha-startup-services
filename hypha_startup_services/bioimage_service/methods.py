"""Service methods for the BioImage service."""

import logging
from typing import Dict, Any, List, Optional
from mem0 import AsyncMemory
from hypha_startup_services.bioimage_service.data_index import BioimageIndex
from hypha_startup_services.bioimage_service.utils import (
    infer_entity_type,
    _create_node_content,
    _create_node_metadata,
    _create_technology_content,
    _create_technology_metadata,
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
    entity_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get details for a specific entity (node or technology).
    Entity type is inferred if not provided.

    Args:
        bioimage_index: The bioimage index instance.
        entity_id: The ID of the entity to retrieve.
        entity_type: Optional entity type ('node' or 'technology'). Inferred if not provided.

    Returns:
        A dictionary containing the entity details.
    """
    # Infer entity type if not provided
    if entity_type is None:
        entity_type = infer_entity_type(bioimage_index, entity_id)
        if entity_type is None:
            return {
                "error": f"Entity not found: {entity_id}",
                "entity_id": entity_id,
            }

    if entity_type.lower() == "node":
        entity = bioimage_index.get_node_by_id(entity_id)
        if not entity:
            return {
                "error": f"Node not found: {entity_id}",
                "entity_id": entity_id,
                "entity_type": entity_type,
            }
        return {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "entity_details": entity,
        }

    elif entity_type.lower() == "technology":
        entity = bioimage_index.get_technology_by_id(entity_id)
        if not entity:
            return {
                "error": f"Technology not found: {entity_id}",
                "entity_id": entity_id,
                "entity_type": entity_type,
            }
        return {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "entity_details": entity,
        }

    else:
        return {
            "error": f"Invalid entity type: {entity_type}. Must be 'node' or 'technology'.",
            "entity_id": entity_id,
            "entity_type": entity_type,
        }


async def get_related_entities(
    bioimage_index: BioimageIndex,
    entity_id: str,
    entity_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get entities related to a specific entity.
    Entity type is inferred if not provided.

    Args:
        bioimage_index: The bioimage index instance.
        entity_id: The ID of the entity to find relationships for.
        entity_type: Optional entity type ('node' or 'technology'). Inferred if not provided.

    Returns:
        A dictionary containing related entities.
    """
    # Infer entity type if not provided
    if entity_type is None:
        entity_type = infer_entity_type(bioimage_index, entity_id)
        if entity_type is None:
            return {
                "error": f"Entity not found: {entity_id}",
                "entity_id": entity_id,
            }

    if entity_type.lower() == "node":
        # For a node, get the technologies it provides
        technologies = bioimage_index.get_technologies_by_node_id(entity_id)
        if not technologies and not bioimage_index.get_node_by_id(entity_id):
            return {
                "error": f"Node not found: {entity_id}",
                "entity_id": entity_id,
                "entity_type": entity_type,
            }

        return {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "related_technologies": technologies,
            "total_related": len(technologies),
        }

    elif entity_type.lower() == "technology":
        # For a technology, get the nodes that provide it
        nodes = bioimage_index.get_nodes_by_technology_id(entity_id)
        if not nodes and not bioimage_index.get_technology_by_id(entity_id):
            return {
                "error": f"Technology not found: {entity_id}",
                "entity_id": entity_id,
                "entity_type": entity_type,
            }

        return {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "related_nodes": nodes,
            "total_related": len(nodes),
        }

    else:
        return {
            "error": f"Invalid entity type: {entity_type}. Must be 'node' or 'technology'.",
            "entity_id": entity_id,
            "entity_type": entity_type,
        }


async def initialize_bioimage_database(
    memory: AsyncMemory,
    nodes_data: List[Dict[str, Any]],
    technologies_data: List[Dict[str, Any]],
) -> Dict[str, int]:
    """
    Initialize the bioimage database in mem0 with semantic embeddings.
    Uses flat metadata structure (no nested metadata.metadata).

    Args:
        memory: AsyncMemory instance
        nodes_data: List of node data dictionaries
        technologies_data: List of technology data dictionaries

    Returns:
        Dictionary with initialization results
    """
    logger.info(
        "Initializing bioimage database with %d nodes and %d technologies",
        len(nodes_data),
        len(technologies_data),
    )

    added_items = {"nodes": 0, "technologies": 0}

    # Add nodes to mem0 with flat metadata
    for node in nodes_data:
        content = _create_node_content(node)
        metadata = _create_node_metadata(node)

        await memory.add(
            messages=[{"role": "user", "content": content}],
            agent_id=EBI_AGENT_ID,
            metadata=metadata,
            infer=False,
        )
        added_items["nodes"] += 1

    # Add technologies to mem0 with flat metadata
    for tech in technologies_data:
        content = _create_technology_content(tech)
        metadata = _create_technology_metadata(tech)

        await memory.add(
            messages=[{"role": "user", "content": content}],
            agent_id=EBI_AGENT_ID,
            metadata=metadata,
            infer=False,
        )
        added_items["technologies"] += 1

    logger.info("Database initialization complete: %s", added_items)
    return added_items


async def query(
    memory: AsyncMemory,
    bioimage_index: BioimageIndex,
    query_text: str,
    entity_types: Optional[List[str]] = None,
    include_related: bool = True,
    limit: int = 10,
) -> Dict[str, Any]:
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
        entity_types=entity_types,
        limit=limit,
    )

    # Step 2: For each result, find related entities using bioimage_index
    enhanced_results = []
    for result in semantic_results["results"]:
        enhanced_result = result.copy()

        if include_related:
            entity_id = result["metadata"].get("entity_id")
            entity_type = result["metadata"].get("entity_type")

            if entity_id:
                related_entities = await get_related_entities(
                    entity_id=entity_id,
                    bioimage_index=bioimage_index,
                    entity_type=entity_type,
                )
                enhanced_result["related_entities"] = related_entities
            else:
                enhanced_result["related_entities"] = []

        enhanced_results.append(enhanced_result)

    return {
        "query": query_text,
        "entity_types": entity_types,
        "semantic_results": enhanced_results,
        "total_results": len(enhanced_results),
        "include_related": include_related,
    }
