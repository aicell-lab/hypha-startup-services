import logging
from typing import Dict, Any
from mem0 import AsyncMemory

logger = logging.getLogger(__name__)

# Constants for mem0 integration
EBI_AGENT_ID = "ebi_bioimage_assistant"
EBI_WORKSPACE = "ebi_data"


def _create_node_content(node: Dict[str, Any]) -> str:
    """Create content string for a node."""
    name = node.get("name", "Unknown")
    description = node.get("description", "")
    country = (
        node.get("country", {}).get("name", "Unknown")
        if isinstance(node.get("country"), dict)
        else str(node.get("country", "Unknown"))
    )
    technologies = (
        ", ".join(node.get("technologies", [])) if node.get("technologies") else "None"
    )
    return f"Bioimaging node: {name} in {country}. Description: {description}. Technologies: {technologies}"


def _create_node_metadata(node: Dict[str, Any]) -> Dict[str, Any]:
    """Create flat metadata for a node."""
    country_info = node.get("country", {})
    metadata = {
        "entity_type": "node",
        "entity_id": node.get("id"),
        "name": node.get("name"),
        "country": (
            country_info.get("name")
            if isinstance(country_info, dict)
            else str(country_info)
        ),
        "country_code": (
            country_info.get("iso_a2") if isinstance(country_info, dict) else None
        ),
        "description": node.get("description"),
        "technologies": (
            ",".join(node.get("technologies", [])) if node.get("technologies") else None
        ),
    }
    # Remove None values to avoid empty metadata fields
    return {k: v for k, v in metadata.items() if v is not None}


def _create_technology_content(tech: Dict[str, Any]) -> str:
    """Create content string for a technology."""
    name = tech.get("name", "Unknown")
    description = tech.get("description", "")
    category = (
        tech.get("category", {}).get("name", "Unknown")
        if isinstance(tech.get("category"), dict)
        else str(tech.get("category", "Unknown"))
    )
    abbr = tech.get("abbr", "")
    return f"Bioimaging technology: {name} ({abbr}). Category: {category}. Description: {description}"


def _create_technology_metadata(tech: Dict[str, Any]) -> Dict[str, Any]:
    """Create flat metadata for a technology."""
    category_info = tech.get("category", {})
    metadata = {
        "entity_type": "technology",
        "entity_id": tech.get("id"),
        "name": tech.get("name"),
        "abbreviation": tech.get("abbr"),
        "category": (
            category_info.get("name")
            if isinstance(category_info, dict)
            else str(category_info)
        ),
        "description": tech.get("description"),
    }
    # Remove None values to avoid empty metadata fields
    return {k: v for k, v in metadata.items() if v is not None}


def _extract_metadata_from_memory(memory_item: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and flatten metadata from memory item."""
    metadata = memory_item.get("metadata", {})
    # Handle nested metadata structure if it exists
    if isinstance(metadata, dict) and "metadata" in metadata:
        return metadata.get("metadata", {})
    return metadata


async def semantic_bioimage_search(
    memory: AsyncMemory,
    search_query: str,
    limit: int = 10,
) -> Dict[str, Any]:
    """
    Perform semantic search on the bioimage database.

    Args:
        memory: AsyncMemory instance
        query: Natural language query
        entity_types: Filter by entity types (node, technology)
        limit: Maximum number of results

    Returns:
        Dictionary with search results and metadata
    """
    logger.info("Performing semantic search for: '%s'", search_query)

    search_results = await memory.search(
        query=search_query,
        agent_id=EBI_AGENT_ID,
        limit=limit,
    )

    # Process results
    memories = search_results.get("results", [])
    processed_results = []

    for memory_item in memories:
        metadata = _extract_metadata_from_memory(memory_item)
        processed_results.append(
            {
                "memory": memory_item.get("memory", ""),
                "score": memory_item.get("score", 0.0),
                "metadata": metadata,
            }
        )

    return {
        "query": search_query,
        "results": processed_results,
        "total_results": len(processed_results),
    }
