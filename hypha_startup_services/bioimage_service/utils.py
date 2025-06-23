import logging
import re
from typing import Dict, Any
from mem0 import AsyncMemory

logger = logging.getLogger(__name__)

# Constants for mem0 integration
EBI_AGENT_ID = "ebi_bioimage_assistant"
EBI_WORKSPACE = "ebi_data"


def clean_text_for_json(text: str) -> str:
    """
    Clean text to prevent JSON parsing issues.

    Args:
        text: The text to clean

    Returns:
        Cleaned text safe for JSON encoding
    """
    if not isinstance(text, str):
        text = str(text)

    # Remove null bytes and control characters
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)

    # Replace problematic quotes with safe alternatives
    text = text.replace('"', "'").replace("'", "'")

    # Remove or replace other potentially problematic characters
    text = re.sub(r"[\\]", "/", text)  # Replace backslashes

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Ensure text is not too long (mem0 might have limits)
    if len(text) > 8000:
        text = text[:8000] + "..."

    return text


def create_node_content(node: Dict[str, Any]) -> str:
    """Create content string for a node."""
    name = clean_text_for_json(node.get("name", "Unknown"))
    description = clean_text_for_json(node.get("description", ""))
    country = (
        clean_text_for_json(node.get("country", {}).get("name", "Unknown"))
        if isinstance(node.get("country"), dict)
        else clean_text_for_json(str(node.get("country", "Unknown")))
    )
    technologies = (
        ", ".join([clean_text_for_json(tech) for tech in node.get("technologies", [])])
        if node.get("technologies")
        else "None"
    )
    return f"Bioimaging node: {name} in {country}. Description: {description}. Technologies: {technologies}"


def create_node_metadata(node: Dict[str, Any]) -> Dict[str, Any]:
    """Create flat metadata for a node."""
    country_info = node.get("country", {})
    metadata = {
        "entity_type": "node",
        "entity_id": clean_text_for_json(str(node.get("id", ""))),
        "name": clean_text_for_json(node.get("name", "")),
        "country": (
            clean_text_for_json(country_info.get("name", ""))
            if isinstance(country_info, dict)
            else clean_text_for_json(str(country_info))
        ),
        "country_code": (
            clean_text_for_json(country_info.get("iso_a2", ""))
            if isinstance(country_info, dict)
            else None
        ),
        "description": clean_text_for_json(node.get("description", "")),
        "technologies": (
            ",".join(
                [clean_text_for_json(tech) for tech in node.get("technologies", [])]
            )
            if node.get("technologies")
            else None
        ),
    }
    # Remove None values and empty strings to avoid empty metadata fields
    return {k: v for k, v in metadata.items() if v is not None and v != ""}


def create_technology_content(tech: Dict[str, Any]) -> str:
    """Create content string for a technology."""
    name = clean_text_for_json(tech.get("name", "Unknown"))
    description = clean_text_for_json(tech.get("description", ""))
    category = (
        clean_text_for_json(tech.get("category", {}).get("name", "Unknown"))
        if isinstance(tech.get("category"), dict)
        else clean_text_for_json(str(tech.get("category", "Unknown")))
    )
    abbr = clean_text_for_json(tech.get("abbr", ""))
    return f"Bioimaging technology: {name} ({abbr}). Category: {category}. Description: {description}"


def create_technology_metadata(tech: Dict[str, Any]) -> Dict[str, Any]:
    """Create flat metadata for a technology."""
    category_info = tech.get("category", {})
    metadata = {
        "entity_type": "technology",
        "entity_id": clean_text_for_json(str(tech.get("id", ""))),
        "name": clean_text_for_json(tech.get("name", "")),
        "abbreviation": clean_text_for_json(tech.get("abbr", "")),
        "category": (
            clean_text_for_json(category_info.get("name", ""))
            if isinstance(category_info, dict)
            else clean_text_for_json(str(category_info))
        ),
        # "description": tech.get("description"),
    }
    # Remove None values and empty strings to avoid empty metadata fields
    return {k: v for k, v in metadata.items() if v is not None and v != ""}


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
        metadata = memory_item.get("metadata", {})
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
