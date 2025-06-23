"""Data processing utilities for the Weaviate BioImage service."""

import logging
from typing import Any, Dict, List
from hypha_startup_services.bioimage_service.data_index import BioimageIndex
from hypha_startup_services.common.chunking import chunk_text

logger = logging.getLogger(__name__)


def process_bioimage_nodes(
    nodes: List[Dict[str, Any]], chunk_size: int, chunk_overlap: int
) -> List[Dict[str, Any]]:
    """Process bioimage nodes into objects with chunking.

    Args:
        nodes: List of bioimage nodes
        chunk_size: Maximum tokens per chunk
        chunk_overlap: Token overlap between chunks

    Returns:
        List of processed objects ready for insertion
    """
    objects = []

    for node in nodes:
        node_text = _create_node_text(node)
        chunks = chunk_text(node_text, chunk_size, chunk_overlap)

        for chunk_idx, chunk in enumerate(chunks):
            objects.append(
                {
                    "text": chunk,
                    "entity_type": "node",
                    "entity_id": node.get("id", ""),
                    "name": node.get("name", ""),
                    "country": node.get("country", {}).get("name", ""),
                    "chunk_index": chunk_idx,
                    "total_chunks": len(chunks),
                }
            )

    return objects


def process_bioimage_technologies(
    technologies: List[Dict[str, Any]], chunk_size: int, chunk_overlap: int
) -> List[Dict[str, Any]]:
    """Process bioimage technologies into objects with chunking.

    Args:
        technologies: List of bioimage technologies
        chunk_size: Maximum tokens per chunk
        chunk_overlap: Token overlap between chunks

    Returns:
        List of processed objects ready for insertion
    """
    objects = []

    for tech in technologies:
        tech_text = _create_technology_text(tech)
        chunks = chunk_text(tech_text, chunk_size, chunk_overlap)

        for chunk_idx, chunk in enumerate(chunks):
            objects.append(
                {
                    "text": chunk,
                    "entity_type": "technology",
                    "entity_id": tech.get("id", ""),
                    "name": tech.get("name", ""),
                    "abbreviation": tech.get("abbr", ""),
                    "category": tech.get("category", {}).get("name", ""),
                    "chunk_index": chunk_idx,
                    "total_chunks": len(chunks),
                }
            )

    return objects


def process_bioimage_index(
    bioimage_index: BioimageIndex, chunk_size: int, chunk_overlap: int
) -> List[Dict[str, Any]]:
    """Process complete bioimage index into objects.

    Args:
        bioimage_index: The bioimage index containing nodes and technologies
        chunk_size: Maximum tokens per chunk
        chunk_overlap: Token overlap between chunks

    Returns:
        List of all processed objects ready for insertion
    """
    objects = []

    # Process nodes
    nodes = bioimage_index.get_all_nodes()
    objects.extend(process_bioimage_nodes(nodes, chunk_size, chunk_overlap))

    # Process technologies
    technologies = bioimage_index.get_all_technologies()
    objects.extend(
        process_bioimage_technologies(technologies, chunk_size, chunk_overlap)
    )

    return objects


def _create_node_text(node: Dict[str, Any]) -> str:
    """Create text representation of a bioimage node."""
    name = node.get("name", "")
    country = node.get("country", {}).get("name", "")
    description = node.get("description", "")

    return f"Bioimaging node: {name} in {country}. Description: {description}"


def _create_technology_text(tech: Dict[str, Any]) -> str:
    """Create text representation of a bioimage technology."""
    name = tech.get("name", "")
    abbr = tech.get("abbr", "")
    category = tech.get("category", {}).get("name", "")
    description = tech.get("description", "")

    return f"Bioimaging technology: {name} ({abbr}). Category: {category}. Description: {description}"
