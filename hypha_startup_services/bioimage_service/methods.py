"""Service methods for the BioImage service."""

import logging
from typing import Dict, Any
from hypha_startup_services.bioimage_service.data_index import BioimageIndex

logger = logging.getLogger(__name__)


async def get_nodes_by_technology_id(
    bioimage_index: BioimageIndex,
    technology_id: str,
    include_technology_details: bool = True,
) -> Dict[str, Any]:
    """
    Get all nodes that provide a specific technology.

    Args:
        bioimage_index: The bioimage index instance.
        technology_id: The ID of the technology to search for.
        include_technology_details: Whether to include technology details in response.

    Returns:
        A dictionary containing the matching nodes and optionally technology details.
    """
    index = bioimage_index

    # Get the technology details
    technology = index.get_technology_by_id(technology_id)
    if not technology:
        return {
            "error": f"Technology not found: {technology_id}",
            "technology_id": technology_id,
            "nodes": [],
        }

    # Get all nodes that provide this technology
    nodes = index.get_nodes_by_technology_id(technology_id)

    result = {
        "technology_id": technology_id,
        "nodes": nodes,
        "total_nodes": len(nodes),
    }

    if include_technology_details:
        result["technology"] = technology

    logger.info("Found %d nodes for technology %s", len(nodes), technology_id)
    return result


async def get_technologies_by_node_id(
    bioimage_index: BioimageIndex,
    node_id: str,
    include_node_details: bool = True,
) -> Dict[str, Any]:
    """
    Get all technologies provided by a specific node.

    Args:
        bioimage_index: The bioimage index instance.
        node_id: The ID of the node to search for.
        include_node_details: Whether to include node details in response.

    Returns:
        A dictionary containing the matching technologies and optionally node details.
    """
    index = bioimage_index

    # Get the node details
    node = index.get_node_by_id(node_id)
    if not node:
        return {
            "error": f"Node not found: {node_id}",
            "node_id": node_id,
            "technologies": [],
        }

    # Get all technologies provided by this node
    technologies = index.get_technologies_by_node_id(node_id)

    result = {
        "node_id": node_id,
        "technologies": technologies,
        "total_technologies": len(technologies),
    }

    if include_node_details:
        result["node"] = node

    logger.info("Found %d technologies for node %s", len(technologies), node_id)
    return result


async def get_node_details(
    bioimage_index: BioimageIndex, node_id: str
) -> Dict[str, Any]:
    """
    Get detailed information about a specific node.

    Args:
        bioimage_index: The bioimage index instance.
        node_id: The ID of the node.

    Returns:
        Node details or error if not found.
    """
    index = bioimage_index
    node = index.get_node_by_id(node_id)

    if not node:
        return {
            "error": f"Node not found: {node_id}",
            "node_id": node_id,
        }

    return {
        "node_id": node_id,
        "node": node,
    }


async def get_technology_details(
    bioimage_index: BioimageIndex, technology_id: str
) -> Dict[str, Any]:
    """
    Get detailed information about a specific technology.

    Args:
        bioimage_index: The bioimage index instance.
        technology_id: The ID of the technology.

    Returns:
        Technology details or error if not found.
    """
    index = bioimage_index
    technology = index.get_technology_by_id(technology_id)

    if not technology:
        return {
            "error": f"Technology not found: {technology_id}",
            "technology_id": technology_id,
        }

    return {
        "technology_id": technology_id,
        "technology": technology,
    }


async def search_nodes(
    bioimage_index: BioimageIndex,
    query: str,
    limit: int = 10,
) -> Dict[str, Any]:
    """
    Search for nodes by name.

    Args:
        bioimage_index: The bioimage index instance.
        query: Search query string.
        limit: Maximum number of results to return.

    Returns:
        A dictionary containing matching nodes.
    """
    index = bioimage_index
    nodes = index.search_nodes_by_name(query)

    # Limit results
    limited_nodes = nodes[:limit] if limit > 0 else nodes

    return {
        "query": query,
        "nodes": limited_nodes,
        "total_results": len(nodes),
        "returned_results": len(limited_nodes),
    }


async def search_technologies(
    bioimage_index: BioimageIndex,
    query: str,
    limit: int = 10,
) -> Dict[str, Any]:
    """
    Search for technologies by name.

    Args:
        bioimage_index: The bioimage index instance.
        query: Search query string.
        limit: Maximum number of results to return.

    Returns:
        A dictionary containing matching technologies.
    """
    index = bioimage_index
    technologies = index.search_technologies_by_name(query)

    # Limit results
    limited_technologies = technologies[:limit] if limit > 0 else technologies

    return {
        "query": query,
        "technologies": limited_technologies,
        "total_results": len(technologies),
        "returned_results": len(limited_technologies),
    }


async def get_all_nodes(
    bioimage_index: BioimageIndex, limit: int = 100
) -> Dict[str, Any]:
    """
    Get all nodes in the index.

    Args:
        bioimage_index: The bioimage index instance.
        limit: Maximum number of results to return.

    Returns:
        A dictionary containing all nodes.
    """
    index = bioimage_index
    nodes = index.get_all_nodes()

    # Limit results
    limited_nodes = nodes[:limit] if limit > 0 else nodes

    return {
        "nodes": limited_nodes,
        "total_nodes": len(nodes),
        "returned_nodes": len(limited_nodes),
    }


async def get_all_technologies(
    bioimage_index: BioimageIndex, limit: int = 100
) -> Dict[str, Any]:
    """
    Get all technologies in the index.

    Args:
        bioimage_index: The bioimage index instance.
        limit: Maximum number of results to return.

    Returns:
        A dictionary containing all technologies.
    """
    index = bioimage_index
    technologies = index.get_all_technologies()

    # Limit results
    limited_technologies = technologies[:limit] if limit > 0 else technologies

    return {
        "technologies": limited_technologies,
        "total_technologies": len(technologies),
        "returned_technologies": len(limited_technologies),
    }


async def get_service_statistics(
    bioimage_index: BioimageIndex,
) -> Dict[str, Any]:
    """
    Get statistics about the bioimage service index.

    Args:
        bioimage_index: The bioimage index instance.

    Returns:
        A dictionary containing service statistics.
    """
    index = bioimage_index
    stats = index.get_statistics()

    return {
        "service": "bioimage_service",
        "status": "active",
        "statistics": stats,
    }
