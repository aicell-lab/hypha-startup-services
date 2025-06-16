"""Tests for enhanced bioimage service with mem0 semantic search capabilities."""

import pytest
import pytest_asyncio
from tests.conftest import get_user_server
from hypha_startup_services.bioimage_service.methods import (
    get_entity_details,
    get_related_entities,
    initialize_bioimage_database,
    query,
)
from hypha_startup_services.bioimage_service.data_index import (
    BioimageIndex,
    load_external_data,
    EBI_NODES_DATA,
    EBI_TECHNOLOGIES_DATA,
)
from hypha_startup_services.mem0_service.mem0_client import get_mem0


@pytest_asyncio.fixture
async def bioimage_index():
    """Create a fresh bioimage index for testing."""
    return load_external_data()


@pytest_asyncio.fixture
async def mem0_memory():
    """Get mem0 memory instance for testing."""
    return await get_mem0()


@pytest.mark.asyncio
async def test_enhanced_bioimage_initialize_database(bioimage_index, mem0_memory):
    """Test initializing the bioimage database with mem0."""
    # Initialize the database
    result = await initialize_bioimage_database(
        memory=mem0_memory,
        nodes_data=EBI_NODES_DATA,
        technologies_data=EBI_TECHNOLOGIES_DATA,
    )

    assert result is not None
    assert isinstance(result, dict)
    assert "nodes" in result
    assert "technologies" in result
    assert result["nodes"] > 0
    assert result["technologies"] > 0


@pytest.mark.asyncio
async def test_enhanced_bioimage_find_related_entities_by_technology_id(
    bioimage_index, mem0_memory
):
    """Test finding related entities by technology ID (auto-infer type)."""
    # First initialize the database
    await initialize_bioimage_database(
        memory=mem0_memory,
        nodes_data=EBI_NODES_DATA,
        technologies_data=EBI_TECHNOLOGIES_DATA,
    )

    # Find entities related to 4Pi technology (should automatically infer it's a technology)
    tech_id = "68a3b6c4-9c19-4446-9617-22e7d37e0f2c"

    result = await get_related_entities(
        bioimage_index=bioimage_index, entity_id=tech_id
    )

    assert result is not None
    assert isinstance(result, dict)

    # Should find nodes that provide this technology
    assert "related_nodes" in result
    assert len(result["related_nodes"]) > 0

    # Check that we get the right structure for related entities
    for related_entity in result["related_nodes"]:
        assert "id" in related_entity
        assert "name" in related_entity


@pytest.mark.asyncio
async def test_enhanced_bioimage_find_related_entities_by_node_id(
    bioimage_index, mem0_memory
):
    """Test finding related entities by node ID (auto-infer type)."""
    # Find entities related to Italian ALM Node (should automatically infer it's a node)
    node_id = "7409a98f-1bdb-47d2-80e7-c89db73efedd"

    result = await get_related_entities(
        bioimage_index=bioimage_index, entity_id=node_id
    )

    assert result is not None
    assert isinstance(result, dict)

    # Should find technologies that this node provides
    assert "related_technologies" in result
    assert len(result["related_technologies"]) > 0

    # Check that we get the right structure for related entities
    for related_entity in result["related_technologies"]:
        assert "id" in related_entity
        assert "name" in related_entity


@pytest.mark.asyncio
async def test_enhanced_bioimage_semantic_query(bioimage_index, mem0_memory):
    """Test semantic query functionality."""
    # First initialize the database
    await initialize_bioimage_database(
        memory=mem0_memory,
        nodes_data=EBI_NODES_DATA,
        technologies_data=EBI_TECHNOLOGIES_DATA,
    )

    result = await query(
        memory=mem0_memory,
        bioimage_index=bioimage_index,
        query_text="electron microscopy",
    )

    assert result is not None
    assert isinstance(result, dict)
    assert "semantic_results" in result
    assert "query" in result
    assert result["query"] == "electron microscopy"


@pytest.mark.asyncio
async def test_enhanced_bioimage_semantic_query_with_context(
    bioimage_index, mem0_memory
):
    """Test semantic query with related entity context."""
    # First initialize the database
    await initialize_bioimage_database(
        memory=mem0_memory,
        nodes_data=EBI_NODES_DATA,
        technologies_data=EBI_TECHNOLOGIES_DATA,
    )

    result = await query(
        memory=mem0_memory,
        bioimage_index=bioimage_index,
        query_text="super resolution microscopy",
        include_related=True,
        limit=10,
    )

    assert result is not None
    assert isinstance(result, dict)
    assert "semantic_results" in result
    assert "query" in result
    assert isinstance(result["semantic_results"], list)

    # Check if related entities are included when available
    if result["semantic_results"]:
        for semantic_result in result["semantic_results"]:
            if result.get("include_related", False):
                assert "related_entities" in semantic_result


@pytest.mark.asyncio
async def test_enhanced_bioimage_traditional_methods_still_work(
    bioimage_index, mem0_memory
):
    """Test that traditional exact matching methods still work."""
    # Test getting nodes by technology ID
    tech_id = "68a3b6c4-9c19-4446-9617-22e7d37e0f2c"
    result = await get_related_entities(
        bioimage_index=bioimage_index, entity_id=tech_id
    )

    assert result is not None
    assert isinstance(result, dict)
    assert "entity_id" in result
    # The new API returns "related_nodes" for technologies, not "related_entities"
    assert "related_nodes" in result or "related_technologies" in result
    assert result["entity_id"] == tech_id
    # Check that we have some related entities
    related_count = len(result.get("related_nodes", [])) + len(
        result.get("related_technologies", [])
    )
    assert related_count > 0


@pytest.mark.asyncio
async def test_enhanced_bioimage_error_handling_invalid_entity(
    bioimage_index, mem0_memory
):
    """Test error handling for invalid entity ID."""
    result = await get_related_entities(
        bioimage_index=bioimage_index, entity_id="invalid-entity-id-12345"
    )

    assert result is not None
    # The function returns an error dict for invalid entity IDs
    assert isinstance(result, dict)
    assert "error" in result


@pytest.mark.asyncio
async def test_enhanced_bioimage_entity_agnostic_functions(bioimage_index, mem0_memory):
    """Test the new entity-type-agnostic functions."""

    # Test get_entity_details for a node
    node_id = "7409a98f-1bdb-47d2-80e7-c89db73efedd"
    result = await get_entity_details(bioimage_index=bioimage_index, entity_id=node_id)

    assert result is not None
    assert result["entity_id"] == node_id
    assert result["entity_type"] == "node"
    assert result["entity_details"] is not None  # The regular API uses "entity_details"
    assert result["entity_details"]["name"] == "Advanced Light Microscopy Italian Node"

    # Test get_entity_details for a technology
    tech_id = "68a3b6c4-9c19-4446-9617-22e7d37e0f2c"
    result = await get_entity_details(bioimage_index=bioimage_index, entity_id=tech_id)

    assert result is not None
    assert result["entity_id"] == tech_id
    assert result["entity_type"] == "technology"
    assert result["entity_details"] is not None  # The regular API uses "entity_details"
    assert result["entity_details"]["name"] == "4Pi microscopy"

    # Test get_related_entities for a node
    result = await get_related_entities(
        bioimage_index=bioimage_index, entity_id=node_id
    )

    assert result is not None
    assert result["entity_id"] == node_id
    assert result["entity_type"] == "node"
    # Check that we have related technologies
    assert "related_technologies" in result
    assert len(result["related_technologies"]) > 0
    assert result["total_related"] > 0

    # Test get_related_entities for a technology
    result = await get_related_entities(
        bioimage_index=bioimage_index, entity_id=tech_id
    )

    assert result is not None
    assert result["entity_id"] == tech_id
    assert result["entity_type"] == "technology"
    # Check that we have related nodes
    assert "related_nodes" in result
    assert len(result["related_nodes"]) > 0
    assert result["total_related"] > 0
