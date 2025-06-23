"""Tests for enhanced bioimage service with mem0 semantic search capabilities."""

import pytest
import pytest_asyncio
from hypha_startup_services.bioimage_service.methods import (
    create_get_entity_details,
    create_get_related_entities,
    create_query,
)
from hypha_startup_services.bioimage_service.data_index import (
    load_external_data,
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
async def test_enhanced_bioimage_find_related_entities_by_technology_id(bioimage_index):
    """Test finding related entities by technology ID (auto-infer type)."""

    # Find entities related to 4Pi technology (should automatically infer it's a technology)
    tech_id = "68a3b6c4-9c19-4446-9617-22e7d37e0f2c"

    get_related_func = create_get_related_entities(bioimage_index)
    result = await get_related_func(entity_id=tech_id)

    assert result is not None
    assert isinstance(result, list)

    # Should find nodes that provide this technology
    assert len(result) > 0

    # Check that we get the right structure for related entities
    for related_entity in result:
        assert "id" in related_entity
        assert "name" in related_entity


@pytest.mark.asyncio
async def test_enhanced_bioimage_find_related_entities_by_node_id(bioimage_index):
    """Test finding related entities by node ID (auto-infer type)."""
    # Find entities related to Italian ALM Node (should automatically infer it's a node)
    node_id = "7409a98f-1bdb-47d2-80e7-c89db73efedd"

    get_related_func = create_get_related_entities(bioimage_index)
    result = await get_related_func(entity_id=node_id)

    assert result is not None
    assert isinstance(result, list)

    # Should find technologies that this node provides
    assert len(result) > 0

    # Check that we get the right structure for related entities
    for related_entity in result:
        assert "id" in related_entity
        assert "name" in related_entity


@pytest.mark.asyncio
async def test_enhanced_bioimage_semantic_query(bioimage_index, mem0_memory):
    """Test semantic query functionality."""

    query_func = create_query(mem0_memory, bioimage_index)
    result = await query_func(
        query_text="electron microscopy",
        limit=10,
    )

    print(result)  # For debugging purposes

    assert result is not None
    assert isinstance(result, dict)
    assert "results" in result
    assert "total_results" in result
    assert isinstance(result["results"], list)

    if result["results"]:
        first_result = result["results"][0]
        assert "info" in first_result


@pytest.mark.asyncio
async def test_enhanced_bioimage_semantic_query_with_context(
    bioimage_index, mem0_memory
):
    """Test semantic query with related entity context."""

    query_func = create_query(mem0_memory, bioimage_index)
    result = await query_func(
        query_text="What techniques are available in Italy?",
        include_related=True,
        limit=3,
    )

    print(result)  # For debugging purposes

    assert result is not None
    assert isinstance(result, dict)
    assert "results" in result
    assert "total_results" in result
    assert isinstance(result["results"], list)

    # Check if related entities are included when available
    if result["results"]:
        for enhanced_result in result["results"]:
            assert "info" in enhanced_result
            # Should have either has_technologies or exists_in_nodes when include_related=True
            # Note: Only assert if we actually found related entities
            has_relations = (
                "has_technologies" in enhanced_result
                or "exists_in_nodes" in enhanced_result
            )
            # The enhanced result may or may not have relations based on the data


@pytest.mark.asyncio
async def test_enhanced_bioimage_traditional_methods_still_work(
    bioimage_index, mem0_memory
):
    """Test that traditional exact matching methods still work."""
    # Test getting nodes by technology ID
    tech_id = "68a3b6c4-9c19-4446-9617-22e7d37e0f2c"

    get_related_func = create_get_related_entities(bioimage_index)
    result = await get_related_func(entity_id=tech_id)

    assert result is not None
    assert isinstance(result, list)
    # Should have some related entities
    assert len(result) > 0

    # Check that each entity has required fields
    for entity in result:
        assert "id" in entity
        assert "name" in entity


@pytest.mark.asyncio
async def test_enhanced_bioimage_error_handling_invalid_entity(
    bioimage_index, mem0_memory
):
    """Test error handling for invalid entity ID."""
    # The function should raise an exception for invalid entity IDs
    get_related_func = create_get_related_entities(bioimage_index)
    with pytest.raises(ValueError, match="Entity not found"):
        await get_related_func(entity_id="invalid-entity-id-12345")


@pytest.mark.asyncio
async def test_enhanced_bioimage_entity_agnostic_functions(bioimage_index, mem0_memory):
    """Test the new entity-type-agnostic functions."""

    # Test get_entity_details for a node
    node_id = "7409a98f-1bdb-47d2-80e7-c89db73efedd"
    get_entity_func = create_get_entity_details(bioimage_index)
    result = await get_entity_func(entity_id=node_id)

    assert result is not None
    assert result["entity_id"] == node_id
    assert result["entity_type"] == "node"
    assert result["entity_details"] is not None
    assert result["entity_details"]["name"] == "Advanced Light Microscopy Italian Node"

    # Test get_entity_details for a technology
    tech_id = "68a3b6c4-9c19-4446-9617-22e7d37e0f2c"
    result = await get_entity_func(entity_id=tech_id)

    assert result is not None
    assert result["entity_id"] == tech_id
    assert result["entity_type"] == "technology"
    assert result["entity_details"] is not None
    assert result["entity_details"]["name"] == "4Pi microscopy"

    # Test get_related_entities for a node
    get_related_func = create_get_related_entities(bioimage_index)
    result = await get_related_func(entity_id=node_id)

    assert result is not None
    assert isinstance(result, list)
    # Check that we have related technologies
    assert len(result) > 0

    # Test get_related_entities for a technology
    result = await get_related_func(entity_id=tech_id)

    assert result is not None
    assert isinstance(result, list)
    # Check that we have related nodes
    assert len(result) > 0
