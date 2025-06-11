"""Tests for the BioImage service."""

import pytest
from hypha_startup_services.bioimage_service.data_index import (
    BioimageIndex,
    EBI_NODES_DATA,
    EBI_TECHNOLOGIES_DATA,
)
from hypha_startup_services.bioimage_service.methods import (
    get_nodes_by_technology_id,
    get_technologies_by_node_id,
    get_node_details,
    get_technology_details,
    search_nodes,
    search_technologies,
    get_all_nodes,
    get_all_technologies,
    get_service_statistics,
)


@pytest.fixture
def bioimage_index():
    """Create a fresh bioimage index for testing."""
    index = BioimageIndex()
    index.load_data(EBI_NODES_DATA, EBI_TECHNOLOGIES_DATA)
    return index


@pytest.mark.asyncio
async def test_bioimage_index_basic_functionality(bioimage_index):
    """Test basic functionality of the bioimage index."""
    # Test statistics
    stats = bioimage_index.get_statistics()
    assert stats["total_nodes"] >= 3
    assert stats["total_technologies"] >= 3
    assert stats["total_relationships"] > 0

    # Test node retrieval
    node = bioimage_index.get_node_by_id("7409a98f-1bdb-47d2-80e7-c89db73efedd")
    assert node is not None
    assert node["name"] == "Advanced Light Microscopy Italian Node"

    # Test technology retrieval
    tech = bioimage_index.get_technology_by_id("f0acc857-fc72-4094-bf14-c36ac40801c5")
    assert tech is not None
    assert "3D Correlative Light and Electron Microscopy" in tech["name"]


@pytest.mark.asyncio
async def test_get_nodes_by_technology_id():
    """Test getting nodes by technology ID."""
    # Test with known technology ID
    result = await get_nodes_by_technology_id("f0acc857-fc72-4094-bf14-c36ac40801c5")

    assert "technology_id" in result
    assert "nodes" in result
    assert "total_nodes" in result
    assert "technology" in result  # Should include technology details by default

    # Should find both Italian and Polish nodes that have 3D-CLEM
    assert result["total_nodes"] >= 2

    # Check that all returned nodes have the expected structure
    for node in result["nodes"]:
        assert "id" in node
        assert "name" in node
        assert "description" in node


@pytest.mark.asyncio
async def test_get_nodes_by_technology_id_not_found():
    """Test getting nodes for non-existent technology ID."""
    result = await get_nodes_by_technology_id("nonexistent-tech-id")

    assert "error" in result
    assert result["technology_id"] == "nonexistent-tech-id"
    assert result["nodes"] == []


@pytest.mark.asyncio
async def test_get_technologies_by_node_id():
    """Test getting technologies by node ID."""
    # Test with known node ID (Italian node)
    result = await get_technologies_by_node_id("7409a98f-1bdb-47d2-80e7-c89db73efedd")

    assert "node_id" in result
    assert "technologies" in result
    assert "total_technologies" in result
    assert "node" in result  # Should include node details by default

    # Should find multiple technologies
    assert result["total_technologies"] >= 2

    # Check that all returned technologies have the expected structure
    for tech in result["technologies"]:
        assert "id" in tech
        assert "name" in tech
        assert "description" in tech


@pytest.mark.asyncio
async def test_get_technologies_by_node_id_not_found():
    """Test getting technologies for non-existent node ID."""
    result = await get_technologies_by_node_id("nonexistent-node-id")

    assert "error" in result
    assert result["node_id"] == "nonexistent-node-id"
    assert result["technologies"] == []


@pytest.mark.asyncio
async def test_get_node_details():
    """Test getting node details."""
    # Test with known node ID
    result = await get_node_details("7409a98f-1bdb-47d2-80e7-c89db73efedd")

    assert "node_id" in result
    assert "node" in result
    assert result["node"]["name"] == "Advanced Light Microscopy Italian Node"

    # Test with non-existent node ID
    result = await get_node_details("nonexistent-node-id")
    assert "error" in result


@pytest.mark.asyncio
async def test_get_technology_details():
    """Test getting technology details."""
    # Test with known technology ID
    result = await get_technology_details("f0acc857-fc72-4094-bf14-c36ac40801c5")

    assert "technology_id" in result
    assert "technology" in result
    assert (
        "3D Correlative Light and Electron Microscopy" in result["technology"]["name"]
    )

    # Test with non-existent technology ID
    result = await get_technology_details("nonexistent-tech-id")
    assert "error" in result


@pytest.mark.asyncio
async def test_search_nodes():
    """Test searching nodes by name."""
    # Test search with partial match
    result = await search_nodes("microscopy", limit=5)

    assert "query" in result
    assert "nodes" in result
    assert "total_results" in result
    assert "returned_results" in result

    # Should find nodes with "microscopy" in the name
    assert result["total_results"] >= 2
    assert len(result["nodes"]) <= 5  # Respects limit


@pytest.mark.asyncio
async def test_search_technologies():
    """Test searching technologies by name."""
    # Test search with partial match
    result = await search_technologies("microscopy", limit=5)

    assert "query" in result
    assert "technologies" in result
    assert "total_results" in result
    assert "returned_results" in result

    # Should find technologies with "microscopy" in the name
    assert result["total_results"] >= 1


@pytest.mark.asyncio
async def test_get_all_nodes():
    """Test getting all nodes."""
    result = await get_all_nodes(limit=10)

    assert "nodes" in result
    assert "total_nodes" in result
    assert "returned_nodes" in result

    assert result["total_nodes"] >= 3
    assert len(result["nodes"]) <= 10  # Respects limit


@pytest.mark.asyncio
async def test_get_all_technologies():
    """Test getting all technologies."""
    result = await get_all_technologies(limit=10)

    assert "technologies" in result
    assert "total_technologies" in result
    assert "returned_technologies" in result

    assert result["total_technologies"] >= 3
    assert len(result["technologies"]) <= 10  # Respects limit


@pytest.mark.asyncio
async def test_get_service_statistics():
    """Test getting service statistics."""
    result = await get_service_statistics()

    assert "service" in result
    assert "status" in result
    assert "statistics" in result

    assert result["service"] == "bioimage_service"
    assert result["status"] == "active"

    stats = result["statistics"]
    assert "total_nodes" in stats
    assert "total_technologies" in stats
    assert "total_relationships" in stats


@pytest.mark.asyncio
async def test_relationship_consistency(bioimage_index):
    """Test that relationships are consistent between nodes and technologies."""
    # Get all node-to-tech relationships
    for node_id, tech_ids in bioimage_index.node_to_technologies.items():
        for tech_id in tech_ids:
            # Check that the reverse relationship exists
            assert tech_id in bioimage_index.technology_to_nodes
            assert node_id in bioimage_index.technology_to_nodes[tech_id]

    # Get all tech-to-node relationships
    for tech_id, node_ids in bioimage_index.technology_to_nodes.items():
        for node_id in node_ids:
            # Check that the reverse relationship exists
            assert node_id in bioimage_index.node_to_technologies
            assert tech_id in bioimage_index.node_to_technologies[node_id]


@pytest.mark.asyncio
async def test_synthetic_technology_handling(bioimage_index):
    """Test that synthetic technologies are created for unrecognized references."""
    # The test data includes some non-UUID technology references
    # These should be converted to synthetic technologies

    # Check that synthetic technologies exist for string references
    synthetic_tech = bioimage_index.get_technology_by_id(
        "synthetic-correlative-microscopy"
    )
    assert synthetic_tech is not None
    assert synthetic_tech.get("synthetic") is True
    assert "correlative_microscopy" in synthetic_tech["name"]
