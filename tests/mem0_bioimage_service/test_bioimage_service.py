"""Tests for the BioImage service."""

import pytest
import pytest_asyncio
from tests.conftest import get_user_server
from hypha_startup_services.common.data_index import (
    BioimageIndex,
    get_entity_details,
    get_related_entities,
)


# Sample EBI data - in a real implementation this would be loaded from external sources
EBI_NODES_DATA = [
    {
        "id": "7409a98f-1bdb-47d2-80e7-c89db73efedd",
        "name": "Advanced Light Microscopy Italian Node",
        "description": "The Italian ALM Node comprises five imaging facilities located in Naples, Genoa, Padua, Florence and Milan specializing in correlative light electron microscopy, super-resolution, and functional imaging.",
        "country": {"name": "Italy", "iso_a2": "IT"},
        "technologies": [
            "660fd1fc-a138-5740-b298-14b0c3b24fb9",
            "68a3b6c4-9c19-4446-9617-22e7d37e0f2c",  # 4Pi microscopy
            "correlative_microscopy",
            "super_resolution",
            "functional_imaging",
        ],
    },
    {
        "id": "099e48ff-7204-46ea-8828-10025e945081",
        "name": "Advanced Light Microscopy Node Poland",
        "description": "Multi-sited, multimodal EuroBioimaging Node offering open access to multi-modal ALM, CLEM, EM, functional imaging, high-throughput microscopy and super-resolution microscopy.",
        "country": {"name": "Poland", "iso_a2": "PL"},
        "technologies": [
            "660fd1fc-a138-5740-b298-14b0c3b24fb9",
            "multi_modal_alm",
            "clem",
            "electron_microscopy",
            "high_throughput",
        ],
    },
    {
        "id": "bc123456-789a-bcde-f012-3456789abcde",
        "name": "German BioImaging Node",
        "description": "German node providing advanced microscopy services including super-resolution and live-cell imaging.",
        "country": {"name": "Germany", "iso_a2": "DE"},
        "technologies": [
            "68a3b6c4-9c19-4446-9617-22e7d37e0f2c",  # 4Pi microscopy
            "super_resolution",
            "live_cell_imaging",
        ],
    },
]

EBI_TECHNOLOGIES_DATA = [
    {
        "id": "660fd1fc-a138-5740-b298-14b0c3b24fb9",
        "name": "3D Correlative Light and Electron Microscopy (3D-CLEM)",
        "abbr": "3D-CLEM",
        "description": "3D CLEM combines volume EM methods with 3D light microscopy techniques requiring 3D registration between modalities.",
        "category": {"name": "Correlative Light Microscopy and Electron Microscopy"},
    },
    {
        "id": "68a3b6c4-9c19-4446-9617-22e7d37e0f2c",
        "name": "4Pi microscopy",
        "abbr": "4Pi",
        "description": "Laser scanning fluorescence microscope with improved axial resolution using two opposing objective lenses for coherent wavefront matching.",
        "category": {"name": "Fluorescence Nanoscopy"},
    },
    {
        "id": "abc12345-6789-abcd-ef01-23456789abcd",
        "name": "Super-resolution microscopy",
        "abbr": "SRM",
        "description": "Techniques that surpass the diffraction limit of light microscopy to achieve nanometer resolution.",
        "category": {"name": "Fluorescence Nanoscopy"},
    },
]


@pytest_asyncio.fixture
async def mem0_live_service():
    """Mem0 BioImage service fixture for live (admin) environment."""
    server = await get_user_server("HYPHA_TOKEN")
    service = await server.get_service("aria-agents/mem0")
    yield service
    await server.disconnect()


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
    tech = bioimage_index.get_technology_by_id("660fd1fc-a138-5740-b298-14b0c3b24fb9")
    assert tech is not None
    assert "3D Correlative Light and Electron Microscopy" in tech["name"]


@pytest.mark.asyncio
async def test_get_nodes_by_technology_id(bioimage_index):
    """Test getting nodes by technology ID using get_related_entities."""
    # Test with known technology ID - this should find nodes that provide this technology
    result = get_related_entities(
        bioimage_index=bioimage_index,
        entity_id="660fd1fc-a138-5740-b298-14b0c3b24fb9",  # 3D-CLEM technology
    )

    # Should return a list of nodes that have this technology
    assert isinstance(result, list)
    assert (
        len(result) >= 2
    )  # Should find both Italian and Polish nodes that have 3D-CLEM

    # Check that all returned nodes have the expected structure
    for node in result:
        assert "id" in node
        assert "name" in node
        assert "description" in node


@pytest.mark.asyncio
async def test_get_nodes_by_technology_id_not_found(bioimage_index):
    """Test getting nodes for non-existent technology ID."""
    # Should raise ValueError for non-existent technology ID
    with pytest.raises(ValueError, match="Entity not found"):
        get_related_entities(
            bioimage_index=bioimage_index, entity_id="nonexistent-tech-id"
        )


@pytest.mark.asyncio
async def test_get_technologies_by_node_id(bioimage_index):
    """Test getting technologies by node ID using get_related_entities."""
    # Test with known node ID (Italian node) - this should find technologies provided by this node
    result = get_related_entities(
        bioimage_index=bioimage_index,
        entity_id="7409a98f-1bdb-47d2-80e7-c89db73efedd",  # Italian node
    )

    # Should return a list of technologies provided by this node
    assert isinstance(result, list)
    assert len(result) >= 2  # Should find multiple technologies

    # Check that all returned technologies have the expected structure
    for tech in result:
        assert "id" in tech
        assert "name" in tech
        assert "description" in tech


@pytest.mark.asyncio
async def test_get_technologies_by_node_id_not_found(bioimage_index):
    """Test getting technologies for non-existent node ID."""
    # Should raise ValueError for non-existent node ID
    with pytest.raises(ValueError, match="Entity not found"):
        get_related_entities(
            bioimage_index=bioimage_index, entity_id="nonexistent-node-id"
        )


@pytest.mark.asyncio
async def test_get_node_details(bioimage_index):
    """Test getting node details using get_entity_details."""
    # Test with known node ID
    result = await get_entity_details(
        bioimage_index=bioimage_index,
        entity_id="7409a98f-1bdb-47d2-80e7-c89db73efedd",  # Italian node
    )

    assert "entity_id" in result
    assert "entity_type" in result
    assert result["entity_type"] == "node"
    assert "entity_details" in result
    assert result["entity_details"]["name"] == "Advanced Light Microscopy Italian Node"

    # Test with non-existent node ID - should raise ValueError
    with pytest.raises(ValueError, match="Entity not found"):
        await get_entity_details(
            bioimage_index=bioimage_index, entity_id="nonexistent-node-id"
        )


@pytest.mark.asyncio
async def test_get_technology_details(bioimage_index):
    """Test getting technology details using get_entity_details."""
    # Test with known technology ID
    result = await get_entity_details(
        bioimage_index=bioimage_index,
        entity_id="660fd1fc-a138-5740-b298-14b0c3b24fb9",  # 3D-CLEM technology
    )

    assert "entity_id" in result
    assert "entity_type" in result
    assert result["entity_type"] == "technology"
    assert "entity_details" in result
    assert (
        "3D Correlative Light and Electron Microscopy"
        in result["entity_details"]["name"]
    )

    # Test with non-existent technology ID - should raise ValueError
    with pytest.raises(ValueError, match="Entity not found"):
        await get_entity_details(
            bioimage_index=bioimage_index, entity_id="nonexistent-tech-id"
        )


@pytest.mark.asyncio
async def test_search_nodes_via_index(bioimage_index):
    """Test searching nodes by name using the index directly."""
    # Test search with partial match
    nodes = bioimage_index.search_nodes_by_name("microscopy")

    assert len(nodes) > 0
    # Should find nodes with "microscopy" in the name
    assert len(nodes) >= 2

    # Check that results have expected structure
    for node in nodes:
        assert "id" in node
        assert "name" in node
        assert "microscopy" in node["name"].lower()


@pytest.mark.asyncio
async def test_search_technologies_via_index(bioimage_index):
    """Test searching technologies by name using the index directly."""
    # Test search with partial match
    technologies = bioimage_index.search_technologies_by_name("microscopy")

    assert len(technologies) > 0
    # Should find technologies with "microscopy" in the name
    assert len(technologies) >= 1

    # Check that results have expected structure
    for tech in technologies:
        assert "id" in tech
        assert "name" in tech
        assert "microscopy" in tech["name"].lower()


@pytest.mark.asyncio
async def test_get_all_nodes_via_index(bioimage_index):
    """Test getting all nodes using the index directly."""
    nodes = bioimage_index.get_all_nodes()

    assert len(nodes) > 0
    assert len(nodes) >= 3

    # Check that all nodes have expected structure
    for node in nodes:
        assert "id" in node
        assert "name" in node


@pytest.mark.asyncio
async def test_get_all_technologies_via_index(bioimage_index):
    """Test getting all technologies using the index directly."""
    technologies = bioimage_index.get_all_technologies()

    assert len(technologies) > 0
    assert len(technologies) >= 3

    # Check that all technologies have expected structure
    for tech in technologies:
        assert "id" in tech
        assert "name" in tech


@pytest.mark.asyncio
async def test_get_service_statistics_via_index(bioimage_index):
    """Test getting service statistics using the index directly."""
    stats = bioimage_index.get_statistics()

    assert "total_nodes" in stats
    assert "total_technologies" in stats
    assert "total_relationships" in stats

    assert stats["total_nodes"] >= 3
    assert stats["total_technologies"] >= 3
    assert stats["total_relationships"] > 0


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


@pytest.mark.asyncio
async def test_mem0_bioimage_integration(mem0_bioimage_live_service, mem0_live_service):
    """Test integration between mem0 search and bioimage service."""
    # Test queries
    queries = [
        "Which bioimage nodes are in sweden?",
        "I want to use an advanced microscope. which bioimage node should i go to?",
    ]

    for query in queries:
        print(f"\nTesting query: {query}")

        await mem0_live_service.init_agent(
            agent_id="ebi_file_loader",
            description="Test agent for EBI file loading",
        )

        # Search in mem0 with the specified agent_id and workspace
        search_result = await mem0_bioimage_live_service.search(
            query_text=query,
            limit=3,  # Get top 3 results
        )

        # Validate mem0 search results
        assert search_result is not None, f"No search results for query: {query}"
        assert "results" in search_result, "Search result missing 'results' key"
        results = search_result["results"]

        assert (
            len(results) == 3
        ), f"Wrong number of results returned (expected 3): {len(results)}"

        print(f"Found {len(results)} mem0 results")

        # Test bioimage service with known IDs (deterministic)
        # Test with known technology ID
        tech_result = await mem0_bioimage_live_service.get(
            "660fd1fc-a138-5740-b298-14b0c3b24fb9"  # 3D-CLEM
        )

        # Validate bioimage service technology lookup
        assert tech_result is not None
        assert "nodes" in tech_result
        assert "technology" in tech_result
        assert (
            len(tech_result["nodes"]) >= 1
        ), "Expected at least one node for known technology"

        # Test with known node ID
        node_result = await mem0_bioimage_live_service.get(
            "7409a98f-1bdb-47d2-80e7-c89db73efedd"  # Italian node
        )

        # Validate bioimage service node lookup
        assert node_result is not None
        assert "technologies" in node_result
        assert "node" in node_result
        assert (
            len(node_result["technologies"]) >= 1
        ), "Expected at least one technology for known node"

        # Test service statistics
        stats_result = await mem0_bioimage_live_service.get_statistics()
        assert stats_result is not None
        assert "service" in stats_result
        assert "statistics" in stats_result
        assert stats_result["service"] == "mem0_bioimage_service"

        print(f"✅ Query '{query}' processed successfully:")
        print(f"   - Found {len(tech_result['nodes'])} nodes for technology lookup")
        print(
            f"   - Found {len(node_result['technologies'])} technologies for node lookup"
        )
        print(
            f"   - Service statistics: {stats_result['statistics']['total_nodes']} nodes, {stats_result['statistics']['total_technologies']} technologies"
        )


# Fixtures for test and live/prod mem0_bioimage services (mirroring test_bioimage_service_unit.py)
@pytest_asyncio.fixture
async def mem0_bioimage_test_service():
    """Mem0 BioImage service fixture for test instance."""

    server = await get_user_server("PERSONAL_TOKEN")
    service = await server.get_service("aria-agents/mem0-bioimage-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def mem0_bioimage_live_service():
    """Mem0 BioImage service fixture for live/prod instance."""
    server = await get_user_server("PERSONAL_TOKEN")
    service = await server.get_service("aria-agents/mem0-bioimage")
    yield service
    await server.disconnect()


# Add search method tests for mem0_bioimage
@pytest.mark.asyncio
async def test_mem0_bioimage_search_test_service(mem0_bioimage_test_service):
    # Use a query that should return results
    query = "microscopy"
    result = await mem0_bioimage_test_service.search(query_text=query, limit=3)
    assert result is not None, "Search result should not be None"
    assert isinstance(result, dict), "Search result should be a dict"
    assert "results" in result, "Search result missing 'results' key"
    assert isinstance(result["results"], list), "'results' should be a list"
    assert len(result["results"]) > 0, "Search should return at least one result"
    for item in result["results"]:
        assert (
            "info" in item and "exists_in_nodes" in item
        ), "Each result should have an id or name"


@pytest.mark.asyncio
async def test_mem0_bioimage_search_live_service(mem0_bioimage_live_service):
    # Use a query that should return results
    query = "microscopy"
    result = await mem0_bioimage_live_service.search(query_text=query, limit=3)
    assert result is not None, "Search result should not be None"
    assert isinstance(result, dict), "Search result should be a dict"
    assert "results" in result, "Search result missing 'results' key"
    assert isinstance(result["results"], list), "'results' should be a list"
    assert len(result["results"]) > 0, "Search should return at least one result"
    for item in result["results"]:
        assert (
            "entity_id" in item or "name" in item
        ), "Each result should have an id or name"


# If there is a query method, add a test for it as well
@pytest.mark.asyncio
async def test_mem0_bioimage_query_test_service(mem0_bioimage_test_service):
    result = await mem0_bioimage_test_service.search(query_text="microscopy")
    assert result is not None
    assert isinstance(result, dict)
    assert "results" in result
    assert isinstance(result["results"], list)
    assert len(result["results"]) > 0


@pytest.mark.asyncio
async def test_mem0_bioimage_query_live_service(mem0_bioimage_live_service):
    result = await mem0_bioimage_live_service.search(query_text="microscopy")
    assert result is not None
    assert isinstance(result, dict)
    assert "results" in result
    assert isinstance(result["results"], list)
    assert len(result["results"]) > 0
