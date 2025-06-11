"""Tests for the BioImage service."""

import os
import pytest
from dotenv import load_dotenv
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService
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
from hypha_startup_services.bioimage_service.data_index import (
    BioimageIndex,
    EBI_NODES_DATA,
    EBI_TECHNOLOGIES_DATA,
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
async def test_get_nodes_by_technology_id(bioimage_index):
    """Test getting nodes by technology ID."""
    # Test with known technology ID
    result = await get_nodes_by_technology_id(
        bioimage_index, "f0acc857-fc72-4094-bf14-c36ac40801c5"
    )

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
async def test_get_nodes_by_technology_id_not_found(bioimage_index):
    """Test getting nodes for non-existent technology ID."""
    result = await get_nodes_by_technology_id(bioimage_index, "nonexistent-tech-id")

    assert "error" in result
    assert result["technology_id"] == "nonexistent-tech-id"
    assert result["nodes"] == []


@pytest.mark.asyncio
async def test_get_technologies_by_node_id(bioimage_index):
    """Test getting technologies by node ID."""
    # Test with known node ID (Italian node)
    result = await get_technologies_by_node_id(
        bioimage_index, "7409a98f-1bdb-47d2-80e7-c89db73efedd"
    )

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
async def test_get_technologies_by_node_id_not_found(bioimage_index):
    """Test getting technologies for non-existent node ID."""
    result = await get_technologies_by_node_id(bioimage_index, "nonexistent-node-id")

    assert "error" in result
    assert result["node_id"] == "nonexistent-node-id"
    assert result["technologies"] == []


@pytest.mark.asyncio
async def test_get_node_details(bioimage_index):
    """Test getting node details."""
    # Test with known node ID
    result = await get_node_details(
        bioimage_index, "7409a98f-1bdb-47d2-80e7-c89db73efedd"
    )

    assert "node_id" in result
    assert "node" in result
    assert result["node"]["name"] == "Advanced Light Microscopy Italian Node"

    # Test with non-existent node ID
    result = await get_node_details(bioimage_index, "nonexistent-node-id")
    assert "error" in result


@pytest.mark.asyncio
async def test_get_technology_details(bioimage_index):
    """Test getting technology details."""
    # Test with known technology ID
    result = await get_technology_details(
        bioimage_index, "f0acc857-fc72-4094-bf14-c36ac40801c5"
    )

    assert "technology_id" in result
    assert "technology" in result
    assert (
        "3D Correlative Light and Electron Microscopy" in result["technology"]["name"]
    )

    # Test with non-existent technology ID
    result = await get_technology_details(bioimage_index, "nonexistent-tech-id")
    assert "error" in result


@pytest.mark.asyncio
async def test_search_nodes(bioimage_index):
    """Test searching nodes by name."""
    # Test search with partial match
    result = await search_nodes(bioimage_index, "microscopy", limit=5)

    assert "query" in result
    assert "nodes" in result
    assert "total_results" in result
    assert "returned_results" in result

    # Should find nodes with "microscopy" in the name
    assert result["total_results"] >= 2
    assert len(result["nodes"]) <= 5  # Respects limit


@pytest.mark.asyncio
async def test_search_technologies(bioimage_index):
    """Test searching technologies by name."""
    # Test search with partial match
    result = await search_technologies(bioimage_index, "microscopy", limit=5)

    assert "query" in result
    assert "technologies" in result
    assert "total_results" in result
    assert "returned_results" in result

    # Should find technologies with "microscopy" in the name
    assert result["total_results"] >= 1


@pytest.mark.asyncio
async def test_get_all_nodes(bioimage_index):
    """Test getting all nodes."""
    result = await get_all_nodes(bioimage_index, limit=10)

    assert "nodes" in result
    assert "total_nodes" in result
    assert "returned_nodes" in result

    assert result["total_nodes"] >= 3
    assert len(result["nodes"]) <= 10  # Respects limit


@pytest.mark.asyncio
async def test_get_all_technologies(bioimage_index):
    """Test getting all technologies."""
    result = await get_all_technologies(bioimage_index, limit=10)

    assert "technologies" in result
    assert "total_technologies" in result
    assert "returned_technologies" in result

    assert result["total_technologies"] >= 3
    assert len(result["technologies"]) <= 10  # Respects limit


@pytest.mark.asyncio
async def test_get_service_statistics(bioimage_index):
    """Test getting service statistics."""
    result = await get_service_statistics(bioimage_index)

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


@pytest.mark.asyncio
async def test_mem0_bioimage_integration():
    """Test integration between mem0 search and bioimage service."""

    load_dotenv()

    # Get the HYPHA_TOKEN from environment
    token = os.environ.get("HYPHA_TOKEN")
    if not token:
        pytest.skip("HYPHA_TOKEN not available - skipping integration test")

    # Connect to Hypha server
    server = await connect_to_server(
        {
            "server_url": "https://hypha.aicell.io",
            "token": token,
        }
    )

    # Ensure we have the correct server type
    if not isinstance(server, RemoteService):
        raise TypeError("connect_to_server did not return a RemoteService instance")

    # Get the services
    try:
        mem0_service = await server.get_service("aria-agents/mem0-test")
        bioimage_service = await server.get_service("aria-agents/bioimage-test")
    except Exception as e:
        await server.disconnect()
        pytest.skip(f"Services not available: {e}")

    try:
        # Test queries
        queries = [
            "Which bioimage nodes are in sweden?",
            "I want to use an advanced microscope. which bioimage node should i go to?",
        ]

        for query in queries:
            print(f"\nTesting query: {query}")

            # Search in mem0 with the specified agent_id and workspace
            search_result = await mem0_service.search(
                query=query,
                agent_id="ebi_file_loader",
                workspace="aria-agents",
                limit=3,  # Get top 3 results
            )

            # Validate mem0 search results
            assert search_result is not None, f"No search results for query: {query}"
            assert "results" in search_result, "Search result missing 'results' key"
            results = search_result["results"]
            assert len(results) > 0, f"No search results found for query: {query}"
            assert (
                len(results) <= 3
            ), f"Too many results returned (expected max 3): {len(results)}"

            print(f"Found {len(results)} mem0 results")

            # Test bioimage service with known IDs (deterministic)
            # Test with known technology ID
            tech_result = await bioimage_service.get_nodes_by_technology_id(
                technology_id="f0acc857-fc72-4094-bf14-c36ac40801c5"  # 3D-CLEM
            )

            # Validate bioimage service technology lookup
            assert tech_result is not None
            assert "nodes" in tech_result
            assert "technology" in tech_result
            assert (
                len(tech_result["nodes"]) >= 1
            ), "Expected at least one node for known technology"

            # Test with known node ID
            node_result = await bioimage_service.get_technologies_by_node_id(
                node_id="7409a98f-1bdb-47d2-80e7-c89db73efedd"  # Italian node
            )

            # Validate bioimage service node lookup
            assert node_result is not None
            assert "technologies" in node_result
            assert "node" in node_result
            assert (
                len(node_result["technologies"]) >= 1
            ), "Expected at least one technology for known node"

            # Test service statistics
            stats_result = await bioimage_service.get_service_statistics()
            assert stats_result is not None
            assert "service" in stats_result
            assert "statistics" in stats_result
            assert stats_result["service"] == "bioimage_service"

            print(f"✅ Query '{query}' processed successfully:")
            print(f"   - Found {len(tech_result['nodes'])} nodes for technology lookup")
            print(
                f"   - Found {len(node_result['technologies'])} technologies for node lookup"
            )
            print(
                f"   - Service statistics: {stats_result['statistics']['total_nodes']} nodes, {stats_result['statistics']['total_technologies']} technologies"
            )

    except Exception as e:
        print(f"Test failed with error: {e}")
        raise
    finally:
        await server.disconnect()
