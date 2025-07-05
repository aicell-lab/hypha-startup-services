"""Tests to confirm and fix the bioimage service query issue."""

import pytest
import pytest_asyncio
from hypha_startup_services.mem0_bioimage_service.methods import (
    search,
)
from hypha_startup_services.common.data_index import (
    load_external_data,
    get_related_entities,
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
async def test_query_returns_only_info_issue(bioimage_index, mem0_memory):
    """Test to confirm that query currently only returns 'info' field."""

    result = await search(
        memory=mem0_memory,
        bioimage_index=bioimage_index,
        query_text="confocal microscopy",
        include_related=True,
        limit=5,
    )

    print("Query result:", result)

    assert result is not None
    assert isinstance(result, dict)
    assert "results" in result
    assert "total_results" in result

    if result["results"]:
        first_result = result["results"][0]
        print("First result keys:", first_result.keys())
        print("First result:", first_result)

        # This test confirms the issue - currently only 'info' is returned
        assert "info" in first_result

        # The info field should be a string containing the memory content
        assert isinstance(first_result["info"], str)

        # If this was a technology or node with an entity_id in metadata,
        # we should also see related entities
        # For now, let's just check if we have related entity fields
        has_related_fields = (
            "exists_in_nodes" in first_result or "has_technologies" in first_result
        )
        print(f"Has related fields: {has_related_fields}")

        # If no related fields, this confirms the issue
        if not has_related_fields:
            print(
                "CONFIRMED: Query only returns 'info' field, missing related entities"
            )


@pytest.mark.asyncio
async def test_query_should_include_related_entities(bioimage_index, mem0_memory):
    """Test that query includes related entities when include_related=True."""

    result = await search(
        memory=mem0_memory,
        bioimage_index=bioimage_index,
        query_text="electron microscopy",
        include_related=True,
        limit=3,
    )

    assert result is not None
    assert "results" in result

    for result_item in result["results"]:
        print("Result item:", result_item)

        # Should have info field
        assert "info" in result_item

        # If the semantic search result has metadata with entity_id,
        # we should get related entities
        info = result_item.get("info", {})
        if "entity_id" in info and "entity_type" in info:
            entity_type = info["entity_type"]

            if entity_type == "technology":
                # Technologies should have related nodes
                assert (
                    "exists_in_nodes" in result_item
                ), f"Technology result should include 'exists_in_nodes' field: {result_item}"
                assert isinstance(
                    result_item["exists_in_nodes"], list
                ), "exists_in_nodes should be a list"
            elif entity_type == "node":
                # Nodes should have related technologies
                assert (
                    "has_technologies" in result_item
                ), f"Node result should include 'has_technologies' field: {result_item}"
                assert isinstance(
                    result_item["has_technologies"], list
                ), "has_technologies should be a list"


@pytest.mark.asyncio
async def test_query_without_related_entities(bioimage_index, mem0_memory):
    """Test that query doesn't include related entities when include_related=False."""

    result = await search(
        memory=mem0_memory,
        bioimage_index=bioimage_index,
        query_text="microscopy",
        include_related=False,
        limit=3,
    )

    assert result is not None
    assert "results" in result

    for result_item in result["results"]:
        # Should only have info field
        assert "info" in result_item

        # Should NOT have related entity fields
        assert "exists_in_nodes" not in result_item
        assert "has_technologies" not in result_item


@pytest.mark.asyncio
async def test_get_related_entities_works_correctly(bioimage_index):
    """Test that get_related_entities function works correctly."""

    # Test with a known technology ID that should have related nodes
    tech_id = "660fd1fc-a138-5740-b298-14b0c3b24fb9"  # 4Pi microscopy

    related_entities = get_related_entities(
        bioimage_index=bioimage_index,
        entity_id=tech_id,
    )

    print(f"Related entities for tech {tech_id}:", related_entities)

    assert isinstance(related_entities, list)
    assert len(related_entities) > 0

    # Each related entity should have name or id
    for entity in related_entities:
        assert "name" in entity or "id" in entity


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
