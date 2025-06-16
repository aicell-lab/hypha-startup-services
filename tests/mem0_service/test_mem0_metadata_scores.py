"""Tests for metadata preservation and score variances in mem0 search results."""

import uuid
import asyncio
import time
import pytest
from tests.mem0_service.utils import (
    TEST_AGENT_ID,
    TEST_RUN_ID,
    cleanup_mem0_memories,
)
from tests.conftest import USER1_WS


def generate_unique_id() -> str:
    """Generate a unique ID for test data."""
    return f"test-{uuid.uuid4().hex[:8]}-{int(time.time())}"


@pytest.mark.asyncio
async def test_metadata_preservation(mem0_service):
    """Test that metadata is correctly preserved in search results."""

    # Clean up and create test agent
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    test_run_id = f"{TEST_RUN_ID}-metadata-{generate_unique_id()}"

    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        run_id=test_run_id,
        workspace=USER1_WS,
        description="Metadata preservation test agent",
    )

    # Create test items with rich metadata
    test_items = [
        {
            "content": "This is a test node about microscopy in Italy",
            "metadata": {
                "type": "node",
                "node_id": "node-123",
                "country": "Italy",
                "technologies": ["tech-1", "tech-2"],
            },
        },
        {
            "content": "This is about advanced light microscopy technology",
            "metadata": {
                "type": "technology",
                "tech_id": "tech-1",
                "category_type": "Light Microscopy",  # Changed from 'category' to avoid conflict with mem0 system field
                "applications": ["cell imaging", "tissue analysis"],
            },
        },
        {
            "content": "Electron microscopy is used for high-resolution imaging",
            "metadata": {
                "type": "technology",
                "tech_id": "tech-2",
                "category_type": "Electron Microscopy",  # Changed from 'category' to avoid conflict with mem0 system field
                "resolution": "sub-nanometer",
            },
        },
    ]

    # Add items to mem0
    for i, item in enumerate(test_items):
        message = {
            "role": "user",
            "content": item["content"],
        }  # Changed from "system" to "user"
        result = await mem0_service.add(
            messages=[message],
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
            run_id=test_run_id,
            metadata=item["metadata"],
            infer=False,  # Force direct storage without LLM processing
        )
        assert result is not None
        assert "results" in result

    await asyncio.sleep(5)  # Longer wait for indexing

    # Search for each item
    search_queries = [
        "microscopy Italy",
        "advanced light microscopy",
        "electron microscopy high-resolution",
    ]

    all_scores = []
    for i, query in enumerate(search_queries):
        results = await mem0_service.search(
            query=query,
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
            # Don't include run_id in search - similar to ebi_data_loader approach
            limit=5,
        )

        # Verify results exist
        assert results is not None
        assert "results" in results
        assert len(results["results"]) > 0, f"No results found for query: {query}"

        # Check the top result
        top_result = results["results"][0]
        print(f"Query: {query}, Top result metadata: {top_result.get('metadata', {})}")

        # Store scores for later analysis
        for result in results["results"]:
            all_scores.append(result.get("score", 0))

        # Verify metadata is preserved correctly
        assert (
            "metadata" in top_result
        ), f"Metadata missing in result for query: {query}"
        result_metadata = top_result["metadata"]

        # Our custom metadata is stored under metadata.metadata due to mem0's structure
        assert (
            "metadata" in result_metadata and result_metadata["metadata"] is not None
        ), f"Custom metadata missing in result for query: {query}"

        custom_metadata = result_metadata["metadata"]

        # Check that custom metadata contains expected fields based on the test item
        expected_metadata = test_items[i]["metadata"]
        for key, value in expected_metadata.items():
            assert (
                key in custom_metadata
            ), f"Metadata key '{key}' missing in search result"
            assert (
                custom_metadata[key] == value
            ), f"Metadata value for '{key}' doesn't match: expected {value}, got {custom_metadata[key]}"

    # Check that not all scores are 1.000
    assert (
        len(set(all_scores)) > 1
    ), "All search scores are identical, expected variance"
    assert not all(
        score == 1.0 for score in all_scores
    ), "All scores are 1.000, expected variance"


@pytest.mark.asyncio
async def test_score_variance(mem0_service):
    """Test that search scores have variance and aren't all 1.000."""

    # Clean up and create test agent
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    test_run_id = f"{TEST_RUN_ID}-scores-{generate_unique_id()}"

    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        run_id=test_run_id,
        workspace=USER1_WS,
        description="Score variance test agent",
    )

    # Add several diverse items to test score variance
    test_contents = [
        "The quick brown fox jumps over the lazy dog",
        "Machine learning algorithms are transforming various industries",
        "Python is a high-level programming language with easy syntax",
        "Vector databases store embeddings for similarity search",
        "Microscopy techniques allow scientists to visualize cellular structures",
        "Quantum computing uses quantum bits instead of classical bits",
        "Climate change is affecting global weather patterns",
        "Neural networks are inspired by the human brain's structure",
    ]

    for i, content in enumerate(test_contents):
        message = {
            "role": "user",
            "content": content,
        }  # Changed from "system" to "user"
        await mem0_service.add(
            messages=[message],
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
            run_id=test_run_id,
            metadata={
                "item_id": f"item-{i}",
                "category_type": f"category-{i % 3}",
            },  # Changed from 'category'
            infer=False,  # Force direct storage without LLM processing
        )

    # Perform search with different queries to test score variance
    search_queries = [
        "fox dog animal",
        "machine learning artificial intelligence",
        "programming languages python",
        "vector embeddings similarity",
        "quantum computing qubits",
    ]

    all_scores = []
    for query in search_queries:
        results = await mem0_service.search(
            query=query,
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
            # Don't include run_id in search - similar to ebi_data_loader approach
            limit=5,
        )

        # Verify results exist
        assert results is not None
        assert "results" in results

        # Store all scores
        for result in results["results"]:
            score = result.get("score", 0)
            all_scores.append(score)
            print(
                f"Query: '{query}', Content: '{result.get('memory', '')[:30]}...', Score: {score}"
            )

    # Verify score diversity
    print(f"Score range: {min(all_scores)} to {max(all_scores)}")
    print(
        f"Number of unique scores: {len(set(all_scores))} out of {len(all_scores)} results"
    )

    # Assert that there are different scores (not all 1.000)
    assert (
        len(set(all_scores)) > 1
    ), "All search scores are identical, expected variance"

    # Assert that scores have a reasonable range (not all exactly 1.000)
    # Allow small floating point differences by using a threshold
    assert (
        max(all_scores) - min(all_scores) > 0.01
    ), "Score range too small, expected variance"

    # Assert that not all scores are 1.0
    assert not all(
        abs(score - 1.0) < 0.001 for score in all_scores
    ), "All scores are approximately 1.000"
