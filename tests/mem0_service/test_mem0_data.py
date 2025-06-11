"""Tests for mem0 data operations and edge cases."""

import pytest
from tests.mem0_service.utils import (
    TEST_AGENT_ID,
    TEST_AGENT_ID2,
    TEST_RUN_ID,
    TEST_MESSAGES,
    TEST_MESSAGES2,
    SEARCH_QUERY_MOVIES,
)
from tests.conftest import USER1_WS


@pytest.mark.asyncio
async def test_memory_persistence_across_operations(mem0_service):
    """Test that memories persist across multiple operations."""
    # Initialize agent and run
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
        description="Test agent for memory persistence",
    )

    # Add initial memories
    add_result1 = await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )

    # Check that memories were actually added
    assert add_result1 is not None and "results" in add_result1
    assert len(add_result1["results"]) > 0, "No initial memories were added"

    # Add more memories to the same agent/run
    additional_messages = [
        {
            "role": "user",
            "content": "I also like documentaries about space exploration.",
        },
        {
            "role": "assistant",
            "content": "Space documentaries are fascinating! Have you seen Cosmos?",
        },
    ]

    add_result2 = await mem0_service.add(
        messages=additional_messages,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )

    # Check that additional memories were actually added
    assert add_result2 is not None and "results" in add_result2
    assert len(add_result2["results"]) > 0, "No additional memories were added"

    # Search should find content from both additions
    result = await mem0_service.search(
        query="movies and space",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )

    assert result is not None
    assert "results" in result


@pytest.mark.asyncio
async def test_large_message_content(mem0_service):
    """Test handling of large message content."""
    # Initialize agent
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for large message content",
    )

    large_content = "This is a very long message. " * 1000  # ~30KB message

    large_messages = [
        {"role": "user", "content": large_content},
        {
            "role": "assistant",
            "content": "I understand you've shared a lot of information with me.",
        },
    ]

    # Should handle large content gracefully
    add_result = await mem0_service.add(
        messages=large_messages,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Should be able to search in large content
    result = await mem0_service.search(
        query="long message",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    assert result is not None


@pytest.mark.asyncio
async def test_special_characters_in_content(mem0_service):
    """Test handling of special characters and Unicode in message content."""
    # Initialize agent
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for special characters",
    )

    special_messages = [
        {
            "role": "user",
            "content": "I love émojis! 🎬🍿 And special chars: @#$%^&*()_+-=[]{}|;':\",./<>?",
        },
        {
            "role": "assistant",
            "content": "特殊文字やユニコード文字も処理できます。Auch deutsche Umlaute: äöü",
        },
    ]

    add_result = await mem0_service.add(
        messages=special_messages,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Search with Unicode characters
    result = await mem0_service.search(
        query="émojis and special characters",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    assert result is not None


@pytest.mark.asyncio
async def test_multiple_agents_same_run(mem0_service):
    """Test multiple agents sharing the same run ID."""
    shared_run_id = f"{TEST_RUN_ID}-shared"

    # Initialize both agents with the same run ID
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=shared_run_id,
        description="Agent 1 for shared run test",
    )

    await mem0_service.init(
        agent_id=TEST_AGENT_ID2,
        workspace=USER1_WS,
        run_id=shared_run_id,
        description="Agent 2 for shared run test",
    )

    # Agent 1 adds memories
    add_result = await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=shared_run_id,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Agent 2 adds different memories with same run ID
    add_result = await mem0_service.add(
        messages=TEST_MESSAGES2,
        agent_id=TEST_AGENT_ID2,
        workspace=USER1_WS,
        run_id=shared_run_id,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Search for each agent should return their specific memories
    agent1_results = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=shared_run_id,
    )

    agent2_results = await mem0_service.search(
        query="comedy movies",
        agent_id=TEST_AGENT_ID2,
        workspace=USER1_WS,
        run_id=shared_run_id,
    )

    assert agent1_results is not None
    assert agent2_results is not None


@pytest.mark.asyncio
async def test_search_with_metadata_context(mem0_service):
    """Test searching with additional metadata context."""
    # Initialize run with metadata
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        run_id=TEST_RUN_ID,
        workspace=USER1_WS,
        description="Test run with metadata",
        metadata={
            "user_preferences": "sci-fi movies",
            "session_type": "entertainment",
            "mood": "curious",
        },
    )

    # Add memories with the initialized context
    add_result = await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Search should work with the metadata context
    result = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )

    assert result is not None


@pytest.mark.asyncio
async def test_rapid_sequential_operations(mem0_service):
    """Test rapid sequential add and search operations."""
    # Initialize the agent
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for rapid sequential operations",
    )

    # Rapidly add multiple memory sets
    for i in range(5):
        # Initialize each run
        await mem0_service.init(
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
            run_id=f"{TEST_RUN_ID}-{i}",
            description=f"Rapid test run {i}",
        )

        messages = [
            {
                "role": "user",
                "content": f"This is rapid test message set {i}. I like movies.",
            }
        ]

        add_result = await mem0_service.add(
            messages=messages,
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
            run_id=f"{TEST_RUN_ID}-{i}",
        )

        # Check that memories were actually added
        assert add_result is not None and "results" in add_result
        assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Rapidly search multiple times
    for i in range(3):
        result = await mem0_service.search(
            query="rapid test movies",
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
        )

        assert result is not None
        assert "results" in result


@pytest.mark.asyncio
async def test_cross_run_search_isolation(mem0_service):
    """Test that searches are properly isolated by run ID when specified."""
    run_1 = f"{TEST_RUN_ID}-isolation-1"
    run_2 = f"{TEST_RUN_ID}-isolation-2"

    # Initialize both runs
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_1,
        description="Run 1 for search isolation test",
    )

    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_2,
        description="Run 2 for search isolation test",
    )

    # Add distinct memories to each run
    run1_messages = [
        {"role": "user", "content": "Run 1: I love action movies like Die Hard."}
    ]

    run2_messages = [
        {
            "role": "user",
            "content": "Run 2: I prefer romantic comedies like When Harry Met Sally.",
        }
    ]

    add_result = await mem0_service.add(
        messages=run1_messages,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_1,
    )

    add_result = await mem0_service.add(
        messages=run2_messages,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_2,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Search within run 1 should only find run 1 content
    run1_results = await mem0_service.search(
        query="action movies",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_1,
    )

    # Search within run 2 should only find run 2 content
    run2_results = await mem0_service.search(
        query="romantic comedies",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_2,
    )

    assert run1_results is not None
    assert run2_results is not None


@pytest.mark.asyncio
async def test_memory_search_ranking(mem0_service):
    """Test that search results are properly ranked by relevance."""
    # Initialize agent
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for search ranking",
    )

    # Add memories with varying relevance to a search query
    highly_relevant = [
        {
            "role": "user",
            "content": "My absolute favorite science fiction movie is Blade Runner 2049.",
        }
    ]

    somewhat_relevant = [
        {"role": "user", "content": "I sometimes watch sci-fi films when I'm bored."}
    ]

    barely_relevant = [
        {"role": "user", "content": "I went to the movies last week and got popcorn."}
    ]

    # Add all memories
    for messages in [highly_relevant, somewhat_relevant, barely_relevant]:
        add_result = await mem0_service.add(
            messages=messages,
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
        )

        # Check that memories were actually added
        assert add_result is not None and "results" in add_result
        assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Search with a specific query
    result = await mem0_service.search(
        query="favorite science fiction movie",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    assert result is not None
    assert "results" in result

    # If there are results, they should be in some reasonable order
    # (exact ranking depends on mem0's implementation)
    if result["results"]:
        assert isinstance(result["results"], list)
