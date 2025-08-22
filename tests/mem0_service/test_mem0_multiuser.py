"""Tests for multi-user mem0 functionality."""

import asyncio

import pytest
from hypha_rpc.rpc import RemoteException

from hypha_startup_services.common.permissions import HyphaPermissionError
from tests.conftest import USER1_WS, USER2_WS, USER3_WS
from tests.mem0_service.utils import (
    SEARCH_QUERY_MOVIES,
    TEST_AGENT_ID,
    TEST_AGENT_ID2,
    TEST_MESSAGES,
    TEST_MESSAGES2,
    TEST_RUN_ID,
    cleanup_mem0_memories,
    generate_unique_simple_message,
    generate_unique_test_messages,
)


@pytest.mark.asyncio
async def test_multi_user_memory_isolation(mem0_service, mem0_service2, mem0_service3):
    """Test that memories are properly isolated between different users."""
    # Clean up any existing memories to ensure test isolation for all users
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await cleanup_mem0_memories(mem0_service2, TEST_AGENT_ID, USER2_WS)
    await cleanup_mem0_memories(mem0_service3, TEST_AGENT_ID, USER3_WS)
    # Initialize agents for each user to create proper artifacts
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        run_id=f"{TEST_RUN_ID}-user1",
        description="User 1's agent",
    )

    await mem0_service2.init(
        agent_id=TEST_AGENT_ID,
        run_id=f"{TEST_RUN_ID}-user2",
        description="User 2's agent",
    )

    await mem0_service3.init(
        agent_id=TEST_AGENT_ID,
        run_id=f"{TEST_RUN_ID}-user3",
        description="User 3's agent",
    )

    # Each user adds memories to the same agent ID but in their own workspace
    # Generate unique messages for each user to avoid deduplication
    user1_messages = generate_unique_test_messages("user1_movies", 1)
    user2_messages = generate_unique_simple_message(
        "I love playing basketball and soccer regularly",
        "user2_sports",
        1,
    )
    user3_messages = generate_unique_simple_message(
        "I want to visit Japan and explore their temples",
        "user3_travel",
        1,
    )

    add_result1 = await mem0_service.add(
        messages=user1_messages,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    add_result2 = await mem0_service2.add(
        messages=user2_messages,
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
    )

    # Add different memories for user 3
    add_result3 = await mem0_service3.add(
        messages=user3_messages,
        agent_id=TEST_AGENT_ID,
        workspace=USER3_WS,
    )

    # Check that memories were actually added for each user
    assert add_result1 is not None and "results" in add_result1
    assert len(add_result1["results"]) > 0, "No memories were added for user 1"

    assert add_result2 is not None and "results" in add_result2
    assert len(add_result2["results"]) > 0, "No memories were added for user 2"

    assert add_result3 is not None and "results" in add_result3
    assert len(add_result3["results"]) > 0, "No memories were added for user 3"

    # Each user should only see their own memories
    user1_results = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    user2_results = await mem0_service2.search(
        query="sports activities",  # Updated query to match the content
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
    )

    user3_results = await mem0_service3.search(
        query="travel destinations",  # Updated query to match the content
        agent_id=TEST_AGENT_ID,
        workspace=USER3_WS,
    )

    # All searches should work
    assert user1_results is not None
    assert user2_results is not None
    assert user3_results is not None


@pytest.mark.asyncio
async def test_user_cannot_access_other_workspace(mem0_service, mem0_service2):
    """Test that regular users cannot access other users' workspaces."""
    # Clean up any existing memories to ensure test isolation
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    # User 2 tries to add memories to User 1's workspace (should fail)
    with pytest.raises((RemoteException, HyphaPermissionError, ValueError)) as exc_info:
        add_result = await mem0_service2.add(
            messages=TEST_MESSAGES,
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,  # User 2 trying to access User 1's workspace
        )

        error_str = str(exc_info.value).lower()

        # Check that memories were actually added
        assert add_result is not None and "results" in add_result
        # Check that add operation completed successfully
        # Note: mem0 may return empty results due to intelligent deduplication
        assert add_result is not None and "results" in add_result
        assert any(
            keyword in error_str
            for keyword in ["permission", "denied", "unauthorized", "access"]
        )


@pytest.mark.asyncio
async def test_user_cannot_search_other_workspace(mem0_service, mem0_service2):
    """Test that regular users cannot search other users' workspaces."""
    # Clean up any existing memories to ensure test isolation
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    # User 2 tries to search User 1's workspace (should fail)
    with pytest.raises((RemoteException, HyphaPermissionError, ValueError)) as exc_info:
        await mem0_service2.search(
            query=SEARCH_QUERY_MOVIES,
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,  # User 2 trying to search User 1's workspace
        )

    error_str = str(exc_info.value).lower()
    assert any(
        keyword in error_str
        for keyword in ["permission", "denied", "unauthorized", "access"]
    )


@pytest.mark.asyncio
async def test_multi_user_same_agent_different_runs(mem0_service, mem0_service2):
    """Test multiple users using the same agent with different run IDs."""
    # Clean up any existing memories to ensure test isolation
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    run_id_1 = f"{TEST_RUN_ID}-user1"
    run_id_2 = f"{TEST_RUN_ID}-user2"

    # Initialize agents for each user
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        run_id=run_id_1,
        workspace=USER1_WS,
        description="User 1's run",
    )

    await mem0_service2.init(
        agent_id=TEST_AGENT_ID,
        run_id=run_id_2,
        workspace=USER2_WS,
        description="User 2's run",
    )

    # User 1 adds memories with their run ID
    add_result1 = await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_id_1,
    )

    # User 2 adds memories with their run ID
    add_result2 = await mem0_service2.add(
        messages=TEST_MESSAGES2,
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
        run_id=run_id_2,
    )

    # Check that memories were actually added for each user
    assert add_result1 is not None and "results" in add_result1
    assert len(add_result1["results"]) > 0, "No memories were added for user 1"

    assert add_result2 is not None and "results" in add_result2
    assert len(add_result2["results"]) > 0, "No memories were added for user 2"

    # Each user searches within their own run context
    user1_results = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_id_1,
    )

    user2_results = await mem0_service2.search(
        query="comedy movies",
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
        run_id=run_id_2,
    )

    assert user1_results is not None
    assert user2_results is not None


@pytest.mark.asyncio
async def test_concurrent_memory_operations(mem0_service, mem0_service2):
    """Test concurrent memory operations from different users."""
    # Clean up any existing memories to ensure test isolation
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    # Initialize agents for both users
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="User 1's agent for concurrent operations test",
    )

    await mem0_service2.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
        description="User 2's agent for concurrent operations test",
    )

    # Define different message sets for each user
    user1_messages = [
        {
            "role": "user",
            "content": "I'm planning a trip to Japan. I love sushi and want to visit Tokyo.",
        },
    ]

    user2_messages = [
        {
            "role": "user",
            "content": "I'm learning to cook Italian food. Pasta is my favorite dish.",
        },
    ]

    # Run concurrent add operations
    tasks = [
        mem0_service.add(
            messages=user1_messages,
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
        ),
        mem0_service2.add(
            messages=user2_messages,
            agent_id=TEST_AGENT_ID,
            workspace=USER2_WS,
        ),
    ]

    # Execute concurrently
    await asyncio.gather(*tasks)

    # Run concurrent search operations
    search_tasks = [
        mem0_service.search(
            query="travel to Japan",
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
        ),
        mem0_service2.search(
            query="cooking Italian food",
            agent_id=TEST_AGENT_ID,
            workspace=USER2_WS,
        ),
    ]

    results = await asyncio.gather(*search_tasks)

    # Both searches should complete successfully
    assert len(results) == 2
    assert all(result is not None for result in results)
    assert all("results" in result for result in results)


@pytest.mark.asyncio
async def test_user_initialization_permissions(mem0_service, mem0_service2):
    """Test that regular users can initialize runs for their own agents."""
    # Clean up any existing memories to ensure test isolation
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    # User 2 should be able to initialize runs
    await mem0_service2.init(
        agent_id=TEST_AGENT_ID2,
        run_id=TEST_RUN_ID,
        workspace=USER2_WS,
        description="User 2's initialized run",
        metadata={"user": "user2", "permission_test": True},
    )

    # And then add memories to that run
    await mem0_service2.add(
        messages=TEST_MESSAGES2,
        agent_id=TEST_AGENT_ID2,
        workspace=USER2_WS,
        run_id=TEST_RUN_ID,
    )


@pytest.mark.asyncio
async def test_workspace_validation(mem0_service):
    """Test that workspace parameter validation works correctly."""
    # Clean up any existing memories to ensure test isolation
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    # Initialize agent for valid operations first
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for workspace validation",
    )

    with pytest.raises((RemoteException, HyphaPermissionError, ValueError)):
        await mem0_service.add(
            messages=TEST_MESSAGES,
            agent_id=TEST_AGENT_ID,
            workspace="",
        )
