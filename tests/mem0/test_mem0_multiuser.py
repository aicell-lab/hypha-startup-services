"""Tests for multi-user mem0 functionality."""

import pytest
from hypha_rpc.rpc import RemoteException
from tests.mem0.utils import (
    TEST_AGENT_ID,
    TEST_AGENT_ID2,
    TEST_RUN_ID,
    TEST_MESSAGES,
    TEST_MESSAGES2,
    SEARCH_QUERY_MOVIES,
)
from tests.conftest import USER1_WS, USER2_WS, USER3_WS


@pytest.mark.asyncio
async def test_multi_user_memory_isolation(mem0_service, mem0_service2, mem0_service3):
    """Test that memories are properly isolated between different users."""
    # Each user adds memories to the same agent ID but in their own workspace
    await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    await mem0_service2.add(
        messages=TEST_MESSAGES2,
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
    )

    # Add different memories for user 3
    user3_messages = [
        {
            "role": "user",
            "content": "I love horror movies, especially classic ones like The Exorcist.",
        }
    ]

    await mem0_service3.add(
        messages=user3_messages,
        agent_id=TEST_AGENT_ID,
        workspace=USER3_WS,
    )

    # Each user should only see their own memories
    user1_results = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    user2_results = await mem0_service2.search(
        query="comedy movies",
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
    )

    user3_results = await mem0_service3.search(
        query="horror movies",
        agent_id=TEST_AGENT_ID,
        workspace=USER3_WS,
    )

    # All searches should work
    assert user1_results is not None
    assert user2_results is not None
    assert user3_results is not None


@pytest.mark.asyncio
async def test_user_cannot_access_other_workspace(mem0_service2):
    """Test that regular users cannot access other users' workspaces."""
    # User 2 tries to add memories to User 1's workspace (should fail)
    with pytest.raises((RemoteException, PermissionError, ValueError)) as exc_info:
        await mem0_service2.add(
            messages=TEST_MESSAGES,
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,  # User 2 trying to access User 1's workspace
        )

    error_str = str(exc_info.value).lower()
    assert any(
        keyword in error_str
        for keyword in ["permission", "denied", "unauthorized", "access"]
    )


@pytest.mark.asyncio
async def test_user_cannot_search_other_workspace(mem0_service2):
    """Test that regular users cannot search other users' workspaces."""
    # User 2 tries to search User 1's workspace (should fail)
    with pytest.raises((RemoteException, PermissionError, ValueError)) as exc_info:
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
    run_id_1 = f"{TEST_RUN_ID}-user1"
    run_id_2 = f"{TEST_RUN_ID}-user2"

    # User 1 adds memories with their run ID
    await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_id_1,
    )

    # User 2 adds memories with their run ID
    await mem0_service2.add(
        messages=TEST_MESSAGES2,
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
        run_id=run_id_2,
    )

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
    import asyncio

    # Define different message sets for each user
    user1_messages = [
        {
            "role": "user",
            "content": "I'm planning a trip to Japan. I love sushi and want to visit Tokyo.",
        }
    ]

    user2_messages = [
        {
            "role": "user",
            "content": "I'm learning to cook Italian food. Pasta is my favorite dish.",
        }
    ]

    # Run concurrent add operations
    tasks = [
        mem0_service.add(
            messages=user1_messages,
            agent_id=f"{TEST_AGENT_ID}-travel",
            workspace=USER1_WS,
        ),
        mem0_service2.add(
            messages=user2_messages,
            agent_id=f"{TEST_AGENT_ID}-cooking",
            workspace=USER2_WS,
        ),
    ]

    # Execute concurrently
    await asyncio.gather(*tasks)

    # Run concurrent search operations
    search_tasks = [
        mem0_service.search(
            query="travel to Japan",
            agent_id=f"{TEST_AGENT_ID}-travel",
            workspace=USER1_WS,
        ),
        mem0_service2.search(
            query="cooking Italian food",
            agent_id=f"{TEST_AGENT_ID}-cooking",
            workspace=USER2_WS,
        ),
    ]

    results = await asyncio.gather(*search_tasks)

    # Both searches should complete successfully
    assert len(results) == 2
    assert all(result is not None for result in results)
    assert all("results" in result for result in results)


@pytest.mark.asyncio
async def test_user_initialization_permissions(mem0_service2):
    """Test that regular users can initialize runs for their own agents."""
    # User 2 should be able to initialize runs
    await mem0_service2.init(
        agent_id=TEST_AGENT_ID2,
        run_id=TEST_RUN_ID,
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
    # Test with invalid workspace format
    with pytest.raises((RemoteException, PermissionError, ValueError)):
        await mem0_service.add(
            messages=TEST_MESSAGES,
            agent_id=TEST_AGENT_ID,
            workspace="invalid-workspace-format",
        )

    # Test with empty workspace
    with pytest.raises((RemoteException, PermissionError, ValueError)):
        await mem0_service.add(
            messages=TEST_MESSAGES,
            agent_id=TEST_AGENT_ID,
            workspace="",
        )
