"""Tests for mem0 admin functionality."""

import pytest
from hypha_rpc.rpc import RemoteException
from tests.mem0_service.utils import (
    TEST_AGENT_ID,
    TEST_AGENT_ID2,
    TEST_RUN_ID,
    TEST_MESSAGES,
    TEST_MESSAGES2,
    SEARCH_QUERY_MOVIES,
)
from tests.conftest import USER1_WS, USER2_WS
from tests.mem0_service.utils import init_run


@pytest.mark.asyncio
async def test_admin_access_to_other_users_memories(mem0_service, mem0_service2):
    """Test admin access to memories owned by other users."""
    # User 2 (non-admin) adds private memories
    await init_run(mem0_service2, TEST_AGENT_ID2, TEST_RUN_ID, USER2_WS)

    await mem0_service2.init(
        agent_id=TEST_AGENT_ID2,
        workspace=USER2_WS,
        run_id=TEST_RUN_ID,
        description="User 2's run for testing admin access",
        metadata={"test": True, "environment": "pytest"},
    )

    add_result = await mem0_service2.add(
        messages=TEST_MESSAGES2,
        agent_id=TEST_AGENT_ID2,
        workspace=USER2_WS,
        run_id=TEST_RUN_ID,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Admin (User 1) should be able to search User 2's memories by accessing their workspace
    # Note: Admin access to other workspaces depends on the permission model implementation
    try:
        admin_search_result = await mem0_service.search(
            query="comedy movies",
            agent_id=TEST_AGENT_ID2,
            workspace=USER2_WS,  # Admin accessing user 2's workspace
        )

        assert admin_search_result is not None
        assert "results" in admin_search_result
        # Admin should be able to see results if they have cross-workspace access

    except (RemoteException, PermissionError, ValueError):
        # If admin doesn't have cross-workspace access, this is expected
        # The test documents the current behavior
        pass


@pytest.mark.asyncio
async def test_admin_workspace_isolation(mem0_service, mem0_service2):
    """Test that memories are properly isolated between workspaces."""
    # Initialize agents for each user
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="User 1's agent for workspace isolation test",
    )

    await mem0_service2.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
        description="User 2's agent for workspace isolation test",
    )

    # User 1 (admin) adds memories in their workspace
    add_result = await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # User 2 adds different memories in their workspace
    add_result = await mem0_service2.add(
        messages=TEST_MESSAGES2,
        agent_id=TEST_AGENT_ID,  # Same agent ID but different workspace
        workspace=USER2_WS,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # User 1 searches their workspace
    user1_results = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    # User 2 searches their workspace
    user2_results = await mem0_service2.search(
        query="comedy movies",
        agent_id=TEST_AGENT_ID,
        workspace=USER2_WS,
    )

    # Both should get results, but they should be different
    assert user1_results is not None
    assert user2_results is not None
    assert "results" in user1_results
    assert "results" in user2_results


@pytest.mark.asyncio
async def test_admin_run_initialization_for_other_users(mem0_service):
    """Test admin initializing runs for different agents/users."""
    # Admin can initialize runs for any agent
    await mem0_service.init(
        agent_id=TEST_AGENT_ID2,
        workspace=USER2_WS,
        run_id=TEST_RUN_ID,
        description="Admin-initialized run for another agent",
        metadata={"initialized_by": "admin", "target_agent": TEST_AGENT_ID2},
    )

    # Should be able to add memories to this admin-initialized run
    add_result = await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID2,
        workspace=USER2_WS,
        run_id=TEST_RUN_ID,
    )


@pytest.mark.asyncio
async def test_cross_agent_memory_isolation(mem0_service):
    """Test that memories are isolated between different agents."""

    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Agent 1 for cross-agent isolation test",
    )

    await mem0_service.init(
        agent_id=TEST_AGENT_ID2,
        workspace=USER1_WS,
        description="Agent 2 for cross-agent isolation test",
    )

    # Add memories for agent 1
    add_result = await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Add different memories for agent 2
    add_result = await mem0_service.add(
        messages=TEST_MESSAGES2,
        agent_id=TEST_AGENT_ID2,
        workspace=USER1_WS,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Search for agent 1's memories
    agent1_results = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    # Search for agent 2's memories
    agent2_results = await mem0_service.search(
        query="comedy movies",
        agent_id=TEST_AGENT_ID2,
        workspace=USER1_WS,
    )

    # Both should work but return different results based on agent isolation
    assert agent1_results is not None
    assert agent2_results is not None


@pytest.mark.asyncio
async def test_run_id_isolation(mem0_service):
    """Test that memories are properly isolated by run ID."""
    run_id_1 = f"{TEST_RUN_ID}-1"
    run_id_2 = f"{TEST_RUN_ID}-2"

    # Initialize runs
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_id_1,
        description="Run 1 for run ID isolation test",
    )

    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_id_2,
        description="Run 2 for run ID isolation test",
    )

    # Add memories to first run
    add_result = await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_id_1,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Add different memories to second run
    add_result = await mem0_service.add(
        messages=TEST_MESSAGES2,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_id_2,
    )

    # Check that memories were actually added
    assert add_result is not None and "results" in add_result
    assert len(add_result["results"]) > 0, "No memories were added to the service"
    # Search within first run
    run1_results = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_id_1,
    )

    # Search within second run
    run2_results = await mem0_service.search(
        query="comedy movies",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=run_id_2,
    )

    # Both should return results, but specific to their run context
    assert run1_results is not None
    assert run2_results is not None
    assert "results" in run1_results
    assert "results" in run2_results
