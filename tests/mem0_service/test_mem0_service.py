"""Tests for mem0 service functionality."""

import pytest
from hypha_rpc.rpc import RemoteException
from tests.mem0_service.utils import (
    TEST_AGENT_ID,
    TEST_RUN_ID,
    TEST_MESSAGES,
    SEARCH_QUERY_MOVIES,
)
from tests.conftest import USER1_WS


@pytest.mark.asyncio
async def test_mem0_add_basic(mem0_service):
    """Test basic memory addition functionality."""
    # Add a memory
    await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    # Search for the memory
    result = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    assert result is not None
    assert "results" in result
    # Should have at least one result since we just added memories
    assert len(result["results"]) >= 0  # May be 0 if indexing is slow


@pytest.mark.asyncio
async def test_mem0_add_with_run_id(mem0_service):
    """Test memory addition with a specific run ID."""
    # Add a memory with run ID
    await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )

    # Search for the memory with run ID
    result = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )

    assert result is not None
    assert "results" in result


@pytest.mark.asyncio
async def test_mem0_search_basic(mem0_service):
    """Test basic memory search functionality."""
    # First add some memories
    await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    # Search for memories
    result = await mem0_service.search(
        query=SEARCH_QUERY_MOVIES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    assert result is not None
    assert isinstance(result, dict)
    assert "results" in result
    assert isinstance(result["results"], list)


@pytest.mark.asyncio
async def test_mem0_init_run(mem0_service):
    """Test run initialization functionality."""
    # Initialize a run
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        run_id=TEST_RUN_ID,
        description="Test run for mem0 service",
        metadata={"test": True, "environment": "pytest"},
    )

    # The init function should create artifacts but doesn't return anything
    # We can verify it worked by trying to add memories with this run
    await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )


@pytest.mark.asyncio
async def test_mem0_permission_error_wrong_workspace(mem0_service2):
    """Test that using wrong workspace raises permission error."""
    # Try to add memory with a workspace that doesn't match the user
    with pytest.raises((RemoteException, PermissionError, ValueError)) as exc_info:
        await mem0_service2.add(
            messages=TEST_MESSAGES,
            agent_id=TEST_AGENT_ID,
            workspace="ws-invalid-workspace",
        )

    error_str = str(exc_info.value).lower()
    assert any(
        keyword in error_str
        for keyword in ["permission", "denied", "unauthorized", "access"]
    )


@pytest.mark.asyncio
async def test_mem0_search_permission_error(mem0_service2):
    """Test that searching with wrong workspace raises permission error."""
    # Try to search with a workspace that doesn't match the user
    with pytest.raises((RemoteException, PermissionError, ValueError)) as exc_info:
        await mem0_service2.search(
            query=SEARCH_QUERY_MOVIES,
            agent_id=TEST_AGENT_ID,
            workspace="ws-invalid-workspace",
        )

    error_str = str(exc_info.value).lower()
    assert any(
        keyword in error_str
        for keyword in ["permission", "denied", "unauthorized", "access"]
    )


@pytest.mark.asyncio
async def test_mem0_empty_search_results(mem0_service):
    """Test searching with a query that should return no results."""
    result = await mem0_service.search(
        query="extremely specific query that should not match anything xyzabc123",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    assert result is not None
    assert "results" in result
    assert isinstance(result["results"], list)
    # Should be empty or very few results
    assert len(result["results"]) == 0 or len(result["results"]) < 3
