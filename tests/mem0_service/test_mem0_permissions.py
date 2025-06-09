"""Tests for mem0 permissions and error handling."""

import pytest
from hypha_rpc.rpc import RemoteException
from tests.mem0_service.utils import (
    TEST_AGENT_ID,
    TEST_RUN_ID,
    TEST_MESSAGES,
)
from tests.conftest import USER1_WS, USER2_WS


@pytest.mark.asyncio
async def test_invalid_agent_id_format(mem0_service):
    """Test handling of invalid agent ID formats."""
    invalid_agent_ids = [
        "",  # Empty string
        None,  # None value (if allowed by typing)
        "a" * 1000,  # Very long string
        "agent/with/slashes",  # Special characters
        "agent with spaces",  # Spaces
    ]

    for invalid_id in invalid_agent_ids:
        if invalid_id is None:
            continue  # Skip None as it would cause typing errors

        with pytest.raises((RemoteException, ValueError, TypeError)):
            await mem0_service.add(
                messages=TEST_MESSAGES,
                agent_id=invalid_id,
                workspace=USER1_WS,
            )


@pytest.mark.asyncio
async def test_invalid_workspace_format(mem0_service):
    """Test handling of invalid workspace formats."""
    invalid_workspaces = [
        "",  # Empty string
        "workspace-without-prefix",  # Missing ws- prefix
        "ws-",  # Just prefix
        "ws-user-",  # Incomplete format
        "invalid-format",  # Wrong format entirely
    ]

    for invalid_ws in invalid_workspaces:
        with pytest.raises((RemoteException, ValueError, PermissionError)):
            await mem0_service.add(
                messages=TEST_MESSAGES,
                agent_id=TEST_AGENT_ID,
                workspace=invalid_ws,
            )


@pytest.mark.asyncio
async def test_invalid_run_id_format(mem0_service):
    """Test handling of invalid run ID formats."""
    invalid_run_ids = [
        "",  # Empty string
        "a" * 1000,  # Very long string
        "run/with/slashes",  # Special characters
        "run with spaces",  # Spaces
    ]

    for invalid_id in invalid_run_ids:
        with pytest.raises((RemoteException, ValueError)):
            await mem0_service.add(
                messages=TEST_MESSAGES,
                agent_id=TEST_AGENT_ID,
                workspace=USER1_WS,
                run_id=invalid_id,
            )


@pytest.mark.asyncio
async def test_empty_messages(mem0_service):
    """Test handling of empty or invalid messages."""
    invalid_message_sets = [
        [],  # Empty list
        [{}],  # Empty message object
        [{"role": "user"}],  # Missing content
        [{"content": "Hello"}],  # Missing role
        [{"role": "invalid", "content": "Hello"}],  # Invalid role
    ]

    for invalid_messages in invalid_message_sets:
        # Some of these might be accepted depending on mem0's validation
        # We test that the service handles them gracefully
        try:
            await mem0_service.add(
                messages=invalid_messages,
                agent_id=TEST_AGENT_ID,
                workspace=USER1_WS,
            )
        except (RemoteException, ValueError, TypeError):
            # Expected for invalid formats
            pass


@pytest.mark.asyncio
async def test_permission_denied_scenarios(mem0_service2):
    """Test various permission denied scenarios."""
    # Try to access another user's workspace
    with pytest.raises((RemoteException, PermissionError, ValueError)):
        await mem0_service2.add(
            messages=TEST_MESSAGES,
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,  # User 2 accessing User 1's workspace
        )

    # Try to search another user's workspace
    with pytest.raises((RemoteException, PermissionError, ValueError)):
        await mem0_service2.search(
            query="test query",
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,  # User 2 searching User 1's workspace
        )


@pytest.mark.asyncio
async def test_artifact_permission_validation(mem0_service):
    """Test that artifact permissions are properly validated."""
    # Initialize a run (creates artifacts)
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        run_id=TEST_RUN_ID,
        description="Test artifact permissions",
    )

    # Add memories (should work with proper permissions)
    await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )

    # Search memories (should work with proper permissions)
    result = await mem0_service.search(
        query="test query",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
    )

    assert result is not None


@pytest.mark.asyncio
async def test_malformed_search_query(mem0_service):
    """Test handling of malformed search queries."""
    # First add some valid memories
    await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    # Test various query formats
    query_tests = [
        "",  # Empty query
        " ",  # Whitespace only
        "a" * 1000,  # Very long query
        "!@#$%^&*()",  # Special characters only
        "\n\t\r",  # Whitespace characters
    ]

    for query in query_tests:
        # These should either work or fail gracefully
        try:
            result = await mem0_service.search(
                query=query,
                agent_id=TEST_AGENT_ID,
                workspace=USER1_WS,
            )
            # If it succeeds, should return proper format
            assert result is not None
            assert "results" in result
        except (RemoteException, ValueError):
            # Expected for some malformed queries
            pass


@pytest.mark.asyncio
async def test_concurrent_permission_checks(mem0_service, mem0_service2):
    """Test that permission checks work correctly under concurrent access."""
    import asyncio

    # Create multiple concurrent operations with different permission contexts
    tasks = []

    # Valid operations for each user
    tasks.append(
        mem0_service.add(
            messages=TEST_MESSAGES,
            agent_id=f"{TEST_AGENT_ID}-user1",
            workspace=USER1_WS,
        )
    )

    tasks.append(
        mem0_service2.add(
            messages=TEST_MESSAGES,
            agent_id=f"{TEST_AGENT_ID}-user2",
            workspace=USER2_WS,
        )
    )

    # Invalid operations (should fail)
    tasks.append(
        mem0_service2.add(
            messages=TEST_MESSAGES,
            agent_id=f"{TEST_AGENT_ID}-invalid",
            workspace=USER1_WS,  # User 2 trying to access User 1's workspace
        )
    )

    # Gather results with exception handling
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # First two should succeed, third should fail
    assert not isinstance(results[0], Exception)  # User 1 valid operation
    assert not isinstance(results[1], Exception)  # User 2 valid operation
    assert isinstance(results[2], Exception)  # User 2 invalid operation


@pytest.mark.asyncio
async def test_service_error_recovery(mem0_service):
    """Test that the service can recover from errors."""
    # Try an operation that might fail
    try:
        await mem0_service.add(
            messages=[],  # Empty messages might cause an error
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
        )
    except (RemoteException, ValueError, TypeError):
        pass  # Expected to potentially fail

    # Service should still work for valid operations after an error
    await mem0_service.add(
        messages=TEST_MESSAGES,
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    result = await mem0_service.search(
        query="test recovery",
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
    )

    assert result is not None
    assert "results" in result
