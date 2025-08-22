"""Tests for mem0 set_permissions functionality."""

import pytest
from hypha_rpc.rpc import RemoteException

from hypha_startup_services.common.artifacts import (
    artifact_exists,
    delete_artifact,
    get_artifact,
)
from tests.conftest import USER1_WS
from tests.mem0_service.utils import (
    TEST_AGENT_ID,
    TEST_RUN_ID,
    cleanup_mem0_memories,
)


async def cleanup_agent_artifacts(
    agent_id: str,
    workspace: str,
    run_id: str | None = None,
):
    """Clean up agent artifacts to ensure test isolation."""
    try:
        # Delete base agent artifact
        base_artifact_id = f"{agent_id}:{workspace}"
        if await artifact_exists(base_artifact_id):
            await delete_artifact(base_artifact_id)
            print(f"Deleted base artifact: {base_artifact_id}")

        # Delete run-specific artifact if run_id is provided
        if run_id:
            run_artifact_id = f"{agent_id}:{workspace}:{run_id}"
            if await artifact_exists(run_artifact_id):
                await delete_artifact(run_artifact_id)
                print(f"Deleted run artifact: {run_artifact_id}")
    except (RemoteException, ValueError) as e:
        print(f"Failed to cleanup artifacts: {e}")
        # Don't fail the test if cleanup fails


@pytest.mark.asyncio
async def test_mem0_set_permissions_merge(mem0_service):
    """Test setting permissions with merge=True (default behavior)."""
    # Clean up and initialize agent
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for permissions merge",
    )

    # Set initial permissions
    initial_permissions = {"user1": "rw", "user2": "r"}

    await mem0_service.set_permissions(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        permissions=initial_permissions,
    )

    # Get the agent to verify initial permissions
    artifact_id = f"{TEST_AGENT_ID}:{USER1_WS}"
    agent_data = await get_artifact(artifact_id)

    assert agent_data["config"]["permissions"]["user1"] == "rw"
    assert agent_data["config"]["permissions"]["user2"] == "r"

    # Add additional permissions with merge=True (default)
    additional_permissions = {
        "user3": "*",
        "user1": "r",  # This should override user1's permission from "rw" to "r"
    }

    await mem0_service.set_permissions(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        permissions=additional_permissions,
        merge=True,
    )

    # Verify permissions were merged correctly
    updated_agent_data = await get_artifact(artifact_id)
    permissions = updated_agent_data["config"]["permissions"]

    assert permissions["user2"] == "r"  # Should remain unchanged
    assert permissions["user1"] == "r"  # Should be overwritten from "rw" to "r"
    assert permissions["user3"] == "*"  # Should be added

    # Clean up
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)


@pytest.mark.asyncio
async def test_mem0_set_permissions_no_merge(mem0_service):
    """Test setting permissions with merge=False (replace behavior)."""
    # Clean up and initialize agent
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for permissions no merge",
    )

    # Set initial permissions
    initial_permissions = {"user1": "rw", "user2": "r", "admin_user": "*"}

    await mem0_service.set_permissions(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        permissions=initial_permissions,
    )

    # Replace permissions entirely with merge=False
    new_permissions = {"user3": "r", "user4": "r", "user5": "rw"}

    await mem0_service.set_permissions(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        permissions=new_permissions,
        merge=False,
    )

    # Verify permissions were replaced entirely
    artifact_id = f"{TEST_AGENT_ID}:{USER1_WS}"
    updated_agent_data = await get_artifact(artifact_id)
    permissions = updated_agent_data["config"]["permissions"]

    assert permissions["user3"] == "r"  # New read permissions
    assert permissions["user4"] == "r"  # New read permissions
    assert permissions["user5"] == "rw"  # New read-write permissions
    assert "user1" not in permissions  # Old permissions should be gone
    assert "user2" not in permissions  # Old permissions should be gone
    assert "admin_user" not in permissions  # Old permissions should be gone

    # Clean up
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)


@pytest.mark.asyncio
async def test_mem0_set_permissions_default_merge(mem0_service):
    """Test that merge=True is the default behavior for backward compatibility."""
    # Clean up and initialize agent
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for default merge",
    )

    # Set initial permissions
    initial_permissions = {"user1": "r", "user2": "rw"}

    await mem0_service.set_permissions(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        permissions=initial_permissions,
    )

    # Add permissions without specifying merge parameter (should default to True)
    additional_permissions = {"admin_user": "*"}

    await mem0_service.set_permissions(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        permissions=additional_permissions,
        # Note: not specifying merge parameter
    )

    # Verify permissions were merged (default behavior)
    artifact_id = f"{TEST_AGENT_ID}:{USER1_WS}"
    updated_agent_data = await get_artifact(artifact_id)
    permissions = updated_agent_data["config"]["permissions"]

    assert permissions["user1"] == "r"  # Original permissions preserved
    assert permissions["user2"] == "rw"  # Original permissions preserved
    assert permissions["admin_user"] == "*"  # New permissions added

    # Clean up
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)


@pytest.mark.asyncio
async def test_mem0_set_permissions_empty_config(mem0_service):
    """Test setting permissions when config has no existing permissions."""
    # Clean up and initialize agent
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for empty config",
    )

    # Set permissions when no permissions exist yet
    new_permissions = {"user1": "r", "user2": "rw"}

    await mem0_service.set_permissions(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        permissions=new_permissions,
        merge=True,
    )

    # Verify permissions were set correctly
    artifact_id = f"{TEST_AGENT_ID}:{USER1_WS}"
    updated_agent_data = await get_artifact(artifact_id)

    permissions = updated_agent_data["config"]["permissions"]

    assert permissions["user1"] == "r"
    assert permissions["user2"] == "rw"

    # Clean up
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)


@pytest.mark.asyncio
async def test_mem0_set_permissions_with_run_id(mem0_service):
    """Test setting permissions for a specific run."""
    # Clean up and initialize agent with run_id
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await cleanup_agent_artifacts(TEST_AGENT_ID, USER1_WS, TEST_RUN_ID)
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
        description="Test agent for run-specific permissions",
    )

    # Set permissions for the specific run
    run_permissions = {"user1": "rw", "run_user": "r"}

    await mem0_service.set_permissions(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        run_id=TEST_RUN_ID,
        permissions=run_permissions,
    )

    # Verify permissions were set correctly for the run
    artifact_id = f"{TEST_AGENT_ID}:{USER1_WS}:{TEST_RUN_ID}"
    run_agent_data = await get_artifact(artifact_id)

    permissions = run_agent_data["config"]["permissions"]

    assert permissions["user1"] == "rw"
    assert permissions["run_user"] == "r"

    # Clean up
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await cleanup_agent_artifacts(TEST_AGENT_ID, USER1_WS, TEST_RUN_ID)


@pytest.mark.asyncio
async def test_mem0_set_permissions_nonexistent_agent(mem0_service):
    """Test setting permissions for a non-existent agent should fail."""
    # Try to set permissions for an agent that doesn't exist
    with pytest.raises(RemoteException) as exc_info:
        await mem0_service.set_permissions(
            agent_id="NonExistentAgent",
            workspace=USER1_WS,
            permissions={"user1": "r"},
        )

    # The error message should indicate the agent needs to be initialized first
    assert "Please call init_agent() before setting permissions" in str(exc_info.value)


@pytest.mark.asyncio
async def test_mem0_set_permissions_cross_user_access(mem0_service, mem0_service2):
    """Test that users cannot set permissions for other users' agents."""
    # User 1 creates an agent
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="User 1's private agent",
    )

    # User 2 tries to set permissions for User 1's agent
    with pytest.raises(RemoteException):
        await mem0_service2.set_permissions(
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,  # User 2 trying to access User 1's workspace
            permissions={"user2": "rw"},
        )

    # Clean up
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)


@pytest.mark.asyncio
async def test_mem0_set_permissions_invalid_format(mem0_service):
    """Test setting permissions with invalid permission formats."""
    # Clean up and initialize agent
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await mem0_service.init(
        agent_id=TEST_AGENT_ID,
        workspace=USER1_WS,
        description="Test agent for invalid permissions",
    )

    # Test with invalid permission values
    invalid_permissions = {"user1": "invalid_permission"}

    # This should either succeed (if validation is lenient) or fail gracefully
    try:
        await mem0_service.set_permissions(
            agent_id=TEST_AGENT_ID,
            workspace=USER1_WS,
            permissions=invalid_permissions,
        )

        # If it succeeds, verify the permissions were set
        artifact_id = f"{TEST_AGENT_ID}:{USER1_WS}"
        agent_data = await get_artifact(artifact_id)
        assert "user1" in agent_data["config"]["permissions"]

    except (RemoteException, ValueError, TypeError):
        # Expected if validation is strict
        pass

    # Clean up
    await cleanup_mem0_memories(mem0_service, TEST_AGENT_ID, USER1_WS)
    await cleanup_agent_artifacts(TEST_AGENT_ID, USER1_WS)
