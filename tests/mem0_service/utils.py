"""Common utilities for mem0 tests."""

from typing import Any
from hypha_rpc.rpc import RemoteService

# Test agent and run IDs
TEST_AGENT_ID = "test-agent-123"
TEST_AGENT_ID2 = "test-agent-456"
TEST_RUN_ID = "test-run-789"
TEST_RUN_ID2 = "test-run-101112"

# Common test messages for mem0
TEST_MESSAGES = [
    {
        "role": "user",
        "content": "I love watching science fiction movies. My favorite is The Matrix.",
    },
    {
        "role": "assistant",
        "content": "That's great! The Matrix is a classic sci-fi film. What do you like most about it?",
    },
    {
        "role": "user",
        "content": "I enjoy the philosophical themes and the action sequences.",
    },
]

TEST_MESSAGES2 = [
    {
        "role": "user",
        "content": "I prefer comedy movies. My favorite is The Grand Budapest Hotel.",
    },
    {
        "role": "assistant",
        "content": "Wes Anderson's films have a very distinctive style! What draws you to his work?",
    },
]

# Search queries
SEARCH_QUERY_MOVIES = "What are my favorite movies?"
SEARCH_QUERY_PREFERENCES = "What kind of movies do I like?"


async def cleanup_mem0_memories(
    service, agent_id: str, workspace: str, run_id: str | None = None
):
    """Clean up memories for a specific agent and workspace."""
    # For now, we can't delete specific memories easily in mem0,
    # but we can search and potentially manage them
    # This is a placeholder for future cleanup functionality
    _ = (service, agent_id, workspace, run_id)  # Suppress unused warnings


async def init_user(
    service: RemoteService,
    agent_id: str,
    workspace: str,
    description: str | None = None,
    metadata: dict[str, Any] | None = None,
):
    """Initialize a user agent in the specified workspace."""
    if description is None:
        description = f"User agent {agent_id}"

    if metadata is None:
        metadata = {"is_test": True, "target_agent": agent_id}

    await service.init(
        agent_id=agent_id,
        workspace=workspace,
        description=description,
        metadata=metadata,
    )
    print(f"Initialized agent {agent_id} in workspace {workspace}")


async def init_run(
    service: RemoteService,
    agent_id: str,
    run_id: str,
    workspace: str,
    description: str | None = None,
    metadata: dict[str, Any] | None = None,
):
    """Initialize a run for the specified agent in the given workspace."""
    if metadata is None:
        metadata = {"is_test": True, "target_agent": agent_id}

    if description is None:
        description = f"Run {run_id} for agent {agent_id}"

    await service.init(
        agent_id=agent_id,
        workspace=workspace,
        run_id=run_id,
        description=description,
        metadata=metadata,
    )
    print(
        f"IN TEST UTILS: Initialized run {run_id} for agent {agent_id} in workspace {workspace}"
    )
