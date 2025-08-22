"""Common utilities for mem0 tests."""

import time
import uuid
from typing import Any

from hypha_rpc.rpc import ServiceProxy

# Test agent and run IDs
TEST_AGENT_ID = "test-agent-123"
TEST_AGENT_ID2 = "test-agent-456"
TEST_RUN_ID = "test-run-789"
TEST_RUN_ID2 = "test-run-101112"


def generate_unique_test_messages(
    test_name: str = "default",
    iteration: int = 0,
) -> list[dict[str, str]]:
    """Generate unique test messages that won't be deduplicated by mem0."""
    timestamp = int(time.time() * 1000)  # milliseconds for uniqueness
    unique_id = str(uuid.uuid4())[:8]

    return [
        {
            "role": "user",
            "content": f"Test {test_name} iteration {iteration} at {timestamp} (ID: {unique_id}). I love watching science fiction movies like The Matrix.",
        },
        {
            "role": "assistant",
            "content": f"That's interesting! The Matrix is a classic. Test context: {test_name}-{iteration}-{unique_id}",
        },
        {
            "role": "user",
            "content": f"I enjoy the philosophical themes and action sequences. Context: {test_name} {timestamp}",
        },
    ]


def generate_unique_simple_message(
    content_base: str,
    test_name: str = "default",
    iteration: int = 0,
) -> list[dict[str, str]]:
    """Generate a unique simple message that won't be deduplicated."""
    timestamp = int(time.time() * 1000)
    unique_id = str(uuid.uuid4())[:8]

    return [
        {
            "role": "user",
            "content": f"{content_base} (Test: {test_name}, iteration: {iteration}, timestamp: {timestamp}, ID: {unique_id})",
        },
    ]


# Updated test messages with uniqueness
TEST_MESSAGES = generate_unique_test_messages("base_test")

TEST_MESSAGES2 = generate_unique_test_messages("comedy_test")

# Even more distinct messages for different users
TEST_MESSAGES3 = generate_unique_test_messages("horror_test")

# Completely different topic messages for testing
SPORTS_MESSAGES = generate_unique_simple_message(
    "I love playing basketball and tennis",
    "sports_test",
)
FOOD_MESSAGES = generate_unique_simple_message(
    "My favorite cuisine is Italian food",
    "food_test",
)
TRAVEL_MESSAGES = generate_unique_simple_message(
    "I want to visit Japan and learn about their culture",
    "travel_test",
)

# Search queries
SEARCH_QUERY_MOVIES = "What are my favorite movies?"
SEARCH_QUERY_PREFERENCES = "What kind of movies do I like?"


async def cleanup_mem0_memories(
    service: ServiceProxy,
    agent_id: str,
    workspace: str,
    run_id: str | None = None,
):
    """Clean up memories for a specific agent and workspace."""
    try:
        # Delete all memories for this workspace/agent combination
        delete_result = await service.delete_all(
            agent_id=agent_id,
            workspace=workspace,
            run_id=run_id,
        )
        print(
            f"Cleaned up memories for agent {agent_id}, workspace {workspace}: {delete_result}",
        )
        return delete_result
    except Exception as e:
        print(
            f"Failed to cleanup memories for agent {agent_id}, workspace {workspace}: {e}",
        )
        # Don't fail the test if cleanup fails - just continue
        return None


async def init_user(
    service: ServiceProxy,
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
    service: ServiceProxy,
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
        f"IN TEST UTILS: Initialized run {run_id} for agent {agent_id} in workspace {workspace}",
    )
