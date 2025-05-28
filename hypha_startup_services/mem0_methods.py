from typing import Any
from mem0 import Memory
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.artifact import create_artifact
from hypha_startup_services.permissions import require_permission
from hypha_startup_services.utils.models import PermissionParams, CreateArtifactParams


async def init_run(
    agent_id: str,
    run_id: str,
    *,
    description: str | None = None,
    metadata: dict[str, Any] | None = None,
    server: RemoteService,
    context: dict[str, Any],
) -> None:
    """
    Initialize a run by creating artifacts for the agent and the specific run.

    This creates two artifacts:
    1. A base artifact for the agent in the workspace
    2. A specific artifact for this run

    Args:
        agent_id: ID of the agent
        run_id: ID of the run
        description: Optional description for the artifacts
        metadata: Optional metadata for the artifacts
        server: The Hypha server instance
        memory: The Memory instance (currently unused)
        context: Context from Hypha-rpc for permissions
    """
    # Create base artifact for agent
    base_artifact_params = CreateArtifactParams.from_mem0_params(
        context=context,
        agent_id=agent_id,
        description=description or f"Memory artifact for agent {agent_id}",
        permissions="r",
        metadata=metadata,
    )

    await create_artifact(
        server=server,
        artifact_params=base_artifact_params,
    )

    # Create run-specific artifact
    run_artifact_params = base_artifact_params.with_run_id(run_id, permissions="*")
    run_artifact_params.description = (
        description or f"Memory artifact for agent {agent_id}, run {run_id}"
    )

    await create_artifact(
        server=server,
        artifact_params=run_artifact_params,
    )


async def mem0_add(
    messages: Any,
    agent_id: str,
    workspace: str,
    *,
    server: RemoteService,
    memory: Memory,
    context: dict[str, Any],
    run_id: str | None = None,
    **kwargs,
) -> None:
    """
    Add a new item to the memory service.

    Args:
        messages: The item to add, typically a list of messages or a single message.
        agent_id: ID of the agent adding the item.
        workspace: Workspace of the user adding the item.
        server: The Hypha server instance.
        memory: The Memory instance to add the item to.
        context: Context from Hypha-rpc for permissions.
        run_id: ID of the run to associate with the item. Defaults to None.
        **kwargs: Additional keyword arguments for the memory service.

    Raises:
        HyphaPermissionError: If the user does not have permission to add items to the memory.
        ValueError: If the permission parameters are invalid.
    """
    permission_params = PermissionParams.from_mem0_params(
        agent_id=agent_id,
        workspace=workspace,
        context=context,
        run_id=run_id,
        operation="rw",
    )

    await require_permission(server, permission_params)

    memory.add(messages, user_id=workspace, agent_id=agent_id, run_id=run_id, **kwargs)


async def mem0_search(
    query: str,
    agent_id: str,
    workspace: str,
    *,
    server: RemoteService,
    memory: Memory,
    context: dict[str, Any],
    run_id: str | None = None,
    **kwargs,
) -> dict[str, Any]:
    """
    Search for memories based on a query.

    Args:
        query: Query to search for.
        agent_id: ID of the agent to search for.
        workspace: Workspace of the user to search for.
        server: The Hypha server instance.
        memory: The Memory instance to perform the search on.
        context: Context from Hypha-rpc for permissions.
        run_id: ID of the run to search for. Defaults to None.
        **kwargs: Additional keyword arguments for the memory service.

    Raises:
        HyphaPermissionError: If the user does not have permission to search in the memory.
        ValueError: If the permission parameters are invalid.

    Returns:
        A dictionary containing the search results, typically under a "results" key,
        and potentially "relations" if graph store is enabled.
        Example for v1.1+: `{"results": [{"id": "...", "memory": "...", "score": 0.8, ...}]}`
    """
    permission_params = PermissionParams.from_mem0_params(
        agent_id=agent_id,
        workspace=workspace,
        context=context,
        run_id=run_id,
        operation="r",
    )

    await require_permission(server, permission_params)

    return memory.search(
        query,
        user_id=workspace,
        agent_id=agent_id,
        run_id=run_id,
        **kwargs,
    )
