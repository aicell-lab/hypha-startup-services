from typing import Any
from mem0 import AsyncMemory
import logging
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.common.artifacts import (
    create_artifact,
    artifact_exists,
)
from hypha_startup_services.common.permissions import (
    require_permission,
    AgentPermissionParams,
)
from hypha_startup_services.mem0_service.utils.models import AgentArtifactParams
from hypha_startup_services.common.workspace_utils import ws_from_context

logger = logging.getLogger(__name__)


async def init_agent(
    agent_id: str,
    *,
    description: str | None = None,
    metadata: dict[str, Any] | None = None,
    server: RemoteService,
    context: dict[str, Any],
) -> None:
    """
    Initialize an agent by creating an artifact for the agent in the workspace.

    This creates a base artifact for the agent in the workspace.

    Args:
        agent_id: ID of the agent
        description: Optional description for the artifact
        metadata: Optional metadata for the artifact
        server: The Hypha server instance
        context: Context from Hypha-rpc for permissions
    """
    accessor_ws = ws_from_context(context)

    agent_artifact_params = AgentArtifactParams(
        agent_id=agent_id,
        creator_id=accessor_ws,
        general_permission="r",
        desc=description,
        metadata=metadata,
        artifact_type="collection",
    )

    await create_artifact(
        server=server,
        artifact_params=agent_artifact_params,
    )


async def init_run(
    agent_id: str,
    workspace: str | None = None,
    run_id: str | None = None,
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
        memory: The AsyncMemory instance (currently unused)
        context: Context from Hypha-rpc for permissions
    """
    accessor_ws = ws_from_context(context)

    agent_artifact_params = AgentArtifactParams(
        agent_id=agent_id,
        creator_id=accessor_ws,
        desc=description,
        metadata=metadata,
        artifact_type="collection",
    )

    if workspace is None:
        workspace = accessor_ws

    workspace_artifact_params = agent_artifact_params.for_workspace(workspace)

    parent_id = agent_artifact_params.artifact_id
    assert parent_id is not None and await artifact_exists(
        server=server,
        artifact_id=parent_id,
    ), "Please call init_agent() before initializing workspace agent."

    await create_artifact(
        server=server,
        artifact_params=workspace_artifact_params,
    )

    if run_id is not None:
        run_artifact_params = workspace_artifact_params.for_run(run_id)
        await create_artifact(
            server=server,
            artifact_params=run_artifact_params,
        )


async def mem0_add(
    messages: Any,
    agent_id: str,
    workspace: str | None = None,
    *,
    server: RemoteService,
    memory: AsyncMemory,
    context: dict[str, Any],
    run_id: str | None = None,
    **kwargs,
) -> dict[str, Any] | list[Any]:
    """
    Add a new item to the memory service.

    Args:
        messages: The item to add, typically a list of messages or a single message.
        agent_id: ID of the agent adding the item.
        workspace: Workspace of the user adding the item.
        server: The Hypha server instance.
        memory: The AsyncMemory instance to add the item to.
        context: Context from Hypha-rpc for permissions.
        run_id: ID of the run to associate with the item. Defaults to None.
        **kwargs: Additional keyword arguments for the memory service.

    Raises:
        HyphaPermissionError: If the user does not have permission to add items to the memory.
        ValueError: If the permission parameters are invalid.
    """

    accessor_ws = ws_from_context(context)

    if workspace is None:
        workspace = accessor_ws

    permission_params = AgentPermissionParams(
        agent_id=agent_id,
        accessed_workspace=workspace,
        accessor_workspace=accessor_ws,
        run_id=run_id,
        operation="rw",
    )

    assert await artifact_exists(
        server=server,
        artifact_id=permission_params.artifact_id,
    ), "Please call init() before adding memories."

    await require_permission(server, permission_params)

    add_result = await memory.add(
        messages, user_id=workspace, agent_id=agent_id, run_id=run_id, **kwargs
    )
    logger.info("Added messages to memory: %s", add_result)
    return add_result


async def mem0_search(
    query: str,
    agent_id: str,
    workspace: str | None = None,
    *,
    server: RemoteService,
    memory: AsyncMemory,
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
        memory: The AsyncMemory instance to perform the search on.
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
    accessor_ws = ws_from_context(context)

    if workspace is None:
        workspace = accessor_ws

    permission_params = AgentPermissionParams(
        agent_id=agent_id,
        accessed_workspace=workspace,
        accessor_workspace=accessor_ws,
        run_id=run_id,
        operation="r",
    )

    assert await artifact_exists(
        server=server,
        artifact_id=permission_params.artifact_id,
    ), "Please call init() before adding memories."

    await require_permission(server, permission_params)

    results = await memory.search(
        query,
        user_id=workspace,
        agent_id=agent_id,
        run_id=run_id,
        **kwargs,
    )
    logger.info("Search results for query '%s': %s", query, results)
    return results
