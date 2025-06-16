from typing import Any
import logging
from hypha_rpc.rpc import RemoteService

# Apply patches before importing AsyncMemory to ensure they take effect
from hypha_startup_services.mem0_service.weaviate_patches import apply_all_patches

apply_all_patches()

from mem0 import AsyncMemory
from hypha_rpc.utils import ObjectProxy
from hypha_startup_services.common.artifacts import (
    create_artifact,
    artifact_exists,
)
from hypha_startup_services.common.permissions import (
    require_permission,
    AgentPermissionParams,
)
from hypha_startup_services.common.utils import proxy_to_dict
from hypha_startup_services.mem0_service.utils.models import AgentArtifactParams
from hypha_startup_services.common.workspace_utils import ws_from_context
from hypha_startup_services.common.workspace_utils import validate_workspace
from hypha_startup_services.common.run_utils import validate_run_id


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

    validate_workspace(workspace)

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
        validate_run_id(run_id)
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
    context: dict[str, Any] | ObjectProxy,
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

    validate_workspace(workspace)

    if run_id is not None:
        validate_run_id(run_id)

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

    converted_messages = proxy_to_dict(messages)
    converted_kwargs = proxy_to_dict(kwargs)

    add_result = await memory.add(
        converted_messages, agent_id=agent_id, run_id=run_id, **converted_kwargs  # type: ignore
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

    validate_workspace(workspace)

    if run_id is not None:
        validate_run_id(run_id)

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
        agent_id=agent_id,
        run_id=run_id,
        **kwargs,
    )
    logger.info("Search results for query '%s': %s", query, results)
    return results


async def mem0_delete_all(
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
    Delete all memories for a specific agent and workspace.

    Args:
        agent_id: ID of the agent whose memories to delete.
        workspace: Workspace of the user whose memories to delete.
        server: The Hypha server instance.
        memory: The AsyncMemory instance to delete memories from.
        context: Context from Hypha-rpc for permissions.
        run_id: ID of the run whose memories to delete. Defaults to None.
        **kwargs: Additional keyword arguments for the memory service.

    Raises:
        HyphaPermissionError: If the user does not have permission to delete memories.
        ValueError: If the permission parameters are invalid.

    Returns:
        A dictionary containing the deletion result, typically with a "message" key.
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
    ), "Please call init() before deleting memories."

    await require_permission(server, permission_params)

    if run_id:
        logger.warning(
            "Run-specific deletion not implemented, deleting all memories for agent %s",
            agent_id,
        )

    delete_result = await memory.delete_all(agent_id=agent_id, **kwargs)
    logger.info(
        "Deleted all memories for agent %s: %s",
        agent_id,
        delete_result,
    )
    return delete_result


async def mem0_get_all(
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
    Get all memories for a specific agent and workspace.

    Args:
        agent_id: ID of the agent whose memories to get.
        workspace: Workspace of the user whose memories to get.
        server: The Hypha server instance.
        memory: The AsyncMemory instance to get memories from.
        context: Context from Hypha-rpc for permissions.
        run_id: ID of the run whose memories to get. Defaults to None.
        **kwargs: Additional keyword arguments for the memory service.

    Returns:
        A dictionary containing all memories.
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
    ), "Please call init() before getting memories."

    await require_permission(server, permission_params)
    # Use the memory.get_all method if available
    get_all_result = await memory.get_all(agent_id=agent_id, **kwargs)
    logger.info("Retrieved all memories for agent %s", agent_id)
    return get_all_result
