from typing import Any
import logging
from hypha_rpc.rpc import RemoteService, RemoteException
from hypha_startup_services.mem0_service.utils.constants import ADMIN_WORKSPACES
from hypha_startup_services.mem0_service.utils.models import (
    PermissionParams,
    HyphaPermissionError,
)
from hypha_startup_services.mem0_service.artifact import get_artifact

logger = logging.getLogger(__name__)


async def get_user_permissions(
    server: RemoteService, permission_params: PermissionParams
) -> dict[str, Any]:
    """
    Get user permissions for a specific artifact.

    Args:
        server: The RemoteService instance
        permission_params: The PermissionParams instance containing user and artifact details

    Returns:
        A dictionary containing the user's permissions for the artifact

    Raises:
        RemoteException: If there's a server communication error
    """
    try:
        artifact = await get_artifact(server, permission_params.artifact_id)
    except RemoteException as e:
        logger.error(
            "Failed to retrieve artifact %s: %s", permission_params.artifact_id, e
        )
        return {}

    permissions = artifact.get("config", {}).get("permissions", {})
    return permissions.get(permission_params.accessor_workspace, {})


async def user_has_operation_permission(
    server: RemoteService, permission_params: PermissionParams
) -> bool:
    """
    Check if the user has the requested permissions for a specific artifact.

    Args:
        server: The RemoteService instance
        permission_params: The PermissionParams instance containing permission details

    Returns:
        True if the user has the requested permissions, False otherwise
    """
    user_permissions = await get_user_permissions(server, permission_params)

    if user_permissions == "*":
        return True

    return permission_params.operation in user_permissions


def is_admin_workspace(workspace: str) -> bool:
    """
    Check if the given workspace is an admin workspace.

    Args:
        workspace: The workspace to check

    Returns:
        True if the workspace is an admin workspace, False otherwise
    """
    return workspace in ADMIN_WORKSPACES


async def has_permission(
    server: RemoteService,
    permission_params: PermissionParams,
) -> bool:
    """
    Check if a user has permission to perform an operation.

    First checks if the user is in admin workspaces, then checks artifact-specific permissions.

    Args:
        server: The RemoteService instance
        permission_params: The PermissionParams instance containing all permission details

    Returns:
        True if the user has permission, False otherwise
    """
    # Admin workspaces have full access
    if is_admin_workspace(permission_params.accessor_workspace):
        logger.debug(
            "Granting permission to admin workspace: %s",
            permission_params.accessor_workspace,
        )
        return True

    # Check artifact-specific permissions
    if await user_has_operation_permission(server, permission_params):
        logger.debug(
            "Granting permission to workspace %s for operation %s on artifact %s",
            permission_params.accessor_workspace,
            permission_params.operation,
            permission_params.artifact_id,
        )
        return True

    logger.info(
        "Permission denied for workspace %s, operation %s on artifact %s",
        permission_params.accessor_workspace,
        permission_params.operation,
        permission_params.artifact_id,
    )
    return False


async def require_permission(
    server: RemoteService,
    permission_params: PermissionParams,
) -> None:
    """
    Ensure user has permission or raise HyphaPermissionError.

    Raises:
        HyphaPermissionError: If the user doesn't have the required permission
    """

    if not await has_permission(server, permission_params):
        raise HyphaPermissionError(
            f"Permission denied for {permission_params.operation} operation "
            f"on agent '{permission_params.agent_id}' in ws '{permission_params.accessed_workspace}'",
            permission_params,
        )


async def create_artifact(
    server: RemoteService,
    permission_params: PermissionParams,
) -> None:
    """
    Create an artifact for the given permission parameters.

    Args:
        server: The RemoteService instance
        permission_params: The PermissionParams instance containing artifact details

    Raises:
        HyphaPermissionError: If the user does not have permission to create the artifact
    """
    if not await has_permission(server, permission_params):
        raise HyphaPermissionError(
            f"Permission denied for creating artifact {permission_params.artifact_id}",
            permission_params,
        )

    # Logic to create the artifact would go here
    # For now, we just log the creation
    logger.info(
        "Creating artifact %s for user %s",
        permission_params.artifact_id,
        permission_params.agent_id,
    )
