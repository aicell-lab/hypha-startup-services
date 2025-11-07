"""Unified permissions module for Hypha startup services.

This module provides a consistent permission system across all services,
based on the mem0 service design with enhancements for flexibility.
"""

import logging
from abc import ABC, abstractmethod
from typing import Literal, TypedDict, cast

from hypha_rpc.rpc import RemoteException
from pydantic import BaseModel, Field

from .artifacts import get_artifact
from .constants import ADMIN_WORKSPACES
from .utils import (
    get_application_artifact_name,
    get_full_collection_name,
)

logger = logging.getLogger(__name__)

# Type alias for permission operations
PermissionOperation = Literal[
    "n",
    "l",
    "l+",
    "lv",
    "lv+",
    "lf",
    "lf+",
    "r",
    "r+",
    "rw",
    "rw+",
    "*",
]


class HyphaPermissionError(Exception):
    """Custom exception for permission-related errors."""

    def __init__(
        self,
        message: str,
        permission_params: "BasePermissionParams | None" = None,
    ) -> None:
        """Initialize HyphaPermissionError.

        Args:
            message (str): Error message.
            permission_params (BasePermissionParams | None, optional):
                Permission parameters associated with the error. Defaults to None.

        """
        self.permission_params = permission_params
        super().__init__(message)


class BasePermissionParams(ABC, BaseModel):
    """Abstract base class for permission parameters.

    This defines the interface that all permission parameter classes must implement.
    """

    accessor_workspace: str = Field(
        description="The workspace of the user accessing the resource",
    )
    operation: PermissionOperation = Field(
        default="r",
        description="The requested operation",
    )

    @property
    @abstractmethod
    def artifact_id(self) -> str:
        """Return the artifact ID for permission checking."""

    @property
    @abstractmethod
    def resource_description(self) -> str:
        """Return a human-readable description of the resource being accessed."""


class ArtifactPermissionParams(BasePermissionParams):
    """Permission parameters for direct artifact access."""

    artifact_name: str = Field(
        description="The name/ID of the artifact being accessed",
    )

    @property
    def artifact_id(self) -> str:
        """Get the artifact ID."""
        return self.artifact_name

    @property
    def resource_description(self) -> str:
        """Get a human-readable description of the resource."""
        return f"artifact '{self.artifact_name}'"


class AgentPermissionParams(BasePermissionParams):
    """Permission parameters for mem0-style agent operations."""

    agent_id: str = Field(description="The ID of the agent")
    accessed_workspace: str = Field(description="The workspace being accessed")
    run_id: str | None = Field(default=None, description="The ID of the run")

    @property
    def artifact_id(self) -> str:
        """Generate artifact ID for mem0-style agent operations."""
        artifact_id = self.agent_id
        if self.accessed_workspace:
            artifact_id += f":{self.accessed_workspace}"
            if self.run_id:
                artifact_id += f":{self.run_id}"
        return artifact_id

    @property
    def resource_description(self) -> str:
        """Get a human-readable description of the resource.

        Returns:
            str: A description of the resource.

        """
        desc = f"agent '{self.agent_id}' in workspace '{self.accessed_workspace}'"
        if self.run_id:
            desc += f" with run '{self.run_id}'"
        return desc


class ApplicationPermissionParams(BasePermissionParams):
    """Permission parameters for Weaviate-style application operations."""

    collection_name: str = Field(description="The collection name")
    application_id: str = Field(description="The application ID")
    application_workspace: str = Field(description="The workspace of the application")

    @property
    def artifact_id(self) -> str:
        """Generate artifact ID for Weaviate-style application operations."""
        full_collection_name = get_full_collection_name(self.collection_name)
        return get_application_artifact_name(
            full_collection_name,
            self.application_workspace,
            self.application_id,
        )

    @property
    def resource_description(self) -> str:
        """Get a human-readable description of the resource."""
        return (
            f"application '{self.application_id}'"
            f" in collection '{self.collection_name}'"
        )


class CollectionPermissionParams(BasePermissionParams):
    """Permission parameters for Weaviate-style collection operations."""

    collection_names: list[str] = Field(description="List of collection names")

    @property
    def artifact_id(self) -> str:
        """For collections, we check the first one as the primary artifact."""
        if not self.collection_names:
            return ""

        return get_full_collection_name(self.collection_names[0])

    @property
    def resource_description(self) -> str:
        """Get a human-readable description of the resource.

        Returns:
            str: A description of the resource.

        """
        if len(self.collection_names) == 1:
            return f"collection '{self.collection_names[0]}'"
        return f"collections {self.collection_names}"


def is_admin_workspace(workspace: str) -> bool:
    """Check if the given workspace is an admin workspace.

    Args:
        workspace: The workspace to check

    Returns:
        True if the workspace is an admin workspace, False otherwise

    """
    return workspace in ADMIN_WORKSPACES


class ArtifactConfig(TypedDict, total=False):
    """Artifact configuration shape.

    "permissions" is expected to be present and a mapping of
    workspace -> permission string when the artifact is created by
    our services, but the overall config can include additional
    provider-specific keys that we don't model.
    """

    permissions: dict[str, str]


class ArtifactData(TypedDict, total=False):
    """Artifact data as returned by artifact manager.

    Only the "config" portion is modeled here since that's all we access.
    """

    config: ArtifactConfig


async def get_user_permissions(
    permission_params: BasePermissionParams,
) -> str:
    """Get user permissions for a specific artifact.

    Args:
        permission_params: The permission parameters

    Returns:
        A dictionary or string containing the user's permissions for the artifact

    """
    try:
        artifact_raw = await get_artifact(permission_params.artifact_id)
    except RemoteException as e:
        error_msg = f"Failed to retrieve artifact {permission_params.artifact_id}: {e}"
        logger.exception(error_msg)
        return ""
    artifact = cast("ArtifactData", artifact_raw)
    config: ArtifactConfig = artifact.get("config", {})
    permissions: dict[str, str] = config.get("permissions", {})

    return permissions.get(permission_params.accessor_workspace, "")


async def user_has_operation_permission(
    permission_params: BasePermissionParams,
) -> bool:
    """Check if the user has the requested permissions for a specific artifact.

    Args:
        permission_params: The permission parameters

    Returns:
        True if the user has the requested permissions, False otherwise

    """
    user_permissions = await get_user_permissions(permission_params)

    if user_permissions == "*":
        return True

    if user_permissions in ("*", "rw+"):
        return True

    return permission_params.operation in user_permissions


async def has_permission(
    permission_params: BasePermissionParams,
) -> bool:
    """Check if a user has permission to perform an operation.

    First checks if the user is in admin workspaces,
    then checks artifact-specific permissions.

    Args:
        permission_params: The permission parameters

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

    # Special handling for collection permissions (check all collections)
    if isinstance(permission_params, CollectionPermissionParams):
        for collection_name in permission_params.collection_names:
            collection_params = CollectionPermissionParams(
                accessor_workspace=permission_params.accessor_workspace,
                operation=permission_params.operation,
                collection_names=[collection_name],
            )
            if not await user_has_operation_permission(collection_params):
                logger.info(
                    "Permission denied for workspace %s, operation %s on collection %s",
                    permission_params.accessor_workspace,
                    permission_params.operation,
                    collection_name,
                )
                return False
        return True

    # Check artifact-specific permissions
    if await user_has_operation_permission(permission_params):
        logger.debug(
            "Granting permission to workspace %s for operation %s on %s",
            permission_params.accessor_workspace,
            permission_params.operation,
            permission_params.resource_description,
        )
        return True

    logger.info(
        "Permission denied for workspace %s, operation %s on %s",
        permission_params.accessor_workspace,
        permission_params.operation,
        permission_params.resource_description,
    )
    return False


async def require_permission(
    permission_params: BasePermissionParams,
) -> None:
    """Ensure user has permission or raise HyphaPermissionError.

    Raises:
        HyphaPermissionError: If the user doesn't have the required permission

    """
    if not await has_permission(permission_params):
        error_msg = (
            f"Permission denied for {permission_params.operation} operation "
            f"on {permission_params.resource_description}"
        )
        raise HyphaPermissionError(error_msg, permission_params)


def make_artifact_permissions(owners: str | list[str]) -> dict[str, str]:
    """Generate permissions dictionary for artifacts.

    This function creates a permission structure compatible with both services.

    Args:
        owners: A user ID or list of user IDs who own the artifact

    Returns:
        A permissions dictionary with appropriate permission levels

    """
    if isinstance(owners, str):
        owners = [owners]

    permissions = {
        "*": "r",  # Default read access for all users
    }

    # Give full access to owners
    for owner in owners:
        permissions[owner] = "*"

    # Give full access to admin workspaces
    for admin_ws in ADMIN_WORKSPACES:
        permissions[admin_ws] = "*"

    return permissions


# Convenience functions for backward compatibility


def assert_is_admin_ws(user_ws: str) -> None:
    """Assert that user has admin permissions."""
    if not is_admin_workspace(user_ws):
        error_msg = f"User workspace '{user_ws}' is not an admin workspace"
        raise HyphaPermissionError(error_msg)


async def assert_has_artifact_permission(
    user_ws: str,
    artifact_name: str,
    operation: PermissionOperation = "r",
) -> None:
    """Assert that user has permission for an artifact."""
    params = ArtifactPermissionParams(
        accessor_workspace=user_ws,
        artifact_name=artifact_name,
        operation=operation,
    )
    await require_permission(params)


async def assert_has_collection_permission(
    user_ws: str,
    collection_names: str | list[str],
    operation: PermissionOperation = "r",
) -> None:
    """Assert that user has permission for collections."""
    if isinstance(collection_names, str):
        collection_names = [collection_names]

    params = CollectionPermissionParams(
        accessor_workspace=user_ws,
        collection_names=collection_names,
        operation=operation,
    )
    await require_permission(params)


async def assert_has_application_permission(
    collection_name: str,
    application_id: str,
    accessor_ws: str,
    application_workspace: str,
    operation: PermissionOperation = "r",
) -> None:
    """Assert that user has permission for an application."""
    params = ApplicationPermissionParams(
        accessor_workspace=accessor_ws,
        collection_name=collection_name,
        application_id=application_id,
        application_workspace=application_workspace,
        operation=operation,
    )
    await require_permission(params)
