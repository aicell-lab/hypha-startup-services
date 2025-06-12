from typing import Any, Literal
from pydantic import BaseModel, Field
from hypha_startup_services.mem0_service.utils.constants import ARTIFACT_DELIMITER
from hypha_startup_services.common.artifacts import BaseArtifactParams
from hypha_startup_services.common.permissions import BasePermissionParams


# Type alias for permission operations
PermissionOperation = Literal[
    "n", "l", "l+", "lv", "lv+", "lf", "lf+", "r", "r+", "rw", "rw+", "*"
]


class HyphaPermissionError(Exception):
    """Custom exception for permission-related errors."""

    def __init__(
        self, message: str, permission_params: "PermissionParams | None" = None
    ):
        self.permission_params = permission_params
        super().__init__(message)


class PermissionParams(BasePermissionParams):
    """
    Model for permission parameters with validation and computed properties.
    """

    accessor_workspace: str = Field(
        description="The workspace of the user accessing the artifact",
    )
    agent_id: str = Field(description="The ID of the agent")
    accessed_workspace: str = Field(description="The workspace of the user")
    run_id: str | None = Field(default=None, description="The ID of the run")
    operation: PermissionOperation = Field(
        default="r",
        description=(
            "The requested operation:\n"
            "  n: No access to the artifact\n"
            "  l: List-only access (includes list)\n"
            "  l+: List and create access (includes list, create, and commit)\n"
            "  lv: List and list vectors access (includes list and list_vectors)\n"
            "  lv+: List, list vectors, create, and commit access\n"
            "  lf: List and list files access (includes list and list_files)\n"
            "  lf+: List, list files, create, and commit access\n"
            "  r: Read-only access (includes read, get_file, list_files, list, search_vectors, and get_vector)\n"
            "  r+: Read, write, and create access"
            " (includes read, get_file, put_file, list_files, list, search_vectors, "
            " get_vector, create, commit, add_vectors, and add_documents)\n"
            "  rw: Read, write, and create access with file management\n"
            "  rw+: Read, write, create, and manage access\n"
            "  *: Full access to all operations"
        ),
    )

    @property
    def artifact_id(self) -> str:
        """
        Returns the artifact ID based on agent_id, workspace, and run_id.

        Format: {agent_id}:{workspace} or {agent_id}:{workspace}:{run_id}
        """
        artifact_id = f"{self.agent_id}{ARTIFACT_DELIMITER}{self.accessed_workspace}"
        if self.run_id:
            artifact_id += f"{ARTIFACT_DELIMITER}{self.run_id}"
        return artifact_id


class AgentArtifactParams(BaseModel, BaseArtifactParams):
    """
    Model for artifact parameters with validation and computed properties.
    """

    agent_id: str = Field(
        description="The agent ID associated with this artifact",
    )
    creator_id: str = Field(
        description="The workspace of the artifact creator, used for permission checks",
    )
    _workspace: str | None = None
    _run_id: str | None = None
    desc: str | None = Field(
        default=None,
        description="A description of the artifact",
    )
    general_permission: PermissionOperation | None = Field(
        default=None,
        description="Permissions for the artifact, mapping user IDs to permission levels",
    )
    workspace_permission: PermissionOperation | None = Field(
        default=None,
        description="Permissions for the workspace, mapping user IDs to permission levels",
    )
    metadata: dict[str, Any] | None = Field(
        default=None,
        description="Metadata associated with the artifact",
    )
    artifact_type: str = Field(
        default="collection",
        description="The type of the artifact, e.g., 'collection', 'file', etc.",
    )

    @property
    def artifact_id(self) -> str:
        """
        Returns the artifact ID based on agent_id, workspace, and run_id.

        Format: {agent_id}:{workspace} or {agent_id}:{workspace}:{run_id}
        """
        artifact_id = self.agent_id
        if self._workspace:
            artifact_id += f"{ARTIFACT_DELIMITER}{self._workspace}"
            if self._run_id:
                artifact_id += f"{ARTIFACT_DELIMITER}{self._run_id}"
        return artifact_id

    @property
    def parent_id(self) -> str | None:
        """
        Returns the parent artifact ID if it exists, otherwise None.
        """
        parent_id = None
        if self._workspace:
            parent_id = self.agent_id
            if self._run_id:
                parent_id += f"{ARTIFACT_DELIMITER}{self._workspace}"
        return parent_id

    @property
    def description(self) -> str:
        """
        Returns the description of the artifact.
        """
        if self.desc:
            return self.desc

        description = f"Artifact for agent {self.agent_id}"
        if self._workspace:
            description += f" in workspace {self._workspace}"
        if self._run_id:
            description += f" with run ID {self._run_id}"
        return description

    @property
    def manifest(self) -> dict[str, Any]:
        """
        Returns the artifact manifest as a dictionary.
        """
        return {
            "name": f"Artifact for agent {self.agent_id}",
            "description": self.description or "",
            "collection": [],
            "metadata": self.metadata or {},
        }

    @property
    def creation_dict(self) -> dict[str, Any]:
        """
        Convert the ArtifactParams instance to a dictionary suitable for artifact creation.

        Returns:
            A dictionary representation of the ArtifactParams instance.
        """
        permission_dict = {self.creator_id: "*", "*": self.general_permission or "r"}
        if self._workspace:
            permission_dict[self._workspace] = self.workspace_permission or "*"

        return {
            "parent_id": self.parent_id,
            "alias": self.artifact_id,
            "type": self.artifact_type,
            "config": {
                "permissions": permission_dict,
            },
            "manifest": self.manifest,
        }

    def for_workspace(
        self, workspace: str, operation: PermissionOperation = "*"
    ) -> "AgentArtifactParams":
        """
        Create a new ArtifactParams instance with a workspace added.

        This updates the artifact_id to include the workspace and creates a new instance.

        Args:
            workspace: The workspace to add
            operation: The operation permission for the new artifact

        Returns:
            A new ArtifactParams instance with the workspace included
        """
        return self.model_copy(
            update={
                "_workspace": workspace,
                "workspace_permission": operation,
            }
        )

    def for_run(
        self,
        run_id: str,
        workspace: str | None = None,
        operation: PermissionOperation = "*",
    ) -> "AgentArtifactParams":
        """
        Create a new ArtifactParams instance with a run_id added.

        This updates the artifact_id to include the run_id and creates a new instance.

        Args:
            run_id: The run ID to add
            workspace: The workspace for the new artifact
            operation: The operation permission for the new artifact

        Returns:
            A new ArtifactParams instance with the run_id included
        """

        return self.model_copy(
            update={
                "_workspace": workspace or self._workspace,
                "_run_id": run_id,
                "workspace_permission": operation,
            }
        )
