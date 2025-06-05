from typing import Any, Literal
from pydantic import BaseModel, Field
from hypha_startup_services.mem0_service.utils.constants import ARTIFACT_DELIMITER


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


class PermissionParams(BaseModel):
    """
    Model for permission parameters with validation and computed properties.
    """

    accessor_ws: str = Field(
        description="The workspace of the user accessing the artifact",
    )
    agent_id: str = Field(description="The ID of the agent")
    accessed_ws: str = Field(description="The workspace of the user")
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
        artifact_id = f"{self.agent_id}{ARTIFACT_DELIMITER}{self.accessed_ws}"
        if self.run_id:
            artifact_id += f"{ARTIFACT_DELIMITER}{self.run_id}"
        return artifact_id

    @classmethod
    def from_mem0_params(
        cls,
        agent_id: str,
        workspace: str,
        context: dict[str, Any],
        run_id: str | None = None,
        operation: PermissionOperation = "r",
    ) -> "PermissionParams":
        """
        Create PermissionParams from mem0 function parameters.

        This is a convenience method for creating PermissionParams instances
        from the common parameter pattern used in mem0 functions.

        Args:
            agent_id: The ID of the agent
            workspace: The workspace of the user
            context: Context from Hypha-rpc containing user information
            run_id: Optional ID of the run
            operation: The requested operation (see operation field description for valid values)

        Returns:
            A new PermissionParams instance

        Raises:
            ValueError: If context is invalid or missing required fields
        """
        # Validate context structure before creating the instance
        if not context:
            raise ValueError("Context cannot be empty")

        user_info = context.get("user", {})
        if not user_info:
            raise ValueError("Context must contain 'user' information")

        scope = user_info.get("scope", {})
        if not scope or "current_workspace" not in scope:
            raise ValueError("Context must contain user scope with current_workspace")

        try:
            accessor_ws = context["user"]["scope"]["current_workspace"]
        except KeyError as e:
            raise KeyError(f"Invalid context structure: missing {e}") from e

        return cls(
            accessor_ws=accessor_ws,
            agent_id=agent_id,
            accessed_ws=workspace,
            run_id=run_id,
            operation=operation,
        )


class CreateArtifactParams(BaseModel):
    """
    Model for artifact parameters with validation and computed properties.
    """

    artifact_id: str = Field(
        description="The ID of the artifact",
    )
    description: str | None = Field(
        default=None,
        description="A description of the artifact",
    )
    permissions: dict[str, PermissionOperation] | None = Field(
        default=None,
        description="Permissions for the artifact, mapping user IDs to permission levels",
    )
    metadata: dict[str, Any] | None = Field(
        default=None,
        description="Metadata associated with the artifact",
    )
    parent_id: str | None = Field(
        default=None,
        description="The ID of the parent artifact, if any",
    )
    artifact_type: str = Field(
        default="collection",
        description="The type of the artifact, e.g., 'collection', 'file', etc.",
    )
    workspace: str | None = Field(
        default=None,
        description="The workspace associated with this artifact",
    )
    agent_id: str | None = Field(
        default=None,
        description="The agent ID associated with this artifact",
    )
    run_id: str | None = Field(
        default=None,
        description="The run ID associated with this artifact",
    )

    @property
    def manifest(self) -> dict[str, Any]:
        """
        Returns the artifact manifest as a dictionary.
        """
        return {
            "name": self.artifact_id,
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
        return {
            "parent_id": self.parent_id,
            "alias": self.artifact_id,
            "type": self.artifact_type,
            "config": {
                "permissions": self.permissions,
            },
            "manifest": self.manifest,
        }

    @classmethod
    def from_mem0_params(
        cls,
        agent_id: str,
        context: dict[str, Any],
        run_id: str | None = None,
        description: str | None = None,
        permissions: PermissionOperation = "r",
        metadata: dict[str, Any] | None = None,
        parent_id: str | None = None,
        artifact_type: str = "collection",
    ) -> "CreateArtifactParams":
        """
        Create ArtifactParams from mem0 function parameters.

        This is a convenience method for creating ArtifactParams instances
        from the common parameter pattern used in mem0 functions.

        Args:
            agent_id: The ID of the agent
            context: Context from Hypha-rpc containing user information
            run_id: Optional ID of the run
            description: A description of the artifact
            permissions: Either a dict mapping users to permissions, or a single permission level
            metadata: Metadata associated with the artifact
            parent_id: The ID of the parent artifact, if any
            artifact_type: The type of the artifact

        Returns:
            A new ArtifactParams instance

        Raises:
            ValueError: If context is invalid or missing required fields
        """
        try:
            workspace = context["user"]["scope"]["current_workspace"]
        except KeyError as e:
            raise KeyError(f"Invalid context structure: missing {e}") from e

        # Generate artifact_id
        artifact_id = f"{agent_id}{ARTIFACT_DELIMITER}{workspace}"
        if run_id:
            artifact_id += f"{ARTIFACT_DELIMITER}{run_id}"

        processed_permissions: dict[str, PermissionOperation] = {workspace: permissions}

        return cls(
            artifact_id=artifact_id,
            description=description,
            permissions=processed_permissions,
            metadata=metadata,
            parent_id=parent_id,
            artifact_type=artifact_type,
            workspace=workspace,
            agent_id=agent_id,
            run_id=run_id,
        )

    def with_run_id(
        self, run_id: str, permissions: PermissionOperation
    ) -> "CreateArtifactParams":
        """
        Create a new ArtifactParams instance with a run_id added.

        This updates the artifact_id to include the run_id and creates a new instance.

        Args:
            run_id: The run ID to add

        Returns:
            A new ArtifactParams instance with the run_id included
        """
        # Generate new artifact_id with run_id
        base_artifact_id = f"{self.agent_id}{ARTIFACT_DELIMITER}{self.workspace}"
        new_artifact_id = f"{base_artifact_id}{ARTIFACT_DELIMITER}{run_id}"
        parent_id = base_artifact_id

        return self.model_copy(
            update={
                "artifact_id": new_artifact_id,
                "run_id": run_id,
                "permissions": {self.workspace: permissions},
                "parent_id": parent_id,
            }
        )
