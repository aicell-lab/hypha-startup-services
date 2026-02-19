"""Models for Weaviate artifact parameters."""

from collections.abc import Sequence
from typing import Any, Literal, TypedDict

from pydantic import BaseModel, Field

from hypha_startup_services.common.artifacts import BaseArtifactParams
from hypha_startup_services.common.constants import ARTIFACT_DELIMITER

from .format_utils import (
    get_full_collection_name,
)

# TODO: refactor this file significantly
# Idea: composition, not inheritance: every class has an artifact params instance

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

PermissionMap = dict[str, PermissionOperation]


# Define CollectionConfig using functional syntax to support "class" key
CollectionConfig = TypedDict(
    "CollectionConfig",
    {
        "class": str,
        "description": str,
        "vectorizer": str,
        "properties": list[dict[str, Any]],
        "multiTenancyConfig": dict[str, Any],
        "vectorIndexConfig": dict[str, Any],
        "moduleConfig": dict[str, Any],
        "vectorConfig": dict[str, Any],
        "invertedIndexConfig": dict[str, Any],
        "replicationConfig": dict[str, Any],
        "shardingConfig": dict[str, Any],
        "vectorIndexType": str,
    },
    total=False,
)


class ApplicationReturn(TypedDict):
    """Return type for application creation."""

    application_id: str
    collection_name: str
    description: str
    owner: str
    artifact_name: str
    result: dict[str, Any]


class ApplicationArtifactReturn(TypedDict):
    """Return type for application artifact helpers."""

    artifact_name: str
    # Add other keys if available/known
    parent_id: str | None


class DataDeleteManyReturn(TypedDict):
    """Return type for delete many operation."""

    failed: int
    matches: int
    objects: Sequence[Any] | None
    successful: int


class ServiceQueryReturn(TypedDict, total=False):
    """Return type for query operations."""

    objects: Sequence[Any]
    generated: str | None


HyphaContext = dict[str, Any]


class WeaviateArtifactParams(BaseModel, BaseArtifactParams):
    """Weaviate artifact parameters with validation and computed properties."""

    artifact_name: str = Field(
        description="The name/ID of the artifact",
    )
    desc: str | None = Field(
        default=None,
        description="A description of the artifact",
    )
    permissions: dict[str, str] | None = Field(
        default=None,
        description="Artifact permissions, mapping user IDs to permission levels",
    )
    metadata: dict[str, Any] | None = Field(
        default=None,
        description="Metadata associated with the artifact",
    )
    artifact_type: str = Field(
        default="collection",
        description="The type of the artifact, e.g., 'collection', 'file', etc.",
    )
    parent_id: str | None = Field(
        default=None,
        description="Parent artifact ID if this is a child artifact",
    )

    @property
    def artifact_id(self) -> str:
        """Returns the artifact ID."""
        return self.artifact_name

    @property
    def description(self) -> str:
        """Returns the description of the artifact."""
        if self.desc:
            return self.desc
        return f"Weaviate artifact {self.artifact_name}"

    @property
    def manifest(self) -> dict[str, Any]:
        """Returns the artifact manifest as a dictionary."""
        return {
            "name": self.artifact_name,
            "description": self.description,
            "collection": [],
            "metadata": self.metadata or {},
        }

    @property
    def creation_dict(self) -> dict[str, Any]:
        """Convert instance to a dictionary suitable for artifact creation."""
        return {
            "parent_id": self.parent_id,
            "alias": self.artifact_name,
            "type": self.artifact_type,
            "config": {
                "permissions": self.permissions,
            },
            "manifest": self.manifest,
        }


class CollectionArtifactParams(WeaviateArtifactParams):
    """Model for collection artifact parameters."""

    collection_name: str = Field(
        description="The short name of the collection",
    )

    def __init__(self, **data: Any) -> None:
        """Initialize CollectionArtifactParams."""
        # Auto-generate artifact_name from collection_name if not provided
        if "artifact_name" not in data and "collection_name" in data:
            data["artifact_name"] = get_full_collection_name(data["collection_name"])
        super().__init__(**data)

    @property
    def manifest(self) -> dict[str, Any]:
        """Returns the artifact manifest as a dictionary."""
        return {
            "name": self.artifact_name,
            "description": self.description,
            "collection": [],
            "metadata": self.metadata or {},
        }


class ApplicationArtifactParams(WeaviateArtifactParams):
    """Model for application artifact parameters."""

    collection_name: str = Field(
        description="The short name of the collection",
    )
    application_id: str = Field(
        description="The ID of the application",
    )
    user_workspace: str = Field(
        description="The user workspace for the application",
    )

    def __init__(self, **data: Any) -> None:
        """Initialize ApplicationArtifactParams."""
        # Auto-generate artifact_name and parent_id if not provided
        if "artifact_name" not in data:
            full_collection_name = get_full_collection_name(data["collection_name"])
            data["artifact_name"] = (
                f"{full_collection_name}{ARTIFACT_DELIMITER}"
                f"{data['user_workspace']}{ARTIFACT_DELIMITER}"
                f"{data['application_id']}"
            )

        if "parent_id" not in data:
            data["parent_id"] = get_full_collection_name(data["collection_name"])

        super().__init__(**data)

    @property
    def manifest(self) -> dict[str, Any]:
        """Returns the artifact manifest as a dictionary."""
        metadata = self.metadata or {}
        metadata.update(
            {
                "application_id": self.application_id,
                "short_collection_name": self.collection_name,
            },
        )

        return {
            "name": self.artifact_name,
            "description": self.description,
            "collection": [],
            "metadata": metadata,
        }
