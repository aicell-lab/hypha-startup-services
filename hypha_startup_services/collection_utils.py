"""Utility functions for managing Weaviate collections."""

import uuid
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections import CollectionAsync
from weaviate.collections.classes.config import CollectionConfig
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.artifacts import get_artifact

WORKSPACE_DELIMITER = "__DELIM__"
ARTIFACT_DELIMITER = ":"
SHARED_WORKSPACE = "SHARED"
ADMIN_WORKSPACES = ["hugo.dettner@scilifelab.se"]
SESSION_DELIMITER = "@"


def acquire_collection(
    client: WeaviateAsyncClient, collection_name: str
) -> CollectionAsync:
    """Acquire a collection from the client."""
    collection_name = full_collection_name(collection_name)
    return client.collections.get(collection_name)


def format_workspace(workspace: str) -> str:
    """Format workspace name to use in collection names.

    Replaces hyphens with underscores and capitalizes the name.
    """
    workspace_formatted = workspace.replace("-", "_").capitalize()
    return workspace_formatted


def name_without_workspace(collection_name: str) -> str:
    """Extract collection name without workspace prefix.

    If the collection name contains the workspace delimiter, returns the part after it.
    Otherwise, returns the original collection name.
    """
    if WORKSPACE_DELIMITER in collection_name:
        return collection_name.split(WORKSPACE_DELIMITER)[1]
    return collection_name


def config_minus_workspace(
    collection_config: CollectionConfig,
) -> dict:
    """Remove workspace from collection config."""
    config_dict = collection_config.to_dict()
    config_dict["class"] = name_without_workspace(config_dict["class"])
    return config_dict


async def collection_to_config_dict(collection: CollectionAsync) -> dict:
    """Convert collection to dict."""
    config = await collection.config.get()
    config_dict = config_minus_workspace(config)
    return config_dict


def assert_valid_collection_name(collection_name: str) -> None:
    """Ensure collection name doesn't contain the workspace delimiter."""
    assert (
        WORKSPACE_DELIMITER not in collection_name
    ), f"Collection name should not contain '{WORKSPACE_DELIMITER}'"


def assert_valid_application_name(application_id: str) -> None:
    """Ensure application name doesn't contain the artifact delimiter."""
    assert (
        ARTIFACT_DELIMITER not in application_id
    ), f"Application ID should not contain '{ARTIFACT_DELIMITER}'"


def stringify_keys(d: dict) -> dict:
    """Convert all keys in a dictionary to strings."""
    return {str(k): v for k, v in d.items()}


def full_collection_name_single(workspace: str, collection_name: str) -> str:
    """Create a full collection name with workspace prefix for a single collection."""
    assert_valid_collection_name(collection_name)

    workspace_formatted = format_workspace(workspace)
    return f"{workspace_formatted}{WORKSPACE_DELIMITER}{collection_name}"


def full_collection_name(name: str | list[str]) -> str:
    """Acquire a collection name from the client."""
    workspace = SHARED_WORKSPACE
    if isinstance(name, list):
        return [full_collection_name_single(workspace, n) for n in name]
    return full_collection_name_single(workspace, name)


def collection_artifact_name(name: str) -> str:
    """Create a full collection artifact name with workspace prefix."""
    return full_collection_name(name)


def application_artifact_name(ws_collection_name: str, application_id: str) -> str:
    """Create a full application artifact name with workspace prefix."""
    assert_valid_collection_name(ws_collection_name)
    assert_valid_application_name(application_id)
    return f"{ws_collection_name}{ARTIFACT_DELIMITER}{application_id}"


def session_artifact_name(
    ws_collection_name: str, application_id: str, session_id: str
) -> str:
    """Create a full session artifact name."""
    assert_valid_collection_name(ws_collection_name)
    assert_valid_application_name(application_id)
    return f"{ws_collection_name}{ARTIFACT_DELIMITER}{application_id}{SESSION_DELIMITER}{session_id}"


def is_in_workspace(collection_name: str, workspace: str) -> bool:
    """Check if a collection belongs to the specified workspace."""
    formatted_workspace = format_workspace(workspace)
    return collection_name.startswith(f"{formatted_workspace}{WORKSPACE_DELIMITER}")


def is_admin_workspace(workspace: str) -> bool:
    """Check if a workspace has admin privileges."""
    return workspace in ADMIN_WORKSPACES


def ws_from_context(context: dict) -> str:
    """Get workspace from context."""
    assert context is not None
    workspace = context.get("ws")
    return workspace


def objects_without_workspace(objects: list[dict]) -> list[dict]:
    """Remove workspace from object IDs."""
    for obj in objects:
        obj.collection = name_without_workspace(obj.collection)
    return objects


def get_artifact_permissions(
    owner: bool = False, admin: bool = False, read_public: bool = True
) -> dict:
    """Generate permissions dictionary for artifacts.

    Args:
        owner: If True, adds $OWNER to write permissions
        admin: If True, adds $ADMIN to admin and write permissions
        read_public: If True, everyone can read. If False, only owner can read.

    Returns:
        A permissions dictionary with read, write, and admin keys
    """
    permissions = {
        "read": ["*"] if read_public else ["$OWNER"],  # Control read access
        "write": [],  # No write by default
        "admin": [],  # No admin by default
    }

    if owner:
        permissions["write"].append("$OWNER")
        if not read_public:
            # Ensure owner is in read permissions if not public
            permissions["read"].append("$OWNER")

    if admin:
        permissions["admin"].append("$ADMIN")
        permissions["write"].append("$ADMIN")
        if not read_public:
            # Ensure admins can read even if not public
            permissions["read"].append("$ADMIN")

    return permissions


async def is_admin(
    server: RemoteService, context: dict[str, Any], collection_name: str = None
) -> bool:
    """Check if the user has admin permissions for collections.

    Args:
        server: The RemoteService instance
        context: The request context containing workspace info
        collection_name: Optional collection name to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """
    workspace = ws_from_context(context)

    # First check if the user is in the admin workspaces list
    if is_admin_workspace(workspace):
        return True

    # If no specific collection is provided, return False for non-admins
    if collection_name is None:
        return False

    # Check if the user has admin permissions for this specific collection artifact
    collection_artifact = collection_artifact_name(collection_name)

    try:
        artifact = await get_artifact(server, collection_artifact, workspace)
        # Check if current user has admin permissions in this artifact
        # This would depend on how permissions are stored in the artifact
        if "permissions" in artifact and "admin" in artifact["permissions"]:
            if workspace in artifact["permissions"]["admin"]:
                return True
    except Exception:
        pass

    return False


def create_application_filter(application_id: str) -> dict:
    """Create a filter for application_id."""
    return {
        "path": ["application_id"],
        "operator": "Equal",
        "valueString": application_id,
    }


def create_session_filter(session_id: str) -> dict:
    """Create a filter for session_id."""
    return {
        "path": ["session_id"],
        "operator": "Equal",
        "valueString": session_id,
    }


def build_query_filter(
    application_id: str = None, session_id: str = None
) -> dict | None:
    """Build a query filter for application_id and optionally session_id.

    Args:
        application_id: The application ID to filter by
        session_id: The optional session ID to filter by

    Returns:
        A Weaviate filter object or None if no filters are requested
    """
    if not application_id:
        return None

    app_filter = create_application_filter(application_id)

    if session_id:
        session_filter = create_session_filter(session_id)
        return {
            "operator": "And",
            "operands": [app_filter, session_filter],
        }

    return app_filter


def apply_query_filter(
    kwargs: dict, application_id: str = None, session_id: str = None
) -> dict:
    """Apply application and session filters to query kwargs if needed.

    Args:
        kwargs: The existing query kwargs
        application_id: The application ID to filter by
        session_id: The optional session ID to filter by

    Returns:
        Updated kwargs dict with filters added
    """
    query_filter = build_query_filter(application_id, session_id)
    if query_filter:
        kwargs["where"] = query_filter
    return kwargs


def create_artifact_metadata(
    collection_name: str = None,
    application_id: str = None,
    session_id: str = None,
    description: str = None,
    workspace: str = None,
    **kwargs,
) -> dict:
    """Create standard metadata for artifacts.

    Args:
        collection_name: The collection name
        application_id: The application ID
        session_id: The session ID
        description: The artifact description
        workspace: The creator's workspace
        **kwargs: Additional metadata fields

    Returns:
        A metadata dictionary with standard fields
    """
    metadata = {
        "created_by": workspace,
        "created_at": str(uuid.uuid1()),
    }

    if collection_name:
        metadata["collection_name"] = collection_name

    if application_id:
        metadata["application_id"] = application_id

    if session_id:
        metadata["session_id"] = session_id

    if description:
        metadata["description"] = description

    # Add any additional metadata
    metadata.update(kwargs)

    return metadata
