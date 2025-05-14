"""Utility functions for managing Weaviate collections."""

from weaviate import WeaviateAsyncClient
from weaviate.collections import CollectionAsync
from weaviate.collections.classes.config import CollectionConfig

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
