from typing import Any
from weaviate.collections import CollectionAsync
from weaviate.collections.classes.config import CollectionConfig
from hypha_startup_services.utils.constants import (
    WORKSPACE_DELIMITER,
    ARTIFACT_DELIMITER,
    SHARED_WORKSPACE,
)


def id_from_context(context: dict) -> str:
    """Get ID from context."""
    assert context is not None
    user_id = context["user"]["id"]
    return user_id


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


def is_in_workspace(collection_name: str, workspace: str) -> bool:
    """Check if a collection belongs to the specified workspace."""
    formatted_workspace = format_workspace(workspace)
    return collection_name.startswith(f"{formatted_workspace}{WORKSPACE_DELIMITER}")


def get_settings_with_workspace(settings: dict[str, Any]) -> dict[str, Any]:
    """Add workspace prefix to the collection name in settings."""
    settings_with_workspace = settings.copy()
    original_class_name = settings_with_workspace["class"]
    settings_with_workspace["class"] = full_collection_name(original_class_name)
    return settings_with_workspace


def add_app_id(
    objects: dict[str, Any] | list[dict[str, Any]],
    application_id: str,
) -> list[dict[str, Any]]:
    """Append the application ID to each object in the list."""
    object_list = objects if isinstance(objects, list) else [objects]
    for obj in object_list:
        obj["application_id"] = application_id
    return object_list
