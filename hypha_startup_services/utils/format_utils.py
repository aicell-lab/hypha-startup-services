from typing import Any
from weaviate.collections import CollectionAsync
from weaviate.collections.classes.config import CollectionConfig
from hypha_startup_services.utils.constants import (
    COLLECTION_DELIMITER,
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


def get_short_name(collection_name: str) -> str:
    """Extract collection name without workspace prefix.

    If the collection name contains the workspace delimiter, returns the part after it.
    Otherwise, returns the original collection name.
    """
    if COLLECTION_DELIMITER in collection_name:
        return collection_name.split(COLLECTION_DELIMITER)[1]
    return collection_name


def config_with_short_name(
    collection_config: CollectionConfig,
) -> dict:
    """Remove workspace from collection config."""
    config_dict = collection_config.to_dict()
    config_dict["class"] = get_short_name(config_dict["class"])
    return config_dict


async def collection_to_config_dict(collection: CollectionAsync) -> dict:
    """Convert collection to a dictionary with shortened collection name.

    Gets the collection's configuration and converts the full collection name
    to a short name (without workspace prefix).

    Args:
        collection: The collection object to convert

    Returns:
        Dictionary representation of the collection configuration with short name
    """
    config = await collection.config.get()
    config_dict = config_with_short_name(config)
    return config_dict


def assert_valid_collection_name(collection_name: str) -> None:
    """Ensure collection name doesn't contain the workspace delimiter."""
    assert (
        COLLECTION_DELIMITER not in collection_name
    ), f"Collection name should not contain '{COLLECTION_DELIMITER}'"


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
    return f"{workspace_formatted}{COLLECTION_DELIMITER}{collection_name}"


def get_full_collection_name(name: str | list[str]) -> str:
    """Acquire a collection name from the client."""
    workspace = SHARED_WORKSPACE
    if isinstance(name, list):
        return [full_collection_name_single(workspace, n) for n in name]
    return full_collection_name_single(workspace, name)


def get_settings_full_name(settings: dict[str, Any]) -> dict[str, Any]:
    """Add workspace prefix to the collection name in settings."""
    settings_full_name = settings.copy()
    original_class_name = settings_full_name["class"]
    settings_full_name["class"] = get_full_collection_name(original_class_name)
    return settings_full_name


def add_app_id(
    objects: dict[str, Any] | list[dict[str, Any]],
    application_id: str,
) -> list[dict[str, Any]]:
    """Append the application ID to each object in the list or to a single object.

    If objects is a single dictionary, it gets converted to a list with one item.
    The application_id is added as a property to each object.

    Args:
        objects: Single object or list of objects to add application_id to
        application_id: Application ID to add to each object

    Returns:
        List of objects with application_id added
    """
    object_list = objects if isinstance(objects, list) else [objects]
    for obj in object_list:
        obj["application_id"] = application_id
    return object_list
