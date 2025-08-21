from typing import Any

from weaviate.collections import CollectionAsync
from weaviate.collections.classes.config import CollectionConfig

from hypha_startup_services.common.constants import COLLECTION_DELIMITER
from hypha_startup_services.common.utils import get_full_collection_name


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
) -> dict[str, Any]:
    """Remove workspace from collection config."""
    config_dict = collection_config.to_dict()
    config_dict["class"] = get_short_name(config_dict["class"])
    return config_dict


async def collection_to_config_dict(collection: CollectionAsync) -> dict[str, Any]:
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


def get_full_collection_names(short_names: list[str]) -> list[str]:
    """Get full collection names from a list of short names."""
    return [get_full_collection_name(short_name) for short_name in short_names]


def get_settings_full_name(settings: dict[str, Any]) -> dict[str, Any]:
    """Add workspace prefix to the collection name in settings."""
    settings_full_name = settings.copy()
    original_class_name = settings_full_name["class"]
    settings_full_name["class"] = get_full_collection_name(original_class_name)
    return settings_full_name


def add_app_id(
    objects: list[dict[str, Any]],
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
    for obj in objects:
        obj["application_id"] = application_id
    return objects
