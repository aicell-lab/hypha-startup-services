"""Utilities for formatting Weaviate collection names and configurations."""

from typing import TYPE_CHECKING, Protocol, cast

from weaviate.collections import CollectionAsync

from hypha_startup_services.common.constants import COLLECTION_DELIMITER
from hypha_startup_services.common.utils import get_full_collection_name

if TYPE_CHECKING:
    from .models import CollectionConfig


class SupportsToDict(Protocol):
    """Structural type for objects exposing a to_dict method."""

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable dictionary representation."""
        ...


def get_short_name(collection_name: str) -> str:
    """Extract collection name without workspace prefix.

    If the collection name contains the workspace delimiter, returns the part after it.
    Otherwise, returns the original collection name.
    """
    if COLLECTION_DELIMITER in collection_name:
        return collection_name.split(COLLECTION_DELIMITER)[1]
    return collection_name


def config_with_short_name(
    collection_config: SupportsToDict,
) -> "CollectionConfig":
    """Remove workspace from collection config."""
    config_dict = collection_config.to_dict()
    class_name = config_dict.get("class")
    if not isinstance(class_name, str):
        error_msg = "The 'class' field in collection config must be a string."
        raise TypeError(error_msg)

    config_dict["class"] = get_short_name(class_name)
    return cast("CollectionConfig", config_dict)


async def collection_to_config_dict(collection: CollectionAsync) -> "CollectionConfig":
    """Convert collection to a dictionary with shortened collection name.

    Gets the collection's configuration and converts the full collection name
    to a short name (without workspace prefix).

    Args:
        collection: The collection object to convert

    Returns:
        Dictionary representation of the collection configuration with short name

    """
    config = await collection.config.get()
    return config_with_short_name(config)


def get_full_collection_names(short_names: list[str]) -> list[str]:
    """Get full collection names from a list of short names."""
    return [get_full_collection_name(short_name) for short_name in short_names]


def get_settings_full_name(settings: "CollectionConfig") -> "CollectionConfig":
    """Add workspace prefix to the collection name in settings."""
    settings_full_name = settings.copy()
    original_class_name = settings_full_name.get("class")

    if not isinstance(original_class_name, str):
        error_msg = "The 'class' field in settings must be a string."
        raise TypeError(error_msg)

    settings_full_name["class"] = get_full_collection_name(original_class_name)
    return settings_full_name


def add_app_id(
    objects: list[dict[str, object]],
    application_id: str,
) -> list[dict[str, object]]:
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
