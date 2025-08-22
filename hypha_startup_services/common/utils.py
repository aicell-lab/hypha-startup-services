"""Common utility functions shared between services."""

from typing import Any

from hypha_rpc.utils import ObjectProxy

from .constants import (
    ARTIFACT_DELIMITER,
    COLLECTION_DELIMITER,
    SHARED_WORKSPACE,
)


def proxy_to_dict(proxy: dict[str, Any] | ObjectProxy) -> dict[Any, Any]:
    """Convert an ObjectProxy to a regular dictionary."""
    if isinstance(proxy, ObjectProxy):
        return proxy.toDict()
    return proxy


def assert_valid_application_name(application_id: str) -> None:
    """Ensure application name doesn't contain the artifact delimiter."""
    if ARTIFACT_DELIMITER in application_id:
        error_msg = f"Application ID should not contain '{ARTIFACT_DELIMITER}'"
        raise ValueError(error_msg)


def get_application_artifact_name(
    full_collection_name: str,
    user_ws: str,
    application_id: str,
) -> str:
    """Create a full application artifact name."""
    assert_valid_application_name(application_id)
    return (
        f"{full_collection_name}{ARTIFACT_DELIMITER}{user_ws}"
        f"{ARTIFACT_DELIMITER}{application_id}"
    )


def stringify_keys(d: dict[Any, Any]) -> dict[str, Any]:
    """Convert all keys in a dictionary to strings."""
    return {str(k): v for k, v in d.items()}


def format_workspace(workspace: str) -> str:
    """Format workspace name to use in collection names.

    Replaces hyphens with underscores and capitalizes the name.
    """
    return workspace.replace("-", "_").capitalize()


def assert_valid_collection_name(collection_name: str) -> None:
    """Ensure collection name doesn't contain the workspace delimiter."""
    if COLLECTION_DELIMITER in collection_name:
        error_msg = f"Collection name should not contain '{COLLECTION_DELIMITER}'"
        raise ValueError(error_msg)


def get_full_collection_name(short_name: str) -> str:
    """Create a full collection name with workspace prefix for a single collection."""
    assert_valid_collection_name(short_name)

    workspace_formatted = format_workspace(SHARED_WORKSPACE)
    return f"{workspace_formatted}{COLLECTION_DELIMITER}{short_name}"
