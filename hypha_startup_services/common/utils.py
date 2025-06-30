"""Common utility functions shared between services."""

import asyncio
from functools import wraps
from typing import Any
from hypha_rpc.utils import ObjectProxy
from .constants import (
    ARTIFACT_DELIMITER,
    COLLECTION_DELIMITER,
    SHARED_WORKSPACE,
)


def proxy_to_dict(proxy: dict[str, Any] | ObjectProxy) -> Any:
    """Convert an ObjectProxy to a regular dictionary."""
    if isinstance(proxy, ObjectProxy):
        return proxy.toDict()
    return proxy


def assert_valid_application_name(application_id: str) -> None:
    """Ensure application name doesn't contain the artifact delimiter."""
    assert (
        ARTIFACT_DELIMITER not in application_id
    ), f"Application ID should not contain '{ARTIFACT_DELIMITER}'"


def get_application_artifact_name(
    full_collection_name: str, user_ws: str, application_id: str
) -> str:
    """Create a full application artifact name."""
    assert_valid_application_name(application_id)
    return f"{full_collection_name}{ARTIFACT_DELIMITER}{user_ws}{ARTIFACT_DELIMITER}{application_id}"


def stringify_keys(d: dict) -> dict:
    """Convert all keys in a dictionary to strings."""
    return {str(k): v for k, v in d.items()}


def format_workspace(workspace: str) -> str:
    """Format workspace name to use in collection names.

    Replaces hyphens with underscores and capitalizes the name.
    """
    workspace_formatted = workspace.replace("-", "_").capitalize()
    return workspace_formatted


def assert_valid_collection_name(collection_name: str) -> None:
    """Ensure collection name doesn't contain the workspace delimiter."""
    assert (
        COLLECTION_DELIMITER not in collection_name
    ), f"Collection name should not contain '{COLLECTION_DELIMITER}'"


def get_full_collection_name(short_name: str) -> str:
    """Create a full collection name with workspace prefix for a single collection."""
    assert_valid_collection_name(short_name)

    workspace_formatted = format_workspace(SHARED_WORKSPACE)
    return f"{workspace_formatted}{COLLECTION_DELIMITER}{short_name}"


def create_partial_with_schema(func, **kwargs):
    """Create a partial function while preserving the __schema__ attribute."""

    # Check if the function is async
    if asyncio.iscoroutinefunction(func):
        # Create async wrapper for async functions
        @wraps(func)
        async def async_wrapper(*args, **wrapper_kwargs):
            # Merge kwargs, giving priority to the original kwargs
            merged_kwargs = {**wrapper_kwargs, **kwargs}
            return await func(*args, **merged_kwargs)

        # Copy the schema if it exists
        if hasattr(func, "__schema__"):
            setattr(async_wrapper, "__schema__", func.__schema__)

        return async_wrapper

    # Create sync wrapper for sync functions
    @wraps(func)
    def sync_wrapper(*args, **wrapper_kwargs):
        # Merge kwargs, giving priority to the original kwargs
        merged_kwargs = {**wrapper_kwargs, **kwargs}
        return func(*args, **merged_kwargs)

    # Copy the schema if it exists
    if hasattr(func, "__schema__"):
        setattr(sync_wrapper, "__schema__", func.__schema__)

    return sync_wrapper
