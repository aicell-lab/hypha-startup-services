"""Common utility functions shared between services."""

import asyncio
import copy
import inspect
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
    """Create a partial function while preserving and updating the __schema__ attribute."""
    # Get the original function signature
    original_sig = inspect.signature(func)

    # Create new parameters list without pre-filled ones
    new_params = []
    for name, param in original_sig.parameters.items():
        if name not in kwargs:
            new_params.append(param)

    # Create new signature
    new_signature = inspect.Signature(new_params)

    # Check if the function is async
    if asyncio.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapper(*args, **wrapper_kwargs):
            # Get the underlying function if it's wrapped by schema_function
            underlying_func = getattr(func, "__original__", func)

            # Bind the wrapper arguments to parameter names
            wrapper_bound = new_signature.bind(*args, **wrapper_kwargs)
            wrapper_bound.apply_defaults()

            # Create final kwargs by merging wrapper args with pre-filled
            final_kwargs = {**kwargs, **wrapper_bound.arguments}

            # Call underlying function with keyword arguments only
            return await underlying_func(**final_kwargs)

        wrapper = async_wrapper
    else:

        @wraps(func)
        def sync_wrapper(*args, **wrapper_kwargs):
            # Get the underlying function if it's wrapped by schema_function
            underlying_func = getattr(func, "__original__", func)

            # Bind the wrapper arguments to parameter names
            wrapper_bound = new_signature.bind(*args, **wrapper_kwargs)
            wrapper_bound.apply_defaults()

            # Create final kwargs by merging wrapper args with pre-filled
            final_kwargs = {**kwargs, **wrapper_bound.arguments}

            # Call underlying function with keyword arguments only
            return underlying_func(**final_kwargs)

        wrapper = sync_wrapper

    # Set the correct signature
    setattr(wrapper, "__signature__", new_signature)

    # Handle schema if it exists
    if hasattr(func, "__schema__"):
        original_schema = func.__schema__
        updated_schema = copy.deepcopy(original_schema)

        # Remove pre-filled parameters from the schema
        if "parameters" in updated_schema:
            parameters = updated_schema["parameters"]
            if "properties" in parameters:
                # Remove properties for pre-filled parameters
                for param_name in kwargs:
                    if param_name in parameters["properties"]:
                        del parameters["properties"][param_name]

            if "required" in parameters:
                # Remove pre-filled parameters from required list
                parameters["required"] = [
                    param for param in parameters["required"] if param not in kwargs
                ]

        setattr(wrapper, "__schema__", updated_schema)

    return wrapper
