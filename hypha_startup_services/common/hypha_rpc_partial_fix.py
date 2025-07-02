"""
Minimal monkey patch for hypha-RPC to fix partial function handling.

This is the production-ready version that can be included in the service.
"""

import inspect
from functools import partial


def patched_fill_missing_args_and_kwargs(original_func_sig, args, kwargs):
    """
    Patched version that handles partial functions correctly.

    This function detects if the current call stack involves a partial function
    and adjusts the signature processing accordingly.
    """
    # Get the current frame to inspect the calling context
    frame = inspect.currentframe()
    effective_sig = original_func_sig

    try:
        # Look up the call stack to find if we're dealing with a partial function
        caller_frame = frame.f_back
        while caller_frame:
            local_vars = caller_frame.f_locals

            # Look for function references that might be partial functions
            for var_name, var_value in local_vars.items():
                if isinstance(var_value, partial):
                    # Found a partial function - check if it matches our call
                    if hasattr(var_value, "func"):
                        underlying_sig = inspect.signature(var_value.func)
                        if underlying_sig == original_func_sig:
                            # Use the partial's signature and remove pre-filled args from kwargs
                            effective_sig = inspect.signature(var_value)
                            pre_filled = set(var_value.keywords.keys())
                            kwargs = {
                                k: v for k, v in kwargs.items() if k not in pre_filled
                            }
                            break

            if effective_sig != original_func_sig:
                break

            caller_frame = caller_frame.f_back
    finally:
        del frame

    # Use the original hypha-RPC logic with the effective signature
    try:
        from hypha_rpc.utils.schema import Field, PYDANTIC_AVAILABLE

        if PYDANTIC_AVAILABLE:
            from pydantic.fields import FieldInfo
            from pydantic_core import PydanticUndefined
    except ImportError:
        Field = None
        PYDANTIC_AVAILABLE = False
        FieldInfo = None
        PydanticUndefined = None

    # Bind arguments using the effective signature
    bound_args = effective_sig.bind_partial(*args, **kwargs)

    # Handle Field defaults (same as original function)
    for name, param in effective_sig.parameters.items():
        if name not in kwargs and name not in bound_args.arguments:
            if (
                Field
                and isinstance(param.default, Field)
                or (
                    PYDANTIC_AVAILABLE
                    and FieldInfo
                    and isinstance(param.default, FieldInfo)
                )
            ):
                # Extract actual default value from Field/FieldInfo
                if Field and isinstance(param.default, Field):
                    if param.default.default != inspect._empty:
                        bound_args.arguments[name] = param.default.default
                elif (
                    PYDANTIC_AVAILABLE
                    and FieldInfo
                    and isinstance(param.default, FieldInfo)
                ):
                    if (
                        param.default.default != PydanticUndefined
                        and param.default.default != inspect._empty
                    ):
                        bound_args.arguments[name] = param.default.default

    bound_args.apply_defaults()
    return bound_args.args, bound_args.kwargs


def apply_hypha_rpc_partial_fix():
    """Apply the minimal monkey patch to fix partial function handling in hypha-RPC."""
    try:
        import hypha_rpc.utils.schema as schema_module

        # Store the original function
        if not hasattr(schema_module, "_original_fill_missing_args_and_kwargs"):
            setattr(
                schema_module,
                "_original_fill_missing_args_and_kwargs",
                schema_module.fill_missing_args_and_kwargs,
            )

        # Apply the patch
        schema_module.fill_missing_args_and_kwargs = (
            patched_fill_missing_args_and_kwargs
        )

        return True

    except ImportError:
        # hypha_rpc not available - this is fine for testing
        return False
    except Exception:
        # Other errors - don't crash the service
        return False


def revert_hypha_rpc_partial_fix():
    """Revert the monkey patch (for testing purposes)."""
    try:
        import hypha_rpc.utils.schema as schema_module

        if hasattr(schema_module, "_original_fill_missing_args_and_kwargs"):
            schema_module.fill_missing_args_and_kwargs = getattr(
                schema_module, "_original_fill_missing_args_and_kwargs"
            )
            delattr(schema_module, "_original_fill_missing_args_and_kwargs")
            return True
        return False

    except (ImportError, Exception):
        return False
