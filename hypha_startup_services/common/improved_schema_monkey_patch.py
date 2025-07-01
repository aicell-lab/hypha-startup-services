"""
Improved Monkey patch for hypha-RPC schema.py to fix partial function handling.

This patch ensures that when hypha-RPC processes partial functions,
it uses the correct signature for argument binding.
"""

import inspect
from functools import partial
from inspect import Signature


def improved_patched_fill_missing_args_and_kwargs(original_func_sig, args, kwargs):
    """
    Improved patched version of fill_missing_args_and_kwargs that handles partial functions correctly.

    The original function tries to bind arguments using the original function signature,
    but for partial functions, we need to use a signature that excludes pre-filled parameters.
    """
    # Import the original function to get Field and FieldInfo handling
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

    # More robust detection of partial functions by inspecting the call stack
    original_func = None
    frame = inspect.currentframe()
    try:
        # Look through the call stack to find the original function
        caller_frame = frame.f_back
        search_depth = 0
        max_depth = 10  # Limit search depth to avoid infinite loops

        while caller_frame and search_depth < max_depth:
            local_vars = caller_frame.f_locals

            # Look for various possible variable names that might contain the original function
            for var_name in ["original_func", "func", "wrapped_func", "target_func"]:
                if var_name in local_vars:
                    func_candidate = local_vars[var_name]
                    if isinstance(func_candidate, partial):
                        original_func = func_candidate
                        break

            if original_func:
                break

            caller_frame = caller_frame.f_back
            search_depth += 1

    except Exception:
        # If stack inspection fails, original_func remains None
        pass
    finally:
        del frame

    # Alternative approach: look for signs that this is a partial function call
    # by checking if any parameters in kwargs match what would be pre-filled
    if not original_func:
        # Try to detect if there are parameter conflicts indicating a partial function
        try:
            # This will fail if there are pre-filled parameters causing conflicts
            test_bind = original_func_sig.bind_partial(*args, **kwargs)
        except TypeError as e:
            if "multiple values for argument" in str(e):
                # This is likely a partial function with pre-filled parameters
                # Try to identify which parameters are causing conflicts
                error_msg = str(e)
                if "multiple values for argument 'client'" in error_msg:
                    # Remove client from signature and try again
                    effective_sig = Signature(
                        [
                            param
                            for name, param in original_func_sig.parameters.items()
                            if name != "client"
                        ]
                    )
                    try:
                        bound_args = effective_sig.bind_partial(*args, **kwargs)
                    except Exception:
                        # If this still fails, fall back to original
                        bound_args = original_func_sig.bind_partial(*args, **kwargs)
                else:
                    # Re-raise the original error
                    raise
            else:
                # Re-raise the original error
                raise
        else:
            # No conflict, use the original binding
            bound_args = test_bind
    else:
        # We found a partial function, handle it specially
        skip_args = set(original_func.keywords.keys())
        effective_sig = Signature(
            [
                param
                for name, param in original_func_sig.parameters.items()
                if name not in skip_args
            ]
        )
        try:
            bound_args = effective_sig.bind_partial(*args, **kwargs)
        except TypeError:
            # If effective signature binding fails, try original signature
            bound_args = original_func_sig.bind_partial(*args, **kwargs)

    # Handle missing arguments the same way as the original function
    for name, param in original_func_sig.parameters.items():
        if name not in kwargs and name not in bound_args.arguments:
            if Field and isinstance(param.default, Field):
                # For Field objects, extract the actual default value
                if (
                    param.default.default is not Ellipsis
                    and param.default.default != inspect._empty
                ):
                    bound_args.arguments[name] = param.default.default
            elif (
                PYDANTIC_AVAILABLE
                and FieldInfo
                and isinstance(param.default, FieldInfo)
            ):
                # For FieldInfo objects, extract the actual default value
                if (
                    param.default.default is not PydanticUndefined
                    and param.default.default is not Ellipsis
                    and param.default.default != inspect._empty
                ):
                    bound_args.arguments[name] = param.default.default

    bound_args.apply_defaults()
    return bound_args.args, bound_args.kwargs


def apply_improved_schema_monkey_patch():
    """Apply the improved monkey patch to hypha-RPC's schema module."""
    try:
        import hypha_rpc.utils.schema as schema_module

        # Store the original function in case we need to revert
        if not hasattr(schema_module, "_original_fill_missing_args_and_kwargs"):
            schema_module._original_fill_missing_args_and_kwargs = (
                schema_module.fill_missing_args_and_kwargs
            )

        # Apply the improved patch
        schema_module.fill_missing_args_and_kwargs = (
            improved_patched_fill_missing_args_and_kwargs
        )

        print(
            "Applied improved hypha-RPC schema monkey patch for partial function support"
        )
        return True

    except ImportError:
        print(
            "Warning: Could not apply improved hypha-RPC schema monkey patch - hypha_rpc not available"
        )
        return False
    except Exception as e:
        print(f"Warning: Failed to apply improved hypha-RPC schema monkey patch: {e}")
        return False
