"""
Monkey patch for hypha-RPC schema.py to fix partial function handling.

This patch ensures that when hypha-RPC processes partial functions,
it uses the correct signature for argument binding.
"""

import inspect
from functools import partial
from inspect import Signature


def patched_fill_missing_args_and_kwargs(original_func_sig, args, kwargs):
    """
    Patched version of fill_missing_args_and_kwargs that handles partial functions correctly.

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

    # Check if we're dealing with a partial function by looking at the calling context
    # We need to inspect the call stack to find the original function
    frame = inspect.currentframe()
    try:
        # Go up the call stack to find the wrapper function
        caller_frame = frame.f_back
        while caller_frame:
            local_vars = caller_frame.f_locals
            if "original_func" in local_vars:
                original_func = local_vars["original_func"]
                if isinstance(original_func, partial):
                    # For partial functions, create a signature that excludes pre-filled parameters
                    skip_args = set(original_func.keywords.keys())
                    effective_sig = Signature(
                        [
                            param
                            for name, param in original_func_sig.parameters.items()
                            if name not in skip_args
                        ]
                    )
                    bound_args = effective_sig.bind_partial(*args, **kwargs)
                    break
                else:
                    bound_args = original_func_sig.bind_partial(*args, **kwargs)
                    break
            caller_frame = caller_frame.f_back
        else:
            # Fallback to original behavior
            bound_args = original_func_sig.bind_partial(*args, **kwargs)
    except Exception:
        # If anything goes wrong, fallback to original behavior
        bound_args = original_func_sig.bind_partial(*args, **kwargs)
    finally:
        del frame

    # Handle missing arguments the same way as the original function
    for name, param in original_func_sig.parameters.items():
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
                # Extract the actual default value from Field/FieldInfo objects
                bound_args.arguments[name] = param.default.default
            else:
                # Use the parameter default directly
                bound_args.arguments[name] = param.default

    bound_args.apply_defaults()
    return bound_args.args, bound_args.kwargs


def apply_schema_monkey_patch():
    """Apply the monkey patch to hypha-RPC's schema module."""
    try:
        import hypha_rpc.utils.schema as schema_module

        # Store the original function in case we need to revert
        if not hasattr(schema_module, "_original_fill_missing_args_and_kwargs"):
            schema_module._original_fill_missing_args_and_kwargs = (
                schema_module.fill_missing_args_and_kwargs
            )

        # Apply the patch
        schema_module.fill_missing_args_and_kwargs = (
            patched_fill_missing_args_and_kwargs
        )

        print("Applied hypha-RPC schema monkey patch for partial function support")
        return True

    except ImportError:
        print(
            "Warning: Could not apply hypha-RPC schema monkey patch - hypha_rpc not available"
        )
        return False
    except Exception as e:
        print(f"Warning: Failed to apply hypha-RPC schema monkey patch: {e}")
        return False


def revert_schema_monkey_patch():
    """Revert the monkey patch."""
    try:
        import hypha_rpc.utils.schema as schema_module

        if hasattr(schema_module, "_original_fill_missing_args_and_kwargs"):
            schema_module.fill_missing_args_and_kwargs = (
                schema_module._original_fill_missing_args_and_kwargs
            )
            delattr(schema_module, "_original_fill_missing_args_and_kwargs")
            print("Reverted hypha-RPC schema monkey patch")
            return True
        else:
            print("No monkey patch to revert")
            return False

    except ImportError:
        print(
            "Warning: Could not revert hypha-RPC schema monkey patch - hypha_rpc not available"
        )
        return False
    except Exception as e:
        print(f"Warning: Failed to revert hypha-RPC schema monkey patch: {e}")
        return False
