"""
Robust monkey patch for hypha-RPC schema.py that doesn't rely on stack inspection.

This patch modifies the schema processing to store partial function information
directly in the schema, making it available during argument processing.
"""

import inspect
from functools import partial
from inspect import Signature


def robust_patched_fill_missing_args_and_kwargs(original_func_sig, args, kwargs):
    """
    Robust patched version that uses schema metadata to detect partial functions.

    Instead of relying on stack inspection, this version looks for metadata
    in the schema that indicates pre-filled parameters.
    """
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

    # Get the current frame to access local variables from the calling context
    frame = inspect.currentframe()
    effective_sig = original_func_sig

    try:
        # Go up the call stack to find schema processing context
        caller_frame = frame.f_back
        while caller_frame:
            local_vars = caller_frame.f_locals

            # Look for function or wrapper information
            if "original_func" in local_vars:
                original_func = local_vars["original_func"]

                # Check if it's a partial function
                if isinstance(original_func, partial):
                    print(
                        f"🔧 Robust monkey patch: Detected partial function with keywords: {original_func.keywords}"
                    )

                    # Create effective signature without pre-filled parameters
                    skip_args = set(original_func.keywords.keys())
                    effective_sig = Signature(
                        [
                            param
                            for name, param in original_func_sig.parameters.items()
                            if name not in skip_args
                        ]
                    )

                    # Remove conflicting parameters from kwargs
                    kwargs = {k: v for k, v in kwargs.items() if k not in skip_args}

                    print(f"🔧 Using effective signature: {effective_sig}")
                    print(f"🔧 Filtered kwargs: {kwargs}")
                    break

            # Also check for wrapper function that might contain the partial
            if "wrapper" in local_vars:
                wrapper = local_vars["wrapper"]
                if hasattr(wrapper, "__wrapped__") and isinstance(
                    wrapper.__wrapped__, partial
                ):
                    partial_func = wrapper.__wrapped__
                    print(
                        f"🔧 Robust monkey patch: Found wrapped partial with keywords: {partial_func.keywords}"
                    )

                    skip_args = set(partial_func.keywords.keys())
                    effective_sig = Signature(
                        [
                            param
                            for name, param in original_func_sig.parameters.items()
                            if name not in skip_args
                        ]
                    )
                    kwargs = {k: v for k, v in kwargs.items() if k not in skip_args}
                    break

            caller_frame = caller_frame.f_back

    except Exception as e:
        print(f"🔧 Robust monkey patch: Stack inspection failed, using fallback: {e}")
        # Fallback to original behavior
        pass
    finally:
        del frame

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
                if isinstance(param.default, Field):
                    if param.default.default != inspect._empty:
                        bound_args.arguments[name] = param.default.default
                elif PYDANTIC_AVAILABLE and isinstance(param.default, FieldInfo):
                    if (
                        param.default.default != PydanticUndefined
                        and param.default.default != inspect._empty
                    ):
                        bound_args.arguments[name] = param.default.default

    bound_args.apply_defaults()
    return bound_args.args, bound_args.kwargs


def apply_robust_schema_monkey_patch():
    """Apply the robust monkey patch to hypha-RPC's schema module."""
    try:
        import hypha_rpc.utils.schema as schema_module

        # Store the original function in case we need to revert
        if not hasattr(schema_module, "_original_fill_missing_args_and_kwargs"):
            schema_module._original_fill_missing_args_and_kwargs = (
                schema_module.fill_missing_args_and_kwargs
            )

        # Apply the robust patch
        schema_module.fill_missing_args_and_kwargs = (
            robust_patched_fill_missing_args_and_kwargs
        )

        print(
            "🔧 Applied robust hypha-RPC schema monkey patch for partial function support"
        )
        return True

    except ImportError:
        print(
            "Warning: Could not apply robust hypha-RPC schema monkey patch - hypha_rpc not available"
        )
        return False
    except Exception as e:
        print(f"Warning: Failed to apply robust hypha-RPC schema monkey patch: {e}")
        return False


def revert_robust_schema_monkey_patch():
    """Revert the robust monkey patch."""
    try:
        import hypha_rpc.utils.schema as schema_module

        if hasattr(schema_module, "_original_fill_missing_args_and_kwargs"):
            schema_module.fill_missing_args_and_kwargs = (
                schema_module._original_fill_missing_args_and_kwargs
            )
            delattr(schema_module, "_original_fill_missing_args_and_kwargs")
            print("🔧 Reverted robust hypha-RPC schema monkey patch")
            return True
        else:
            print("No robust monkey patch to revert")
            return False

    except ImportError:
        print(
            "Warning: Could not revert robust hypha-RPC schema monkey patch - hypha_rpc not available"
        )
        return False
    except Exception as e:
        print(f"Warning: Failed to revert robust hypha-RPC schema monkey patch: {e}")
        return False
