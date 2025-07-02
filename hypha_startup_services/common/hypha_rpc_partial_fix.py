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
    # Work with copies to avoid side effects
    effective_sig = original_func_sig
    working_kwargs = kwargs.copy()

    # Try to detect partial functions in the call stack
    try:
        frame = inspect.currentframe()
        if frame and frame.f_back:
            caller_frame = frame.f_back
            while caller_frame:
                local_vars = caller_frame.f_locals

                # Look for partial functions
                for var_name, var_value in local_vars.items():
                    if isinstance(var_value, partial) and hasattr(var_value, "func"):
                        underlying_sig = inspect.signature(var_value.func)
                        if underlying_sig == original_func_sig:
                            # Found matching partial function
                            effective_sig = inspect.signature(var_value)
                            pre_filled = set(var_value.keywords.keys())

                            # Remove pre-filled arguments from kwargs
                            working_kwargs = {
                                k: v
                                for k, v in working_kwargs.items()
                                if k not in pre_filled
                            }

                            # Debug output for remote troubleshooting
                            print(f"🔧 PARTIAL FIX: Detected partial {var_name}")
                            print(f"   Pre-filled: {pre_filled}")
                            print(f"   Original kwargs: {list(kwargs.keys())}")
                            print(f"   Working kwargs: {list(working_kwargs.keys())}")
                            print(f"   Effective sig: {effective_sig}")

                            break

                if effective_sig != original_func_sig:
                    break

                caller_frame = caller_frame.f_back
    except Exception as e:
        # If frame inspection fails, fall back to original behavior
        print(f"🔧 PARTIAL FIX: Frame inspection failed: {e}")
        pass
    finally:
        # Clean up frame references
        try:
            del frame
        except:
            pass

    # Import hypha-RPC dependencies
    try:
        from hypha_rpc.utils.schema import Field, PYDANTIC_AVAILABLE

        if PYDANTIC_AVAILABLE:
            from pydantic.fields import FieldInfo
            from pydantic_core import PydanticUndefined
        else:
            FieldInfo = None
            PydanticUndefined = None
    except ImportError:
        Field = None
        PYDANTIC_AVAILABLE = False
        FieldInfo = None
        PydanticUndefined = None

    # Bind arguments using the effective signature and working kwargs
    try:
        bound_args = effective_sig.bind_partial(*args, **working_kwargs)
        print(
            f"🔧 PARTIAL FIX: Binding succeeded with {len(args)} args and {len(working_kwargs)} kwargs"
        )
    except Exception as e:
        print(f"🔧 PARTIAL FIX: Binding failed: {e}")
        print(f"   Signature: {effective_sig}")
        print(f"   Args: {args}")
        print(f"   Working kwargs: {list(working_kwargs.keys())}")
        raise

    # Handle Field/FieldInfo defaults (same as original hypha-RPC logic)
    for name, param in effective_sig.parameters.items():
        if name not in working_kwargs and name not in bound_args.arguments:
            if Field and isinstance(param.default, Field):
                if (
                    hasattr(param.default, "default")
                    and param.default.default != inspect.Parameter.empty
                ):
                    bound_args.arguments[name] = param.default.default
            elif (
                PYDANTIC_AVAILABLE
                and FieldInfo
                and isinstance(param.default, FieldInfo)
            ):
                if (
                    hasattr(param.default, "default")
                    and param.default.default != PydanticUndefined
                    and param.default.default != inspect.Parameter.empty
                ):
                    bound_args.arguments[name] = param.default.default

    bound_args.apply_defaults()
    return bound_args.args, bound_args.kwargs

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
