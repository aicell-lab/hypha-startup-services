#!/usr/bin/env python3
"""
Reproduce the exact error that's happening in the remote service.
"""

import asyncio
import inspect
from functools import partial


async def reproduce_exact_remote_error():
    """Reproduce the exact error scenario happening in the remote service."""
    print("🔍 Reproducing the exact remote error scenario")

    # Apply our current monkey patch
    from hypha_startup_services.common.hypha_rpc_partial_fix import (
        apply_hypha_rpc_partial_fix,
    )

    apply_hypha_rpc_partial_fix()

    # Create the exact function structure that's failing
    from hypha_rpc.utils.schema import schema_function

    @schema_function
    async def mock_query(
        client,
        context: dict,
        query_text: str = "default",
        entity_types: list = None,
        limit: int = 10,
    ):
        """Mock the exact query function structure."""
        return f"Query: {query_text}, limit: {limit}"

    print(f"✅ Created mock function")
    print(f"   Signature: {inspect.signature(mock_query)}")

    # Create partial exactly like the service registration
    query_func = partial(mock_query, client="mock_client")

    print(f"✅ Created partial function")
    print(f"   Partial signature: {inspect.signature(query_func)}")

    # Simulate the exact call that's failing
    # This is what hypha-RPC passes when the remote service is called
    args = ()
    kwargs = {
        "query_text": "imaging",
        "limit": 3,
        "context": {"user": {"scope": {"current_workspace": "test"}}},
    }

    print(f"\n🔍 Simulating the exact failing call...")
    print(f"   Args: {args}")
    print(f"   Kwargs: {list(kwargs.keys())} = {kwargs}")

    # Test our monkey patch
    from hypha_rpc.utils.schema import fill_missing_args_and_kwargs

    try:
        original_sig = inspect.signature(mock_query)
        print(f"   Original signature: {original_sig}")

        # This is where the error occurs
        new_args, new_kwargs = fill_missing_args_and_kwargs(original_sig, args, kwargs)

        print(f"❌ Unexpected success - monkey patch worked!")
        print(f"   New args: {new_args}")
        print(f"   New kwargs: {new_kwargs}")

        # Try to call the partial function
        result = await query_func(*new_args, **new_kwargs)
        print(f"✅ Function call succeeded: {result}")
        return True

    except Exception as e:
        print(f"✅ Reproduced the exact error: {type(e).__name__}: {e}")
        print(
            f"   Error at line: {e.__traceback__.tb_lineno if e.__traceback__ else 'unknown'}"
        )

        # Let's debug what's happening in our monkey patch
        print(f"\n🔍 Debugging the monkey patch...")
        debug_monkey_patch_state(original_sig, args, kwargs)

        return False


def debug_monkey_patch_state(original_func_sig, args, kwargs):
    """Debug what's happening inside our monkey patch."""
    print(f"🔧 Debugging monkey patch state...")

    # Simulate what our monkey patch should be doing
    frame = inspect.currentframe()
    effective_sig = original_func_sig

    try:
        # Look up the call stack to find partial functions
        caller_frame = frame.f_back
        found_partial = False

        while caller_frame and not found_partial:
            local_vars = caller_frame.f_locals

            for var_name, var_value in local_vars.items():
                if isinstance(var_value, partial):
                    print(f"   Found partial function: {var_name}")
                    print(f"   Partial keywords: {var_value.keywords}")

                    if hasattr(var_value, "func"):
                        underlying_sig = inspect.signature(var_value.func)
                        print(f"   Underlying signature: {underlying_sig}")
                        print(f"   Original signature: {original_func_sig}")
                        print(
                            f"   Signatures match: {underlying_sig == original_func_sig}"
                        )

                        if underlying_sig == original_func_sig:
                            effective_sig = inspect.signature(var_value)
                            pre_filled = set(var_value.keywords.keys())
                            print(f"   Effective signature: {effective_sig}")
                            print(f"   Pre-filled args: {pre_filled}")

                            # This is the key fix - remove pre-filled args from kwargs
                            cleaned_kwargs = {
                                k: v for k, v in kwargs.items() if k not in pre_filled
                            }
                            print(f"   Original kwargs: {list(kwargs.keys())}")
                            print(f"   Cleaned kwargs: {list(cleaned_kwargs.keys())}")

                            found_partial = True
                            break

            caller_frame = caller_frame.f_back

        if not found_partial:
            print(f"   ❌ No matching partial function found in call stack")

    finally:
        del frame


if __name__ == "__main__":
    success = asyncio.run(reproduce_exact_remote_error())

    if success:
        print(f"\n🎉 Monkey patch is working correctly!")
    else:
        print(f"\n❌ Monkey patch needs to be fixed")
