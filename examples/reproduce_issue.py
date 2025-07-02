#!/usr/bin/env python3
"""
Recreate the hypha-RPC partial function issue locally without running a server.

This reproduces the exact error that happens in remote service calls.
"""

import asyncio
import inspect
from functools import partial


async def test_issue_reproduction():
    """Reproduce the exact issue that happens with partial functions in hypha-RPC."""
    print("🔍 Reproducing hypha-RPC partial function issue locally")

    # Step 1: Create a function with schema (like our bioimage query function)
    from hypha_rpc.utils.schema import schema_function

    @schema_function
    async def mock_query(
        client,
        context: dict,
        query_text: str = "default_query",
        entity_types: list | None = None,
        limit: int = 10,
    ):
        """Mock query function that simulates the bioimage query."""
        return f"Query: {query_text}, types: {entity_types}, limit: {limit}"

    print(f"✅ Created mock function with schema")
    print(f"   Original signature: {inspect.signature(mock_query)}")
    print(f"   Has __schema__: {hasattr(mock_query, '__schema__')}")

    # Step 2: Create partial function (this is what the service registration does)
    partial_query = partial(mock_query, client="mock_client")

    print(f"\n❌ Created partial function - this loses schema!")
    print(f"   Partial signature: {inspect.signature(partial_query)}")
    print(f"   Has __schema__: {hasattr(partial_query, '__schema__')}")

    # Step 3: Simulate what hypha-RPC does when processing function calls
    print(f"\n🔍 Simulating hypha-RPC argument processing...")

    # This simulates the remote call arguments
    args = ()
    kwargs = {
        "context": {"user": {"scope": {"current_workspace": "test"}}},
        "query_text": "microscopy",
        "limit": 5,
    }

    print(f"   Simulated call args: {args}")
    print(f"   Simulated call kwargs: {list(kwargs.keys())}")

    # Step 4: Test hypha-RPC's fill_missing_args_and_kwargs function
    from hypha_rpc.utils.schema import fill_missing_args_and_kwargs

    try:
        # This is where the error occurs - hypha-RPC uses the original function signature
        # but tries to call the partial function
        original_sig = inspect.signature(mock_query)  # This is what hypha-RPC sees

        print(f"   hypha-RPC uses original signature: {original_sig}")

        # Process arguments using original signature
        new_args, new_kwargs = fill_missing_args_and_kwargs(original_sig, args, kwargs)

        print(f"   Processed args: {new_args}")
        print(f"   Processed kwargs: {new_kwargs}")

        # Now try to call the partial function with processed arguments
        print(f"\n🚨 Attempting to call partial function with processed args...")
        result = await partial_query(*new_args, **new_kwargs)

        print(f"❌ Unexpected success: {result}")

    except Exception as e:
        print(f"✅ Reproduced the error: {type(e).__name__}: {e}")
        if "multiple values for argument" in str(e):
            print(f"   🎯 This is the exact error we see in remote services!")
        return True

    return False


async def test_solution_approach():
    """Test different approaches to solving the issue."""
    print(f"\n🛠️  Testing solution approaches...")

    # Approach: Create wrapper that preserves schema and fixes signature
    from hypha_rpc.utils.schema import schema_function

    @schema_function
    async def mock_query(
        client, context: dict, query_text: str = "default", limit: int = 10
    ):
        return f"client={client}, query={query_text}, limit={limit}"

    def create_proper_partial(func, **pre_filled):
        """Create a partial function that preserves schema and has correct signature."""
        import copy
        from functools import wraps

        # Get original signature and create new one without pre-filled params
        original_sig = inspect.signature(func)
        new_params = [
            param
            for name, param in original_sig.parameters.items()
            if name not in pre_filled
        ]
        new_sig = inspect.Signature(new_params)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Merge pre-filled args with call-time args
            merged_kwargs = {**pre_filled, **kwargs}
            return await func(*args, **merged_kwargs)

        # Set correct signature and preserve schema
        wrapper.__signature__ = new_sig
        if hasattr(func, "__schema__"):
            # Update schema to remove pre-filled parameters
            schema = copy.deepcopy(func.__schema__)
            if "parameters" in schema:
                params = schema["parameters"]
                if "properties" in params:
                    for param_name in pre_filled:
                        params["properties"].pop(param_name, None)
                if "required" in params:
                    params["required"] = [
                        p for p in params["required"] if p not in pre_filled
                    ]
            wrapper.__schema__ = schema

        return wrapper

    # Test the solution
    proper_partial = create_proper_partial(mock_query, client="test_client")

    print(f"✅ Created proper partial wrapper")
    print(f"   Wrapper signature: {inspect.signature(proper_partial)}")
    print(f"   Has __schema__: {hasattr(proper_partial, '__schema__')}")

    # Test with hypha-RPC processing
    args = ()
    kwargs = {"context": {"test": "data"}, "query_text": "test"}

    from hypha_rpc.utils.schema import fill_missing_args_and_kwargs

    try:
        # Use wrapper's signature (this should work correctly)
        wrapper_sig = inspect.signature(proper_partial)
        new_args, new_kwargs = fill_missing_args_and_kwargs(wrapper_sig, args, kwargs)

        result = await proper_partial(*new_args, **new_kwargs)
        print(f"✅ Solution works: {result}")
        return True

    except Exception as e:
        print(f"❌ Solution failed: {e}")
        return False


if __name__ == "__main__":
    print("🧪 Testing hypha-RPC partial function issue reproduction\n")

    # Test issue reproduction
    issue_reproduced = asyncio.run(test_issue_reproduction())

    if issue_reproduced:
        print(f"\n✅ Successfully reproduced the issue locally!")

        # Test solution
        solution_works = asyncio.run(test_solution_approach())

        if solution_works:
            print(f"\n✅ Solution approach works!")
        else:
            print(f"\n❌ Solution needs refinement")
    else:
        print(f"\n❌ Could not reproduce the issue locally")
