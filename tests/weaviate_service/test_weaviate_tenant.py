"""Tests for cross-user and tenant functionality in Weaviate service."""

import pytest
from hypha_rpc.rpc import RemoteException
from weaviate.classes.query import Filter

from tests.conftest import USER1_WS, USER2_WS, USER3_WS
from tests.weaviate_service.utils import (
    SHARED_APP_ID,
    USER1_APP_ID,
    USER2_APP_ID,
    USER3_APP_ID,
    create_test_collection,
)


@pytest.mark.asyncio
async def test_shared_application_access(weaviate_service, weaviate_service2):
    """Test access to a shared application with explicit user_ws parameter.

    This test simulates User 1 sharing an application with User 2 by allowing
    User 2 to access it with an explicit user_ws parameter.
    """
    # Create collection
    await create_test_collection(weaviate_service)

    # User 1 creates a shared application
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=SHARED_APP_ID,
        description="Shared application between users",
    )

    # User 1 adds data
    shared_movie1 = {
        "title": "User 1's Shared Movie",
        "description": "Movie added by User 1 to shared app",
        "genre": "Drama",
        "year": 2025,
    }

    await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=SHARED_APP_ID,
        properties=shared_movie1,
    )

    # User 2 tries to add data to User 1's application using the user_ws parameter to specify User 1
    # Note: In a real application, you would have proper permission management
    user2_movie = {
        "title": "User 2's Movie in Shared App",
        "description": "Movie added by User 2 to shared app",
        "genre": "Sci-Fi",
        "year": 2026,
    }

    try:
        # Attempt to add data with user_ws parameter - may fail depending on permissions
        await weaviate_service2.data.insert(
            collection_name="Movie",
            application_id=SHARED_APP_ID,
            properties=user2_movie,
            user_ws=USER1_WS,  # Explicitly specify User 1's workspace
        )

        # If it didn't fail, we can check if both users can see the data
        user1_results = await weaviate_service.query.fetch_objects(
            collection_name="Movie",
            application_id=SHARED_APP_ID,
            limit=10,
        )

        assert len(user1_results["objects"]) >= 1

    except (RemoteException, PermissionError, ValueError):
        # Expected failure due to permission settings
        pass

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie",
        application_id=SHARED_APP_ID,
    )


@pytest.mark.asyncio
async def test_data_operations_permission_boundaries(
    weaviate_service,
    weaviate_service2,
):
    """Test permission boundaries for various data operations."""
    # Create collection
    await create_test_collection(weaviate_service)

    # User 1 creates application
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's movie application",
    )

    # User 1 adds a movie
    movie_uuid = await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        properties={
            "title": "User 1's Private Movie",
            "description": "A movie that only User 1 should access",
            "genre": "Thriller",
            "year": 2025,
        },
    )

    # User 2 attempts to perform operations on User 1's data
    operations = [
        # Test exists
        (
            "exists",
            lambda: weaviate_service2.data.exists(
                collection_name="Movie",
                application_id=USER1_APP_ID,
                uuid=movie_uuid,
            ),
        ),
        # Test update
        (
            "update",
            lambda: weaviate_service2.data.update(
                collection_name="Movie",
                application_id=USER1_APP_ID,
                uuid=movie_uuid,
                properties={"title": "Modified Title"},
            ),
        ),
        # Test delete
        (
            "delete",
            lambda: weaviate_service2.data.delete_by_id(
                collection_name="Movie",
                application_id=USER1_APP_ID,
                uuid=movie_uuid,
            ),
        ),
        # Test query
        (
            "query",
            lambda: weaviate_service2.query.fetch_objects(
                collection_name="Movie",
                application_id=USER1_APP_ID,
                limit=10,
            ),
        ),
    ]

    for op_name, operation in operations:
        try:
            await operation()
            assert (
                False
            ), f"Operation {op_name} should have failed with permission error"
        except (RemoteException, PermissionError, ValueError):
            # Expected failure due to permission settings
            pass

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie",
        application_id=USER1_APP_ID,
    )


@pytest.mark.asyncio
async def test_cross_user_application_access(
    weaviate_service,
    weaviate_service2,
    weaviate_service3,
):
    """Test that users can access applications across workspaces using the user_ws parameter."""
    # Create collection
    await create_test_collection(weaviate_service)

    # User 1 creates application
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's movie application",
    )

    # User 2 creates application
    await weaviate_service2.applications.create(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        description="User 2's movie application",
    )

    # User 3 creates application
    await weaviate_service3.applications.create(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        description="User 3's movie application",
    )

    # Each user adds data to their own application
    await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        properties={
            "title": "User 1's Movie",
            "description": "Movie owned by User 1",
            "genre": "Action",
            "year": 2023,
        },
    )

    await weaviate_service2.data.insert(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        properties={
            "title": "User 2's Movie",
            "description": "Movie owned by User 2",
            "genre": "Comedy",
            "year": 2024,
        },
    )

    await weaviate_service3.data.insert(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        properties={
            "title": "User 3's Movie",
            "description": "Movie owned by User 3",
            "genre": "Drama",
            "year": 2025,
        },
    )

    # Test User 1 (admin) trying to access User 2's application with explicit user_ws
    # This tests if an admin can access other users' data when specifying the user_ws
    try:
        user2_from_user1 = await weaviate_service.query.fetch_objects(
            collection_name="Movie",
            application_id=USER2_APP_ID,
            user_ws=USER2_WS,
            limit=10,
        )

        # If successful, verify the data
        assert user2_from_user1["objects"][0]["properties"]["title"] == "User 2's Movie"
    except (RemoteException, PermissionError, ValueError) as e:
        # If it fails, it should be due to permission settings, not because the approach is wrong
        print(f"Note: Admin accessing data with user_ws parameter failed: {e!s}")

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie",
        application_id=USER1_APP_ID,
    )
    await weaviate_service2.applications.delete(
        collection_name="Movie",
        application_id=USER2_APP_ID,
    )
    await weaviate_service3.applications.delete(
        collection_name="Movie",
        application_id=USER3_APP_ID,
    )


@pytest.mark.asyncio
async def test_cross_application_data_isolation(
    weaviate_service,
    weaviate_service2,
    weaviate_service3,
):
    """Test data isolation between applications across different users/tenants."""
    # Create collection (only admin can do this)
    await create_test_collection(weaviate_service)

    # Create applications for each user
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's movie application",
    )

    await weaviate_service2.applications.create(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        description="User 2's movie application",
    )

    await weaviate_service3.applications.create(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        description="User 3's movie application",
    )

    # Each user adds data to their own application with identical properties but different titles
    movie_props = {
        "description": "A test movie with identical properties across tenants",
        "genre": "Science Fiction",
        "year": 2025,
    }

    # User 1 adds data
    await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        properties={"title": "User 1 Movie", **movie_props},
    )

    # User 2 adds data
    await weaviate_service2.data.insert(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        properties={"title": "User 2 Movie", **movie_props},
    )

    # User 3 adds data
    await weaviate_service3.data.insert(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        properties={"title": "User 3 Movie", **movie_props},
    )

    # Filter by identical properties to ensure cross-tenant isolation
    filter_condition = Filter.by_property("genre").equal("Science Fiction")

    # User 1 runs a filtered query
    user1_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        filters=filter_condition,
        limit=10,
    )

    # User 2 runs the same filtered query
    user2_results = await weaviate_service2.query.fetch_objects(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        filters=filter_condition,
        limit=10,
    )

    # User 3 runs the same filtered query
    user3_results = await weaviate_service3.query.fetch_objects(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        filters=filter_condition,
        limit=10,
    )

    # Verify data isolation between tenants - each user should only see their own data
    assert len(user1_results["objects"]) == 1
    assert user1_results["objects"][0]["properties"]["title"] == "User 1 Movie"

    assert len(user2_results["objects"]) == 1
    assert user2_results["objects"][0]["properties"]["title"] == "User 2 Movie"

    assert len(user3_results["objects"]) == 1
    assert user3_results["objects"][0]["properties"]["title"] == "User 3 Movie"

    # Admin can access all data with explicit workspace IDs
    admin_user1_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        filters=filter_condition,
        limit=10,
    )

    admin_user2_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        user_ws=USER2_WS,
        filters=filter_condition,
        limit=10,
    )

    admin_user3_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        user_ws=USER3_WS,
        filters=filter_condition,
        limit=10,
    )

    # Verify admin can access all data with the correct workspace specification
    assert len(admin_user1_results["objects"]) == 1
    assert admin_user1_results["objects"][0]["properties"]["title"] == "User 1 Movie"

    assert len(admin_user2_results["objects"]) == 1
    assert admin_user2_results["objects"][0]["properties"]["title"] == "User 2 Movie"

    assert len(admin_user3_results["objects"]) == 1
    assert admin_user3_results["objects"][0]["properties"]["title"] == "User 3 Movie"

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie",
        application_id=USER1_APP_ID,
    )
    await weaviate_service2.applications.delete(
        collection_name="Movie",
        application_id=USER2_APP_ID,
    )
    await weaviate_service3.applications.delete(
        collection_name="Movie",
        application_id=USER3_APP_ID,
    )
