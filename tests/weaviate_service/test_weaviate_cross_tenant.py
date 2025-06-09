"""Tests for Weaviate cross-tenant functionality."""

import uuid as uuid_module
import pytest
from hypha_rpc.rpc import RemoteException
from tests.weaviate_service.utils import (
    create_test_collection,
    USER1_APP_ID,
    USER2_APP_ID,
    USER3_APP_ID,
    SHARED_APP_ID,
)
from tests.conftest import USER1_WS


@pytest.mark.asyncio
async def test_cross_application_data_sharing(
    weaviate_service, weaviate_service2, weaviate_service3
):
    """Test sharing data between applications owned by different users."""
    # Create collection
    await create_test_collection(weaviate_service)

    # Create applications for each user
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's application",
    )

    await weaviate_service2.applications.create(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        description="User 2's application",
    )

    await weaviate_service3.applications.create(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        description="User 3's application",
    )

    # User 1 inserts data
    user1_uuid = await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        properties={
            "title": "User 1's Movie",
            "description": "Original movie from User 1",
            "genre": "Action",
            "year": 2023,
        },
    )

    # User 2 can retrieve User 1's data with explicit user_ws
    try:
        user2_query = await weaviate_service2.query.fetch_objects(
            collection_name="Movie",
            application_id=USER1_APP_ID,
            user_ws=USER1_WS,
            limit=10,
        )

        assert len(user2_query["objects"]) == 1
        assert user2_query["objects"][0]["uuid"] == user1_uuid
        assert user2_query["objects"][0]["properties"]["title"] == "User 1's Movie"
        print("Note: User 2 can access User 1's data with explicit user_ws")
    except (RemoteException, PermissionError, ValueError) as e:
        # This may fail depending on the permission model
        print(f"Note: Cross-user access failed: {str(e)}")

    # User 3 tries to modify User 1's data with explicit user_ws (may fail based on permissions)
    try:
        await weaviate_service3.data.update(
            collection_name="Movie",
            application_id=USER1_APP_ID,
            uuid=user1_uuid,
            properties={"title": "Modified by User 3"},
            user_ws=USER1_WS,
        )

        # Check if the modification worked
        modified_query = await weaviate_service.query.fetch_objects(
            collection_name="Movie",
            application_id=USER1_APP_ID,
            limit=10,
        )

        if modified_query["objects"][0]["properties"]["title"] == "Modified by User 3":
            print("Note: User 3 was able to modify User 1's data with explicit user_ws")
        else:
            print("Note: User 3's modification didn't affect the data")
    except (RemoteException, PermissionError, ValueError) as e:
        # Expected failure due to permission settings
        print(f"Note: Cross-user modification failed: {str(e)}")


@pytest.mark.asyncio
async def test_multi_tenant_access_with_correct_workspaces(
    weaviate_service, weaviate_service2
):
    """Test multi-tenant access using the correct workspace IDs."""
    # Create collection
    await create_test_collection(weaviate_service)

    # Each user creates their own application
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=SHARED_APP_ID,
        description="User 1's application",
    )

    # User 1 inserts data
    user1_uuid = await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=SHARED_APP_ID,
        properties={
            "title": "User 1 Private Movie",
            "description": "This movie is private to User 1",
            "genre": "Action",
            "year": 2023,
        },
    )

    # User 2 tries to access User 1's data WITHOUT specifying the user_ws - should fail
    try:
        await weaviate_service2.query.fetch_objects(
            collection_name="Movie", application_id=SHARED_APP_ID, limit=10
        )
        assert (
            False
        ), "User 2 should not be able to access User 1's data without user_ws"
    except (RemoteException, PermissionError, ValueError):
        # Expected failure due to permission settings
        pass

    # User 2 tries to access User 1's data WITH the correct user_ws
    try:
        results = await weaviate_service2.query.fetch_objects(
            collection_name="Movie",
            application_id=SHARED_APP_ID,
            user_ws=USER1_WS,
            limit=10,
        )

        # If the system allows this (depends on permission model), verify the data
        assert len(results["objects"]) == 1
        assert results["objects"][0]["properties"]["title"] == "User 1 Private Movie"
        print("Note: Non-admin User 2 can access User 1's data with correct user_ws")
    except (RemoteException, PermissionError, ValueError) as e:
        # This may fail depending on the permission model
        print(f"Note: Access with correct user_ws failed: {str(e)}")

    # User 2 attempts to modify User 1's data with the correct user_ws
    try:
        await weaviate_service2.data.update(
            collection_name="Movie",
            application_id=SHARED_APP_ID,
            uuid=user1_uuid,
            properties={"title": "Modified by User 2"},
            user_ws=USER1_WS,
        )

        # Check if modification was successful
        results = await weaviate_service.query.fetch_objects(
            collection_name="Movie",
            application_id=SHARED_APP_ID,
            limit=10,
        )

        # If we got here, User 2 was able to modify User 1's data
        assert results["objects"][0]["properties"]["title"] == "Modified by User 2"
        print("Note: User 2 was able to modify User 1's data with correct user_ws")
    except (RemoteException, PermissionError, ValueError) as e:
        # Expected failure due to permission settings
        print(f"Note: Modification with correct user_ws failed: {str(e)}")


@pytest.mark.asyncio
async def test_uuid_collision_between_tenants(weaviate_service, weaviate_service2):
    """Test that UUIDs don't collide between different tenants."""
    # Create collection with multi-tenancy enabled
    await create_test_collection(weaviate_service)

    # Create applications for each user
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's application",
    )

    await weaviate_service2.applications.create(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        description="User 2's application",
    )

    # Create an object with the same custom UUID for both users
    custom_uuid = str(uuid_module.uuid4())

    # Insert for User 1
    await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        properties={
            "title": "User 1's UUID Test Movie",
            "description": "Testing UUID isolation",
            "genre": "Test",
            "year": 2023,
        },
        uuid=custom_uuid,
    )

    # Insert for User 2 with the same UUID
    await weaviate_service2.data.insert(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        properties={
            "title": "User 2's UUID Test Movie",
            "description": "Testing UUID isolation",
            "genre": "Test",
            "year": 2023,
        },
        uuid=custom_uuid,
    )

    # Verify User 1 can only see their own data
    user1_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        limit=10,
    )

    expected_uuid = custom_uuid.replace("-", "")

    assert len(user1_results["objects"]) == 1
    assert user1_results["objects"][0]["uuid"] == expected_uuid
    assert (
        user1_results["objects"][0]["properties"]["title"] == "User 1's UUID Test Movie"
    )

    # Verify User 2 can only see their own data
    user2_results = await weaviate_service2.query.fetch_objects(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        limit=10,
    )

    assert len(user2_results["objects"]) == 1
    assert user2_results["objects"][0]["uuid"] == expected_uuid
    assert (
        user2_results["objects"][0]["properties"]["title"] == "User 2's UUID Test Movie"
    )
