"""Tests for Weaviate admin functionality."""

import pytest
from hypha_rpc.rpc import RemoteException
from tests.weaviate_test_utils import (
    create_test_collection,
    USER1_APP_ID,
    USER2_APP_ID,
)
from tests.conftest import USER2_WS


@pytest.mark.asyncio
async def test_admin_access_to_other_users_data(weaviate_service, weaviate_service2):
    """Test admin access to data owned by other users."""
    # Create collection
    await create_test_collection(weaviate_service)

    # Create separate applications for each user
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

    # User 2 adds private data
    test_object = {
        "title": "User 2's Private Movie",
        "description": "This movie belongs only to User 2",
        "genre": "Horror",
        "year": 2022,
    }

    user2_uuid = await weaviate_service2.data.insert(
        collection_name="Movie", application_id=USER2_APP_ID, properties=test_object
    )

    # Admin (User 1) should be able to access User 2's data with user_ws parameter
    admin_query = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        user_ws=USER2_WS,
        limit=10,
    )

    assert len(admin_query["objects"]) == 1
    assert admin_query["objects"][0]["uuid"] == user2_uuid
    assert admin_query["objects"][0]["properties"]["title"] == "User 2's Private Movie"

    # Admin should also be able to update User 2's data
    await weaviate_service.data.update(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        uuid=user2_uuid,
        properties={"title": "Modified by Admin"},
        user_ws=USER2_WS,
    )

    # Verify admin's update was applied
    admin_query = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        user_ws=USER2_WS,
        limit=10,
    )

    assert admin_query["objects"][0]["properties"]["title"] == "Modified by Admin"


@pytest.mark.asyncio
async def test_admin_only_collection_creation(weaviate_service, weaviate_service2):
    """Test that only admin users can create collections."""
    # Admin should be able to create collections
    collection = await create_test_collection(weaviate_service)
    assert isinstance(collection, dict)

    # Non-admin should not be able to create collections
    try:
        class_obj = {
            "class": "TestCollection2",
            "description": "A test collection created by non-admin",
            "properties": [
                {
                    "name": "name",
                    "dataType": ["text"],
                    "description": "The name of the test item",
                }
            ],
        }

        await weaviate_service2.collections.create(class_obj)
        assert False, "Non-admin user should not be able to create collections"
    except (RemoteException, PermissionError, ValueError) as e:
        # Expected error for non-admin user
        assert (
            "admin" in str(e).lower() or "permission" in str(e).lower()
        ), f"Error should mention admin permissions, but got: {str(e)}"


@pytest.mark.asyncio
async def test_admin_only_collection_deletion(weaviate_service, weaviate_service2):
    """Test that only admin users can delete collections."""
    # Create a collection with admin
    await create_test_collection(weaviate_service)

    # Non-admin should not be able to delete collections
    try:
        await weaviate_service2.collections.delete("Movie")
        assert False, "Non-admin user should not be able to delete collections"
    except (RemoteException, PermissionError, ValueError) as e:
        # Expected error for non-admin user
        assert (
            "admin" in str(e).lower() or "permission" in str(e).lower()
        ), f"Error should mention admin permissions, but got: {str(e)}"

    # Admin should be able to delete collections
    await weaviate_service.collections.delete("Movie")

    # Verify collection was deleted
    collections = await weaviate_service.collections.list_all()
    assert not any("Movie" in coll_name for coll_name in collections.keys())


@pytest.mark.asyncio
async def test_admin_only_collection_list(weaviate_service, weaviate_service2):
    """Test that only admin users can list all collections."""
    # Create two collections with admin
    await create_test_collection(weaviate_service)

    class_obj = {
        "class": "TestCollection2",
        "description": "Another test collection",
        "properties": [
            {
                "name": "name",
                "dataType": ["text"],
                "description": "The name of the test item",
            }
        ],
    }

    await weaviate_service.collections.create(class_obj)

    # Admin should be able to list all collections
    collections = await weaviate_service.collections.list_all()
    assert len(collections) >= 2
    assert any(coll_name == "Movie" for coll_name in collections.keys())
    assert any(coll_name == "TestCollection2" for coll_name in collections.keys())

    # Non-admin should not be able to list all collections
    try:
        await weaviate_service2.collections.list_all()
        assert False, "Non-admin user should not be able to list all collections"
    except (RemoteException, PermissionError, ValueError) as e:
        # Expected error for non-admin user
        assert (
            "admin" in str(e).lower() or "permission" in str(e).lower()
        ), f"Error should mention admin permissions, but got: {str(e)}"

    # Clean up
    await weaviate_service.collections.delete("Movie")
    await weaviate_service.collections.delete("TestCollection2")
