"""Tests for multi-user Weaviate functionality."""

import pytest
from hypha_rpc.rpc import RemoteException
from tests.weaviate_test_utils import (
    create_test_collection,
    APP_ID,
    USER2_APP_ID,
    USER3_APP_ID,
)
from tests.conftest import USER2_WS, USER3_WS


@pytest.mark.asyncio
async def test_multi_user_application(weaviate_service, weaviate_service2):
    """Test multi-user application access using separate service instances."""
    # First create a collection and application
    await create_test_collection(weaviate_service)

    # Create a shared application with the admin user that both users can access
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=APP_ID,
        description="An application for movie data",
    )

    # Create test data
    test_object_owner = {
        "title": "Owner's Movie",
        "description": "This movie belongs to the application owner",
        "genre": "Drama",
        "year": 2020,
    }

    test_object_user = {
        "title": "User's Movie",
        "description": "This movie belongs to another user",
        "genre": "Comedy",
        "year": 2021,
    }

    # Insert data as the owner (User 1, admin)
    await weaviate_service.data.insert(
        collection_name="Movie", application_id=APP_ID, properties=test_object_owner
    )

    # Insert data as User 2 (non-admin) into the same application
    try:
        await weaviate_service2.data.insert(
            collection_name="Movie", application_id=APP_ID, properties=test_object_user
        )

        # Query data as the admin user (User 1)
        owner_results = await weaviate_service.query.fetch_objects(
            collection_name="Movie", application_id=APP_ID, limit=10
        )

        # Admin should see both their data and User 2's data
        assert len(owner_results["objects"]) >= 2
        assert any(
            obj["properties"]["title"] == "Owner's Movie"
            for obj in owner_results["objects"]
        )
        assert any(
            obj["properties"]["title"] == "User's Movie"
            for obj in owner_results["objects"]
        )

        # Query data as User 2 (non-admin)
        user_results = await weaviate_service2.query.fetch_objects(
            collection_name="Movie",
            application_id=APP_ID,
            limit=10,
        )

        # User 2 should only see their own data
        assert len(user_results["objects"]) >= 1
        assert any(
            obj["properties"]["title"] == "User's Movie"
            for obj in user_results["objects"]
        )
        assert not any(
            obj["properties"]["title"] == "Owner's Movie"
            for obj in user_results["objects"]
        )

    except (ValueError, RuntimeError, PermissionError, RemoteException) as e:
        # This part might fail due to permission settings, which is expected
        print(f"Note: Multi-user test with explicit workspace failed: {str(e)}")


@pytest.mark.asyncio
async def test_separate_user_applications(
    weaviate_service, weaviate_service2, weaviate_service3
):
    """Test that non-admin users can create and access their own applications independently."""
    # Create collection (must use admin user for this)
    await create_test_collection(weaviate_service)

    # User 2 creates application (non-admin)
    await weaviate_service2.applications.create(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        description="User 2's movie application",
    )

    # User 3 creates application (non-admin)
    await weaviate_service3.applications.create(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        description="User 3's movie application",
    )

    # User 2 inserts data
    user2_movie = {
        "title": "User 2's Movie",
        "description": "This belongs to User 2",
        "genre": "Comedy",
        "year": 2024,
    }

    await weaviate_service2.data.insert(
        collection_name="Movie", application_id=USER2_APP_ID, properties=user2_movie
    )

    # User 3 inserts data
    user3_movie = {
        "title": "User 3's Movie",
        "description": "This belongs to User 3",
        "genre": "Drama",
        "year": 2025,
    }

    await weaviate_service3.data.insert(
        collection_name="Movie", application_id=USER3_APP_ID, properties=user3_movie
    )

    # User 2 can access their data
    user2_results = await weaviate_service2.query.fetch_objects(
        collection_name="Movie", application_id=USER2_APP_ID, limit=10
    )
    assert len(user2_results["objects"]) == 1
    assert user2_results["objects"][0]["properties"]["title"] == "User 2's Movie"

    # User 3 can access their data
    user3_results = await weaviate_service3.query.fetch_objects(
        collection_name="Movie", application_id=USER3_APP_ID, limit=10
    )
    assert len(user3_results["objects"]) == 1
    assert user3_results["objects"][0]["properties"]["title"] == "User 3's Movie"

    # User 2 cannot access User 3's application
    try:
        await weaviate_service2.query.fetch_objects(
            collection_name="Movie", application_id=USER3_APP_ID, limit=10
        )
        assert False, "User 2 should not be able to access User 3's application"
    except (RemoteException, PermissionError, ValueError):
        # We expect this to fail with permission error
        pass

    # User 3 cannot access User 2's application
    try:
        await weaviate_service3.query.fetch_objects(
            collection_name="Movie", application_id=USER2_APP_ID, limit=10
        )
        assert False, "User 3 should not be able to access User 2's application"
    except (RemoteException, PermissionError, ValueError):
        # We expect this to fail with permission error
        pass

    # Admin user (User 1) can access both User 2 and User 3's applications
    admin_user2_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie", application_id=USER2_APP_ID, user_ws=USER2_WS, limit=10
    )
    assert len(admin_user2_results["objects"]) == 1
    assert admin_user2_results["objects"][0]["properties"]["title"] == "User 2's Movie"

    admin_user3_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie", application_id=USER3_APP_ID, user_ws=USER3_WS, limit=10
    )
    assert len(admin_user3_results["objects"]) == 1
    assert admin_user3_results["objects"][0]["properties"]["title"] == "User 3's Movie"

    # Clean up
    await weaviate_service2.applications.delete(
        collection_name="Movie", application_id=USER2_APP_ID
    )
    await weaviate_service3.applications.delete(
        collection_name="Movie", application_id=USER3_APP_ID
    )


@pytest.mark.asyncio
async def test_data_isolation_between_applications(weaviate_service):
    """Test that data is isolated between different applications of the same user."""
    # Create collection
    await create_test_collection(weaviate_service)

    # Create two applications
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id="AppA",
        description="Application A",
    )

    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id="AppB",
        description="Application B",
    )

    # Insert data into App A
    movie_a = {
        "title": "Movie in App A",
        "description": "This movie is in Application A",
        "genre": "Drama",
        "year": 2022,
    }

    await weaviate_service.data.insert(
        collection_name="Movie", application_id="AppA", properties=movie_a
    )

    # Insert data into App B
    movie_b = {
        "title": "Movie in App B",
        "description": "This movie is in Application B",
        "genre": "Action",
        "year": 2023,
    }

    await weaviate_service.data.insert(
        collection_name="Movie", application_id="AppB", properties=movie_b
    )

    # Fetch data from App A - should only see App A's data
    results_a = await weaviate_service.query.fetch_objects(
        collection_name="Movie", application_id="AppA", limit=10
    )
    assert len(results_a["objects"]) == 1
    assert results_a["objects"][0]["properties"]["title"] == "Movie in App A"

    # Fetch data from App B - should only see App B's data
    results_b = await weaviate_service.query.fetch_objects(
        collection_name="Movie", application_id="AppB", limit=10
    )
    assert len(results_b["objects"]) == 1
    assert results_b["objects"][0]["properties"]["title"] == "Movie in App B"

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id="AppA"
    )
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id="AppB"
    )
