"""Tests for Weaviate application functionality."""

import pytest
from tests.weaviate_test_utils import (
    create_test_collection,
    create_test_application,
    APP_ID,
    USER1_APP_ID,
)


@pytest.mark.asyncio
async def test_create_application(weaviate_service):
    """Test creating a Weaviate application with proper schema configuration."""
    await create_test_collection(weaviate_service)
    await weaviate_service.applications.create(
        application_id=APP_ID,
        collection_name="Movie",
        description="An application for movie data",
    )


@pytest.mark.asyncio
async def test_application_exists(weaviate_service):
    """Test checking if an application exists."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Check if application exists
    exists = await weaviate_service.applications.exists(
        collection_name="Movie",
        application_id=APP_ID,
    )

    assert exists is True

    # Check if non-existent application returns False
    exists = await weaviate_service.applications.exists(
        collection_name="Movie",
        application_id="NonExistentApp",
    )

    assert exists is False


@pytest.mark.asyncio
async def test_application_delete(weaviate_service):
    """Test deleting an application."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Add some data to the application
    test_object = {
        "title": "Avatar",
        "description": "A paraplegic Marine dispatched to the moon Pandora on a unique mission",
        "genre": "Science Fiction",
        "year": 2009,
    }

    await weaviate_service.data.insert(
        collection_name="Movie", application_id=APP_ID, properties=test_object
    )

    # Delete the application
    result = await weaviate_service.applications.delete(
        collection_name="Movie", application_id=APP_ID
    )

    assert result is not None
    assert "successful" in result

    # Verify the application no longer exists
    exists = await weaviate_service.applications.exists(
        collection_name="Movie", application_id=APP_ID
    )

    assert exists is False

    # Verify that the data was also deleted
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie", application_id=APP_ID, limit=10
    )

    assert len(query_result["objects"]) == 0


@pytest.mark.asyncio
async def test_application_get(weaviate_service):
    """Test getting an application's details."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Get the application details
    application = await weaviate_service.applications.get(
        collection_name="Movie", application_id=APP_ID
    )

    assert application is not None
    assert isinstance(application, dict)
    # Check whether artifact info is included
    assert "alias" in application
    assert "description" in application["manifest"]


@pytest.mark.asyncio
async def test_application_exists_across_users(weaviate_service, weaviate_service2):
    """Test checking if applications exist across different users."""
    # Create collection
    await create_test_collection(weaviate_service)

    # User 1 creates application
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's movie application",
    )

    # User 1 can see their application exists
    exists = await weaviate_service.applications.exists(
        collection_name="Movie",
        application_id=USER1_APP_ID,
    )
    assert exists is True

    # User 2 cannot see User 1's application
    exists = await weaviate_service2.applications.exists(
        collection_name="Movie",
        application_id=USER1_APP_ID,
    )
    assert exists is False

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id=USER1_APP_ID
    )
