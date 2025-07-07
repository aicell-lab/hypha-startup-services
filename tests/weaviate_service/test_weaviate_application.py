"""Tests for Weaviate application functionality."""

import pytest
from hypha_rpc.rpc import RemoteException
from tests.weaviate_service.utils import (
    create_test_collection,
    create_test_application,
    APP_ID,
    USER1_APP_ID,
)


@pytest.mark.asyncio
async def test_create_application(weaviate_service):
    """Test creating a Weaviate application with proper schema configuration."""
    await create_test_collection(weaviate_service)
    await create_test_application(weaviate_service)


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
    with pytest.raises(RemoteException):
        await weaviate_service.query.fetch_objects(
            collection_name="Movie", application_id=APP_ID, limit=10
        )


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
    with pytest.raises(RemoteException):
        await weaviate_service2.applications.exists(
            collection_name="Movie",
            application_id=USER1_APP_ID,
        )

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id=USER1_APP_ID
    )


@pytest.mark.asyncio
async def test_insert_data_invalid_application(weaviate_service):
    """Test inserting data into an invalid application should fail with an error."""
    # First create a valid collection
    await create_test_application(weaviate_service)

    # Try to insert data into a non-existent application
    test_object = {
        "title": "Test Movie",
        "description": "A test movie for error testing",
        "genre": "Test Genre",
        "year": 2024,
    }

    # This should fail because the application doesn't exist
    with pytest.raises(Exception) as exc_info:
        await weaviate_service.data.insert(
            collection_name="Movie",
            application_id="NonExistentApplication",
            properties=test_object,
        )
    # Verify that an error was raised
    assert exc_info.value is not None
    # The error message should indicate something about the application not existing
    error_message = str(exc_info.value).lower()
    assert any(
        keyword in error_message
        for keyword in [
            "application",
            "not found",
            "not exist",
            "invalid",
            "permission",
        ]
    )


@pytest.mark.asyncio
async def test_application_invalid_collection(weaviate_service):
    """Create a test application with an invalid collection."""
    with pytest.raises(RemoteException) as exc_info:
        await weaviate_service.applications.create(
            application_id=APP_ID,
            collection_name="NonExistentCollection",
            description="An application for movie data",
        )
    assert "Collection 'NonExistentCollection' does not exist." in str(exc_info.value)


@pytest.mark.asyncio
async def test_application_get_artifact(weaviate_service):
    """Test retrieving an application's artifact information."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Get the application artifact
    artifact = await weaviate_service.applications.get_artifact(
        collection_name="Movie",
        application_id=APP_ID,
    )

    assert artifact is not None
    assert isinstance(artifact, str)
    # The artifact should contain application configuration information
    assert "Shared__DELIM__Movie" in artifact

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie",
        application_id=APP_ID,
    )
    await weaviate_service.collections.delete("Movie")


@pytest.mark.asyncio
async def test_application_get_artifact_nonexistent(weaviate_service):
    """Test retrieving artifact for a non-existent application."""
    # First create a collection but no application
    await create_test_collection(weaviate_service)

    # Try to get artifact for non-existent application
    with pytest.raises(RemoteException) as exc_info:
        await weaviate_service.applications.get_artifact(
            collection_name="Movie",
            application_id="NonExistentApp",
        )

    # Verify that an error was raised
    assert exc_info.value is not None
    error_message = str(exc_info.value).lower()
    assert any(
        keyword in error_message
        for keyword in [
            "not found",
            "not exist",
            "artifact",
            "application",
        ]
    )

    # Clean up
    await weaviate_service.collections.delete("Movie")


@pytest.mark.asyncio
async def test_application_set_permissions_merge(weaviate_service):
    """Test setting permissions with merge=True (default behavior)."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Set initial permissions
    initial_permissions = {"user1": "rw", "user2": "r"}

    await weaviate_service.applications.set_permissions(
        collection_name="Movie",
        application_id=APP_ID,
        permissions=initial_permissions,
    )

    # Get the application to verify initial permissions
    app_data = await weaviate_service.applications.get(
        collection_name="Movie",
        application_id=APP_ID,
    )

    assert app_data["config"]["permissions"]["user1"] == "rw"
    assert app_data["config"]["permissions"]["user2"] == "r"

    # Add additional permissions with merge=True (default)
    additional_permissions = {
        "user3": "*",
        "user1": "r",  # This should override user1's permission from "rw" to "r"
    }

    await weaviate_service.applications.set_permissions(
        collection_name="Movie",
        application_id=APP_ID,
        permissions=additional_permissions,
        merge=True,
    )

    # Verify permissions were merged correctly
    updated_app_data = await weaviate_service.applications.get(
        collection_name="Movie",
        application_id=APP_ID,
    )
    permissions = updated_app_data["config"]["permissions"]

    assert permissions["user2"] == "r"  # Should remain unchanged
    assert permissions["user1"] == "r"  # Should be overwritten from "rw" to "r"
    assert permissions["user3"] == "*"  # Should be added

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie",
        application_id=APP_ID,
    )
    await weaviate_service.collections.delete("Movie")


@pytest.mark.asyncio
async def test_application_set_permissions_no_merge(weaviate_service):
    """Test setting permissions with merge=False (replace behavior)."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Set initial permissions
    initial_permissions = {"user1": "rw", "user2": "r", "admin_user": "*"}

    await weaviate_service.applications.set_permissions(
        collection_name="Movie",
        application_id=APP_ID,
        permissions=initial_permissions,
    )

    # Replace permissions entirely with merge=False
    new_permissions = {"user3": "r", "user4": "r", "user5": "rw"}

    await weaviate_service.applications.set_permissions(
        collection_name="Movie",
        application_id=APP_ID,
        permissions=new_permissions,
        merge=False,
    )

    # Verify permissions were replaced entirely
    updated_app_data = await weaviate_service.applications.get(
        collection_name="Movie",
        application_id=APP_ID,
    )
    permissions = updated_app_data["config"]["permissions"]

    assert permissions["user3"] == "r"  # New read permissions
    assert permissions["user4"] == "r"  # New read permissions
    assert permissions["user5"] == "rw"  # New read-write permissions
    assert "user1" not in permissions  # Old permissions should be gone
    assert "user2" not in permissions  # Old permissions should be gone
    assert "admin_user" not in permissions  # Old permissions should be gone

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie",
        application_id=APP_ID,
    )
    await weaviate_service.collections.delete("Movie")


@pytest.mark.asyncio
async def test_application_set_permissions_default_merge(weaviate_service):
    """Test that merge=True is the default behavior for backward compatibility."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Set initial permissions
    initial_permissions = {"user1": "r", "user2": "rw"}

    await weaviate_service.applications.set_permissions(
        collection_name="Movie",
        application_id=APP_ID,
        permissions=initial_permissions,
    )

    # Add permissions without specifying merge parameter (should default to True)
    additional_permissions = {"admin_user": "*"}

    await weaviate_service.applications.set_permissions(
        collection_name="Movie",
        application_id=APP_ID,
        permissions=additional_permissions,
        # Note: not specifying merge parameter
    )

    # Verify permissions were merged (default behavior)
    updated_app_data = await weaviate_service.applications.get(
        collection_name="Movie",
        application_id=APP_ID,
    )
    permissions = updated_app_data["config"]["permissions"]

    assert permissions["user1"] == "r"  # Original permissions preserved
    assert permissions["user2"] == "rw"  # Original permissions preserved
    assert permissions["admin_user"] == "*"  # New permissions added

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie",
        application_id=APP_ID,
    )
    await weaviate_service.collections.delete("Movie")


@pytest.mark.asyncio
async def test_application_set_permissions_empty_manifest(weaviate_service):
    """Test setting permissions when manifest has no existing permissions."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Set permissions when no permissions exist yet
    new_permissions = {"user1": "r", "user2": "rw"}

    await weaviate_service.applications.set_permissions(
        collection_name="Movie",
        application_id=APP_ID,
        permissions=new_permissions,
        merge=True,
    )

    # Verify permissions were set correctly
    updated_app_data = await weaviate_service.applications.get(
        collection_name="Movie",
        application_id=APP_ID,
    )

    permissions = updated_app_data["config"]["permissions"]

    assert permissions["user1"] == "r"
    assert permissions["user2"] == "rw"

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie",
        application_id=APP_ID,
    )
    await weaviate_service.collections.delete("Movie")
