"""Tests for Weaviate data operations functionality."""

import uuid as uuid_module

import pytest
from hypha_rpc.rpc import RemoteService
from weaviate.classes.query import Filter

from tests.weaviate_service.utils import (
    APP_ID,
    StandardMovie,
    create_test_application,
)


@pytest.mark.asyncio
async def test_collection_data_insert(weaviate_service: RemoteService) -> None:
    """Test inserting a single object into a collection."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Create a test object
    test_object: StandardMovie = StandardMovie.THE_MATRIX

    # Insert the object
    uuid = await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=APP_ID,
        properties=test_object,
    )

    # Verify the UUID is valid
    assert uuid is not None
    assert isinstance(uuid, str)

    # Check that the object was inserted correctly
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=APP_ID,
        limit=10,
    )

    assert len(query_result["objects"]) == 1
    assert query_result["objects"][0]["uuid"] == uuid
    assert all(obj["collection"] == "Movie" for obj in query_result["objects"])


@pytest.mark.asyncio
async def test_collection_data_insert_many(weaviate_service: RemoteService) -> None:
    """Test inserting multiple objects into a collection using kwargs."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Create test objects
    test_objects: list[StandardMovie] = [
        StandardMovie.INCEPTION,
        StandardMovie.THE_DARK_KNIGHT,
        StandardMovie.INTERSTELLAR,
    ]
    # Insert the objects
    result = await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=test_objects,
    )

    # Verify the result contains successful operations and UUIDs
    assert result is not None
    assert "has_errors" in result
    assert not result["has_errors"]
    assert "uuids" in result
    assert len(result["uuids"]) == len(test_objects)
    assert all(isinstance(uuid, str) for uuid in result["uuids"])

    # Verify the objects were inserted correctly
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=APP_ID,
        limit=10,
    )

    assert len(query_result["objects"]) == len(test_objects)
    assert all(obj["collection"] == "Movie" for obj in query_result["objects"])


@pytest.mark.asyncio
async def test_collection_data_update(weaviate_service: RemoteService) -> None:
    """Test updating an object in a collection."""
    # First insert a test object
    await create_test_application(weaviate_service)

    test_object: StandardMovie = StandardMovie.PULP_FICTION

    uuid = await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=APP_ID,
        properties=test_object,
    )

    # Update the object
    updated_properties: dict[str, str | int] = {
        "description": "Updated description for Pulp Fiction",
        "year": 1995,  # Deliberately changing for test purposes
    }

    await weaviate_service.data.update(
        collection_name="Movie",
        application_id=APP_ID,
        uuid=uuid,
        properties=updated_properties,
    )

    # Verify the update
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=APP_ID,
        limit=10,
    )

    updated_obj = next(
        (obj for obj in query_result["objects"] if obj["uuid"] == uuid),
        None,
    )
    assert updated_obj is not None
    assert updated_obj["properties"]["description"] == updated_properties["description"]
    assert updated_obj["properties"]["year"] == updated_properties["year"]
    # Original fields not mentioned in the update should remain unchanged
    assert updated_obj["properties"]["title"] == test_object.value.title
    assert updated_obj["properties"]["genre"] == test_object.value.genre


@pytest.mark.asyncio
async def test_collection_data_exists(weaviate_service: RemoteService) -> None:
    """Test checking if an object exists in a collection."""
    # First create a collection and application with a test object
    await create_test_application(weaviate_service)

    test_object: StandardMovie = StandardMovie.THE_GODFATHER

    uuid = await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=APP_ID,
        properties=test_object,
    )

    # Check if the object exists
    exists = await weaviate_service.data.exists(
        collection_name="Movie",
        application_id=APP_ID,
        uuid=uuid,
    )

    assert exists is True

    # Check if a non-existent object returns False
    fake_uuid = str(uuid_module.uuid4())
    exists = await weaviate_service.data.exists(
        collection_name="Movie",
        application_id=APP_ID,
        uuid=fake_uuid,
    )

    assert exists is False


@pytest.mark.asyncio
async def test_collection_data_delete_by_id(weaviate_service: RemoteService) -> None:
    """Test deleting an object by ID from a collection."""
    # First create a collection and application with a test object
    await create_test_application(weaviate_service)

    test_object: StandardMovie = StandardMovie.GOODFELLAS

    uuid = await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=APP_ID,
        properties=test_object,
    )

    # Verify the object exists
    exists = await weaviate_service.data.exists(
        collection_name="Movie",
        application_id=APP_ID,
        uuid=uuid,
    )
    assert exists is True

    # Delete the object
    await weaviate_service.data.delete_by_id(
        collection_name="Movie",
        application_id=APP_ID,
        uuid=uuid,
    )

    # Verify the object no longer exists
    exists = await weaviate_service.data.exists(
        collection_name="Movie",
        application_id=APP_ID,
        uuid=uuid,
    )
    assert exists is False


@pytest.mark.asyncio
async def test_collection_data_delete_many(weaviate_service: RemoteService) -> None:
    """Test deleting multiple objects using filters."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Add some test data
    test_objects: list[StandardMovie] = [
        StandardMovie.STAR_WARS_A_NEW_HOPE,
        StandardMovie.STAR_WARS_THE_EMPIRE_STRIKES_BACK,
        StandardMovie.THE_SHAWSHANK_REDEMPTION,
    ]

    # Insert data
    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=test_objects,
    )

    # Verify objects were inserted
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=APP_ID,
        limit=10,
    )

    assert len(query_result["objects"]) == len(test_objects)

    # Delete objects with science fiction genre
    result = await weaviate_service.data.delete_many(
        collection_name="Movie",
        application_id=APP_ID,
        where=Filter.by_property("genre").equal("Science Fiction"),
    )

    assert result is not None
    assert "successful" in result
    assert (
        result["successful"] == len(test_objects) - 1
    )  # Should have deleted 2 Star Wars movies

    # Verify the remaining objects
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=APP_ID,
        limit=10,
    )

    assert len(query_result["objects"]) == 1
    assert (
        query_result["objects"][0]["properties"]["title"] == "The Shawshank Redemption"
    )
