"""Tests for Weaviate query functionality."""

import pytest
from hypha_rpc.rpc import RemoteService

from tests.weaviate_service.utils import APP_ID, StandardMovie, create_test_application


@pytest.mark.asyncio
async def test_collection_query_fetch_objects(weaviate_service: RemoteService) -> None:
    """Test fetching objects from a collection using kwargs."""
    # First insert test data by running another test
    await create_test_application(weaviate_service)

    # Add test objects
    test_objects: list[StandardMovie] = [
        StandardMovie.INCEPTION,
        StandardMovie.THE_DARK_KNIGHT,
    ]

    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=test_objects,
    )

    # Fetch objects using kwargs with various parameters
    result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=APP_ID,
        limit=1,
        offset=0,
        after="",
        include_vector=False,
    )

    assert result is not None
    assert "objects" in result
    # Should return exactly one result due to limit=1
    assert len(result["objects"]) == 1
    assert all(obj["collection"] == "Movie" for obj in result["objects"])

    # Test with a different limit to get all results
    all_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=APP_ID,
        limit=10,
    )

    assert len(all_results["objects"]) == len(test_objects)


@pytest.mark.asyncio
async def test_collection_query_hybrid(weaviate_service: RemoteService) -> None:
    """Test hybrid query on a collection using kwargs."""
    # First insert test data
    await create_test_application(weaviate_service)

    # Add test objects
    test_objects: list[StandardMovie] = [
        StandardMovie.INCEPTION,
        StandardMovie.THE_DARK_KNIGHT,
        StandardMovie.INTERSTELLAR,
    ]

    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=test_objects,
    )

    # Perform a hybrid search
    result = await weaviate_service.query.hybrid(
        collection_name="Movie",
        application_id=APP_ID,
        query="space science fiction",
        target_vector="description_vector",
        limit=2,
    )

    assert result is not None
    assert "objects" in result
    assert len(result["objects"]) <= len(test_objects)  # Should respect the limit

    # Results should be relevant to the query
    assert any(
        "Science Fiction" in obj["properties"].value.genre for obj in result["objects"]
    )


@pytest.mark.asyncio
async def test_collection_query_near_text(weaviate_service: RemoteService) -> None:
    """Test near_text query on a collection using kwargs."""
    # First insert test data
    await create_test_application(weaviate_service)

    # Add test objects
    test_objects: list[StandardMovie] = [
        StandardMovie.INCEPTION,
        StandardMovie.THE_DARK_KNIGHT,
        StandardMovie.INTERSTELLAR,
    ]

    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=test_objects,
    )

    # Perform a near_text search
    result = await weaviate_service.generate.near_text(
        collection_name="Movie",
        application_id=APP_ID,
        query="space exploration",
        target_vector="description_vector",
        limit=2,
    )

    assert result is not None
    assert "objects" in result
    assert len(result["objects"]) <= len(test_objects)  # Should respect the limit

    # Results should be relevant to the query - Interstellar should be included
    titles = [obj["properties"].value.title for obj in result["objects"]]
    assert "Interstellar" in titles


@pytest.mark.asyncio
async def test_collection_query_near_vector(weaviate_service: RemoteService) -> None:
    """Test querying a collection using near_vector with kwargs."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Create test objects
    test_objects: list[StandardMovie] = [
        StandardMovie.THE_MATRIX,
        StandardMovie.THE_GODFATHER,
    ]

    # Insert data
    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=test_objects,
    )

    dummy_vector = [0.1] * 1024  # Assuming 3072-dimensional vectors

    # Perform near_vector search
    result = await weaviate_service.query.near_vector(
        collection_name="Movie",
        application_id=APP_ID,
        near_vector=dummy_vector,
        target_vector="title_vector",
        include_vector=True,
        limit=2,
    )

    assert result is not None
    assert "objects" in result
    assert len(result["objects"]) <= len(test_objects)  # Should respect the limit

    # Check that vector was included in results
    assert all(
        ("description_vector" in obj["vector"] and "title_vector" in obj["vector"])
        for obj in result["objects"]
    )
