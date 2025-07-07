"""Tests for Weaviate query functionality."""

import pytest
from tests.weaviate_service.utils import create_test_application, APP_ID


@pytest.mark.asyncio
async def test_collection_query_fetch_objects(weaviate_service):
    """Test fetching objects from a collection using kwargs."""
    # First insert test data by running another test
    await create_test_application(weaviate_service)

    # Add test objects
    test_objects = [
        {
            "title": "Inception",
            "description": "A thief who steals corporate secrets through dream-sharing technology",
            "genre": "Science Fiction",
            "year": 2010,
        },
        {
            "title": "The Dark Knight",
            "description": "Batman fights the menace known as the Joker",
            "genre": "Action",
            "year": 2008,
        },
    ]

    await weaviate_service.data.insert_many(
        collection_name="Movie", application_id=APP_ID, objects=test_objects
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

    assert len(all_results["objects"]) == 2


@pytest.mark.asyncio
async def test_collection_query_hybrid(weaviate_service):
    """Test hybrid query on a collection using kwargs."""
    # First insert test data
    await create_test_application(weaviate_service)

    # Add test objects
    test_objects = [
        {
            "title": "Inception",
            "description": "A thief who steals corporate secrets through dream-sharing technology",
            "genre": "Science Fiction",
            "year": 2010,
        },
        {
            "title": "The Dark Knight",
            "description": "Batman fights the menace known as the Joker",
            "genre": "Action",
            "year": 2008,
        },
        {
            "title": "Interstellar",
            "description": "A team of explorers travel through a wormhole in space",
            "genre": "Science Fiction",
            "year": 2014,
        },
    ]

    await weaviate_service.data.insert_many(
        collection_name="Movie", application_id=APP_ID, objects=test_objects
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
    assert len(result["objects"]) <= 2  # Should respect the limit

    # Results should be relevant to the query
    assert any(
        "Science Fiction" in obj["properties"]["genre"] for obj in result["objects"]
    )


@pytest.mark.asyncio
async def test_collection_query_near_text(weaviate_service):
    """Test near_text query on a collection using kwargs."""
    # First insert test data
    await create_test_application(weaviate_service)

    # Add test objects
    test_objects = [
        {
            "title": "Inception",
            "description": "A thief who steals corporate secrets through dream-sharing technology",
            "genre": "Science Fiction",
            "year": 2010,
        },
        {
            "title": "The Dark Knight",
            "description": "Batman fights the menace known as the Joker",
            "genre": "Action",
            "year": 2008,
        },
        {
            "title": "Interstellar",
            "description": "A team of explorers travel through a wormhole in space",
            "genre": "Science Fiction",
            "year": 2014,
        },
    ]

    await weaviate_service.data.insert_many(
        collection_name="Movie", application_id=APP_ID, objects=test_objects
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
    assert len(result["objects"]) <= 2  # Should respect the limit

    # Results should be relevant to the query - Interstellar should be included
    titles = [obj["properties"]["title"] for obj in result["objects"]]
    assert "Interstellar" in titles


@pytest.mark.asyncio
async def test_collection_query_near_vector(weaviate_service):
    """Test querying a collection using near_vector with kwargs."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    # Create test objects
    test_objects = [
        {
            "title": "The Matrix",
            "description": "A computer hacker learns about the true nature of reality",
            "genre": "Science Fiction",
            "year": 1999,
        },
        {
            "title": "The Godfather",
            "description": "The aging patriarch of an organized crime dynasty transfers control to his son",
            "genre": "Crime",
            "year": 1972,
        },
    ]

    # Insert data
    await weaviate_service.data.insert_many(
        collection_name="Movie", application_id=APP_ID, objects=test_objects
    )

    # Get a vector to use for near_vector search
    # This is a simplified example - in a real application we would use a proper embedding
    dummy_vector = [0.1] * 3072  # Assuming 3072-dimensional vectors

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
    assert len(result["objects"]) <= 2  # Should respect the limit

    # Check that vector was included in results
    assert all(
        ("description_vector" in obj["vector"] and "title_vector" in obj["vector"])
        for obj in result["objects"]
    )
