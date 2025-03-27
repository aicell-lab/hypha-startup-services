import pytest
from weaviate.collections.collection import Collection


@pytest.mark.asyncio
async def test_create_collection(weaviate_service):
    await weaviate_service.collections.delete("Movie")
    class_obj = {
        "class": "Movie",
        "description": "A movie class",
        "properties": [
            {
                "name": "title",
                "dataType": ["text"],
                "description": "The title of the movie",
            },
            {
                "name": "description",
                "dataType": ["text"],
                "description": "A description of the movie",
            },
            {
                "name": "genre",
                "dataType": ["text"],
                "description": "The genre of the movie",
            },
            {
                "name": "year",
                "dataType": ["int"],
                "description": "The year the movie was released",
            },
        ],
    }

    collection = await weaviate_service.collections.create(class_obj)
    assert isinstance(collection, Collection)


@pytest.mark.asyncio
async def test_get_collection(weaviate_service):
    await test_create_collection(weaviate_service)

    collection = await weaviate_service.collections.get("Movie")

    assert collection is not None
    assert isinstance(collection, Collection)
    # assert collection.name == "Movie"


@pytest.mark.asyncio
async def test_list_collections(weaviate_service):
    # First create a collection
    await test_create_collection(weaviate_service)

    # List collections
    collections = await weaviate_service.collections.list_all(simple=True)

    assert len(collections) >= 1
    assert isinstance(collections, dict)
    assert all(
        isinstance(coll_name, str) and isinstance(coll_obj, Collection)
        for coll_name, coll_obj in collections.items()
    )
    assert any(coll_name == "Movie" for coll_name in collections.keys())


@pytest.mark.asyncio
async def test_delete_collection(weaviate_service):
    await test_create_collection(weaviate_service)

    await weaviate_service.collections.delete("Movie")

    collections = await weaviate_service.collections.list_all(simple=True)
    assert not any("Movie" in coll_name for coll_name in collections.keys())


@pytest.mark.asyncio
async def test_collection_data_insert_many(weaviate_service):
    """Test inserting multiple objects into a collection using kwargs."""
    # First create a collection
    await test_create_collection(weaviate_service)

    # Create test data
    test_objects = [
        {
            "title": "The Matrix",
            "description": "A computer hacker learns about the true nature of reality",
            "genre": "Science Fiction",
            "year": 1999,
        },
        {
            "title": "Inception",
            "description": "A thief who enters people's dreams to steal their secrets",
            "genre": "Science Fiction",
            "year": 2010,
        },
    ]

    # Insert data using kwargs
    result = await weaviate_service.data.insert_many(
        collection_name="Movie", objects=test_objects
    )

    assert result is not None
    assert "objects" in result
    assert len(result["objects"]) == 2

    # Verify objects were inserted by fetching them back
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie", limit=10
    )

    assert query_result is not None
    assert "data" in query_result
    assert "Get" in query_result["data"]
    assert "Movie" in query_result["data"]["Get"]
    assert len(query_result["data"]["Get"]["Movie"]) == 2


@pytest.mark.asyncio
async def test_collection_query_near_vector(weaviate_service):
    """Test querying a collection using near_vector with kwargs."""
    # First insert test data
    await test_collection_data_insert_many(weaviate_service)

    # Create test vector (this is just a mock vector for testing)
    test_vector = [0.1] * 768  # Using a typical embedding dimension

    # Query using kwargs
    result = await weaviate_service.query.near_vector(
        collection_name="Movie", vector=test_vector, limit=1
    )

    assert result is not None
    assert "data" in result
    assert "Get" in result["data"]
    assert "Movie" in result["data"]["Get"]
    # Should return at least one result
    assert len(result["data"]["Get"]["Movie"]) > 0


@pytest.mark.asyncio
async def test_collection_query_fetch_objects(weaviate_service):
    """Test fetching objects from a collection using kwargs."""
    # First insert test data
    await test_collection_data_insert_many(weaviate_service)

    # Fetch objects using kwargs with various parameters
    result = await weaviate_service.query.fetch_objects(
        collection_name="Movie", limit=1, offset=0, after="", include_vector=False
    )

    assert result is not None
    assert "data" in result
    assert "Get" in result["data"]
    assert "Movie" in result["data"]["Get"]
    # Should return exactly one result due to limit=1
    assert len(result["data"]["Get"]["Movie"]) == 1


@pytest.mark.asyncio
async def test_collection_query_hybrid(weaviate_service):
    """Test hybrid query on a collection using kwargs."""
    # First insert test data
    await test_collection_data_insert_many(weaviate_service)

    # Create test vector (this is just a mock vector for testing)
    test_vector = [0.1] * 768  # Using a typical embedding dimension

    # Query using kwargs for hybrid search
    result = await weaviate_service.query.hybrid(
        collection_name="Movie",
        query="Science Fiction",
        vector=test_vector,
        alpha=0.5,  # Weight between text and vector search
        limit=2,
    )

    assert result is not None
    assert "data" in result
    assert "Get" in result["data"]
    assert "Movie" in result["data"]["Get"]
    # Should return results
    assert len(result["data"]["Get"]["Movie"]) > 0


@pytest.mark.asyncio
async def test_collection_query_near_text(weaviate_service):
    """Test near_text query on a collection using kwargs."""
    # First insert test data
    await test_collection_data_insert_many(weaviate_service)

    # Query using kwargs
    result = await weaviate_service.generate.near_text(
        collection_name="Movie", concepts=["hacker", "reality"], limit=1
    )

    assert result is not None
    assert "data" in result
    assert "Get" in result["data"]
    assert "Movie" in result["data"]["Get"]
    # Should return at least one result
    assert len(result["data"]["Get"]["Movie"]) > 0
