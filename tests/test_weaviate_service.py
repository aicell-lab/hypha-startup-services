"""Tests for the Weaviate service functionality including collections and data operations."""

import pytest


@pytest.mark.asyncio
async def test_create_collection(weaviate_service):
    """Test creating a Weaviate collection with proper schema configuration."""
    ollama_endpoint = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se"
    ollama_model = "llama3.2"  # For embeddings - using an available model

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
        "vectorConfig": {
            "title_vector": {
                "vectorizer": {
                    "text2vec-ollama": {
                        "model": ollama_model,
                        "apiEndpoint": ollama_endpoint,
                    }
                },
                "sourceProperties": ["title"],
                "vectorIndexType": "hnsw",  # Added this line
                "vectorIndexConfig": {  # Optional but recommended for completeness
                    "distance": "cosine"
                },
            },
            "description_vector": {
                "vectorizer": {
                    "text2vec-ollama": {
                        "model": ollama_model,
                        "apiEndpoint": ollama_endpoint,
                    }
                },
                "sourceProperties": ["description"],
                "vectorIndexType": "hnsw",  # Added this line
                "vectorIndexConfig": {  # Optional but recommended for completeness
                    "distance": "cosine"
                },
            },
        },
        "moduleConfig": {
            "generative-ollama": {
                "model": ollama_model,
                "apiEndpoint": ollama_endpoint,
            }
        },
    }

    collection = await weaviate_service.collections.create(class_obj)
    assert isinstance(collection, dict)


@pytest.mark.asyncio
async def test_get_collection(weaviate_service):
    """Test retrieving a collection's configuration by name."""
    await test_create_collection(weaviate_service)

    collection = await weaviate_service.collections.get("Movie")

    assert collection is not None
    assert isinstance(collection, dict)
    assert collection["class"] == "Movie"


@pytest.mark.asyncio
async def test_list_collections(weaviate_service):
    """Test listing all available collections in the workspace."""
    # First create a collection
    await test_create_collection(weaviate_service)

    # List collections
    collections = await weaviate_service.collections.list_all()

    assert len(collections) >= 1
    assert isinstance(collections, dict)
    assert all(
        isinstance(coll_name, str) and isinstance(coll_obj, dict)
        for coll_name, coll_obj in collections.items()
    )
    assert any(coll_name == "Movie" for coll_name in collections.keys())


@pytest.mark.asyncio
async def test_delete_collection(weaviate_service):
    """Test deleting a collection and verifying it no longer exists."""
    await test_create_collection(weaviate_service)

    await weaviate_service.collections.delete("Movie")

    collections = await weaviate_service.collections.list_all()
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

    # Verify objects were inserted by fetching them back
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie", limit=10
    )

    assert query_result is not None
    assert "objects" in query_result
    assert len(query_result["objects"]) == 2
    assert all(obj["collection"] == "Movie" for obj in query_result["objects"])
    # check properties
    assert all(obj["properties"]["title"] in ["The Matrix", "Inception"] for obj in query_result["objects"])

    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie", limit=10, return_properties=["title"], include_vector=False
    )
    assert all(obj["properties"]["title"] in ["The Matrix", "Inception"] for obj in query_result["objects"])
    assert all(not obj["vector"] for obj in query_result["objects"])

@pytest.mark.asyncio
async def test_collection_data_insert(weaviate_service):
    """Test inserting a single object into a collection."""
    # First create a collection
    await test_create_collection(weaviate_service)

    # Create test data
    test_object = {
        "title": "The Godfather",
        "description": (
            "The aging patriarch of an organized crime dynasty transfers control to his son"
        ),
        "genre": "Crime",
        "year": 1972,
    }

    # Insert data
    uuid = await weaviate_service.data.insert(
        collection_name="Movie", properties=test_object
    )

    assert uuid is not None

    # Verify object was inserted by fetching it back
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        limit=1,
    )

    assert query_result is not None
    assert "objects" in query_result
    assert len(query_result["objects"]) == 1
    assert query_result["objects"][0]["uuid"] == uuid
    assert all(obj["collection"] == "Movie" for obj in query_result["objects"])


@pytest.mark.asyncio
async def test_collection_data_update(weaviate_service):
    """Test updating an object in a collection."""
    # First insert a test object
    await test_create_collection(weaviate_service)

    test_object = {
        "title": "Pulp Fiction",
        "description": (
            "The lives of two mob hitmen, a boxer, a gangster's wife, and"
            " a pair of diner bandits intertwine"
        ),
        "genre": "Crime",
        "year": 1994,
    }

    uuid = await weaviate_service.data.insert(
        collection_name="Movie", properties=test_object
    )

    # Update the object
    updated_properties = {
        "description": "Updated description for Pulp Fiction",
        "year": 1995,  # Deliberately changing for test purposes
    }

    await weaviate_service.data.update(
        collection_name="Movie", uuid=uuid, properties=updated_properties
    )

    # Verify the update
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        limit=1,
    )

    assert query_result is not None
    assert "objects" in query_result
    assert len(query_result["objects"]) == 1
    assert query_result["objects"][0]["uuid"] == uuid
    assert all(obj["collection"] == "Movie" for obj in query_result["objects"])


@pytest.mark.asyncio
async def test_collection_data_exists(weaviate_service):
    """Test checking if an object exists in a collection."""
    # First insert a test object
    await test_create_collection(weaviate_service)

    test_object = {
        "title": "Fight Club",
        "description": (
            "An insomniac office worker and a devil-may-care soapmaker form an"
            " underground fight club"
        ),
        "genre": "Drama",
        "year": 1999,
    }

    uuid = await weaviate_service.data.insert(
        collection_name="Movie", properties=test_object
    )

    # Check if object exists
    exists = await weaviate_service.data.exists(collection_name="Movie", uuid=uuid)

    assert exists is True

    # Check if non-existent object returns False
    fake_uuid = "00000000-0000-0000-0000-000000000000"
    exists = await weaviate_service.data.exists(collection_name="Movie", uuid=fake_uuid)

    assert exists is False


@pytest.mark.asyncio
async def test_collection_data_delete_by_id(weaviate_service):
    """Test deleting an object by ID from a collection."""
    # First insert a test object
    await test_create_collection(weaviate_service)

    test_object = {
        "title": "Interstellar",
        "description": (
            "A team of explorers travel through a wormhole in space in an attempt to ensure"
            " humanity's survival"
        ),
        "genre": "Science Fiction",
        "year": 2014,
    }

    uuid = await weaviate_service.data.insert(
        collection_name="Movie", properties=test_object
    )

    # Verify object exists
    exists_before = await weaviate_service.data.exists(
        collection_name="Movie", uuid=uuid
    )
    assert exists_before is True

    # Delete the object
    await weaviate_service.data.delete_by_id(collection_name="Movie", uuid=uuid)

    # Verify object no longer exists
    exists_after = await weaviate_service.data.exists(
        collection_name="Movie", uuid=uuid
    )
    assert exists_after is False


@pytest.mark.asyncio
async def test_collection_query_near_vector(weaviate_service):
    """Test querying a collection using near_vector with kwargs."""
    # First insert test data
    await test_collection_data_insert_many(weaviate_service)

    # Create test vector (this is just a mock vector for testing)
    test_vector = [0.1] * 3072  # Using a typical embedding dimension

    # Query using kwargs
    result = await weaviate_service.query.near_vector(
        collection_name="Movie",
        near_vector=test_vector,
        target_vector="title_vector",
        limit=1,
        include_vector=True,
    )

    assert result is not None
    assert "objects" in result
    # Should return at least one result
    assert len(result["objects"]) > 0
    assert all(obj["collection"] == "Movie" for obj in result["objects"])
    # check properties
    assert all(obj["properties"]["title"] in ["The Matrix", "Inception"] for obj in result["objects"])
    # check vector
    assert all(("description_vector" in obj["vector"] and "title_vector" in obj["vector"]) for obj in result["objects"])

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
    assert "objects" in result
    # Should return exactly one result due to limit=1
    assert len(result["objects"]) == 1
    assert all(obj["collection"] == "Movie" for obj in result["objects"])


@pytest.mark.asyncio
async def test_collection_query_hybrid(weaviate_service):
    """Test hybrid query on a collection using kwargs."""
    # First insert test data
    await test_collection_data_insert_many(weaviate_service)

    # Create test vector (this is just a mock vector for testing)
    test_vector = [0.1] * 3072  # Using a typical embedding dimension

    # Query using kwargs for hybrid search
    result = await weaviate_service.query.hybrid(
        collection_name="Movie",
        query="Science Fiction",
        vector=test_vector,
        target_vector="description_vector",
        alpha=0.5,  # Weight between text and vector search
        limit=2,
    )

    assert result is not None
    assert "objects" in result
    # Should return results
    assert len(result["objects"]) > 0
    assert all(obj["collection"] == "Movie" for obj in result["objects"])


@pytest.mark.asyncio
async def test_collection_query_near_text(weaviate_service):
    """Test near_text query on a collection using kwargs."""
    # First insert test data
    await test_collection_data_insert_many(weaviate_service)

    result = await weaviate_service.generate.near_text(
        collection_name="Movie",
        query="A sci-fi film",
        single_prompt="Translate this into French: {title}",
        target_vector="description_vector",
        limit=2,
    )

    assert result is not None
    assert "generated" in result
    assert "objects" in result
    # Should return at least one result
    assert len(result["objects"]) > 0
    assert all(obj["collection"] == "Movie" for obj in result["objects"])
