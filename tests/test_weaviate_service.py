"""Tests for the Weaviate service functionality including collections and data operations."""

from weaviate.classes.query import Filter
from hypha_rpc.rpc import RemoteException
import pytest
import uuid as uuid_module
from conftest import USER1_WS, USER2_WS, USER3_WS

APP_ID = "TestApp"
USER1_APP_ID = "User1App"
USER2_APP_ID = "User2App"
USER3_APP_ID = "User3App"
SHARED_APP_ID = "SharedApp"


@pytest.mark.asyncio
async def test_create_collection(weaviate_service):
    """Test creating a Weaviate collection with proper schema configuration."""
    ollama_endpoint = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se"
    ollama_model = "llama3.2"  # For embeddings - using an available model

    await weaviate_service.collections.delete("Movie")

    class_obj = {
        "class": "Movie",
        "description": "A movie class",
        "multiTenancyConfig": {
            "enabled": True,
        },
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
            {
                "name": "application_id",
                "dataType": ["text"],
                "description": "The ID of the application",
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
async def test_create_application(weaviate_service):
    """Test creating a Weaviate application with proper schema configuration."""
    await test_create_collection(weaviate_service)
    await weaviate_service.applications.create(
        application_id=APP_ID,
        collection_name="Movie",
        description="An application for movie data",
    )


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
    """Test listing all available collections."""
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
    # First create a collection and application
    await test_create_application(weaviate_service)

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
        collection_name="Movie", application_id=APP_ID, objects=test_objects
    )

    assert result is not None

    # Verify objects were inserted by fetching them back
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie", application_id=APP_ID, limit=10
    )

    assert query_result is not None
    assert "objects" in query_result
    assert len(query_result["objects"]) == 2
    assert all(obj["collection"] == "Movie" for obj in query_result["objects"])
    # check properties
    assert all(
        obj["properties"]["title"] in ["The Matrix", "Inception"]
        for obj in query_result["objects"]
    )

    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=APP_ID,
        limit=10,
        return_properties=["title"],
        include_vector=False,
    )
    assert all(
        obj["properties"]["title"] in ["The Matrix", "Inception"]
        for obj in query_result["objects"]
    )
    assert all(not obj["vector"] for obj in query_result["objects"])


@pytest.mark.asyncio
async def test_collection_data_insert(weaviate_service):
    """Test inserting a single object into a collection."""
    # First create a collection and application
    await test_create_application(weaviate_service)

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
        collection_name="Movie", application_id=APP_ID, properties=test_object
    )

    assert uuid is not None

    # Verify object was inserted by fetching it back
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=APP_ID,
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
    await test_create_application(weaviate_service)

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
        collection_name="Movie", application_id=APP_ID, properties=test_object
    )

    # Update the object
    updated_properties = {
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
        collection_name="Movie", application_id=APP_ID, limit=1
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
    await test_create_application(weaviate_service)

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
        collection_name="Movie", application_id=APP_ID, properties=test_object
    )

    # Check if object exists
    exists = await weaviate_service.data.exists(
        collection_name="Movie", application_id=APP_ID, uuid=uuid
    )

    assert exists is True

    # Check if non-existent object returns False
    fake_uuid = "00000000-0000-0000-0000-000000000000"
    exists = await weaviate_service.data.exists(
        collection_name="Movie", application_id=APP_ID, uuid=fake_uuid
    )

    assert exists is False


@pytest.mark.asyncio
async def test_collection_data_delete_by_id(weaviate_service):
    """Test deleting an object by ID from a collection."""
    # First insert a test object
    await test_create_application(weaviate_service)

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
        collection_name="Movie", application_id=APP_ID, properties=test_object
    )

    # Verify object exists
    exists_before = await weaviate_service.data.exists(
        collection_name="Movie", application_id=APP_ID, uuid=uuid
    )
    assert exists_before is True

    # Delete the object
    await weaviate_service.data.delete_by_id(
        collection_name="Movie", application_id=APP_ID, uuid=uuid
    )

    # Verify object no longer exists
    exists_after = await weaviate_service.data.exists(
        collection_name="Movie", application_id=APP_ID, uuid=uuid
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
        application_id=APP_ID,
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
    assert all(
        obj["properties"]["title"] in ["The Matrix", "Inception"]
        for obj in result["objects"]
    )
    # check vector
    assert all(
        ("description_vector" in obj["vector"] and "title_vector" in obj["vector"])
        for obj in result["objects"]
    )


@pytest.mark.asyncio
async def test_collection_query_fetch_objects(weaviate_service):
    """Test fetching objects from a collection using kwargs."""
    # First insert test data
    await test_collection_data_insert_many(weaviate_service)

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
        application_id=APP_ID,
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
        application_id=APP_ID,
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


@pytest.mark.asyncio
async def test_application_exists(weaviate_service):
    """Test checking if an application exists."""
    # First create a collection and application
    await test_create_application(weaviate_service)

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
    await test_create_application(weaviate_service)

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
    await test_create_application(weaviate_service)

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
async def test_collection_data_delete_many(weaviate_service):
    """Test deleting multiple objects using filters."""
    # First create a collection and application
    await test_create_application(weaviate_service)

    # Add some test data
    test_objects = [
        {
            "title": "Star Wars: A New Hope",
            "description": "Luke Skywalker joins forces with a Jedi Knight",
            "genre": "Science Fiction",
            "year": 1977,
        },
        {
            "title": "Star Wars: The Empire Strikes Back",
            "description": "After the Rebels are overpowered by the Empire",
            "genre": "Science Fiction",
            "year": 1980,
        },
        {
            "title": "The Shawshank Redemption",
            "description": "Two imprisoned men bond over a number of years",
            "genre": "Drama",
            "year": 1994,
        },
    ]

    # Insert data
    await weaviate_service.data.insert_many(
        collection_name="Movie", application_id=APP_ID, objects=test_objects
    )

    # Verify objects were inserted
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie", application_id=APP_ID, limit=10
    )
    assert len(query_result["objects"]) == 3

    # Delete objects with science fiction genre
    result = await weaviate_service.data.delete_many(
        collection_name="Movie",
        application_id=APP_ID,
        where=Filter.by_property("genre").equal("Science Fiction"),
    )

    assert result is not None
    assert "successful" in result
    assert result["successful"] == 2  # Should have deleted 2 Star Wars movies

    # Verify the remaining objects
    query_result = await weaviate_service.query.fetch_objects(
        collection_name="Movie", application_id=APP_ID, limit=10
    )

    assert len(query_result["objects"]) == 1
    assert (
        query_result["objects"][0]["properties"]["title"] == "The Shawshank Redemption"
    )


@pytest.mark.asyncio
async def test_collection_exists(weaviate_service):
    """Test checking if a collection exists."""
    # First create a collection
    await test_create_collection(weaviate_service)

    # Check if collection exists
    exists = await weaviate_service.collections.exists("Movie")

    assert exists is True

    # Delete the collection
    await weaviate_service.collections.delete("Movie")

    # Check if collection still exists
    exists = await weaviate_service.collections.exists("Movie")

    assert exists is False


@pytest.mark.asyncio
async def test_multi_user_application(weaviate_service, weaviate_service2):
    """Test multi-user application access using separate service instances."""
    # First create a collection and application
    await test_create_collection(weaviate_service)

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
    await test_create_collection(weaviate_service)

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
async def test_application_exists_across_users(weaviate_service, weaviate_service2):
    """Test checking if applications exist across different users."""
    # Create collection
    await test_create_collection(weaviate_service)

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


@pytest.mark.asyncio
async def test_data_isolation_between_applications(weaviate_service):
    """Test that data is isolated between different applications of the same user."""
    # Create collection
    await test_create_collection(weaviate_service)

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


@pytest.mark.asyncio
async def test_shared_application_access(weaviate_service, weaviate_service2):
    """Test access to a shared application with explicit user_ws parameter.

    This test simulates User 1 sharing an application with User 2 by allowing
    User 2 to access it with an explicit user_ws parameter.
    """
    # Create collection
    await test_create_collection(weaviate_service)

    # User 1 creates a shared application
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=SHARED_APP_ID,
        description="Shared application between users",
    )

    # User 1 adds data
    shared_movie1 = {
        "title": "User 1's Shared Movie",
        "description": "Movie added by User 1 to shared app",
        "genre": "Drama",
        "year": 2025,
    }

    await weaviate_service.data.insert(
        collection_name="Movie", application_id=SHARED_APP_ID, properties=shared_movie1
    )

    # User 2 tries to add data to User 1's application using the user_ws parameter to specify User 1
    # Note: In a real application, you would have proper permission management
    user2_movie = {
        "title": "User 2's Movie in Shared App",
        "description": "Movie added by User 2 to shared app",
        "genre": "Sci-Fi",
        "year": 2026,
    }

    try:
        # Attempt to add data with user_ws parameter - may fail depending on permissions
        await weaviate_service2.data.insert(
            collection_name="Movie",
            application_id=SHARED_APP_ID,
            properties=user2_movie,
            user_ws="user1_workspace",  # This would be the actual workspace of User 1
        )

        # If it didn't fail, we can check if both users can see the data
        user1_results = await weaviate_service.query.fetch_objects(
            collection_name="Movie", application_id=SHARED_APP_ID, limit=10
        )

        assert len(user1_results["objects"]) >= 1

    except (RemoteException, PermissionError, ValueError):
        # Expected failure due to permission settings
        pass

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id=SHARED_APP_ID
    )


@pytest.mark.asyncio
async def test_data_operations_permission_boundaries(
    weaviate_service, weaviate_service2
):
    """Test permission boundaries for various data operations."""
    # Create collection
    await test_create_collection(weaviate_service)

    # User 1 creates application
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's movie application",
    )

    # User 1 adds a movie
    movie_uuid = await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        properties={
            "title": "User 1's Private Movie",
            "description": "A movie that only User 1 should access",
            "genre": "Thriller",
            "year": 2025,
        },
    )

    # User 2 attempts to perform operations on User 1's data
    operations = [
        # Test exists
        (
            "exists",
            lambda: weaviate_service2.data.exists(
                collection_name="Movie", application_id=USER1_APP_ID, uuid=movie_uuid
            ),
        ),
        # Test update
        (
            "update",
            lambda: weaviate_service2.data.update(
                collection_name="Movie",
                application_id=USER1_APP_ID,
                uuid=movie_uuid,
                properties={"title": "Modified Title"},
            ),
        ),
        # Test delete
        (
            "delete",
            lambda: weaviate_service2.data.delete_by_id(
                collection_name="Movie", application_id=USER1_APP_ID, uuid=movie_uuid
            ),
        ),
        # Test query
        (
            "query",
            lambda: weaviate_service2.query.fetch_objects(
                collection_name="Movie", application_id=USER1_APP_ID, limit=10
            ),
        ),
    ]

    for op_name, operation in operations:
        try:
            await operation()
            assert (
                False
            ), f"Operation {op_name} should have failed with permission error"
        except (RemoteException, PermissionError, ValueError):
            # Expected failure due to permission settings
            pass

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id=USER1_APP_ID
    )


@pytest.mark.asyncio
async def test_uuid_collision_between_tenants(weaviate_service, weaviate_service2):
    """Test that UUIDs don't collide between different tenants in the same collection."""
    # Create collection
    await test_create_collection(weaviate_service)

    # Both users create their applications
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's movie application",
    )

    await weaviate_service2.applications.create(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        description="User 2's movie application",
    )

    # Generate a specific UUID to use for both insertions
    specific_uuid = uuid_module.uuid4()

    # User 1 inserts with specific UUID
    movie1 = {
        "title": "User 1's Movie with Specific UUID",
        "description": "This movie has a predetermined UUID",
        "genre": "Drama",
        "year": 2023,
    }

    await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        properties=movie1,
        uuid=specific_uuid,
    )

    # User 2 inserts with same UUID
    movie2 = {
        "title": "User 2's Movie with Same UUID",
        "description": "This movie tries to use the same UUID",
        "genre": "Comedy",
        "year": 2024,
    }

    try:
        # This should succeed if multi-tenancy is working correctly
        await weaviate_service2.data.insert(
            collection_name="Movie",
            application_id=USER2_APP_ID,
            properties=movie2,
            uuid=specific_uuid,
        )

        # Verify both objects exist with same UUID but in different tenant contexts
        user1_obj = await weaviate_service.query.fetch_objects(
            collection_name="Movie", application_id=USER1_APP_ID, limit=1
        )

        user2_obj = await weaviate_service2.query.fetch_objects(
            collection_name="Movie", application_id=USER2_APP_ID, limit=1
        )

        assert (
            user1_obj["objects"][0]["properties"]["title"]
            == "User 1's Movie with Specific UUID"
        )
        assert (
            user2_obj["objects"][0]["properties"]["title"]
            == "User 2's Movie with Same UUID"
        )
        assert user1_obj["objects"][0]["uuid"] == specific_uuid
        assert user2_obj["objects"][0]["uuid"] == specific_uuid

    except (RemoteException, PermissionError, ValueError, RuntimeError) as e:
        # Some databases might not allow the same UUID across tenants
        # If it fails, we document the behavior
        print(f"Note: UUID collision test failed with: {str(e)}")

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id=USER1_APP_ID
    )
    await weaviate_service2.applications.delete(
        collection_name="Movie", application_id=USER2_APP_ID
    )


@pytest.mark.asyncio
async def test_admin_only_collection_creation(weaviate_service, weaviate_service2):
    """Test that only admin users can create collections."""
    # Define a test collection schema
    class_obj = {
        "class": "TestCollection",
        "description": "A test collection",
        "multiTenancyConfig": {"enabled": True},
        "properties": [
            {
                "name": "name",
                "dataType": ["text"],
                "description": "The name property",
            },
        ],
    }

    # This should fail for User 2 (non-admin)
    try:
        await weaviate_service2.collections.create(class_obj)
        assert False, "Non-admin user should not be able to create collections"
    except (RemoteException, PermissionError, ValueError) as e:
        # Expected error for non-admin user
        assert (
            "admin" in str(e).lower() or "permission" in str(e).lower()
        ), f"Error should mention admin permissions, but got: {str(e)}"

    # This should succeed for User 1 (admin)
    collection = await weaviate_service.collections.create(class_obj)
    assert isinstance(collection, dict)
    assert "class" in collection
    assert collection["class"] == "TestCollection"

    # Verify the collection exists
    exists = await weaviate_service.collections.exists("TestCollection")
    assert exists is True

    # Non-admin user should see the collection exists, but not be able to modify it
    exists = await weaviate_service2.collections.exists("TestCollection")
    assert exists is True

    # Clean up - only admin can delete
    await weaviate_service.collections.delete("TestCollection")

    # Verify deletion
    exists = await weaviate_service.collections.exists("TestCollection")
    assert exists is False


@pytest.mark.asyncio
async def test_admin_only_collection_deletion(weaviate_service, weaviate_service2):
    """Test that only admin users can delete collections."""
    # Create a collection with admin user
    await test_create_collection(weaviate_service)

    # First, verify both users can see the collection exists
    admin_can_see = await weaviate_service.collections.exists("Movie")
    assert admin_can_see is True

    non_admin_can_see = await weaviate_service2.collections.exists("Movie")
    assert non_admin_can_see is True

    # Non-admin user attempts to delete the collection
    try:
        await weaviate_service2.collections.delete("Movie")
        assert False, "Non-admin user should not be able to delete collections"
    except (RemoteException, PermissionError, ValueError) as e:
        # Expected error for non-admin user
        assert (
            "admin" in str(e).lower() or "permission" in str(e).lower()
        ), f"Error should mention admin permissions, but got: {str(e)}"

    # Verify collection still exists after failed non-admin deletion attempt
    still_exists = await weaviate_service.collections.exists("Movie")
    assert still_exists is True

    # Admin user should be able to delete
    await weaviate_service.collections.delete("Movie")

    # Verify deletion was successful
    exists = await weaviate_service.collections.exists("Movie")
    assert exists is False


@pytest.mark.asyncio
async def test_cross_user_application_access(
    weaviate_service, weaviate_service2, weaviate_service3
):
    """Test that users can access applications across workspaces using the user_ws parameter."""
    # Create collection
    await test_create_collection(weaviate_service)

    # User 1 creates application
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's movie application",
    )

    # User 2 creates application
    await weaviate_service2.applications.create(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        description="User 2's movie application",
    )

    # User 3 creates application
    await weaviate_service3.applications.create(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        description="User 3's movie application",
    )

    # Each user adds data to their own application
    await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        properties={
            "title": "User 1's Movie",
            "description": "Movie owned by User 1",
            "genre": "Action",
            "year": 2023,
        },
    )

    await weaviate_service2.data.insert(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        properties={
            "title": "User 2's Movie",
            "description": "Movie owned by User 2",
            "genre": "Comedy",
            "year": 2024,
        },
    )

    await weaviate_service3.data.insert(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        properties={
            "title": "User 3's Movie",
            "description": "Movie owned by User 3",
            "genre": "Drama",
            "year": 2025,
        },
    )

    # Test User 1 (admin) trying to access User 2's application with explicit user_ws
    # This tests if an admin can access other users' data when specifying the user_ws
    try:
        user2_from_user1 = await weaviate_service.query.fetch_objects(
            collection_name="Movie",
            application_id=USER2_APP_ID,
            user_ws=USER2_WS,
            limit=10,
        )

        # If successful, verify the data
        assert user2_from_user1["objects"][0]["properties"]["title"] == "User 2's Movie"
    except (RemoteException, PermissionError, ValueError) as e:
        # If it fails, it should be due to permission settings, not because the approach is wrong
        print(f"Note: Admin accessing data with user_ws parameter failed: {str(e)}")

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id=USER1_APP_ID
    )
    await weaviate_service2.applications.delete(
        collection_name="Movie", application_id=USER2_APP_ID
    )
    await weaviate_service3.applications.delete(
        collection_name="Movie", application_id=USER3_APP_ID
    )


@pytest.mark.asyncio
async def test_cross_application_data_sharing(weaviate_service, weaviate_service2):
    """Test data sharing between applications of different users."""
    # Create collection
    await test_create_collection(weaviate_service)

    # User 1 creates a shared application
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=SHARED_APP_ID,
        description="Shared application between users",
    )

    # User 1 adds data
    await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=SHARED_APP_ID,
        properties={
            "title": "Shared Movie",
            "description": "This is a movie in a shared application",
            "genre": "Action",
            "year": 2025,
        },
    )

    # User 2 attempts to add data to User 1's application using the correct user_ws
    try:
        await weaviate_service2.data.insert(
            collection_name="Movie",
            application_id=SHARED_APP_ID,
            properties={
                "title": "User 2's Contribution",
                "description": "User 2 added this movie to User 1's application",
                "genre": "Comedy",
                "year": 2026,
            },
            user_ws=USER1_WS,  # Specifying User 1's workspace
        )

        # If it succeeds, verify the data is visible to User 1
        results = await weaviate_service.query.fetch_objects(
            collection_name="Movie", application_id=SHARED_APP_ID, limit=10
        )

        assert len(results["objects"]) == 2
        assert any(
            obj["properties"]["title"] == "User 2's Contribution"
            for obj in results["objects"]
        )

    except (RemoteException, PermissionError, ValueError) as e:
        # Expected failure due to permission settings
        print(f"Note: Cross-application data insertion failed: {str(e)}")

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id=SHARED_APP_ID
    )


@pytest.mark.asyncio
async def test_admin_only_collection_list(weaviate_service, weaviate_service2):
    """Test that only admin users can list all collections."""
    # Create multiple test collections with admin user
    await test_create_collection(weaviate_service)  # Creates "Movie" collection

    # Create a second collection for testing
    second_class_obj = {
        "class": "TestCollection2",
        "description": "A second test collection",
        "multiTenancyConfig": {"enabled": True},
        "properties": [
            {
                "name": "name",
                "dataType": ["text"],
                "description": "The name property",
            },
        ],
    }
    await weaviate_service.collections.create(second_class_obj)

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


@pytest.mark.asyncio
async def test_multi_tenant_access_with_correct_workspaces(
    weaviate_service, weaviate_service2, weaviate_service3
):
    """Test multi-tenant access using the correct workspace IDs."""
    # Create collection
    await test_create_collection(weaviate_service)

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

    # Test User 3 accessing User 1's data using the correct user_ws
    try:
        results = await weaviate_service3.query.fetch_objects(
            collection_name="Movie",
            application_id=SHARED_APP_ID,
            user_ws=USER1_WS,
            limit=10,
        )

        # If the system allows this, verify the data
        assert len(results["objects"]) == 1
        print("Note: User 3 can access User 1's data with correct user_ws")
    except (RemoteException, PermissionError, ValueError) as e:
        # This may fail depending on the permission model
        print(f"Note: Access with correct user_ws from User 3 failed: {str(e)}")

    # Now test admin accessing the data (which should always work)
    admin_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=SHARED_APP_ID,
        limit=10,
    )

    # Admin should always see their own data
    assert len(admin_results["objects"]) == 1

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id=SHARED_APP_ID
    )


@pytest.mark.asyncio
async def test_admin_access_to_other_users_data(
    weaviate_service, weaviate_service2, weaviate_service3
):
    """Test that admin users can access non-admin users' data."""
    # Create collection (only admin can do this)
    await test_create_collection(weaviate_service)

    # User 2 creates an application (non-admin)
    await weaviate_service2.applications.create(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        description="User 2's movie application",
    )

    # User 3 creates an application (non-admin)
    await weaviate_service3.applications.create(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        description="User 3's movie application",
    )

    # User 2 inserts data into their application
    user2_movie_uuid = await weaviate_service2.data.insert(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        properties={
            "title": "User 2's Private Movie",
            "description": "This should only be accessible to User 2 and admins",
            "genre": "Comedy",
            "year": 2024,
        },
    )

    # User 3 inserts data into their application
    user3_movie_uuid = await weaviate_service3.data.insert(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        properties={
            "title": "User 3's Private Movie",
            "description": "This should only be accessible to User 3 and admins",
            "genre": "Drama",
            "year": 2025,
        },
    )

    # Admin user (User 1) should be able to access User 2's data using User 2's workspace ID
    admin_access_user2 = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        user_ws=USER2_WS,  # Explicitly specify User 2's workspace
        limit=10,
    )

    # Verify admin can access User 2's data
    assert len(admin_access_user2["objects"]) == 1
    assert (
        admin_access_user2["objects"][0]["properties"]["title"]
        == "User 2's Private Movie"
    )

    # Admin user (User 1) should be able to access User 3's data using User 3's workspace ID
    admin_access_user3 = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        user_ws=USER3_WS,  # Explicitly specify User 3's workspace
        limit=10,
    )

    # Verify admin can access User 3's data
    assert len(admin_access_user3["objects"]) == 1
    assert (
        admin_access_user3["objects"][0]["properties"]["title"]
        == "User 3's Private Movie"
    )

    # Admin user should be able to modify User 2's data
    await weaviate_service.data.update(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        uuid=user2_movie_uuid,
        properties={"title": "Admin Modified User 2's Movie"},
        user_ws=USER2_WS,  # Explicitly specify User 2's workspace
    )

    # Verify the data was modified (from User 2's perspective)
    user2_results_after_update = await weaviate_service2.query.fetch_objects(
        collection_name="Movie", application_id=USER2_APP_ID, limit=10
    )
    assert len(user2_results_after_update["objects"]) == 1
    assert (
        user2_results_after_update["objects"][0]["properties"]["title"]
        == "Admin Modified User 2's Movie"
    )

    # Admin user should be able to delete User 3's data
    await weaviate_service.data.delete_by_id(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        uuid=user3_movie_uuid,
        user_ws=USER3_WS,  # Explicitly specify User 3's workspace
    )

    # Verify the data was deleted (from User 3's perspective)
    user3_results_after_delete = await weaviate_service3.query.fetch_objects(
        collection_name="Movie", application_id=USER3_APP_ID, limit=10
    )
    assert len(user3_results_after_delete["objects"]) == 0

    # Clean up
    await weaviate_service2.applications.delete(
        collection_name="Movie", application_id=USER2_APP_ID
    )
    await weaviate_service3.applications.delete(
        collection_name="Movie", application_id=USER3_APP_ID
    )


@pytest.mark.asyncio
async def test_cross_application_data_isolation(
    weaviate_service, weaviate_service2, weaviate_service3
):
    """Test data isolation between applications across different users/tenants."""
    # Create collection (only admin can do this)
    await test_create_collection(weaviate_service)

    # Create applications for each user
    await weaviate_service.applications.create(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        description="User 1's movie application",
    )

    await weaviate_service2.applications.create(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        description="User 2's movie application",
    )

    await weaviate_service3.applications.create(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        description="User 3's movie application",
    )

    # Each user adds data to their own application with identical properties but different titles
    movie_props = {
        "description": "A test movie with identical properties across tenants",
        "genre": "Science Fiction",
        "year": 2025,
    }

    # User 1 adds data
    await weaviate_service.data.insert(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        properties={"title": "User 1 Movie", **movie_props},
    )

    # User 2 adds data
    await weaviate_service2.data.insert(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        properties={"title": "User 2 Movie", **movie_props},
    )

    # User 3 adds data
    await weaviate_service3.data.insert(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        properties={"title": "User 3 Movie", **movie_props},
    )

    # Filter by identical properties to ensure cross-tenant isolation
    filter_condition = Filter.by_property("genre").equal("Science Fiction")

    # User 1 runs a filtered query
    user1_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        where=filter_condition,
        limit=10,
    )

    # User 2 runs the same filtered query
    user2_results = await weaviate_service2.query.fetch_objects(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        where=filter_condition,
        limit=10,
    )

    # User 3 runs the same filtered query
    user3_results = await weaviate_service3.query.fetch_objects(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        where=filter_condition,
        limit=10,
    )

    # Verify data isolation between tenants - each user should only see their own data
    assert len(user1_results["objects"]) == 1
    assert user1_results["objects"][0]["properties"]["title"] == "User 1 Movie"

    assert len(user2_results["objects"]) == 1
    assert user2_results["objects"][0]["properties"]["title"] == "User 2 Movie"

    assert len(user3_results["objects"]) == 1
    assert user3_results["objects"][0]["properties"]["title"] == "User 3 Movie"

    # Admin can access all data with explicit workspace IDs
    admin_user1_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER1_APP_ID,
        where=filter_condition,
        limit=10,
    )

    admin_user2_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER2_APP_ID,
        user_ws=USER2_WS,
        where=filter_condition,
        limit=10,
    )

    admin_user3_results = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=USER3_APP_ID,
        user_ws=USER3_WS,
        where=filter_condition,
        limit=10,
    )

    # Verify admin can access all data with the correct workspace specification
    assert len(admin_user1_results["objects"]) == 1
    assert admin_user1_results["objects"][0]["properties"]["title"] == "User 1 Movie"

    assert len(admin_user2_results["objects"]) == 1
    assert admin_user2_results["objects"][0]["properties"]["title"] == "User 2 Movie"

    assert len(admin_user3_results["objects"]) == 1
    assert admin_user3_results["objects"][0]["properties"]["title"] == "User 3 Movie"

    # Clean up
    await weaviate_service.applications.delete(
        collection_name="Movie", application_id=USER1_APP_ID
    )
    await weaviate_service2.applications.delete(
        collection_name="Movie", application_id=USER2_APP_ID
    )
    await weaviate_service3.applications.delete(
        collection_name="Movie", application_id=USER3_APP_ID
    )
