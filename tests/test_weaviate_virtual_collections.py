"""Tests for Weaviate virtual collections, applications, and sessions."""

import pytest


@pytest.mark.asyncio
async def test_create_collection_and_application(weaviate_service):
    """Test creating a collection and an application with proper artifacts."""

    # Create a test collection
    collection_name = "TestCollection"
    collection_settings = {
        "class": collection_name,
        "description": "A test collection",
        "vectorizer": "none",
        "properties": [
            {
                "name": "content",
                "dataType": ["text"],
                "description": "The content of the document",
            },
            {
                "name": "application_id",
                "dataType": ["text"],
                "description": "The application ID",
            },
            {
                "name": "session_id",
                "dataType": ["text"],
                "description": "The session ID",
            },
        ],
    }

    # Create the collection
    collection_result = await weaviate_service.collections.create(
        settings=collection_settings
    )

    assert collection_result is not None
    assert collection_result["class"] == collection_name

    # Verify collection exists
    collection_exists = await weaviate_service.collections.exists(
        collection_name=collection_name
    )

    assert collection_exists is True

    # Create an application
    application_id = "test_app_1"
    app_description = "Test application 1"

    app_result = await weaviate_service.applications.create(
        collection_name=collection_name,
        application_id=application_id,
        description=app_description,
    )

    assert app_result is not None
    assert app_result["application_id"] == application_id
    assert app_result["collection_name"] == collection_name

    # Verify application exists
    app_exists = await weaviate_service.applications.exists(
        collection_name=collection_name, application_id=application_id
    )

    assert app_exists is True

    # List applications
    apps = await weaviate_service.applications.list_all(collection_name=collection_name)

    assert len(apps) > 0

    # Create a session
    session_id = "test_session_1"
    session_description = "Test session 1"

    session_result = await weaviate_service.sessions.create(
        collection_name=collection_name,
        application_id=application_id,
        session_id=session_id,
        description=session_description,
    )

    assert session_result is not None
    assert session_result["session_id"] == session_id
    assert session_result["application_id"] == application_id

    # List sessions
    sessions = await weaviate_service.sessions.list_all(
        collection_name=collection_name, application_id=application_id
    )

    assert len(sessions) > 0


@pytest.mark.asyncio
async def test_data_operations_with_session(weaviate_service):
    """Test inserting and querying data in a collection with application and session IDs."""

    # Create the collection, application, and session
    collection_name = "TestCollection"
    collection_settings = {
        "class": collection_name,
        "description": "A test collection",
        "vectorizer": "none",
        "properties": [
            {
                "name": "content",
                "dataType": ["text"],
                "description": "The content of the document",
            },
            {
                "name": "application_id",
                "dataType": ["text"],
                "description": "The application ID",
            },
            {
                "name": "session_id",
                "dataType": ["text"],
                "description": "The session ID",
            },
        ],
    }

    await weaviate_service.collections.create(settings=collection_settings)

    # Create an application
    application_id = "test_app_2"
    await weaviate_service.applications.create(
        collection_name=collection_name,
        application_id=application_id,
        description="Test application 2",
    )

    # Create a session
    session_id = "test_session_2"
    await weaviate_service.sessions.create(
        collection_name=collection_name,
        application_id=application_id,
        session_id=session_id,
        description="Test session 2",
    )

    # Insert test data
    test_data = [{"content": "Document 1 content"}, {"content": "Document 2 content"}]

    insert_result = await weaviate_service.data.insert_many(
        collection_name=collection_name,
        application_id=application_id,
        objects=test_data,
        session_id=session_id,
    )

    assert insert_result is not None
    assert insert_result["has_errors"] is False
    assert len(insert_result["uuids"]) == 2

    # Query the data
    query_result = await weaviate_service.query.fetch_objects(
        collection_name=collection_name,
        application_id=application_id,
        session_id=session_id,
        limit=10,
    )

    assert query_result is not None
    assert "objects" in query_result
    assert len(query_result["objects"]) == 2

    # Test filtering by application_id
    another_session_id = "test_session_3"
    await weaviate_service.sessions.create(
        collection_name=collection_name,
        application_id=application_id,
        session_id=another_session_id,
        description="Test session 3",
    )

    # Insert data in another session
    other_test_data = [
        {"content": "Document 3 content"},
        {"content": "Document 4 content"},
    ]

    await weaviate_service.data.insert_many(
        collection_name=collection_name,
        application_id=application_id,
        objects=other_test_data,
        session_id=another_session_id,
    )

    # Query specific session
    session_query_result = await weaviate_service.query.fetch_objects(
        collection_name=collection_name,
        application_id=application_id,
        session_id=session_id,
        limit=10,
    )

    assert session_query_result is not None
    assert len(session_query_result["objects"]) == 2

    # Query all objects in the application
    all_query_result = await weaviate_service.query.fetch_objects(
        collection_name=collection_name,
        application_id=application_id,
        limit=10,
    )

    assert all_query_result is not None
    assert len(all_query_result["objects"]) == 4

    # Delete the sessions
    await weaviate_service.sessions.delete(
        collection_name=collection_name,
        application_id=application_id,
        session_id=session_id,
    )

    await weaviate_service.sessions.delete(
        collection_name=collection_name,
        application_id=application_id,
        session_id=another_session_id,
    )

    # Delete the application
    await weaviate_service.applications.delete(
        collection_name=collection_name, application_id=application_id
    )

    # Delete the collection
    await weaviate_service.collections.delete(name=collection_name)
