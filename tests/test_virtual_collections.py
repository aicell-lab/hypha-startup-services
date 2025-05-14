"""Tests for the completed virtual collections implementation."""

import pytest
import uuid
import pytest_asyncio


@pytest.mark.asyncio
async def test_virtual_collections(weaviate_service):
    """Test creating and using virtual collections with applications and sessions."""
    # Create a unique collection name to avoid conflicts
    collection_name = f"TestColl_{uuid.uuid4().hex[:8]}"

    # 1. Create the collection
    collection_config = {
        "class": collection_name,
        "description": "Test virtual collection",
        "properties": [
            {"name": "content", "dataType": ["text"], "description": "Content text"},
            {
                "name": "application_id",
                "dataType": ["text"],
                "description": "Application ID",
            },
            {"name": "session_id", "dataType": ["text"], "description": "Session ID"},
        ],
    }

    try:
        # Create the collection
        await weaviate_service.collections.create(settings=collection_config)

        # 2. Create two applications in the collection
        app1_id = "app1"
        app2_id = "app2"

        await weaviate_service.applications.create(
            collection_name=collection_name,
            application_id=app1_id,
            description="Test application 1",
        )

        await weaviate_service.applications.create(
            collection_name=collection_name,
            application_id=app2_id,
            description="Test application 2",
        )

        # 3. Create sessions for each application
        session1_app1 = "session1_app1"
        session2_app1 = "session2_app1"
        session1_app2 = "session1_app2"

        await weaviate_service.sessions.create(
            collection_name=collection_name,
            application_id=app1_id,
            session_id=session1_app1,
            description="Session 1 for App 1",
        )

        await weaviate_service.sessions.create(
            collection_name=collection_name,
            application_id=app1_id,
            session_id=session2_app1,
            description="Session 2 for App 1",
        )

        await weaviate_service.sessions.create(
            collection_name=collection_name,
            application_id=app2_id,
            session_id=session1_app2,
            description="Session 1 for App 2",
        )

        # 4. Insert data into different applications and sessions
        objects_app1_session1 = [
            {"content": "Content for App 1, Session 1, Object 1"},
            {"content": "Content for App 1, Session 1, Object 2"},
        ]

        objects_app1_session2 = [
            {"content": "Content for App 1, Session 2, Object 1"},
            {"content": "Content for App 1, Session 2, Object 2"},
        ]

        objects_app2_session1 = [
            {"content": "Content for App 2, Session 1, Object 1"},
            {"content": "Content for App 2, Session 1, Object 2"},
        ]

        # Insert data with application and session IDs
        await weaviate_service.data.insert_many(
            collection_name=collection_name,
            application_id=app1_id,
            objects=objects_app1_session1,
            session_id=session1_app1,
        )

        await weaviate_service.data.insert_many(
            collection_name=collection_name,
            application_id=app1_id,
            objects=objects_app1_session2,
            session_id=session2_app1,
        )

        await weaviate_service.data.insert_many(
            collection_name=collection_name,
            application_id=app2_id,
            objects=objects_app2_session1,
            session_id=session1_app2,
        )

        # 5. Test query filtering by application_id
        app1_results = await weaviate_service.query.fetch_objects(
            collection_name=collection_name, application_id=app1_id, limit=10
        )

        assert app1_results is not None
        assert (
            len(app1_results["objects"]) == 4
        )  # All objects from app1 (both sessions)

        # 6. Test query filtering by application_id and session_id
        app1_session1_results = await weaviate_service.query.fetch_objects(
            collection_name=collection_name,
            application_id=app1_id,
            session_id=session1_app1,
            limit=10,
        )

        assert app1_session1_results is not None
        assert (
            len(app1_session1_results["objects"]) == 2
        )  # Only objects from app1, session1

        # 7. Test application listing
        apps = await weaviate_service.applications.list_all(
            collection_name=collection_name
        )
        assert len(apps) == 2

        # 8. Test session listing
        sessions_app1 = await weaviate_service.sessions.list_all(
            collection_name=collection_name, application_id=app1_id
        )

        assert len(sessions_app1) == 2

        # 9. Test deleting a session
        await weaviate_service.sessions.delete(
            collection_name=collection_name,
            application_id=app1_id,
            session_id=session1_app1,
        )

        # Verify session is deleted
        sessions_app1_after = await weaviate_service.sessions.list_all(
            collection_name=collection_name, application_id=app1_id
        )

        assert len(sessions_app1_after) == 1

        # 10. Test deleting an application
        await weaviate_service.applications.delete(
            collection_name=collection_name, application_id=app2_id
        )

        # Verify application is deleted
        apps_after = await weaviate_service.applications.list_all(
            collection_name=collection_name
        )
        assert len(apps_after) == 1

        # Verify app2's objects are gone
        app2_results = await weaviate_service.query.fetch_objects(
            collection_name=collection_name, application_id=app2_id, limit=10
        )

        assert len(app2_results["objects"]) == 0

    finally:
        # Clean up
        try:
            await weaviate_service.collections.delete(collection_name)
        except Exception as e:
            print(f"Error cleaning up collection: {e}")
