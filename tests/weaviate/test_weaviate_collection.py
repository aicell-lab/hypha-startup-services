"""Tests for the Weaviate collection functionality."""

import pytest
from tests.weaviate.utils import create_test_collection


@pytest.mark.asyncio
async def test_create_collection(weaviate_service):
    """Test creating a Weaviate collection with proper schema configuration."""
    collection = await create_test_collection(weaviate_service)
    assert isinstance(collection, dict)


@pytest.mark.asyncio
async def test_get_collection(weaviate_service):
    """Test retrieving a collection's configuration by name."""
    await create_test_collection(weaviate_service)

    collection = await weaviate_service.collections.get("Movie")

    assert collection is not None
    assert isinstance(collection, dict)
    assert collection["class"] == "Movie"


@pytest.mark.asyncio
async def test_list_collections(weaviate_service):
    """Test listing all available collections."""
    # First create a collection
    await create_test_collection(weaviate_service)

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
    await create_test_collection(weaviate_service)

    collections = await weaviate_service.collections.list_all()
    assert any(coll_name == "Movie" for coll_name in collections.keys())

    await weaviate_service.collections.delete("Movie")

    collections = await weaviate_service.collections.list_all()
    assert not any(coll_name == "Movie" for coll_name in collections.keys())


@pytest.mark.asyncio
async def test_collection_exists(weaviate_service):
    """Test checking if a collection exists."""
    # First create a collection
    await create_test_collection(weaviate_service)

    # Check if collection exists
    exists = await weaviate_service.collections.exists("Movie")

    assert exists is True

    # Delete the collection
    await weaviate_service.collections.delete("Movie")

    # Check if collection still exists
    exists = await weaviate_service.collections.exists("Movie")

    assert exists is False
