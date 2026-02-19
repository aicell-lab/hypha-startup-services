"""Tests for the Weaviate collection functionality."""

from typing import cast

import pytest
from hypha_rpc.rpc import RemoteException, RemoteService

from tests.weaviate_service.utils import create_test_collection


@pytest.mark.asyncio
async def test_create_collection(weaviate_service: RemoteService) -> None:
    """Test creating a Weaviate collection with proper schema configuration."""
    collection = await create_test_collection(weaviate_service)
    assert isinstance(collection, dict)


@pytest.mark.asyncio
async def test_get_collection(weaviate_service: RemoteService) -> None:
    """Test retrieving a collection's configuration by name."""
    await create_test_collection(weaviate_service)

    collection = await weaviate_service.collections.get("Movie")

    assert collection is not None
    assert isinstance(collection, dict)
    assert collection["class"] == "Movie"


@pytest.mark.asyncio
async def test_list_collections(weaviate_service: RemoteService) -> None:
    """Test listing all available collections."""
    # First create a collection
    await create_test_collection(weaviate_service)

    # List collections
    collections_result = await weaviate_service.collections.list_all()
    collections = cast("dict[str, object]", collections_result)

    assert len(collections) >= 1
    assert isinstance(collections, dict)
    assert all(
        isinstance(coll_name, str) and isinstance(coll_obj, dict)
        for coll_name, coll_obj in collections.items()
    )
    assert any(coll_name == "Movie" for coll_name in collections)


@pytest.mark.asyncio
async def test_delete_collection(weaviate_service: RemoteService) -> None:
    """Test deleting a collection and verifying it no longer exists."""
    await create_test_collection(weaviate_service)

    collections = await weaviate_service.collections.list_all()
    assert any(coll_name == "Movie" for coll_name in collections)

    await weaviate_service.collections.delete("Movie")

    collections = await weaviate_service.collections.list_all()
    assert not any(coll_name == "Movie" for coll_name in collections)


@pytest.mark.asyncio
async def test_collection_exists(weaviate_service: RemoteService) -> None:
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


@pytest.mark.asyncio
async def test_collection_get_artifact(weaviate_service: RemoteService) -> None:
    """Test retrieving a collection's artifact name."""
    # First create a collection
    await create_test_collection(weaviate_service)

    # Get the collection artifact name
    artifact_name = await weaviate_service.collections.get_artifact("Movie")

    assert artifact_name is not None
    assert isinstance(artifact_name, str)
    # The artifact name should be the full collection name with workspace prefix
    assert artifact_name == "Shared__DELIM__Movie"

    # Clean up
    await weaviate_service.collections.delete("Movie")


@pytest.mark.asyncio
async def test_collection_get_artifact_nonexistent(
    weaviate_service: RemoteService,
) -> None:
    """Test retrieving artifact name for a non-existent collection."""
    with pytest.raises(
        RemoteException,
        match="Collection 'NonExistentCollection' does not exist.",
    ):
        await weaviate_service.collections.get_artifact("NonExistentCollection")
