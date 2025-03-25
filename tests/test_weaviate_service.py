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
