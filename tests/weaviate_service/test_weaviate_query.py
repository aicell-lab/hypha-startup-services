"""Tests for Weaviate query functionality."""

from dataclasses import asdict

import pytest
from hypha_rpc.rpc import RemoteException, RemoteService

from tests.weaviate_service.utils import (
    APP_ID,
    StandardMovie,
    create_test_application,
    embedding_enabled,
)


def _movie_dicts(movies: list[StandardMovie]) -> list[dict[str, object]]:
    """Convert standard movie enum values into insertion payload dictionaries."""
    return [asdict(movie.value) for movie in movies]


def _genre_from_result_object(result_object: object) -> str:
    """Extract genre value from query result object across response shapes."""
    object_dict = result_object if isinstance(result_object, dict) else {}
    properties = object_dict.get("properties", {})
    if isinstance(properties, dict):
        return str(properties.get("genre", ""))

    value = getattr(properties, "value", None)
    if value is None:
        return ""

    genre = getattr(value, "genre", "")
    return str(genre)


def _title_from_result_object(result_object: object) -> str:
    """Extract title value from query result object across response shapes."""
    object_dict = result_object if isinstance(result_object, dict) else {}
    properties = object_dict.get("properties", {})
    if isinstance(properties, dict):
        return str(properties.get("title", ""))

    value = getattr(properties, "value", None)
    if value is None:
        return ""

    title = getattr(value, "title", "")
    return str(title)


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
        objects=_movie_dicts(test_objects),
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
    movie_payloads = _movie_dicts(test_objects)
    if not embedding_enabled():
        movie_payloads[0]["vector"] = [0.1, 0.2, 0.3, 0.4]
        movie_payloads[1]["vector"] = [0.2, 0.3, 0.4, 0.5]
        movie_payloads[2]["vector"] = [0.3, 0.4, 0.5, 0.6]

    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=movie_payloads,
    )

    hybrid_kwargs: dict[str, object] = {
        "collection_name": "Movie",
        "application_id": APP_ID,
        "query": "space science fiction",
        "limit": 2,
    }
    if embedding_enabled():
        hybrid_kwargs["target_vector"] = "description_vector"
    else:
        hybrid_kwargs["vector"] = [0.1, 0.2, 0.3, 0.4]

    result = await weaviate_service.query.hybrid(**hybrid_kwargs)

    assert result is not None
    assert "objects" in result
    assert len(result["objects"]) <= len(test_objects)  # Should respect the limit

    # Results should be relevant to the query
    assert any(
        "Science Fiction" in _genre_from_result_object(obj)
        for obj in result["objects"]
    )


@pytest.mark.asyncio
async def test_collection_query_near_text(weaviate_service: RemoteService) -> None:
    """Test near_text query on a collection using kwargs."""
    # First insert test data
    await create_test_application(weaviate_service)

    test_objects: list[StandardMovie] = [
        StandardMovie.INCEPTION,
        StandardMovie.THE_DARK_KNIGHT,
        StandardMovie.INTERSTELLAR,
    ]

    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=_movie_dicts(test_objects),
    )

    near_text_kwargs: dict[str, object] = {
        "collection_name": "Movie",
        "application_id": APP_ID,
        "query": "space exploration",
        "limit": 2,
    }
    if embedding_enabled():
        near_text_kwargs["target_vector"] = "description_vector"
        result = await weaviate_service.generate.near_text(**near_text_kwargs)
    else:
        with pytest.raises(RemoteException):
            await weaviate_service.generate.near_text(**near_text_kwargs)
        return

    assert result is not None
    assert "objects" in result
    assert len(result["objects"]) <= len(test_objects)  # Should respect the limit

    # Results should be relevant to the query - Interstellar should be included
    titles = [_title_from_result_object(obj) for obj in result["objects"]]
    assert "Interstellar" in titles


@pytest.mark.asyncio
async def test_collection_query_near_vector(weaviate_service: RemoteService) -> None:
    """Test querying a collection using near_vector with kwargs."""
    # First create a collection and application
    await create_test_application(weaviate_service)

    test_objects: list[StandardMovie] = [
        StandardMovie.THE_MATRIX,
        StandardMovie.THE_GODFATHER,
    ]

    movie_payloads = _movie_dicts(test_objects)
    if not embedding_enabled():
        movie_payloads[0]["vector"] = [0.1, 0.2, 0.3, 0.4]
        movie_payloads[1]["vector"] = [0.9, 0.8, 0.7, 0.6]

    # Insert data
    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=movie_payloads,
    )

    dummy_vector = [0.1] * 1024 if embedding_enabled() else [0.1, 0.2, 0.3, 0.4]

    near_vector_kwargs: dict[str, object] = {
        "collection_name": "Movie",
        "application_id": APP_ID,
        "near_vector": dummy_vector,
        "include_vector": True,
        "limit": 2,
    }
    if embedding_enabled():
        near_vector_kwargs["target_vector"] = "title_vector"

    result = await weaviate_service.query.near_vector(**near_vector_kwargs)

    assert result is not None
    assert "objects" in result
    assert len(result["objects"]) <= len(test_objects)  # Should respect the limit

    if embedding_enabled():
        assert all(
            (
                "description_vector" in obj["vector"]
                and "title_vector" in obj["vector"]
            )
            for obj in result["objects"]
        )
    else:
        assert all("vector" in obj for obj in result["objects"])
