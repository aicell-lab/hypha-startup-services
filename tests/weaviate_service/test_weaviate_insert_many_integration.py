"""Integration test for insert_many with top-level ids.

This mirrors the style of tests in test_weaviate_data.py, but ensures
that passing top-level identifiers (uuid/id) works via DataObject path.

Note: We avoid providing custom vectors here because Weaviate validates
vector dimensionality against configured index. Supplying arbitrary vectors
would make the test brittle across environments. The unit tests cover the
vector separation logic at the service boundary.
"""

import uuid as uuid_module
from typing import TYPE_CHECKING, Any, cast

import pytest

from tests.weaviate_service import utils as weav_utils

if TYPE_CHECKING:
    from utils import MovieInfo


@pytest.mark.asyncio
def _normalize_uuid(u: str) -> str:
    return u.replace("-", "")


@pytest.mark.asyncio
async def test_insert_many_with_top_level_vector_and_uuid(
    weaviate_service: Any,
) -> None:
    """Insert many objects providing vectors and uuids at top-level and verify results.

    Ensures custom vectors are accepted and top-level uuid/id preserved (normalized).
    """
    await cast("Any", weav_utils.create_test_application)(weaviate_service)  # type: ignore[no-untyped-call]

    u1 = str(uuid_module.uuid4())
    u2 = str(uuid_module.uuid4())
    u3 = str(uuid_module.uuid4())

    # Mix of 'uuid' and legacy 'id' to validate both are accepted.
    # Provide named vectors matching collection config (title_vector, description_vector).
    # Dimension inferred from previous error trace (300). Generate simple deterministic floats.
    dim = 300
    title_vector = [float(i) / dim for i in range(dim)]
    description_vector = [float(dim - i) / dim for i in range(dim)]
    named_vectors: dict[str, list[float]] = {
        "title_vector": title_vector,
        "description_vector": description_vector,
    }
    objects: list[MovieInfo] = [
        {
            "title": "Vector One",
            "description": "Custom vector + uuid",
            "genre": "Test",
            "year": 2024,
            "uuid": u1,
            "vector": named_vectors,
        },
        {
            "title": "Vector Two",
            "description": "Custom vector + legacy id",
            "genre": "Test",
            "year": 2024,
            "id": u2,
            "vector": named_vectors,
        },
        {
            "title": "Vector Three",
            "description": "Custom vector + uuid",
            "genre": "Test",
            "year": 2024,
            "uuid": u3,
            "vector": named_vectors,
        },
    ]

    result: dict[str, Any] = await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=weav_utils.APP_ID,
        objects=objects,
    )

    assert result is not None
    assert "has_errors" in result
    assert not result["has_errors"]
    assert "uuids" in result
    assert len(result["uuids"]) == len(objects)

    # Verify objects are present and UUIDs match what we provided
    query_result: dict[str, Any] = await weaviate_service.query.fetch_objects(
        collection_name="Movie",
        application_id=weav_utils.APP_ID,
        limit=10,
    )

    fetched: list[dict[str, Any]] = query_result["objects"]
    fetched_uuids: set[str] = {obj["uuid"] for obj in fetched}
    expected_norm = {_normalize_uuid(u1), _normalize_uuid(u2), _normalize_uuid(u3)}
    assert expected_norm.issubset(fetched_uuids)
    assert all(obj["collection"] == "Movie" for obj in fetched)
    # Ensure vectors were not treated as properties key
    for obj in fetched:
        assert "vector" not in obj["properties"]
