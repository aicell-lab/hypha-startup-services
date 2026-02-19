"""Integration test for query_near_vector metadata not-all-None.

# ruff: noqa: S101, ANN001, ANN201, PLR2004
"""

from dataclasses import asdict
from typing import Any, cast

import pytest

from tests.weaviate_service.utils import (
    APP_ID,
    StandardMovie,
    create_test_application,
    embedding_enabled,
)


@pytest.mark.asyncio
async def test_query_near_vector_metadata_not_all_none(weaviate_service: Any) -> None:
    """Ensure metadata returned by query_near_vector is not entirely None values."""
    await create_test_application(weaviate_service)

    # Insert a few sample objects
    test_objects: list[StandardMovie] = [
        StandardMovie.ARRIVAL,
        StandardMovie.BLADE_RUNNER,
        StandardMovie.GRAVITY,
    ]

    object_payloads = [asdict(movie.value) for movie in test_objects]
    if not embedding_enabled():
        object_payloads[0]["vector"] = [0.1, 0.2, 0.3, 0.4]
        object_payloads[1]["vector"] = [0.9, 0.8, 0.7, 0.6]
        object_payloads[2]["vector"] = [0.2, 0.3, 0.4, 0.5]

    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=object_payloads,
    )

    query_vector: list[float] = (
        [0.0] * 1024 if embedding_enabled() else [0.1, 0.2, 0.3, 0.4]
    )

    near_vector_kwargs: dict[str, object] = {
        "collection_name": "Movie",
        "application_id": APP_ID,
        "near_vector": query_vector,
        "include_vector": True,
        "return_metadata": {"distance": True, "score": True},
        "limit": 3,
    }
    if embedding_enabled():
        near_vector_kwargs["target_vector"] = "title_vector"

    vector_results = await weaviate_service.query.near_vector(**near_vector_kwargs)

    assert vector_results is not None
    assert "objects" in vector_results
    objs = vector_results["objects"]
    assert 1 <= len(objs) <= len(test_objects)

    # Metadata should not be entirely None
    for obj in objs:
        metadata = cast("dict[str, Any] | None", obj.get("metadata"))
        assert isinstance(
            metadata,
            dict,
        ), "metadata missing in near_vector result object"
        assert metadata["distance"] is not None, "metadata distance is None"
        assert metadata["score"] is not None, "metadata score is None"
        assert isinstance(metadata["score"], float), "metadata score is not a float"
        assert isinstance(
            metadata["distance"],
            float,
        ), "metadata distance is not a float"
