"""Integration test for query_near_vector metadata not-all-None.

# ruff: noqa: S101, ANN001, ANN201, PLR2004
"""

from typing import Any, cast

import pytest

from tests.weaviate_service.utils import APP_ID, create_test_application


@pytest.mark.asyncio
async def test_query_near_vector_metadata_not_all_none(weaviate_service: Any) -> None:
    """Ensure metadata returned by query_near_vector is not entirely None values."""
    await create_test_application(weaviate_service)

    # Insert a few sample objects
    test_objects: list[dict[str, str | int]] = [
        {
            "title": "Arrival",
            "description": "A linguist works with the military to communicate with alien lifeforms.",
            "genre": "Science Fiction",
            "year": 2016,
        },
        {
            "title": "Blade Runner",
            "description": "A blade runner must pursue and terminate four replicants.",
            "genre": "Science Fiction",
            "year": 1982,
        },
        {
            "title": "Gravity",
            "description": "Two astronauts work together to survive after an accident.",
            "genre": "Science Fiction",
            "year": 2013,
        },
    ]

    await weaviate_service.data.insert_many(
        collection_name="Movie",
        application_id=APP_ID,
        objects=test_objects,
    )

    # Use a 300-dim vector to match named vector configuration
    query_vector: list[float] = [0.0] * 1024

    vector_results = cast(
        "dict[str, Any]",
        await weaviate_service.query.near_vector(  # relies on service wrapper to accept query_vector
            collection_name="Movie",
            application_id=APP_ID,
            near_vector=query_vector,
            target_vector="title_vector",
            include_vector=True,
            limit=3,
        ),
    )

    assert vector_results is not None
    assert "objects" in vector_results
    objs = cast("list[dict[str, Any]]", vector_results["objects"])
    assert 1 <= len(objs) <= len(test_objects)

    # Metadata should not be entirely None
    for obj in objs:
        metadata = cast("dict[str, Any] | None", obj.get("metadata"))
        assert isinstance(
            metadata,
            dict,
        ), "metadata missing in near_vector result object"
        print(metadata)
        assert metadata["distance"] is not None, "metadata distance is None"
        assert metadata["score"] is not None, "metadata score is None"
        assert isinstance(metadata["score"], float), "metadata score is not a float"
        assert isinstance(
            metadata["distance"],
            float,
        ), "metadata distance is not a float"
