"""Unit tests for insert_many handling of custom vectors and ids.

These tests patch the tenant collection to avoid external Weaviate dependencies
and validate that objects are transformed into DataObject with vectors/uuids
kept out of properties.
"""

from typing import TYPE_CHECKING, Any

import pytest
from weaviate.collections.classes.data import DataObject

from hypha_startup_services.weaviate_service import methods as w_methods

if TYPE_CHECKING:
    from utils import MovieInfo


class _FakeBatchReturn:
    def __init__(self) -> None:
        self.elapsed_seconds = 0.0
        self.errors = {}
        self.uuids = {"0": "fake-uuid"}
        self.has_errors = False


class _FakeData:
    def __init__(self) -> None:
        self.received_objects: list[Any] = []

    async def insert_many(  # NOSONAR S7503
        self,
        *,
        objects: list[Any],
    ) -> _FakeBatchReturn:
        self.received_objects = objects
        return _FakeBatchReturn()


class _FakeTenantCollection:
    def __init__(self) -> None:
        self.data = _FakeData()


@pytest.mark.asyncio
async def test_insert_many_accepts_top_level_vector_and_uuid(monkeypatch: Any) -> None:
    """Ensure vector and uuid top-level fields are kept out of properties."""
    fake_tenant = _FakeTenantCollection()

    async def _fake_prepare_tenant_collection(  # NOSONAR S7503
        *_args: Any,
        **_kwargs: Any,
    ) -> _FakeTenantCollection:
        return fake_tenant

    monkeypatch.setattr(
        w_methods,
        "prepare_tenant_collection",
        _fake_prepare_tenant_collection,
    )

    objs: list[MovieInfo] = [
        {
            "title": "With Vector",
            "description": "desc",
            "year": 2024,
            "vector": [0.1, 0.2, 0.3],
            "uuid": "123e4567-e89b-12d3-a456-426614174000",
        },
    ]

    result = await w_methods.data_insert_many(
        client=None,  # type: ignore[arg-type]
        collection_name="Movie",
        application_id="TestApp",
        objects=objs,
        context=None,
    )

    # Result shape is preserved
    assert result["has_errors"] is False
    assert "uuids" in result

    # Ensure we passed DataObject instances to insert_many
    assert len(fake_tenant.data.received_objects) == 1
    dobj = fake_tenant.data.received_objects[0]
    assert isinstance(dobj, DataObject)

    # Verify properties got application_id and do not contain vector/uuid
    props: dict[str, Any] = dobj.properties  # type: ignore[attr-defined]
    assert "application_id" in props
    assert "vector" not in props
    assert "uuid" not in props
    assert "id" not in props


@pytest.mark.asyncio
async def test_insert_many_accepts_legacy_id_key(monkeypatch: Any) -> None:
    """Ensure legacy 'id' key is treated as uuid and not a property."""
    fake_tenant = _FakeTenantCollection()

    async def _fake_prepare_tenant_collection(  # NOSONAR S7503
        *_args: Any,
        **_kwargs: Any,
    ) -> _FakeTenantCollection:
        return fake_tenant

    monkeypatch.setattr(
        w_methods,
        "prepare_tenant_collection",
        _fake_prepare_tenant_collection,
    )

    objs: list[MovieInfo] = [
        {
            "title": "Legacy ID",
            "description": "desc",
            "year": 2024,
            "id": "123e4567-e89b-12d3-a456-426614174001",
        },
    ]

    await w_methods.data_insert_many(
        client=None,  # type: ignore[arg-type]
        collection_name="Movie",
        application_id="TestApp",
        objects=objs,
        context=None,
    )

    # Ensure we passed DataObject instances to insert_many
    assert len(fake_tenant.data.received_objects) == 1
    dobj = fake_tenant.data.received_objects[0]
    assert isinstance(dobj, DataObject)
    props: dict[str, Any] = dobj.properties  # type: ignore[attr-defined]
    assert "id" not in props
    assert "uuid" not in props
    assert props["application_id"] == "TestApp"
