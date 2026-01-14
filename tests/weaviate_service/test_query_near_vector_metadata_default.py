"""Unit test: query_near_vector defaults to returning metadata.

This verifies our service wrapper passes a non-None `return_metadata`
when the caller does not provide one, so metadata fields are populated
instead of None.
"""

from typing import Any

import pytest

from hypha_startup_services.weaviate_service import methods as w_methods
from tests.weaviate_service.utils import APP_ID


class _FakeQuery:
    def __init__(self) -> None:
        self.last_kwargs: dict[str, Any] | None = None

    async def near_vector(self, **kwargs: Any) -> Any:  # NOSONAR S7503
        # Capture kwargs for assertion and return minimal result shape
        self.last_kwargs = kwargs

        class _Resp:
            def __init__(self) -> None:
                self.objects: list[Any] = []

        return _Resp()


class _FakeTenantCollection:
    def __init__(self) -> None:
        self.query = _FakeQuery()


@pytest.mark.asyncio
async def test_query_near_vector_no_metadata_by_default(monkeypatch: Any) -> None:
    """Ensure query_near_vector does not include return_metadata by default."""
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

    # Call wrapper without providing `return_metadata`
    await w_methods.query_near_vector(
        client=None,  # pyright: ignore[reportArgumentType]
        collection_name="Movie",
        application_id=APP_ID,
        query_vector=[0.0, 0.1, 0.2],
        include_vector=False,
    )

    # Ensure return_metadata was NOT provided to client call (default behavior change)
    assert fake_tenant.query.last_kwargs is not None
    assert "return_metadata" not in fake_tenant.query.last_kwargs


@pytest.mark.asyncio
async def test_query_near_vector_includes_metadata_when_requested(
    monkeypatch: Any,
) -> None:
    """Ensure query_near_vector includes return_metadata when requested."""
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

    # Call wrapper with explicit `return_metadata`
    await w_methods.query_near_vector(
        client=None,  # pyright: ignore[reportArgumentType]
        collection_name="Movie",
        application_id=APP_ID,
        query_vector=[0.0, 0.1, 0.2],
        include_vector=False,
        return_metadata={"distance": True},
    )

    # Ensure return_metadata WAS provided
    assert fake_tenant.query.last_kwargs is not None
    assert "return_metadata" in fake_tenant.query.last_kwargs
    assert fake_tenant.query.last_kwargs["return_metadata"] is not None
