"""Unit test: query_near_vector defaults to returning metadata.

This verifies our service wrapper passes a non-None `return_metadata`
when the caller does not provide one, so metadata fields are populated
instead of None.
"""

from typing import Any

import pytest

from hypha_startup_services.weaviate_service import methods as w_methods


class _FakeQuery:
    def __init__(self) -> None:
        self.last_kwargs: dict[str, Any] | None = None

    async def near_vector(self, **kwargs: Any) -> Any:
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
async def test_query_near_vector_includes_metadata_by_default(monkeypatch: Any) -> None:
    fake_tenant = _FakeTenantCollection()

    async def _fake_prepare_tenant_collection(
        *_args: Any, **_kwargs: Any
    ) -> _FakeTenantCollection:
        return fake_tenant

    monkeypatch.setattr(
        w_methods, "prepare_tenant_collection", _fake_prepare_tenant_collection
    )

    # Call wrapper without providing `return_metadata`
    await w_methods.query_near_vector(  # type: ignore[arg-type]
        client=None,
        collection_name="Movie",
        application_id="TestApp",
        query_vector=[0.0, 0.1, 0.2],
        include_vector=False,
    )

    # Ensure a return_metadata object was provided to the client call
    assert fake_tenant.query.last_kwargs is not None
    assert "return_metadata" in fake_tenant.query.last_kwargs
    assert fake_tenant.query.last_kwargs["return_metadata"] is not None
