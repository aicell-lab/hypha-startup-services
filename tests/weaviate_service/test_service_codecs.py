"""Tests for Weaviate service codecs."""

from collections.abc import Callable
from typing import cast

import pytest
from weaviate.classes.query import Filter
from weaviate.collections.classes.filters import (
    _FilterAnd,
    _FilterOr,
    _Filters,
    _FilterValue,
)

try:
    from weaviate.collections.classes.filters import _FilterNot
except ImportError:  # pragma: no cover - depends on installed weaviate version
    _FilterNot = None  # type: ignore[assignment]

from hypha_startup_services.weaviate_service.service_codecs import (
    register_weaviate_codecs,
)


class CodecServerStub:
    """Capture registered codecs for assertions."""

    def __init__(self) -> None:
        """Initialize in-memory storage for registered codecs."""
        self.codecs: list[dict[str, object]] = []

    def register_codec(self, codec: dict[str, object]) -> None:
        """Store a registered codec payload."""
        self.codecs.append(codec)


def _get_codec(
    server: CodecServerStub,
    codec_name: str,
) -> dict[str, object]:
    """Return a registered codec by name."""
    for codec in server.codecs:
        if codec.get("name") == codec_name:
            return codec
    message = f"Codec not found: {codec_name}"
    raise AssertionError(message)


def _codec_encoder(codec: dict[str, object]) -> Callable[[object], object]:
    """Return the encoder callable from a codec payload."""
    return cast("Callable[[object], object]", codec["encoder"])


def _codec_decoder(codec: dict[str, object]) -> Callable[[object], object]:
    """Return the decoder callable from a codec payload."""
    return cast("Callable[[object], object]", codec["decoder"])


def _assert_filter_tree_equal(
    expected_filter: _Filters,
    actual_filter: _Filters,
) -> None:
    """Assert two Weaviate filters are structurally equivalent."""
    if isinstance(expected_filter, _FilterValue):
        assert isinstance(actual_filter, _FilterValue)
        assert actual_filter.operator == expected_filter.operator
        assert actual_filter.target == expected_filter.target
        assert actual_filter.value == expected_filter.value
        return

    assert type(actual_filter) is type(expected_filter)
    expected_nested = cast("list[_Filters]", expected_filter.filters)
    actual_nested = cast("list[_Filters]", actual_filter.filters)
    assert len(actual_nested) == len(expected_nested)

    for expected_child, actual_child in zip(
        expected_nested,
        actual_nested,
        strict=True,
    ):
        _assert_filter_tree_equal(
            expected_filter=expected_child,
            actual_filter=actual_child,
        )


def test_register_weaviate_filter_codecs() -> None:
    """Register codecs that cover all compound Weaviate filter types."""
    server = CodecServerStub()

    register_weaviate_codecs(cast("object", server))

    registered_names = {codec["name"] for codec in server.codecs}
    assert "weaviate_filter_and" in registered_names
    assert "weaviate_filter_or" in registered_names
    if _FilterNot is None:
        assert "weaviate_filter_not" not in registered_names
    else:
        assert "weaviate_filter_not" in registered_names


def test_filter_value_roundtrip_in_pydantic_codec() -> None:
    """Round-trip `_FilterValue` through the custom pydantic codec."""
    server = CodecServerStub()
    register_weaviate_codecs(cast("object", server))

    pydantic_codec = _get_codec(server=server, codec_name="pydantic_model")
    encoder = _codec_encoder(pydantic_codec)
    decoder = _codec_decoder(pydantic_codec)

    original_filter = Filter.by_property("name").equal("Ada")
    assert isinstance(original_filter, _FilterValue)

    encoded_filter = encoder(original_filter)
    decoded_filter = decoder(encoded_filter)

    assert isinstance(decoded_filter, _FilterValue)
    _assert_filter_tree_equal(
        expected_filter=original_filter,
        actual_filter=decoded_filter,
    )


def test_filter_and_roundtrip_codec() -> None:
    """Round-trip nested `_FilterAnd` trees through the dedicated codec."""
    server = CodecServerStub()
    register_weaviate_codecs(cast("object", server))

    and_codec = _get_codec(server=server, codec_name="weaviate_filter_and")
    encoder = _codec_encoder(and_codec)
    decoder = _codec_decoder(and_codec)

    name_filter = Filter.by_property("name").equal("Ada")
    age_filter = Filter.by_property("age").greater_than(30)
    city_filter = Filter.by_property("city").equal("Uppsala")
    or_filter = Filter.any_of([age_filter, city_filter])
    original_filter = Filter.all_of([name_filter, or_filter])

    assert isinstance(original_filter, _FilterAnd)

    encoded_filter = encoder(original_filter)
    decoded_filter = decoder(encoded_filter)

    assert isinstance(decoded_filter, _FilterAnd)
    _assert_filter_tree_equal(
        expected_filter=original_filter,
        actual_filter=decoded_filter,
    )


@pytest.mark.skipif(
    _FilterNot is None,
    reason="Installed weaviate-client version does not support _FilterNot.",
)
def test_filter_not_roundtrip_codec() -> None:
    """Round-trip `_FilterNot` trees through the dedicated codec."""
    server = CodecServerStub()
    register_weaviate_codecs(cast("object", server))

    not_codec = _get_codec(server=server, codec_name="weaviate_filter_not")
    encoder = _codec_encoder(not_codec)
    decoder = _codec_decoder(not_codec)

    id_filter = Filter.by_property("entity_id").equal("node-1")
    original_filter = Filter.not_(id_filter)

    assert isinstance(original_filter, _FilterNot)

    encoded_filter = encoder(original_filter)
    decoded_filter = decoder(encoded_filter)

    assert isinstance(decoded_filter, _FilterNot)
    _assert_filter_tree_equal(
        expected_filter=original_filter,
        actual_filter=decoded_filter,
    )


def test_filter_or_roundtrip_codec() -> None:
    """Round-trip `_FilterOr` trees through the dedicated codec."""
    server = CodecServerStub()
    register_weaviate_codecs(cast("object", server))

    or_codec = _get_codec(server=server, codec_name="weaviate_filter_or")
    encoder = _codec_encoder(or_codec)
    decoder = _codec_decoder(or_codec)

    id_filter = Filter.by_property("entity_id").equal("node-1")
    type_filter = Filter.by_property("entity_type").equal("node")
    original_filter = Filter.any_of([id_filter, type_filter])

    assert isinstance(original_filter, _FilterOr)

    encoded_filter = encoder(original_filter)
    decoded_filter = decoder(encoded_filter)

    assert isinstance(decoded_filter, _FilterOr)
    _assert_filter_tree_equal(
        expected_filter=original_filter,
        actual_filter=decoded_filter,
    )
