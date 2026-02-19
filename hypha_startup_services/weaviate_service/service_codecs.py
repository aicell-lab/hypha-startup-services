"""Weaviate collection codecs for Hypha RPC serialization.

This module provides encoder and decoder functions for Weaviate collection objects,
allowing them to be serialized and transferred through Hypha RPC.
"""

import uuid
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime
from typing import Any

from hypha_rpc.rpc import RemoteService
from hypha_rpc.utils.pydantic import create_model_from_schema
from pydantic import BaseModel
from weaviate.collections.classes.filters import (
    _FilterAnd,  # type: ignore[reportPrivateUsage]
    _FilterOr,  # type: ignore[reportPrivateUsage]
    _Filters,  # type: ignore[reportPrivateUsage]
    _FilterValue,  # type: ignore[reportPrivateUsage]
    _Operator,  # type: ignore[reportPrivateUsage]
)
from weaviate.collections.classes.internal import Object

try:
    from weaviate.collections.classes.filters import (
        _FilterNot,  # type: ignore[reportPrivateUsage]
    )
except ImportError:  # pragma: no cover - depends on installed weaviate version
    _FilterNot = None  # type: ignore[assignment]

_SPECIAL_FILTER_KEY = "_special_filter"
_FILTER_KIND_KEY = "filter_kind"
_FILTERS_KEY = "filters"
_FILTER_VALUE_KEY = "value"
_KIND_VALUE = "value"
_KIND_AND = "and"
_KIND_OR = "or"
_KIND_NOT = "not"
_ERROR_UNSUPPORTED_FILTER_TYPE = "Unsupported filter object type"
_ERROR_UNSUPPORTED_FILTER_KIND = "Unsupported encoded filter kind"
_ERROR_FILTER_VALUE_MAPPING = "Encoded filter value payload must be a mapping."
_ERROR_NESTED_FILTERS_LIST = "Encoded nested filters must be a list."
_ERROR_NESTED_FILTER_MAPPING = "Each encoded nested filter must be a mapping."
_ERROR_DECODED_NOT_AND = "Decoded filter is not a _FilterAnd instance."
_ERROR_DECODED_NOT_OR = "Decoded filter is not a _FilterOr instance."
_ERROR_DECODED_NOT_NOT = "Decoded filter is not a _FilterNot instance."


class FilterCodecError(ValueError):
    """Base error for invalid encoded Weaviate filter payloads."""


class UnsupportedFilterObjectTypeError(TypeError):
    """Raised when attempting to encode an unknown Weaviate filter type."""


class UnsupportedFilterKindError(FilterCodecError):
    """Raised when an encoded filter payload contains an unknown kind."""


class InvalidEncodedFilterPayloadError(TypeError):
    """Raised when an encoded filter payload has an invalid shape."""


class DecodedFilterTypeMismatchError(TypeError):
    """Raised when decoded payload does not match expected filter type."""


def encode_uuid(obj: uuid.UUID) -> str:
    """Encode UUID to string."""
    return obj.hex


def encode_object(obj: Object[object, object]) -> dict[str, object]:
    """Encode Weaviate Object to dictionary."""
    return {
        "uuid": obj.uuid.hex,
        "vector": obj.vector,
        "properties": obj.properties,
        "metadata": obj.metadata and asdict(obj.metadata),
        "collection": obj.collection,
    }


def _datetime_encoder(dt: datetime) -> str:
    """Encode datetime to ISO format string."""
    return dt.isoformat()


def _encode_pydantic_model(obj: BaseModel) -> dict[str, object]:
    """Encode a pydantic model for Hypha RPC transport."""
    if isinstance(obj, _FilterValue):
        return {
            "_rtype": "pydantic_model",
            "_rvalue": obj.model_dump(mode="json"),
            "_rschema": obj.model_json_schema(),
            _SPECIAL_FILTER_KEY: True,
        }

    return {
        "_rtype": "pydantic_model",
        "_rvalue": obj.model_dump(mode="json"),
        "_rschema": obj.model_json_schema(),
    }


def _decode_pydantic_model(encoded_obj: dict[str, Any]) -> BaseModel:
    """Decode a pydantic model received through Hypha RPC."""
    if encoded_obj.get(_SPECIAL_FILTER_KEY):
        return _decode_filter_value(encoded_obj=encoded_obj)

    model_type = create_model_from_schema(encoded_obj["_rschema"])
    return model_type(**encoded_obj["_rvalue"])


def _decode_filter_value(encoded_obj: dict[str, Any]) -> _FilterValue:
    """Decode a Weaviate `_FilterValue` including its enum operator."""
    filter_data = encoded_obj["_rvalue"].copy()
    operator_value = filter_data.get("operator")
    if isinstance(operator_value, str):
        filter_data["operator"] = _Operator(operator_value)
    return _FilterValue(**filter_data)


def _encode_filter_tree(filter_object: _Filters) -> dict[str, object]:
    """Encode a Weaviate filter tree recursively."""
    if isinstance(filter_object, _FilterValue):
        return {
            _FILTER_KIND_KEY: _KIND_VALUE,
            _FILTER_VALUE_KEY: filter_object.model_dump(mode="json"),
        }

    if isinstance(filter_object, _FilterAnd):
        return {
            _FILTER_KIND_KEY: _KIND_AND,
            _FILTERS_KEY: [
                _encode_filter_tree(filter_object=nested_filter)
                for nested_filter in filter_object.filters
            ],
        }

    if isinstance(filter_object, _FilterOr):
        return {
            _FILTER_KIND_KEY: _KIND_OR,
            _FILTERS_KEY: [
                _encode_filter_tree(filter_object=nested_filter)
                for nested_filter in filter_object.filters
            ],
        }

    if _FilterNot is not None and isinstance(filter_object, _FilterNot):
        return {
            _FILTER_KIND_KEY: _KIND_NOT,
            _FILTERS_KEY: [
                _encode_filter_tree(filter_object=nested_filter)
                for nested_filter in filter_object.filters
            ],
        }

    object_type_name = type(filter_object).__name__
    message = f"{_ERROR_UNSUPPORTED_FILTER_TYPE}: {object_type_name}"
    raise UnsupportedFilterObjectTypeError(message)


def _decode_filter_tree(encoded_filter: Mapping[str, object]) -> _Filters:
    """Decode a recursively encoded Weaviate filter tree."""
    filter_kind = encoded_filter[_FILTER_KIND_KEY]

    if filter_kind == _KIND_VALUE:
        return _decode_filter_value_payload(encoded_filter=encoded_filter)

    nested_filters = _decode_nested_filters(encoded_filter=encoded_filter)
    if filter_kind == _KIND_AND:
        return _FilterAnd(filters=nested_filters)
    if filter_kind == _KIND_OR:
        return _FilterOr(filters=nested_filters)
    if filter_kind == _KIND_NOT:
        if _FilterNot is None:
            raise UnsupportedFilterKindError(_ERROR_UNSUPPORTED_FILTER_KIND)
        return _FilterNot(filter_=nested_filters[0])

    message = f"{_ERROR_UNSUPPORTED_FILTER_KIND}: {filter_kind!r}"
    raise UnsupportedFilterKindError(message)


def _decode_filter_value_payload(
    encoded_filter: Mapping[str, object],
) -> _FilterValue:
    """Decode an encoded `_FilterValue` payload."""
    raw_value = encoded_filter[_FILTER_VALUE_KEY]
    if not isinstance(raw_value, Mapping):
        raise InvalidEncodedFilterPayloadError(_ERROR_FILTER_VALUE_MAPPING)

    filter_data: dict[str, object] = dict(raw_value)
    operator_value = filter_data.get("operator")
    if isinstance(operator_value, str):
        filter_data["operator"] = _Operator(operator_value)
    return _FilterValue(**filter_data)


def _decode_nested_filters(encoded_filter: Mapping[str, object]) -> list[_Filters]:
    """Decode nested filters from an encoded AND/OR/NOT payload."""
    raw_filters = encoded_filter[_FILTERS_KEY]
    if not isinstance(raw_filters, list):
        raise InvalidEncodedFilterPayloadError(_ERROR_NESTED_FILTERS_LIST)

    decoded_filters: list[_Filters] = []
    for nested_filter in raw_filters:
        if not isinstance(nested_filter, Mapping):
            raise InvalidEncodedFilterPayloadError(
                _ERROR_NESTED_FILTER_MAPPING,
            )
        decoded_filters.append(_decode_filter_tree(encoded_filter=nested_filter))
    return decoded_filters


def _decode_filter_and(encoded_filter: dict[str, object]) -> _FilterAnd:
    """Decode an encoded `_FilterAnd` object."""
    decoded_filter = _decode_filter_tree(encoded_filter=encoded_filter)
    if not isinstance(decoded_filter, _FilterAnd):
        raise DecodedFilterTypeMismatchError(_ERROR_DECODED_NOT_AND)
    return decoded_filter


def _decode_filter_or(encoded_filter: dict[str, object]) -> _FilterOr:
    """Decode an encoded `_FilterOr` object."""
    decoded_filter = _decode_filter_tree(encoded_filter=encoded_filter)
    if not isinstance(decoded_filter, _FilterOr):
        raise DecodedFilterTypeMismatchError(_ERROR_DECODED_NOT_OR)
    return decoded_filter


def _decode_filter_not(encoded_filter: dict[str, object]) -> object:
    """Decode an encoded `_FilterNot` object."""
    if _FilterNot is None:
        raise UnsupportedFilterKindError(_ERROR_UNSUPPORTED_FILTER_KIND)

    decoded_filter = _decode_filter_tree(encoded_filter=encoded_filter)
    if not isinstance(decoded_filter, _FilterNot):
        raise DecodedFilterTypeMismatchError(_ERROR_DECODED_NOT_NOT)
    return decoded_filter


def register_weaviate_codecs(server: RemoteService) -> None:
    """Register all Weaviate codecs with the Hypha server."""
    server.register_codec(
        {
            "name": "uuid-uuid",
            "type": uuid.UUID,
            "encoder": encode_uuid,
            "decoder": uuid.UUID,
        },
    )

    server.register_codec(
        {
            "name": "pydantic_model",
            "type": BaseModel,
            "encoder": _encode_pydantic_model,
            "decoder": _decode_pydantic_model,
        },
    )

    server.register_codec(
        {
            "name": "weaviate_filter_and",
            "type": _FilterAnd,
            "encoder": _encode_filter_tree,
            "decoder": _decode_filter_and,
        },
    )

    server.register_codec(
        {
            "name": "weaviate_filter_or",
            "type": _FilterOr,
            "encoder": _encode_filter_tree,
            "decoder": _decode_filter_or,
        },
    )

    if _FilterNot is not None:
        server.register_codec(
            {
                "name": "weaviate_filter_not",
                "type": _FilterNot,
                "encoder": _encode_filter_tree,
                "decoder": _decode_filter_not,
            },
        )

    server.register_codec(
        {
            "name": "weaviate_object",
            "type": Object,
            "encoder": encode_object,
        },
    )

    server.register_codec(
        {
            "name": "datetime-datetime",
            "type": datetime,
            "encoder": _datetime_encoder,
            "decoder": datetime.fromisoformat,
        },
    )
