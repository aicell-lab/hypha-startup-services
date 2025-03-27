"""Weaviate collection codecs for Hypha RPC serialization.

This module provides encoder and decoder functions for Weaviate collection objects,
allowing them to be serialized and transferred through Hypha RPC.
"""

from weaviate.collections import CollectionAsync
from weaviate.collections.classes.config import (
    CollectionConfig,
    CollectionConfigSimple,
)

JsonSerializable = (
    None
    | bool
    | int
    | float
    | str
    | list["JsonSerializable"]
    | dict[str, "JsonSerializable"]
)
type JsonObject = dict[str, JsonSerializable]


def encode_obj(
    _rtype: str,
    data: JsonObject,
    name: str = None,
) -> JsonObject:
    """Encode an object into a serializable format."""

    return {
        "_rtype": _rtype,
        "name": name,
        "data": data,
        "_rintf": True,
    }


async def encode_collection(collection: CollectionAsync) -> JsonObject:
    """Encode a collection by encoding its config. The config contains all necessary information."""

    config = await collection.config.get()
    return encode_obj(
        _rtype="weaviate-collection",
        data=config.to_dict(),
        name=collection.name,
    )


def decode_collection(encoded_collection: JsonObject) -> CollectionConfig:
    """Decode a serialized collection using its config."""
    assert encoded_collection["_rtype"] == "weaviate-collection"

    return encoded_collection


def encode_collection_config_simple(
    collection_config_simple: CollectionConfigSimple,
) -> JsonObject:
    """Encode a simple collection configuration."""
    return encode_obj(
        _rtype="weaviate-collection-config-simple",
        data=collection_config_simple.to_dict(),
    )


def decode_collection_config_simple(
    encoded_collection_config_simple: JsonObject,
) -> CollectionConfigSimple:
    """Decode a serialized simple collection configuration."""
    assert (
        encoded_collection_config_simple["_rtype"]
        == "weaviate-collection-config-simple"
    )
    data = encoded_collection_config_simple["data"]

    return CollectionConfigSimple(**data)


def encode_collection_config(
    collection_config: CollectionConfig,
) -> JsonObject:
    """Encode a full collection configuration."""
    return encode_obj(
        _rtype="weaviate-collection-config",
        data=collection_config.to_dict(),
    )


def decode_collection_config(
    collection_config_obj: JsonObject,
) -> CollectionConfig:
    """Decode a serialized collection configuration."""
    assert collection_config_obj["_rtype"] == "weaviate-collection-config"
    data = collection_config_obj["data"]

    return CollectionConfig(**data)


def register_weaviate_codecs(server):
    """Register all Weaviate codecs with the Hypha server."""
    server.register_codec(
        {
            "name": "weaviate-collection-config-simple",
            "type": CollectionConfigSimple,
            "encoder": encode_collection_config_simple,
            "decoder": decode_collection_config_simple,
        }
    )

    server.register_codec(
        {
            "name": "weaviate-collection-config",
            "type": CollectionConfig,
            "encoder": encode_collection_config,
            "decoder": decode_collection_config,
        }
    )

    server.register_codec(
        {
            "name": "weaviate-collection",
            "type": CollectionAsync,
            "encoder": encode_collection,
            "decoder": decode_collection,
        }
    )
