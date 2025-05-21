"""Weaviate collection codecs for Hypha RPC serialization.

This module provides encoder and decoder functions for Weaviate collection objects,
allowing them to be serialized and transferred through Hypha RPC.
"""

import uuid
from dataclasses import asdict
from weaviate.collections.classes.internal import Object
from weaviate.collections.classes.filters import _FilterValue
from hypha_rpc.rpc import RemoteService  # type: ignore


def register_weaviate_codecs(server: RemoteService) -> None:
    """Register all Weaviate codecs with the Hypha server."""

    server.register_codec(
        {
            "name": "uuid-uuid",
            "type": uuid.UUID,
            "encoder": lambda obj: obj.hex,
            "decoder": uuid.UUID,
        }
    )

    server.register_codec(
        {
            "name": "weaviate_object",
            "type": Object,
            "encoder": lambda obj: {
                "uuid": obj.uuid.hex,
                "vector": obj.vector,
                "properties": obj.properties,
                "metadata": obj.metadata and asdict(obj.metadata),
                "collection": obj.collection,
            },
        }
    )

    server.register_codec(
        {
            "name": "filtervalue",
            "type": _FilterValue,
            "encoder": lambda obj: {
                "_rintf": True,
                "_rtype": "filtervalue",
                "value": obj.value,
                "operator": obj.operator,
                "target": obj.target,
            },
            "decoder": lambda obj: _FilterValue(
                value=obj["value"],
                operator=obj["operator"],
                target=obj["target"],
            ),
        }
    )
