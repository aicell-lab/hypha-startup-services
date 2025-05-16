"""Weaviate collection codecs for Hypha RPC serialization.

This module provides encoder and decoder functions for Weaviate collection objects,
allowing them to be serialized and transferred through Hypha RPC.
"""

import uuid
from dataclasses import asdict
from weaviate.collections.classes.internal import _Object


def register_weaviate_codecs(server):
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
            "type": _Object,
            "encoder": lambda obj: {
                "uuid": obj.uuid.hex,
                "vector": obj.vector,
                "properties": obj.properties,
                "metadata": obj.metadata and asdict(obj.metadata),
                "collection": obj.collection,
            },
        }
    )
