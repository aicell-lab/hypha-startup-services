"""Weaviate collection codecs for Hypha RPC serialization.

This module provides encoder and decoder functions for Weaviate collection objects,
allowing them to be serialized and transferred through Hypha RPC.
"""

import uuid
from dataclasses import asdict

from hypha_rpc.rpc import RemoteService  # type: ignore
from hypha_rpc.utils.pydantic import create_model_from_schema
from pydantic import BaseModel
from weaviate.collections.classes.filters import _FilterValue, _Operator
from weaviate.collections.classes.internal import Object


def register_weaviate_codecs(server: RemoteService) -> None:
    """Register all Weaviate codecs with the Hypha server."""
    server.register_codec(
        {
            "name": "uuid-uuid",
            "type": uuid.UUID,
            "encoder": lambda obj: obj.hex,
            "decoder": uuid.UUID,
        },
    )

    # Override the built-in Pydantic codec to handle _FilterValue specially
    def custom_pydantic_encoder(obj):
        """Custom Pydantic encoder that handles _FilterValue specially."""
        if isinstance(obj, _FilterValue):
            # Use model_dump("json") as suggested by maintainer
            return {
                "_rtype": "pydantic_model",
                "_rvalue": obj.model_dump(mode="json"),
                "_rschema": obj.model_json_schema(),
                "_special_filter": True,  # Mark this as special
            }

        return {
            "_rtype": "pydantic_model",
            "_rvalue": obj.model_dump(mode="json"),
            "_rschema": obj.model_json_schema(),
        }

    def custom_pydantic_decoder(encoded_obj):
        """Custom Pydantic decoder that handles _FilterValue specially."""
        if encoded_obj.get("_special_filter"):
            # Special handling for _FilterValue: convert operator string back to enum
            data = encoded_obj["_rvalue"].copy()
            if "operator" in data and isinstance(data["operator"], str):
                data["operator"] = _Operator(data["operator"])
            return _FilterValue(**data)

        model_type = create_model_from_schema(encoded_obj["_rschema"])
        return model_type(**encoded_obj["_rvalue"])

    server.register_codec(
        {
            "name": "pydantic_model",
            "type": BaseModel,
            "encoder": custom_pydantic_encoder,
            "decoder": custom_pydantic_decoder,
        },
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
        },
    )
