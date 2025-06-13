"""
Monkey patch for mem0's Weaviate integration to fix metadata and score issues.
"""

import json
import logging
import math
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def patch_weaviate_search():
    """
    Monkey patch the Weaviate.search method to fix metadata and score issues.
    """
    try:
        from mem0.vector_stores.weaviate import Weaviate, OutputData
        from weaviate.classes.query import Filter, MetadataQuery

        def patched_search(
            self,
            query: str,
            vectors: List[float],
            limit: int = 5,
            filters: Optional[Dict] = None,
        ) -> List[OutputData]:
            """
            Patched search method with proper metadata and score handling.
            """
            del query  # Unused in hybrid search, but keep for API compatibility

            collection = self.client.collections.get(str(self.collection_name))
            filter_conditions = []
            if filters:
                for key, value in filters.items():
                    if value and key in ["user_id", "agent_id", "run_id"]:
                        filter_conditions.append(Filter.by_property(key).equal(value))
            combined_filter = (
                Filter.all_of(filter_conditions) if filter_conditions else None
            )

            response = collection.query.hybrid(
                query="",
                vector=vectors,
                limit=limit,
                filters=combined_filter,
                # FIX: Include metadata in return_properties
                return_properties=[
                    "hash",
                    "created_at",
                    "updated_at",
                    "user_id",
                    "agent_id",
                    "run_id",
                    "data",
                    "category",
                    "metadata",
                ],
                # FIX: Request both score and distance for better score calculation
                return_metadata=MetadataQuery(score=True, distance=True),
            )

            results = []
            for obj in response.objects:
                payload = (
                    dict(obj.properties)
                    if hasattr(obj.properties, "__dict__")
                    else obj.properties
                )

                for id_field in ["run_id", "agent_id", "user_id"]:
                    if id_field in payload and payload[id_field] is None:
                        del payload[id_field]

                payload["id"] = str(obj.uuid).split("'")[0]

                # FIX: Better score calculation
                score = 1.0  # Default fallback
                if (
                    hasattr(obj.metadata, "distance")
                    and obj.metadata.distance is not None
                ):
                    # Convert distance to similarity score (closer to 0 = higher similarity)
                    # Use exponential decay for better score distribution
                    score = math.exp(-obj.metadata.distance)
                elif hasattr(obj.metadata, "score") and obj.metadata.score is not None:
                    score = float(obj.metadata.score)

                # FIX: Parse JSON metadata if it's a string
                if "metadata" in payload and isinstance(payload["metadata"], str):
                    try:
                        payload["metadata"] = json.loads(payload["metadata"])
                    except (json.JSONDecodeError, TypeError):
                        # Keep as string if not valid JSON
                        pass

                results.append(
                    OutputData(
                        id=str(obj.uuid),
                        score=score,
                        payload=payload,
                    )
                )

            return results

        # Apply the patch
        Weaviate.search = patched_search
        logger.info("Successfully patched Weaviate.search method")
        return True

    except ImportError as e:
        logger.warning("Could not patch Weaviate search: %s", str(e))
        return False
    except (AttributeError, TypeError) as e:
        logger.error("Error patching Weaviate search: %s", str(e))
        return False


def patch_weaviate_insert():
    """
    Monkey patch the Weaviate.insert method to handle metadata JSON conversion.
    """
    try:
        from mem0.vector_stores.weaviate import Weaviate
        import uuid as uuid_module
        from weaviate.util import get_valid_uuid

        def patched_insert(self, vectors, payloads=None, ids=None):
            """
            Patched insert method that converts dict metadata to JSON string.
            """

            with self.client.batch.fixed_size(batch_size=100) as batch:
                for idx, vector in enumerate(vectors):
                    object_id = (
                        ids[idx] if ids and idx < len(ids) else str(uuid_module.uuid4())
                    )
                    object_id = get_valid_uuid(object_id)

                    data_object = (
                        payloads[idx] if payloads and idx < len(payloads) else {}
                    )

                    # Ensure 'id' is not included in properties (it's used as the Weaviate object ID)
                    if "ids" in data_object:
                        del data_object["ids"]

                    # FIX: Extract custom metadata fields and store as JSON string under "metadata" field
                    # Mem0 stores custom metadata as top-level fields, but we need to collect them
                    mem0_system_fields = {
                        "data",
                        "hash",
                        "created_at",
                        "updated_at",
                        "user_id",
                        "agent_id",
                        "run_id",
                        "actor_id",
                        "role",
                        "metadata",
                        "category",
                    }
                    custom_metadata = {}

                    for key, value in list(data_object.items()):
                        if key not in mem0_system_fields:
                            custom_metadata[key] = value
                            # Remove from top-level to avoid duplication
                            del data_object[key]

                    # If there are custom metadata fields, store them as JSON string under "metadata"
                    if custom_metadata:
                        data_object["metadata"] = json.dumps(custom_metadata)

                    batch.add_object(
                        collection=self.collection_name,
                        properties=data_object,
                        uuid=object_id,
                        vector=vector,
                    )

        # Apply the patch
        Weaviate.insert = patched_insert
        logger.info("Successfully patched Weaviate.insert method")
        return True

    except ImportError as e:
        logger.warning("Could not patch Weaviate insert: %s", str(e))
        return False
    except (AttributeError, TypeError) as e:
        logger.error("Error patching Weaviate insert: %s", str(e))
        return False


def patch_weaviate_get():
    """
    Monkey patch the Weaviate.get method to fix metadata handling.
    """
    try:
        from mem0.vector_stores.weaviate import Weaviate, OutputData
        from weaviate.util import get_valid_uuid

        def patched_get(self, vector_id):
            """
            Patched get method with proper metadata handling.
            """
            vector_id = get_valid_uuid(vector_id)
            collection = self.client.collections.get(str(self.collection_name))

            response = collection.query.fetch_object_by_id(
                uuid=vector_id,
                # FIX: Include metadata in return_properties
                return_properties=[
                    "hash",
                    "created_at",
                    "updated_at",
                    "user_id",
                    "agent_id",
                    "run_id",
                    "data",
                    "category",
                    "metadata",
                ],
            )

            payload = (
                dict(response.properties)
                if hasattr(response.properties, "__dict__")
                else response.properties
            )
            payload["id"] = str(response.uuid).split("'")[0]

            # FIX: Parse JSON metadata if it's a string
            if "metadata" in payload and isinstance(payload["metadata"], str):
                try:
                    payload["metadata"] = json.loads(payload["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass

            results = OutputData(
                id=str(response.uuid).split("'")[0],
                score=1.0,
                payload=payload,
            )
            return results

        # Apply the patch
        Weaviate.get = patched_get
        logger.info("Successfully patched Weaviate.get method")
        return True

    except ImportError as e:
        logger.warning("Could not patch Weaviate get: %s", str(e))
        return False
    except (AttributeError, TypeError) as e:
        logger.error("Error patching Weaviate get: %s", str(e))
        return False


def apply_all_patches():
    """Apply all Weaviate patches."""
    logger.info("Attempting to apply Weaviate patches...")

    search_patched = patch_weaviate_search()
    insert_patched = patch_weaviate_insert()  # This was missing!
    get_patched = patch_weaviate_get()

    success_count = sum([search_patched, insert_patched, get_patched])

    if success_count == 3:
        logger.info("All Weaviate patches applied successfully")
        return True
    else:
        logger.warning("Some Weaviate patches failed to apply: %d/3", success_count)
        return False
