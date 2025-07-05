"""
Monkey patch for mem0's Weaviate integration to fix metadata and score issues.
"""

import json
import logging
import math
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Common constants
WEAVIATE_RETURN_PROPERTIES = [
    "hash",
    "created_at",
    "updated_at",
    "user_id",
    "agent_id",
    "run_id",
    "data",
    "category",
    "metadata",
]

MEM0_SYSTEM_FIELDS = {
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


def parse_metadata_field(payload: Dict) -> Dict:
    """Parse JSON metadata field if it's a string."""
    if "metadata" in payload and isinstance(payload["metadata"], str):
        try:
            payload["metadata"] = json.loads(payload["metadata"])
        except (json.JSONDecodeError, TypeError):
            # Keep as string if not valid JSON
            pass
    return payload


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
                return_properties=WEAVIATE_RETURN_PROPERTIES,
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
                payload = parse_metadata_field(payload)

                # FIX: Flatten nested metadata structure to avoid double nesting
                # If metadata contains another metadata dict, merge it up
                if "metadata" in payload and isinstance(payload["metadata"], dict):
                    nested_metadata = payload["metadata"]
                    # Check if this looks like our custom metadata (has entity_id, entity_type)
                    if (
                        "entity_id" in nested_metadata
                        or "entity_type" in nested_metadata
                    ):
                        # Remove the nested metadata and merge its contents into the top level
                        del payload["metadata"]
                        payload.update(nested_metadata)

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
                    # BUT: Only do this if there's no existing metadata field from mem0
                    custom_metadata = {}

                    # If there's already a metadata field, preserve it as-is
                    existing_metadata = data_object.get("metadata")

                    for key, value in list(data_object.items()):
                        if key not in MEM0_SYSTEM_FIELDS:
                            custom_metadata[key] = value
                            # Remove from top-level to avoid duplication
                            del data_object[key]

                    # Only set metadata from custom fields if:
                    # 1. There are custom metadata fields, AND
                    # 2. There's no existing metadata field from mem0
                    if custom_metadata and existing_metadata is None:
                        data_object["metadata"] = json.dumps(custom_metadata)
                    elif custom_metadata and existing_metadata is not None:
                        # If there's both existing metadata and custom fields,
                        # merge them (but this shouldn't happen in normal mem0 usage)
                        if isinstance(existing_metadata, dict):
                            # Merge custom fields into existing metadata dict
                            existing_metadata.update(custom_metadata)
                        else:
                            # If existing metadata is not a dict (e.g., JSON string),
                            # log a warning but don't overwrite it
                            logger.warning(
                                "Found both existing metadata and custom fields, preserving existing metadata"
                            )
                    # If no custom metadata but existing metadata exists, just leave it alone

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
                return_properties=WEAVIATE_RETURN_PROPERTIES,
            )

            payload = (
                dict(response.properties)
                if hasattr(response.properties, "__dict__")
                else response.properties
            )
            payload["id"] = str(response.uuid).split("'")[0]

            # Parse JSON metadata if it's a string
            payload = parse_metadata_field(payload)

            # FIX: Flatten nested metadata structure to avoid double nesting
            # If metadata contains another metadata dict, merge it up
            if "metadata" in payload and isinstance(payload["metadata"], dict):
                nested_metadata = payload["metadata"]
                # Check if this looks like our custom metadata (has entity_id, entity_type)
                if "entity_id" in nested_metadata or "entity_type" in nested_metadata:
                    # Remove the nested metadata and merge its contents into the top level
                    del payload["metadata"]
                    payload.update(nested_metadata)

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


def patch_weaviate_list():
    """
    Monkey patch the Weaviate.list method to fix metadata handling.
    """
    try:
        from mem0.vector_stores.weaviate import Weaviate, OutputData
        from weaviate.classes.query import Filter

        def patched_list(self, filters=None, limit=100):
            """
            Patched list method with proper metadata handling.
            """
            logger.info(
                "Weaviate.list called with filters=%s, limit=%s", filters, limit
            )

            collection = self.client.collections.get(str(self.collection_name))

            # Build filter conditions
            filter_conditions = []
            if filters:
                for key, value in filters.items():
                    if value and key in ["user_id", "agent_id", "run_id"]:
                        filter_conditions.append(Filter.by_property(key).equal(value))
            combined_filter = (
                Filter.all_of(filter_conditions) if filter_conditions else None
            )

            response = collection.query.fetch_objects(
                limit=limit,
                filters=combined_filter,
                return_properties=WEAVIATE_RETURN_PROPERTIES,
            )

            logger.info("Filtered query returned %s objects", len(response.objects))

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

                # Parse JSON metadata if it's a string
                payload = parse_metadata_field(payload)

                # FIX: Flatten nested metadata structure to avoid double nesting
                # If metadata contains another metadata dict, merge it up
                if "metadata" in payload and isinstance(payload["metadata"], dict):
                    nested_metadata = payload["metadata"]
                    # Check if this looks like our custom metadata (has entity_id, entity_type)
                    if (
                        "entity_id" in nested_metadata
                        or "entity_type" in nested_metadata
                    ):
                        # Remove the nested metadata and merge its contents into the top level
                        del payload["metadata"]
                        payload.update(nested_metadata)

                results.append(
                    OutputData(
                        id=str(obj.uuid),
                        score=1.0,  # Default score for list operations
                        payload=payload,
                    )
                )

            # Return wrapped in a list to match original Weaviate.list behavior
            # The mem0 code expects memories[0] to contain the actual results
            logger.info("Returning %s results wrapped in list", len(results))
            return [results]

        # Apply the patch
        Weaviate.list = patched_list  # type: ignore
        logger.info("Successfully patched Weaviate.list method")
        return True

    except ImportError as e:
        logger.warning("Could not patch Weaviate list: %s", str(e))
        return False
    except (AttributeError, TypeError) as e:
        logger.error("Error patching Weaviate list: %s", str(e))
        return False


def patch_mem0_get_all():
    """
    Monkey patch mem0's AsyncMemory.get_all method to fix the coroutine issue.

    The issue is that AsyncMemory.get_all uses executor.submit() to call
    self._get_all_from_vector_store, but that method is async and creates
    a coroutine that never gets awaited.
    """
    try:
        from mem0.memory.main import AsyncMemory
        import concurrent.futures
        import warnings
        from mem0.memory.telemetry import capture_event

        async def patched_get_all(
            self,
            *,
            user_id: Optional[str] = None,
            agent_id: Optional[str] = None,
            run_id: Optional[str] = None,
            filters: Optional[Dict] = None,
            limit: int = 100,
        ):
            """
            Patched get_all method that properly awaits async operations.
            """
            # Build filters manually since _build_filters_and_metadata might not be importable
            effective_filters = {}
            if user_id:
                effective_filters["user_id"] = user_id
            if agent_id:
                effective_filters["agent_id"] = agent_id
            if run_id:
                effective_filters["run_id"] = run_id
            if filters:
                effective_filters.update(filters)

            if not any(
                key in effective_filters for key in ("user_id", "agent_id", "run_id")
            ):
                raise ValueError(
                    "When 'conversation_id' is not provided (classic mode), "
                    "at least one of 'user_id', 'agent_id', or 'run_id' must be specified for get_all."
                )

            capture_event(
                "mem0.get_all",
                self,
                {
                    "limit": limit,
                    "keys": list(effective_filters.keys()),
                    "sync_type": "async",
                },
            )

            # FIX: Properly await the async operations instead of using executor.submit
            all_memories_result = await self._get_all_from_vector_store(
                effective_filters, limit
            )  # noqa: SLF001
            graph_entities_result = None

            if self.enable_graph:
                # For graph operations, we can still use executor if needed
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future_graph_entities = executor.submit(
                        self.graph.get_all, effective_filters, limit
                    )
                    graph_entities_result = future_graph_entities.result()

            if self.enable_graph:
                return {
                    "results": all_memories_result,
                    "relations": graph_entities_result,
                }

            if self.api_version == "v1.0":
                warnings.warn(
                    "The current get_all API output format is deprecated. "
                    "To use the latest format, set `api_version='v1.1'` (which returns a dict with a 'results' key). "
                    "The current format (direct list for v1.0) will be removed in mem0ai 1.1.0 and later versions.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                return all_memories_result
            else:
                return {"results": all_memories_result}

        # Apply the patch
        AsyncMemory.get_all = patched_get_all
        logger.info("Successfully patched AsyncMemory.get_all method")
        return True

    except ImportError as e:
        logger.warning("Could not patch AsyncMemory.get_all: %s", str(e))
        return False
    except (AttributeError, TypeError) as e:
        logger.error("Error patching AsyncMemory.get_all: %s", str(e))
        return False


def patch_mem0_get_all_from_vector_store():
    """
    Monkey patch mem0's AsyncMemory._get_all_from_vector_store method to handle list results.

    The async version only checks for tuple but the sync version checks for (tuple, list).
    This makes them consistent.
    """
    try:
        from mem0.memory.main import AsyncMemory
        import asyncio

        async def patched_get_all_from_vector_store(self, filters, limit):
            """
            Patched _get_all_from_vector_store method that handles list results correctly.
            """
            logger.info(
                "DEBUG: get_all_from_vector_store called with filters=%s, limit=%s",
                filters,
                limit,
            )

            memories_result = await asyncio.to_thread(
                self.vector_store.list, filters=filters, limit=limit
            )
            logger.info(
                "DEBUG: vector_store.list returned type=%s, content=%s",
                type(memories_result),
                memories_result,
            )

            actual_memories = (
                memories_result[0]
                if isinstance(memories_result, (tuple, list))
                and len(memories_result) > 0
                else memories_result
            )
            logger.debug(
                "actual_memories type=%s, length=%s",
                type(actual_memories),
                (
                    len(actual_memories)
                    if hasattr(actual_memories, "__len__")
                    else "no len"
                ),
            )

            promoted_payload_keys = [
                "user_id",
                "agent_id",
                "run_id",
                "actor_id",
                "role",
            ]
            core_and_promoted_keys = {
                "data",
                "hash",
                "created_at",
                "updated_at",
                "id",
                "metadata",  # Exclude existing metadata to prevent nesting
                *promoted_payload_keys,
            }

            formatted_memories = []
            for i, mem in enumerate(actual_memories):
                logger.debug(
                    "Processing memory %s: type=%s, id=%s, payload_type=%s",
                    i,
                    type(mem),
                    getattr(mem, "id", "no id"),
                    type(getattr(mem, "payload", None)),
                )
                from mem0.configs.base import MemoryItem

                # Handle different payload types safely
                payload = getattr(mem, "payload", {})
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (json.JSONDecodeError, ValueError):
                        logger.warning("Could not parse payload as JSON: %s", payload)
                        payload = {"data": payload}  # Fallback: treat as raw data
                elif not isinstance(payload, dict):
                    logger.warning("Unexpected payload type: %s", type(payload))
                    payload = {"data": str(payload)}  # Fallback: convert to string

                memory_item_dict = MemoryItem(
                    id=mem.id,
                    memory=payload.get("data", ""),
                    hash=payload.get("hash"),
                    created_at=payload.get("created_at"),
                    updated_at=payload.get("updated_at"),
                    metadata={},  # Will be set below if needed
                    score=getattr(mem, "score", 1.0),  # Default score
                ).model_dump(exclude={"score"})

                for key in promoted_payload_keys:
                    if key in payload:
                        memory_item_dict[key] = payload[key]

                # Get additional metadata, ensuring payload exists and is iterable
                # Exclude core fields and existing metadata to prevent nesting
                additional_metadata = {}
                if isinstance(payload, dict):
                    additional_metadata = {
                        k: v
                        for k, v in payload.items()
                        if k not in core_and_promoted_keys
                    }

                # If there's existing metadata in the payload, merge it properly
                existing_metadata = {}
                if isinstance(payload, dict) and "metadata" in payload:
                    existing_metadata = payload["metadata"]
                    if isinstance(existing_metadata, dict):
                        # Merge existing metadata with additional metadata
                        additional_metadata.update(existing_metadata)

                # Set the final metadata
                if additional_metadata:
                    memory_item_dict["metadata"] = additional_metadata

                formatted_memories.append(memory_item_dict)

            logger.debug("Returning %s formatted memories", len(formatted_memories))
            return formatted_memories

        # Apply the patch
        AsyncMemory._get_all_from_vector_store = patched_get_all_from_vector_store
        logger.info(
            "Successfully patched AsyncMemory._get_all_from_vector_store method"
        )
        return True

    except ImportError as e:
        logger.warning(
            "Could not patch AsyncMemory._get_all_from_vector_store: %s", str(e)
        )
        return False
    except (AttributeError, TypeError) as e:
        logger.error(
            "Error patching AsyncMemory._get_all_from_vector_store: %s", str(e)
        )
        return False


def patch_ollama_embed():
    """
    Monkey patch the Ollama embedding method to handle list inputs correctly.

    This fixes the issue where mem0 passes a list to the embed method instead of a string.
    """
    try:
        from mem0.embeddings.ollama import OllamaEmbedding
        from typing import Literal

        def patched_embed(
            self,
            text,
            memory_action: Optional[Literal["add", "search", "update"]] = None,
        ):
            """
            Patched embed method that handles both string and list inputs.
            """
            # memory_action is kept for compatibility but not used in this implementation
            del memory_action  # Suppress unused parameter warning

            # If text is a list, join it into a string
            if isinstance(text, list):
                logger.warning(
                    "Received list input for embedding, converting to string: %s", text
                )
                # Join list elements with space, handling various types
                text_parts = []
                for item in text:
                    if isinstance(item, str):
                        text_parts.append(item)
                    else:
                        text_parts.append(str(item))
                text = " ".join(text_parts)
            elif not isinstance(text, str):
                # Convert any other type to string
                logger.warning(
                    "Received non-string input for embedding, converting: %s (type: %s)",
                    text,
                    type(text),
                )
                text = str(text)

            # Call the original embeddings method with the fixed text
            response = self.client.embeddings(model=self.config.model, prompt=text)
            return response["embedding"]

        # Apply the patch
        OllamaEmbedding.embed = patched_embed
        logger.info("Successfully patched OllamaEmbedding.embed method")
        return True

    except ImportError as e:
        logger.warning("Could not patch OllamaEmbedding.embed: %s", str(e))
        return False
    except (AttributeError, TypeError) as e:
        logger.error("Error patching OllamaEmbedding.embed: %s", str(e))
        return False


def patch_mem0_embedding_cache():
    """
    Monkey patch to fix the issue where mem0 tries to use lists as dictionary keys.
    
    This patches the exact problematic operation by overriding dict behavior.
    """
    try:
        import mem0.memory.main
        import asyncio
        
        # Get the AsyncMemory class
        AsyncMemory = mem0.memory.main.AsyncMemory
        
        # Check if the method exists
        if not hasattr(AsyncMemory, '_add_to_vector_store'):
            logger.warning("Could not find AsyncMemory._add_to_vector_store method to patch")
            return False
            
        # Store the original method
        original_method = getattr(AsyncMemory, '_add_to_vector_store')
        
        async def patched_add_to_vector_store(self, messages, metadata, effective_filters, infer=True):
            """
            Patched version that handles unhashable list errors by creating a custom dictionary.
            """
            # First, try the original method as-is
            try:
                return await original_method(self, messages, metadata, effective_filters, infer)
            except TypeError as e:
                if "unhashable type: 'list'" not in str(e):
                    raise
                    
                logger.warning("Caught unhashable list error. Attempting fallback with custom dict.")
                
                # The issue is in mem0's internal logic where it uses new_mem_content as a dict key
                # Let's try to bypass this by temporarily monkey-patching dict assignment
                
                # This is a hacky solution but should work for the specific case
                try:
                    # Try calling the original method but with a custom exception handler
                    # We'll catch the specific line where the error occurs and handle it
                    return await self._patched_add_with_string_keys(messages, metadata, effective_filters, infer)
                except Exception as fallback_error:
                    logger.error(f"Fallback method also failed: {fallback_error}")
                    return []
        
        async def patched_add_with_string_keys(self, messages, metadata, effective_filters, infer=True):
            """
            Custom implementation that ensures all content used as keys is converted to strings.
            This is a simplified approach that doesn't rely on LLM-specific methods.
            """
            try:
                logger.info(f"Custom fallback: processing {len(messages)} messages")
                
                # Create simple facts from user messages without relying on LLM
                facts = []
                for msg in messages:
                    if isinstance(msg, dict) and msg.get('role') == 'user':
                        content = msg.get('content', '')
                        if content:
                            facts.append({
                                'content': content,
                                'category': 'conversation'
                            })
                            logger.info(f"Created fact from user message: {content[:100]}...")
                
                if not facts:
                    logger.info("No user messages found to create facts from")
                    return []
                
                logger.info(f"Created {len(facts)} facts from messages")
                
                # Process each fact and create memories
                created_memories = 0
                for fact in facts:
                    try:
                        # Get the content from the fact
                        new_mem_content = fact.get("content", "")
                        
                        # Convert to string if it's a list (safety check)
                        if isinstance(new_mem_content, list):
                            content_key = " ".join(str(item) for item in new_mem_content)
                            logger.info(f"Converted list content to string: {new_mem_content} -> {content_key}")
                        else:
                            content_key = str(new_mem_content)
                        
                        # Get embeddings for the content
                        embeddings = await asyncio.to_thread(
                            self.embedding_model.embed, content_key, "add"
                        )
                        
                        # Search for similar memories
                        search_results = await self.vector_store.search(
                            query="",
                            vectors=embeddings,
                            limit=5,
                            filters=effective_filters,
                        )
                        
                        similar_memories = [
                            result for result in search_results if result.score >= 0.7
                        ]
                        
                        if similar_memories:
                            # Update existing memory
                            logger.info(f"Found similar memory, updating: {similar_memories[0].id}")
                            existing_memory = similar_memories[0]
                            
                            # Create a simple updated memory without relying on LLM
                            updated_memory = {
                                'content': content_key,
                                'category': fact.get('category', 'conversation'),
                                'data': content_key  # mem0 stores content in 'data' field
                            }
                            
                            await self.vector_store.update(
                                vector_id=existing_memory.id,
                                vector=embeddings,
                                payload={**existing_memory.payload, **updated_memory, **metadata}
                            )
                            created_memories += 1
                        else:
                            # Create new memory
                            logger.info(f"Creating new memory for content: {content_key[:100]}...")
                            
                            # Create a simple memory structure that mem0 expects
                            new_memory = {
                                'content': content_key,
                                'category': fact.get('category', 'conversation'),
                                'data': content_key  # mem0 stores the actual content in 'data'
                            }
                            
                            await self.vector_store.insert(
                                vectors=[embeddings],
                                payloads=[{**new_memory, **metadata}]
                            )
                            created_memories += 1
                            
                    except Exception as fact_error:
                        logger.error(f"Error processing fact: {fact_error}")
                        continue
                
                logger.info(f"Successfully created/updated {created_memories} memories using simplified approach")
                return facts
                
            except Exception as e:
                logger.error(f"Error in patched_add_with_string_keys: {e}")
                return []
        
        # Add the custom method to the class
        setattr(AsyncMemory, '_patched_add_with_string_keys', patched_add_with_string_keys)
        
        # Apply the main patch
        setattr(AsyncMemory, '_add_to_vector_store', patched_add_to_vector_store)
        logger.info("Successfully patched AsyncMemory._add_to_vector_store with string-key fallback")
        return True
        
    except Exception as e:
        logger.error("Error patching mem0 embedding cache: %s", str(e))
        return False


def apply_all_patches():
    """Apply essential Weaviate patches for mem0."""
    logger.info("Applying Weaviate patches...")

    search_patched = patch_weaviate_search()
    insert_patched = patch_weaviate_insert()
    get_patched = patch_weaviate_get()
    list_patched = patch_weaviate_list()
    mem0_get_all_patched = patch_mem0_get_all()
    mem0_get_all_from_vector_store_patched = patch_mem0_get_all_from_vector_store()
    ollama_embed_patched = patch_ollama_embed()
    embedding_cache_patched = patch_mem0_embedding_cache()

    success_count = sum(
        [
            search_patched,
            insert_patched,
            get_patched,
            list_patched,
            mem0_get_all_patched,
            mem0_get_all_from_vector_store_patched,
            ollama_embed_patched,
            embedding_cache_patched,
        ]
    )

    if success_count == 8:
        logger.info("All essential Weaviate and mem0 patches applied successfully")
        return True
    else:
        logger.warning("Some patches failed to apply: %d/8", success_count)
        return False
