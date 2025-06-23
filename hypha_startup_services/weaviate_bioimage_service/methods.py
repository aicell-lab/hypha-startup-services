"""Service methods for the Weaviate BioImage service."""

import logging
from typing import Any, Dict

from weaviate import WeaviateAsyncClient
from hypha_rpc.rpc import RemoteService

from hypha_startup_services.weaviate_service.methods import (
    generate_near_text,
    query_fetch_objects,
)

logger = logging.getLogger(__name__)

# Constants for bioimage collections
BIOIMAGE_COLLECTION = "bioimage_data"
DEFAULT_APPLICATION_ID = "bioimage_app"


async def query(
    client: WeaviateAsyncClient,
    server: RemoteService,
    query_text: str,
    limit: int = 10,
    context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Query bioimage data using natural language.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance
        query_text: Natural language query
        limit: Maximum number of results
        context: Context containing caller information

    Returns:
        Dictionary with query results and generated response
    """
    return await generate_near_text(
        client=client,
        server=server,
        collection_name=BIOIMAGE_COLLECTION,
        application_id=DEFAULT_APPLICATION_ID,
        query=query_text,
        limit=limit,
        single_prompt=(
            "Based on the bioimage data below, provide a comprehensive answer about: {query}. "
            "Focus on nodes (facilities) and technologies that are relevant."
        ),
        grouped_task="Summarize the bioimage information to answer: {query}",
        grouped_properties=["text", "entity_type", "name"],
        context=context,
    )


async def get_entity(
    client: WeaviateAsyncClient,
    server: RemoteService,
    entity_id: str,
    context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Get a specific entity by ID.

    Args:
        client: WeaviateAsyncClient instance
        server: RemoteService instance
        entity_id: ID of the entity to retrieve
        context: Context containing caller information

    Returns:
        Dictionary with entity details
    """
    return await query_fetch_objects(
        client=client,
        server=server,
        collection_name=BIOIMAGE_COLLECTION,
        application_id=DEFAULT_APPLICATION_ID,
        where_filter={"path": ["entity_id"], "operator": "Equal", "value": entity_id},
        limit=10,
        context=context,
    )
