"""Service methods for the Weaviate BioImage service."""

import logging
from typing import Any, Dict, Callable, Coroutine

from weaviate import WeaviateAsyncClient
from hypha_rpc.rpc import RemoteService
from hypha_rpc.utils.schema import schema_function
from pydantic import Field

from hypha_startup_services.weaviate_service.methods import (
    generate_near_text,
    query_fetch_objects,
)

logger = logging.getLogger(__name__)

# Constants for bioimage collections
BIOIMAGE_COLLECTION = "bioimage_data"
DEFAULT_APPLICATION_ID = "bioimage_app"


def create_query(
    client: WeaviateAsyncClient,
    server: RemoteService,
) -> Callable[..., Coroutine[Any, Any, Dict[str, Any]]]:
    """Create a schema function for querying bioimage data with dependency injection."""

    @schema_function
    async def query(
        query_text: str = Field(
            description="Natural language query to search bioimage data"
        ),
        limit: int = Field(
            default=10, description="Maximum number of results to return"
        ),
        context: Dict[str, Any] | None = Field(
            default=None, description="Context containing caller information"
        ),
    ) -> Dict[str, Any]:
        """Query bioimage data using natural language.

        Args:
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

    return query


def create_get_entity(
    client: WeaviateAsyncClient,
    server: RemoteService,
) -> Callable[..., Coroutine[Any, Any, Dict[str, Any]]]:
    """Create a schema function for getting entity by ID with dependency injection."""

    @schema_function
    async def get_entity(
        entity_id: str = Field(description="ID of the entity to retrieve"),
        context: Dict[str, Any] | None = Field(
            default=None, description="Context containing caller information"
        ),
    ) -> Dict[str, Any]:
        """Get a specific entity by ID.

        Args:
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
            where_filter={
                "path": ["entity_id"],
                "operator": "Equal",
                "value": entity_id,
            },
            limit=10,
            context=context,
        )

    return get_entity
