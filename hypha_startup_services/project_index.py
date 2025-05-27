from typing import Any
from functools import partial
from weaviate import WeaviateAsyncClient
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.weaviate_methods import (
    applications_exists,
    applications_create,
    generate_near_text,
    data_insert_many,
)


async def initialize(
    client: WeaviateAsyncClient,
    server: RemoteService,
    application_id: str,
    context: dict[str, Any],
) -> str:
    """Initialize the Project index service.

    This function sets up the Project index service with the necessary
    configurations and endpoints.
    Args:
        client (WeaviateAsyncClient): The Weaviate client instance.
        server (RemoteService): The remote service instance.
    """
    await applications_create(
        client=client,
        server=server,
        collection_name="Document",
        application_id=application_id,
        description=f"Document collection for application {application_id}",
        context=context,
    )

    return f"Project index service initialized for application {application_id}"


async def query_documents(
    client: WeaviateAsyncClient,
    server: RemoteService,
    application_id: str,
    query: str,
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    """Search for documents in the Project index.

    This function checks if the application exists, and if not, creates it.
    If the application exists, it generates a near-text search query to find
    relevant documents.
    Args:
        client (WeaviateAsyncClient): The Weaviate client instance.
        server (RemoteService): The remote service instance.
        application_id (str): The ID of the application to search in.
        query (str): The search query string.
        context (dict[str, Any]): Contextual information for the query.

    Returns:
        list[dict[str, Any]]: A list of documents matching the search query.
    """
    assert await applications_exists(
        server=server,
        collection_name="Document",
        application_id=application_id,
        context=context,
    ), "Application does not exist. Please call init(<application-id>) first."

    return_objects = await generate_near_text(
        client=client,
        server=server,
        collection_name="Document",
        application_id=application_id,
        query=query,
        context=context,
    )

    return return_objects["objects"]


async def add_documents(
    client: WeaviateAsyncClient,
    server: RemoteService,
    application_id: str,
    documents: list[dict[str, Any]],
    # TODO: add chunking config
    context: dict[str, Any],
) -> None:
    """Add documents to the Project index.

    This function is a placeholder and should be implemented with the actual
    logic to add documents.
    """
    assert await applications_exists(
        server=server,
        collection_name="Document",
        application_id=application_id,
        context=context,
    ), "Application does not exist. Please call init(<application-id>) first."
    # TODO: call semantic splitting
    await data_insert_many(
        client=client,
        server=server,
        collection_name="Document",
        application_id=application_id,
        objects=documents,
        context=context,
    )


async def register_index_service(
    server: RemoteService, client: WeaviateAsyncClient, service_id: str
) -> None:
    """Register the Project index service with the Hypha server.

    Sets up all service endpoints for collections, data operations, and queries.
    """

    await server.register_service(
        {
            "name": "Hypha Project Index Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": True,
            },
            "query": partial(query_documents, client, server),
            "init": partial(initialize, client, server),
            "add_documents": partial(add_documents, client, server),
        }
    )
