from functools import partial
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections import CollectionAsync
from weaviate.collections.classes.config import CollectionConfig
from weaviate.collections.classes.internal import QueryReturn
from dotenv import load_dotenv
from hypha_startup_services.weaviate_client import instantiate_and_connect
from hypha_startup_services.service_codecs import register_weaviate_codecs

load_dotenv()

WORKSPACE_DELIMITER = "____"
OLLAMA_MODEL = "llama3.2"  # For embeddings - using an available model
OLLAMA_LLM_MODEL = "llama3.2"  # For generation - using an available model


def format_workspace(workspace: str) -> str:
    workspace_formatted = workspace.replace("-", "_").capitalize()
    return workspace_formatted


def name_without_workspace(collection_name: str) -> str:
    if WORKSPACE_DELIMITER in collection_name:
        return collection_name.split(WORKSPACE_DELIMITER)[1]
    return collection_name


def config_minus_workspace(
    collection_config: CollectionConfig,
) -> dict:
    """Remove workspace from collection config."""
    config_dict = collection_config.to_dict()
    config_dict["class"] = name_without_workspace(config_dict["class"])
    return config_dict


async def collection_to_config_dict(collection: CollectionAsync) -> dict:
    """Convert collection to dict."""
    config = await collection.config.get()
    config_dict = config_minus_workspace(config)
    return config_dict


def assert_valid_collection_name(collection_name: str) -> None:
    assert (
        WORKSPACE_DELIMITER not in collection_name
    ), f"Collection name should not contain '{WORKSPACE_DELIMITER}'"


def stringify_keys(d: dict) -> dict:
    """Convert all keys in a dictionary to strings."""
    return {str(k): v for k, v in d.items()}


def full_collection_name(workspace: str, collection_name: str) -> str:
    assert_valid_collection_name(collection_name)

    workspace_formatted = format_workspace(workspace)
    return f"{workspace_formatted}{WORKSPACE_DELIMITER}{collection_name}"


def is_in_workspace(collection_name: str, workspace: str) -> bool:
    formatted_workspace = format_workspace(workspace)
    return collection_name.startswith(f"{formatted_workspace}{WORKSPACE_DELIMITER}")


async def collections_exists(
    client: WeaviateAsyncClient, collection_name: str, context: dict = None
) -> bool:
    assert context is not None
    workspace = context.get("ws")
    collection_name = full_collection_name(workspace, collection_name)
    return await client.collections.exists(collection_name)


async def collections_create(
    client: WeaviateAsyncClient, settings: dict, context: dict = None
) -> dict[str, Any]:
    assert context is not None
    workspace = context.get("ws")
    settings_with_workspace = settings.copy()
    settings_with_workspace["class"] = full_collection_name(
        workspace, settings_with_workspace["class"]
    )

    collection = await client.collections.create_from_dict(
        settings_with_workspace,
    )

    return await collection_to_config_dict(collection)


async def collections_list_all(
    client: WeaviateAsyncClient, context: dict = None
) -> dict[str, dict]:
    assert context is not None
    collections = await client.collections.list_all(simple=False)
    workspace = context.get("ws")
    return {
        name_without_workspace(coll_name): config_minus_workspace(coll_obj)
        for coll_name, coll_obj in collections.items()
        if is_in_workspace(coll_name, workspace)
    }


async def collections_get(
    client: WeaviateAsyncClient, name: str, context: dict = None
) -> dict[str, Any]:
    assert context is not None
    workspace = context.get("ws")
    collection = client.collections.get(full_collection_name(workspace, name))
    return await collection_to_config_dict(collection)


async def collections_delete(
    client: WeaviateAsyncClient, name: str | list[str], context: dict = None
):
    assert context is not None
    workspace = context.get("ws")
    if isinstance(name, str):
        to_delete = full_collection_name(workspace, name)
    else:
        to_delete = [
            full_collection_name(workspace, collection_name) for collection_name in name
        ]
    return await client.collections.delete(to_delete)


async def collection_data_insert_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    **kwargs,
):
    """Insert many objects into the collection.

    Forwards all kwargs to collection.data.insert_many().
    """
    assert "context" in kwargs, "Context is required"
    context = kwargs.pop("context")
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    response = await collection.data.insert_many(**kwargs)

    return {
        "elapsed_seconds": response.elapsed_seconds,
        "errors": stringify_keys(response.errors),
        "uuids": stringify_keys(response.uuids),
        "has_errors": response.has_errors,
    }


async def collection_query_near_vector(
    client: WeaviateAsyncClient,
    collection_name: str,
    **kwargs,
):
    """Query the collection using a vector.

    Forwards all kwargs to collection.query.near_vector().
    """
    assert "context" in kwargs, "Context is required"
    context = kwargs.pop("context")
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    response: QueryReturn = await collection.query.near_vector(**kwargs)

    return {
        "objects": response.objects,
    }


async def collection_query_fetch_objects(
    client: WeaviateAsyncClient,
    collection_name: str,
    **kwargs,
):
    """Query the collection to fetch objects.

    Forwards all kwargs to collection.query.fetch_objects().
    """
    assert "context" in kwargs, "Context is required"
    context = kwargs.pop("context")
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    response: QueryReturn = await collection.query.fetch_objects(**kwargs)

    return {
        "objects": response.objects,
    }


async def collection_query_hybrid(
    client: WeaviateAsyncClient,
    collection_name: str,
    **kwargs,
):
    """Query the collection using hybrid search.

    Forwards all kwargs to collection.query.hybrid().
    """
    assert "context" in kwargs, "Context is required"
    context = kwargs.pop("context")
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    response: QueryReturn = await collection.query.hybrid(**kwargs)
    return {
        "objects": response.objects,
    }


async def collection_generate_near_text(
    client: WeaviateAsyncClient,
    collection_name: str,
    **kwargs,
):
    """Query the collection using near text search.

    Forwards all kwargs to collection.query.near_text().
    """
    assert "context" in kwargs, "Context is required"
    context = kwargs.pop("context")
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    response = await collection.generate.near_text(**kwargs)
    return {
        "generate": response.generate,
        "objects": response.objects,
    }


async def register_weaviate(server, service_id: str):
    register_weaviate_codecs(server)
    weaviate_url = "https://hypha-weaviate.scilifelab-2-dev.sys.kth.se"
    weaviate_grpc_url = "https://hypha-weaviate-grpc.scilifelab-2-dev.sys.kth.se"

    http_host = weaviate_url.replace("https://", "").replace("http://", "")
    grpc_host = weaviate_grpc_url.replace("https://", "").replace("http://", "")
    is_secure = weaviate_url.startswith("https://")
    is_grpc_secure = weaviate_grpc_url.startswith("https://")
    client = await instantiate_and_connect(
        http_host, is_secure, grpc_host, is_grpc_secure
    )

    await server.register_service(
        {
            "name": "Hypha Weaviate Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": True,
            },
            "collections": {
                "create": partial(collections_create, client),
                "delete": partial(collections_delete, client),
                "list_all": partial(collections_list_all, client),
                "get": partial(collections_get, client),
                "exists": partial(collections_exists, client),
            },
            "data": {
                "insert_many": partial(collection_data_insert_many, client),
            },
            "query": {
                "near_vector": partial(collection_query_near_vector, client),
                "fetch_objects": partial(collection_query_fetch_objects, client),
                "hybrid": partial(collection_query_hybrid, client),
            },
            "generate": {
                "near_text": partial(collection_generate_near_text, client),
            },
        }
    )

    print(
        "Service registered at",
        f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
    )
