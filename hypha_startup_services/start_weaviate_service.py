from functools import partial
import uuid
from typing import Any
from weaviate import WeaviateAsyncClient
from weaviate.collections import CollectionAsync
from weaviate.collections.classes.config import CollectionConfig
from weaviate.collections.classes.internal import QueryReturn, GenerativeReturn
from weaviate.collections.classes.batch import DeleteManyReturn
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


def full_collection_name_single(workspace: str, collection_name: str) -> str:
    assert_valid_collection_name(collection_name)

    workspace_formatted = format_workspace(workspace)
    return f"{workspace_formatted}{WORKSPACE_DELIMITER}{collection_name}"


def full_collection_name(
    name: str | list[str], workspace_or_context: str | dict[str, Any]
) -> str:
    """Acquire a collection name from the client."""
    workspace = (
        ws_from_context(workspace_or_context)
        if isinstance(workspace_or_context, dict)
        else workspace_or_context
    )
    if isinstance(name, list):
        return [full_collection_name_single(workspace, n) for n in name]
    return full_collection_name_single(workspace, name)


def is_in_workspace(collection_name: str, workspace: str) -> bool:
    formatted_workspace = format_workspace(workspace)
    return collection_name.startswith(f"{formatted_workspace}{WORKSPACE_DELIMITER}")


def call_collection_method(
    client: WeaviateAsyncClient,
    collection_name: str,
    method_name: str,
    *args,
    **kwargs,
) -> Any:
    """Call a method on the collection."""
    collection = acquire_collection(client, collection_name)
    method = getattr(collection, method_name)
    return method(*args, **kwargs)


async def collections_exists(
    client: WeaviateAsyncClient, collection_name: str, context: dict = None
) -> bool:
    collection_name = full_collection_name(collection_name, context)
    return await client.collections.exists(collection_name)


def ws_from_context(context: dict) -> str:
    """Get workspace from context."""
    assert context is not None
    workspace = context.get("ws")
    return workspace


def acquire_collection(
    client: WeaviateAsyncClient, collection_name: str, context: dict = None
) -> CollectionAsync:
    """Acquire a collection from the client."""
    collection_name = full_collection_name(collection_name, context)
    return client.collections.get(collection_name)


def objects_without_workspace(objects: list[dict]) -> list[dict]:
    """Remove workspace from object IDs."""
    for obj in objects:
        obj.collection = name_without_workspace(obj.collection)
    return objects


async def collections_create(
    client: WeaviateAsyncClient, settings: dict, context: dict = None
) -> dict[str, Any]:
    settings_with_workspace = settings.copy()
    settings_with_workspace["class"] = full_collection_name(
        settings_with_workspace["class"], context
    )

    collection = await client.collections.create_from_dict(
        settings_with_workspace,
    )

    return await collection_to_config_dict(collection)


async def collections_list_all(
    client: WeaviateAsyncClient, context: dict = None
) -> dict[str, dict]:
    workspace = ws_from_context(context)
    collections = await client.collections.list_all(simple=False)
    return {
        name_without_workspace(coll_name): config_minus_workspace(coll_obj)
        for coll_name, coll_obj in collections.items()
        if is_in_workspace(coll_name, workspace)
    }


async def collections_get(
    client: WeaviateAsyncClient, name: str, context: dict = None
) -> dict[str, Any]:
    collection = acquire_collection(client, name, context)
    return await collection_to_config_dict(collection)


async def collections_delete(
    client: WeaviateAsyncClient, name: str | list[str], context: dict = None
) -> None:
    collection_name = full_collection_name(name, context)
    await client.collections.delete(collection_name)


async def collection_data_insert_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Insert many objects into the collection.

    Forwards all kwargs to collection.data.insert_many().
    """
    collection = acquire_collection(client, collection_name, context)
    response = await collection.data.insert_many(**kwargs)

    return {
        "elapsed_seconds": response.elapsed_seconds,
        "errors": stringify_keys(response.errors),
        "uuids": stringify_keys(response.uuids),
        "has_errors": response.has_errors,
    }


async def collection_data_insert(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> uuid.UUID:
    """Insert an object into the collection.

    Forwards all kwargs to collection.data.insert().
    """
    collection = acquire_collection(client, collection_name, context)
    return await collection.data.insert(**kwargs)


async def collection_query_near_vector(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using a vector.

    Forwards all kwargs to collection.query.near_vector().
    """
    collection = acquire_collection(client, collection_name, context)
    response: QueryReturn = await collection.query.near_vector(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def collection_query_fetch_objects(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Query the collection to fetch objects.

    Forwards all kwargs to collection.query.fetch_objects().
    """
    collection = acquire_collection(client, collection_name, context)
    response: QueryReturn = await collection.query.fetch_objects(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def collection_query_hybrid(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using hybrid search.

    Forwards all kwargs to collection.query.hybrid().
    """
    collection = acquire_collection(client, collection_name, context)
    response: QueryReturn = await collection.query.hybrid(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
    }


async def collection_generate_near_text(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Query the collection using near text search.

    Forwards all kwargs to collection.query.near_text().
    """
    collection = acquire_collection(client, collection_name, context)
    response: GenerativeReturn = await collection.generate.near_text(**kwargs)

    return {
        "objects": objects_without_workspace(response.objects),
        "generated": response.generated,
    }


async def collection_data_update(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> None:
    """Update an object in the collection.

    Forwards all kwargs to collection.data.update().
    """
    collection = acquire_collection(client, collection_name, context)
    await collection.data.update(**kwargs)


async def collection_data_delete_by_id(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> bool:
    """Delete an object by ID from the collection.

    Forwards all kwargs to collection.data.delete_by_id().
    """
    collection = acquire_collection(client, collection_name, context)
    await collection.data.delete_by_id(**kwargs)


async def collection_data_delete_many(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> dict[str, Any]:
    """Delete many objects from the collection.

    Forwards all kwargs to collection.data.delete_many().
    """
    collection = acquire_collection(client, collection_name, context)
    response: DeleteManyReturn = await collection.data.delete_many(**kwargs)

    return {
        "failed": response.failed,
        "matches": response.matches,
        "objects": objects_without_workspace(response.objects),
        "successful": response.successful,
    }


async def collection_data_exists(
    client: WeaviateAsyncClient,
    collection_name: str,
    context: dict[str, Any],
    **kwargs,
) -> bool:
    """Check if an object exists in the collection.

    Forwards all kwargs to collection.data.exists().
    """
    collection = acquire_collection(client, collection_name, context)
    return await collection.data.exists(**kwargs)


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
                "insert": partial(collection_data_insert, client),
                "update": partial(collection_data_update, client),
                "delete_by_id": partial(collection_data_delete_by_id, client),
                "delete_many": partial(collection_data_delete_many, client),
                "exists": partial(collection_data_exists, client),
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
