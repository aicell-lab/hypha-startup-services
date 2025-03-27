from functools import partial
from dotenv import load_dotenv
from hypha_startup_services.service_codecs import register_weaviate_codecs
from hypha_startup_services.weaviate_client import instantiate_and_connect
from weaviate import WeaviateAsyncClient

load_dotenv()

WORKSPACE_DELIMITER = "____"
OLLAMA_MODEL = "llama3.2"  # For embeddings - using an available model
OLLAMA_LLM_MODEL = "llama3.2"  # For generation - using an available model


def format_workspace(workspace: str):
    workspace_formatted = workspace.replace("-", "_").capitalize()
    return workspace_formatted


def assert_valid_collection_name(collection_name: str):
    assert (
        WORKSPACE_DELIMITER not in collection_name
    ), f"Collection name should not contain '{WORKSPACE_DELIMITER}'"


def full_collection_name(workspace: str, collection_name: str):
    assert_valid_collection_name(collection_name)

    workspace_formatted = format_workspace(workspace)
    return f"{workspace_formatted}{WORKSPACE_DELIMITER}{collection_name}"


def is_in_workspace(collection_name: str, workspace: str):
    formatted_workspace = format_workspace(workspace)
    return collection_name.startswith(f"{formatted_workspace}{WORKSPACE_DELIMITER}")


async def collection_exists(
    client: WeaviateAsyncClient, collection_name: str, context: dict = None
) -> bool:
    assert context is not None
    workspace = context.get("ws")
    collection_name = full_collection_name(workspace, collection_name)
    return await client.collections.exists(collection_name)


async def create_collection(
    client: WeaviateAsyncClient, settings: dict, context: dict = None
):
    assert context is not None
    workspace = context.get("ws")
    settings_fixed_name = settings.copy()
    settings_fixed_name["class"] = full_collection_name(
        workspace, settings_fixed_name["class"]
    )

    return await client.collections.create_from_dict(
        settings_fixed_name,
    )


async def list_collections(
    client: WeaviateAsyncClient, simple: bool = None, context: dict = None
):
    assert context is not None
    collections = await client.collections.list_all(simple=simple)
    workspace = context.get("ws")
    return {
        coll_name: coll_obj
        for coll_name, coll_obj in collections.items()
        if is_in_workspace(coll_name, workspace)
    }


async def get_collection(client: WeaviateAsyncClient, name: str, context: dict = None):
    assert context is not None
    workspace = context.get("ws")
    return client.collections.get(full_collection_name(workspace, name))


async def delete_collection(
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
    data: list[dict],
    context: dict = None,
):
    """Insert many objects into the collection."""
    assert context is not None
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    return await collection.data.insert_many(data)


async def collection_query_near_vector(
    client: WeaviateAsyncClient,
    collection_name: str,
    data: dict,
    context: dict = None,
):
    """Query the collection using a vector."""
    assert context is not None
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    return await collection.query.near_vector(data)


async def collection_query_fetch_objects(
    client: WeaviateAsyncClient,
    collection_name: str,
    data: dict,
    context: dict = None,
):
    """Query the collection using a vector."""
    assert context is not None
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    return await collection.query.fetch_objects(data)


async def collection_query_hybrid(
    client: WeaviateAsyncClient,
    collection_name: str,
    data: dict,
    context: dict = None,
):
    """Query the collection using a vector."""
    assert context is not None
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    return await collection.query.hybrid(data)


async def collection_query_near_text(
    client: WeaviateAsyncClient,
    collection_name: str,
    data: dict,
    context: dict = None,
):
    """Query the collection using a vector."""
    assert context is not None
    workspace = context.get("ws")
    collection = client.collections.get(
        full_collection_name(workspace, collection_name)
    )
    return await collection.query.near_text(data)


async def register_weaviate(server):
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

    service_id = "weaviate"
    await server.register_service(
        {
            "name": "Hypha Weaviate Service",
            "id": service_id,
            "config": {
                "visibility": "public",
                "require_context": True,
            },
            "collections": {
                "create": partial(create_collection, client),
                "delete": partial(delete_collection, client),
                "list_all": partial(list_collections, client),
                "get": partial(get_collection, client),
                "exists": partial(collection_exists, client),
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
                "near_text": partial(collection_query_near_text, client),
            },
        }
    )

    print(
        "Service registered at",
        f"{server.config.public_base_url}/{server.config.workspace}/services/{service_id}",
    )
