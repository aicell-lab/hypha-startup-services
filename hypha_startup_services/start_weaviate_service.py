import os
import argparse
import asyncio
from hypha_rpc import connect_to_server
from dotenv import load_dotenv
from weaviate.classes.config import Configure
from hypha_startup_services.service_codecs import register_weaviate_codecs
from weaviate_client import client, OLLAMA_ENDPOINT

load_dotenv()

WORKSPACE_DELIMITER = "____"
OLLAMA_MODEL = "llama3.2"  # For embeddings - using an available model
OLLAMA_LLM_MODEL = "llama3.2"  # For generation - using an available model

generative_config = Configure.Generative.ollama(
    api_endpoint=OLLAMA_ENDPOINT, model=OLLAMA_LLM_MODEL
)

vectorizer_config = [
    Configure.NamedVectors.text2vec_ollama(
        name="title_vector",
        source_properties=["title"],
        api_endpoint=OLLAMA_ENDPOINT,
        model=OLLAMA_MODEL,
    ),
    Configure.NamedVectors.text2vec_ollama(
        name="description_vector",
        source_properties=["description"],
        api_endpoint=OLLAMA_ENDPOINT,
        model=OLLAMA_MODEL,
    ),
]


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


async def create_collection(settings: dict, context: dict = None):
    assert context is not None
    workspace = context.get("ws")
    settings_fixed_name = settings.copy()
    settings_fixed_name["class"] = full_collection_name(
        workspace, settings_fixed_name["class"]
    )

    return client.collections.create_from_dict(
        settings_fixed_name,
    )


async def list_collections(simple: bool = None, context: dict = None):
    assert context is not None
    collections = client.collections.list_all(simple=simple)
    workspace = context.get("ws")
    return {
        coll_name: coll_obj
        for coll_name, coll_obj in collections.items()
        if is_in_workspace(coll_name, workspace)
    }


def get_collection(name: str, context: dict = None):
    assert context is not None
    workspace = context.get("ws")
    return client.collections.get(full_collection_name(workspace, name))


def delete_collection(name: str | list[str], context: dict = None):
    assert context is not None
    workspace = context.get("ws")
    if isinstance(name, str):
        to_delete = full_collection_name(workspace, name)
    else:
        to_delete = [
            full_collection_name(workspace, collection_name) for collection_name in name
        ]
    return client.collections.delete(to_delete)


async def insert_many(collection_name: str, data: list[dict], context: dict = None):
    """Insert many objects into the collection."""
    await client.collections.get(collection_name).data.insert_many(data)
    return True


async def register_weaviate(server):
    # Register all codecs using the helper function
    register_weaviate_codecs(server)

    # Register the service
    service_info = await server.register_service(
        {
            "name": "Hypha Weaviate Service",
            "id": "weaviate",
            "config": {
                "visibility": "public",
                "require_context": True,
            },
            "collections": {
                "create": create_collection,
                "delete": delete_collection,
                "list_all": list_collections,
                "get": get_collection,
            },
            "data": {
                "insert_many": insert_many,
            },
        }
    )
    print("Service registered: ", service_info.id)


async def register_to_existing_server(provided_url, port=None):
    server_url = provided_url if port is None else f"{provided_url}:{port}"
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server = await connect_to_server({"server_url": server_url, "token": token})
    await register_weaviate(server)


def connect_to_remote(args):
    server_url = args.server_url
    loop = asyncio.get_event_loop()
    loop.create_task(register_to_existing_server(server_url))
    loop.run_forever()


def main():
    parser = argparse.ArgumentParser(description="Aria tools launch commands.")

    parser.add_argument("--server-url", type=str, default="https://hypha.aicell.io")

    args = parser.parse_args()
    connect_to_remote(args)


if __name__ == "__main__":
    main()
