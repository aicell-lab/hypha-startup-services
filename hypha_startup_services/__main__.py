import os
import argparse
from argparse import Namespace
import asyncio
from hypha_rpc import connect_to_server  # type: ignore
from hypha_rpc.rpc import RemoteService  # type: ignore
from hypha_startup_services.register_weaviate_service import register_weaviate


async def register_to_existing_server(
    provided_url: str, service_id: str, port: int | None = None
):
    server_url = provided_url if port is None else f"{provided_url}:{port}"
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server = await connect_to_server({"server_url": server_url, "token": token})

    if not isinstance(server, RemoteService):
        raise ValueError("Server is not a RemoteService instance.")

    await register_weaviate(server, service_id)


def connect_to_remote(args: Namespace):
    server_url = args.server_url
    service_id = args.service_id
    port = args.port
    loop = asyncio.get_event_loop()
    loop.create_task(register_to_existing_server(server_url, service_id, port))
    loop.run_forever()


def main():
    parser = argparse.ArgumentParser(
        description="Hypha startup services launch commands."
    )

    parser.add_argument("--server-url", type=str, default="https://hypha.aicell.io")
    parser.add_argument(
        "--port", type=int, help="Port number for the server connection"
    )
    parser.add_argument(
        "--service-id",
        type=str,
        default="mem0",
    )

    args = parser.parse_args()
    connect_to_remote(args)


if __name__ == "__main__":
    main()
