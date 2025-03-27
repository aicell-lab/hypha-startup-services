import os
import argparse
import asyncio
from hypha_rpc import connect_to_server
from hypha_startup_services.start_weaviate_service import register_weaviate


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
    parser.add_argument(
        "--port", type=int, help="Port number for the server connection"
    )

    args = parser.parse_args()
    connect_to_remote(args)


if __name__ == "__main__":
    main()
