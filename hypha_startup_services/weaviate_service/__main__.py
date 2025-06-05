import os
import sys
import subprocess
import argparse
from argparse import Namespace
import asyncio
from websockets.exceptions import InvalidURI
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService, RemoteException
from hypha_startup_services.weaviate_service.register_service import (
    register_weaviate,
)
from hypha_startup_services.weaviate_service.utils.constants import (
    DEFAULT_LOCAL_HOST,
    DEFAULT_LOCAL_EXISTING_HOST,
    DEFAULT_LOCAL_PORT,
    DEFAULT_REMOTE_URL,
    DEFAULT_SERVICE_ID,
)


async def register_to_existing_server(
    provided_url: str, port: int | None, service_id: str
) -> None:
    server_url = provided_url if port is None else f"{provided_url}:{port}"
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"
    server = await connect_to_server({"server_url": server_url, "token": token})

    if not isinstance(server, RemoteService):
        raise ValueError("Server is not a RemoteService instance.")

    await register_weaviate(server, service_id)


async def run_local_server(
    server_url: str, port: int, service_id_existing_server: str
) -> None:
    """Run the local server with the provided arguments.

    Args:
        server_url (str): The URL of the server to connect to.
        port (int): The port of the server.
        service_id_existing_server (str): The ID of the service.
    """
    try:
        await register_to_existing_server(
            DEFAULT_LOCAL_EXISTING_HOST,
            port=port,
            service_id=service_id_existing_server,
        )
    except (ConnectionRefusedError, InvalidURI, RemoteException) as e:
        print("HEREE")
        print(f"Failed to connect to the server at {server_url}:{port}. Error: {e}")
        command = [
            sys.executable,
            "-m",
            "hypha.server",
            f"--host={server_url}",
            f"--port={port}",
            "--startup-functions=hypha_startup_services.weaviate_service.register_service:register_weaviate",
        ]
        subprocess.run(command, check=True)


def connect_to_remote(args: Namespace) -> None:
    """Connect to remote server and register aria tools service."""
    loop = asyncio.get_event_loop()
    loop.create_task(
        register_to_existing_server(args.server_url, args.port, args.service_id)
    )
    loop.run_forever()


def run_locally(args: Namespace) -> None:
    """Run the local server with the provided arguments.

    Args:
        args (Namespace): The arguments parsed from the command line.
    """
    asyncio.run(
        run_local_server(args.server_url, args.port, args.service_id_existing_server)
    )


def main():
    """Main entry point for aria-tools commands."""
    parser = argparse.ArgumentParser(description="Aria tools launch commands.")

    subparsers = parser.add_subparsers()

    # Local server parser
    parser_local = subparsers.add_parser("local")
    parser_local.add_argument("--server-url", type=str, default=DEFAULT_LOCAL_HOST)
    parser_local.add_argument("--port", type=int, default=DEFAULT_LOCAL_PORT)
    parser_local.add_argument(
        "--service-id-existing-server",
        type=str,
        default=DEFAULT_SERVICE_ID,
    )
    parser_local.set_defaults(func=run_locally)

    # Remote server parser
    parser_remote = subparsers.add_parser("remote")
    parser_remote.add_argument("--server-url", type=str, default=DEFAULT_REMOTE_URL)
    parser_remote.add_argument(
        "--port", type=int, help="Port number for the server connection"
    )
    parser_remote.add_argument(
        "--service-id",
        type=str,
        default=DEFAULT_SERVICE_ID,
    )
    parser_remote.set_defaults(func=connect_to_remote)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
