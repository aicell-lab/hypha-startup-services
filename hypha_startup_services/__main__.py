"""Main entry point for hypha startup services."""

import argparse
from argparse import Namespace

from .common.constants import (
    DEFAULT_LOCAL_HOST,
    DEFAULT_LOCAL_PORT,
    DEFAULT_REMOTE_URL,
)
from .common.server_utils import connect_to_remote, run_locally
from .common.service_registry import service_registry, register_services


def create_parser() -> argparse.ArgumentParser:
    """Create the main argument parser."""
    parser = argparse.ArgumentParser(
        description="Hypha startup services launcher", prog="hypha-startup-services"
    )

    parser.add_argument(
        "service",
        choices=["weaviate", "mem0"],
        help="Service to start (weaviate or mem0)",
    )

    subparsers = parser.add_subparsers(dest="mode", help="Running mode")

    # Local server parser
    parser_local = subparsers.add_parser("local", help="Run local server")
    parser_local.add_argument("--server-url", type=str, default=DEFAULT_LOCAL_HOST)
    parser_local.add_argument("--port", type=int, default=DEFAULT_LOCAL_PORT)
    parser_local.add_argument(
        "--service-id-existing-server",
        type=str,
        help="Service ID when connecting to existing server",
    )

    # Remote server parser
    parser_remote = subparsers.add_parser("remote", help="Connect to remote server")
    parser_remote.add_argument("--server-url", type=str, default=DEFAULT_REMOTE_URL)
    parser_remote.add_argument(
        "--port", type=int, help="Port number for the server connection"
    )
    parser_remote.add_argument(
        "--service-id", type=str, help="Service ID for remote connection"
    )

    return parser


def handle_local_mode(args: Namespace) -> None:
    """Handle local mode execution."""
    service_config = service_registry.get_service_config(args.service)

    service_id = (
        args.service_id_existing_server
        if args.service_id_existing_server
        else service_config["default_service_id"]
    )

    run_locally(
        args.server_url,
        args.port,
        service_id,
        service_config["startup_function_path"],
        service_config["register_function"],
    )


def handle_remote_mode(args: Namespace) -> None:
    """Handle remote mode execution."""
    service_config = service_registry.get_service_config(args.service)

    service_id = (
        args.service_id if args.service_id else service_config["default_service_id"]
    )

    connect_to_remote(
        args.server_url, args.port, service_id, service_config["register_function"]
    )


def main() -> None:
    """Main entry point."""
    # Register all available services
    register_services()

    parser = create_parser()
    args = parser.parse_args()

    if not args.mode:
        parser.error("Please specify a mode: local or remote")

    if args.mode == "local":
        handle_local_mode(args)
    elif args.mode == "remote":
        handle_remote_mode(args)
    else:
        parser.error(f"Unknown mode: {args.mode}")


if __name__ == "__main__":
    main()
