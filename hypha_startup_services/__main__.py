"""Main entry point for hypha startup services."""

import argparse
import asyncio
import signal
import sys
from argparse import Namespace
from typing import List

from .common.constants import (
    DEFAULT_LOCAL_HOST,
    DEFAULT_LOCAL_PORT,
    DEFAULT_REMOTE_URL,
)
from .common.server_utils import (
    get_remote_server,
    run_locally,
)
from .common.service_registry import service_registry, register_services


def create_parser() -> argparse.ArgumentParser:
    """Create the main argument parser."""
    parser = argparse.ArgumentParser(
        description="Hypha startup services launcher", prog="hypha-startup-services"
    )

    # Services argument - can be one or more
    parser.add_argument(
        "services",
        nargs="+",
        choices=["weaviate", "mem0", "bioimage"],
        help="Service(s) to start. Single service or multiple services in order.",
    )

    # Connection mode
    connection_group = parser.add_mutually_exclusive_group(required=True)
    connection_group.add_argument(
        "--local", action="store_true", help="Run with local server"
    )
    connection_group.add_argument(
        "--remote", action="store_true", help="Connect to remote server"
    )

    # Common arguments
    parser.add_argument(
        "--server-url", type=str, help="Server URL (defaults based on local/remote)"
    )
    parser.add_argument("--port", type=int, help="Port number")
    parser.add_argument(
        "--service-id", type=str, help="Custom service ID (for single service only)"
    )

    # Service-specific ID overrides (for multiple services)
    parser.add_argument(
        "--weaviate-service-id", type=str, help="Custom Weaviate service ID"
    )
    parser.add_argument("--mem0-service-id", type=str, help="Custom Mem0 service ID")
    parser.add_argument(
        "--bioimage-service-id", type=str, help="Custom Bioimage service ID"
    )

    return parser


def get_default_server_url(is_local: bool) -> str:
    """Get default server URL based on mode."""
    return DEFAULT_LOCAL_HOST if is_local else DEFAULT_REMOTE_URL


def get_default_port(is_local: bool) -> int | None:
    """Get default port based on mode."""
    return DEFAULT_LOCAL_PORT if is_local else None


def get_service_id_for_service(args: Namespace, service_name: str) -> str:
    """Get service ID for a specific service, with fallbacks to defaults."""
    # Check for service-specific override first
    if hasattr(args, f"{service_name}_service_id"):
        service_specific_id = getattr(args, f"{service_name}_service_id")
        if service_specific_id:
            return service_specific_id

    # Check for general service ID override
    if hasattr(args, "service_id") and args.service_id:
        return args.service_id

    # Fall back to service default
    service_config = service_registry.get_service_config(service_name)
    return service_config["default_service_id"]


async def start_multiple_services(
    services: List[str],
    server_url: str,
    port: int | None,
    args: Namespace,
) -> None:
    """Start multiple services sequentially using a shared server connection."""
    print(f"Starting services in order: {', '.join(services)}")
    print(f"Server URL: {server_url}")

    # Create one shared server connection
    server = await get_remote_server(server_url, port)

    try:
        # Register all services using the shared connection
        for service_name in services:
            service_id = get_service_id_for_service(args, service_name)
            print(f"Starting {service_name} service with ID: {service_id}")

            service_config = service_registry.get_service_config(service_name)
            await service_config["register_function"](server, service_id)

            # Small delay between service starts
            await asyncio.sleep(1)

        print("All services started. Running forever...")

        # Set up signal handlers for graceful shutdown
        def signal_handler(_signum, _frame):
            print("\nReceived shutdown signal. Shutting down services...")
            # The finally block will handle disconnection
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Keep the event loop running forever
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down services...")
    finally:
        # Properly disconnect the shared server
        if server and hasattr(server, "disconnect"):
            try:
                print("Disconnecting from server...")
                await server.disconnect()
                print("Server disconnected successfully.")
            except (OSError, RuntimeError) as e:
                print(f"Warning: Failed to disconnect server: {e}")


def handle_single_service(args: Namespace) -> None:
    """Handle single service mode."""
    service_name = args.services[0]  # Get the single service
    server_url = args.server_url or get_default_server_url(args.local)
    port = args.port or get_default_port(args.local)
    service_id = get_service_id_for_service(args, service_name)

    print(f"Starting {service_name} service with ID: {service_id}")
    print(f"Server URL: {server_url}")

    if args.local:
        service_config = service_registry.get_service_config(service_name)
        # For local mode, we need a valid port
        if port is None:
            port = DEFAULT_LOCAL_PORT
        run_locally(
            server_url,
            port,
            service_id,
            service_config["startup_function_path"],
            service_config["register_function"],
        )
    else:
        # For remote mode, run async
        asyncio.run(
            start_single_service_remote(service_name, server_url, port, service_id)
        )


async def start_single_service_remote(
    service_name: str, server_url: str, port: int | None, service_id: str
) -> None:
    """Start a single service in remote mode."""
    server = await get_remote_server(server_url, port)
    service_config = service_registry.get_service_config(service_name)

    try:
        await service_config["register_function"](server, service_id)
        print(f"Service {service_name} registered successfully. Running forever...")

        # Keep running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print(f"\nShutting down {service_name} service...")
    finally:
        if hasattr(server, "disconnect"):
            try:
                await server.disconnect()
            except (OSError, RuntimeError) as e:
                print(f"Warning: Failed to disconnect server: {e}")


def handle_multiple_services(args: Namespace) -> None:
    """Handle multiple services mode."""
    server_url = args.server_url or get_default_server_url(args.local)
    port = args.port or get_default_port(args.local)

    # Multiple services mode only supports remote connections for now
    if args.local:
        print(
            "ERROR: Multiple services mode currently only supports remote connections"
        )
        sys.exit(1)

    asyncio.run(start_multiple_services(args.services, server_url, port, args))


def main() -> None:
    """Main entry point."""
    # Register all available services
    register_services()

    parser = create_parser()
    args = parser.parse_args()

    # Determine mode based on number of services
    if len(args.services) == 1:
        handle_single_service(args)
    else:
        handle_multiple_services(args)


if __name__ == "__main__":
    main()
