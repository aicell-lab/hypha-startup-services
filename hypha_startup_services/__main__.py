"""Main entry point for hypha startup services."""

import argparse
import asyncio
import logging
from argparse import Namespace
from collections.abc import Callable

from hypha_rpc.rpc import RemoteException, RemoteService

from .common.constants import (
    DEFAULT_LOCAL_EXISTING_HOST,
    DEFAULT_LOCAL_HOST,
    DEFAULT_LOCAL_PORT,
    DEFAULT_REMOTE_URL,
)
from .common.probes import add_probes
from .common.server_utils import get_server, run_local_services
from .common.service_registry import register_services, service_registry


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,  # Force reconfiguration even if logging was already configured
    )
    # Ensure root logger level is set
    logging.root.setLevel(logging.INFO)


# Configure logging when module is imported
setup_logging()
logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Create the main argument parser."""
    parser = argparse.ArgumentParser(
        description="Hypha startup services launcher",
        prog="hypha-startup-services",
    )

    # Services argument - can be one or more
    parser.add_argument(
        "services",
        nargs="+",
        choices=["weaviate", "mem0", "mem0-bioimage", "weaviate-bioimage"],
        help="Service(s) to start. Single service or multiple services in order.",
    )

    # Connection mode
    connection_group = parser.add_mutually_exclusive_group(required=True)
    connection_group.add_argument(
        "--local",
        action="store_true",
        help="Run with local server",
    )
    connection_group.add_argument(
        "--remote",
        action="store_true",
        help="Connect to remote server",
    )

    # Common arguments
    parser.add_argument(
        "--server-url",
        type=str,
        help="Server URL (defaults based on local/remote)",
    )
    parser.add_argument("--port", type=int, help="Port number")
    parser.add_argument(
        "--service-id",
        type=str,
        help="Custom service ID (for single service only)",
    )
    parser.add_argument(
        "--client-id",
        type=str,
        help="Client ID for the server connection",
    )

    # Service-specific ID overrides (for multiple services)
    parser.add_argument(
        "--weaviate-service-id",
        type=str,
        help="Custom Weaviate service ID",
    )
    parser.add_argument("--mem0-service-id", type=str, help="Custom Mem0 service ID")
    parser.add_argument(
        "--mem0-bioimage-service-id",
        type=str,
        help="Custom Mem0-Bioimage service ID",
    )
    parser.add_argument(
        "--weaviate-bioimage-service-id",
        type=str,
        help="Custom Weaviate Bioimage service ID",
    )
    parser.add_argument(
        "--probes-service-id",
        type=str,
        help="Custom ID for the probes service (default: startup-services-probes)",
    )

    return parser


def get_service_configurations(
    args: Namespace,
) -> tuple[list[str], list[str], list[Callable]]:
    """Extract service configurations from arguments.

    Args:
        args: Parsed command line arguments

    Returns:
        Tuple of (service_ids, startup_function_paths, register_functions)

    """
    service_ids = []
    startup_function_paths = []
    register_functions = []

    for service_name in args.services:
        registry = service_registry.get_service_config(service_name)
        # Convert hyphenated service names to underscores for attribute lookup
        service_attr_name = service_name.replace("-", "_")
        service_id = (
            args.service_id
            or getattr(args, f"{service_attr_name}_service_id", None)
            or registry["default_service_id"]
        )

        service_ids.append(service_id)
        startup_function_paths.append(registry["startup_function_path"])
        register_functions.append(registry["register_function"])

    return service_ids, startup_function_paths, register_functions


async def start_local_server(
    args: Namespace,
    service_ids: list[str],
    startup_function_paths: list[str],
    port: int = DEFAULT_LOCAL_PORT,
) -> RemoteService | None:
    """Handle local services setup.

    Args:
        args: Parsed command line arguments
        service_ids: List of service IDs
        startup_function_paths: List of startup function paths

    Returns:
        Server connection if connected to existing server, None if started new server

    """
    server_url = args.server_url or DEFAULT_LOCAL_HOST
    for service_id in service_ids:
        logger.info(
            "Service %s available at %s:%s/services/%s",
            service_id,
            server_url,
            port,
            service_id,
        )
    await run_local_services(server_url, port, startup_function_paths)


async def register_services_to_server(
    server: RemoteService,
    register_functions: list[Callable],
    service_ids: list[str],
) -> None:
    """Register services to an existing server.

    Args:
        server: The server connection
        register_functions: List of registration functions
        service_ids: List of service IDs

    """
    for register_function, service_id in zip(
        register_functions, service_ids, strict=False
    ):
        await register_function(server, service_id)
        logger.info("Registered service %s", service_id)


async def serve_services(
    server: RemoteService,
    register_functions: list[Callable],
    service_ids: list[str],
    probes_service_id: str | None = None,
) -> None:
    """Register services and health probes to an existing server.

    Args:
        server: The server connection
        register_functions: List of registration functions
        service_ids: List of service IDs
        probes_service_id: Custom ID for the probes service (optional)

    """
    await register_services_to_server(server, register_functions, service_ids)

    for service_id in service_ids:
        log_service(service_id, server)

    if probes_service_id:
        await add_probes(server, service_ids, probes_service_id)
        logger.info("Health probes registered with ID %s", probes_service_id)

    await server.serve()


def log_service(service_id: str, server: RemoteService) -> None:
    """Log the service URL."""
    base_url = server.config.public_base_url
    workspace = server.config.workspace
    service_url = f"{base_url}/{workspace}/services/{service_id}"
    logger.info("Service %s available at %s", service_id, service_url)


async def handle_services(args: Namespace) -> None:
    """Handle multiple services mode."""
    service_ids, startup_function_paths, register_functions = (
        get_service_configurations(args)
    )
    probes_service_id = args.probes_service_id

    if args.local:
        port = args.port or DEFAULT_LOCAL_PORT
        server_url = args.server_url or DEFAULT_LOCAL_EXISTING_HOST

        try:
            async with get_server(server_url, port, client_id=args.client_id) as server:
                logger.info(
                    "Connected to existing local server at %s:%s",
                    server_url,
                    port,
                )
                await serve_services(
                    server,
                    register_functions,
                    service_ids,
                    probes_service_id,
                )
        except (RemoteException, ValueError, OSError):
            await start_local_server(args, service_ids, startup_function_paths, port)
    else:
        server_url = args.server_url or DEFAULT_REMOTE_URL
        async with get_server(server_url, client_id=args.client_id) as server:
            logger.info("Connected to remote server at %s", server_url)
            await serve_services(
                server,
                register_functions,
                service_ids,
                probes_service_id,
            )


def main() -> None:
    """Main entry point."""
    # Register all available services
    register_services()

    parser = create_parser()
    args = parser.parse_args()

    # Determine mode based on number of services
    asyncio.run(handle_services(args))


if __name__ == "__main__":
    main()
