"""Service registry for startup services."""

from collections.abc import Awaitable, Callable
from typing import Any

from hypha_rpc.rpc import RemoteService

from hypha_startup_services.mem0_bioimage_service.register_service import (
    register_mem0_bioimage_service,
)
from hypha_startup_services.mem0_service.register_service import register_mem0_service
from hypha_startup_services.weaviate_bioimage_service.register_service import (
    register_weaviate_bioimage,
)
from hypha_startup_services.weaviate_service.register_service import register_weaviate

from .constants import (
    DEFAULT_MEM0_BIOIMAGE_SERVICE_ID,
    DEFAULT_MEM0_SERVICE_ID,
    DEFAULT_WEAVIATE_BIOIMAGE_SERVICE_ID,
    DEFAULT_WEAVIATE_SERVICE_ID,
)


class ServiceRegistry:
    """Registry for different startup services."""

    def __init__(self) -> None:
        """Initialize the service registry."""
        self._services: dict[str, dict[str, Any]] = {}

    def register_service_type(
        self,
        service_name: str,
        register_function: Callable[[RemoteService, str], Awaitable[None]],
        startup_function_path: str,
        default_service_id: str,
    ) -> None:
        """Register a service type with its configuration.

        Args:
            service_name: Name of the service (e.g., 'weaviate', 'mem0')
            register_function: Function to register the service
            startup_function_path: Path to the startup function
            default_service_id: Default service ID

        """
        self._services[service_name] = {
            "register_function": register_function,
            "startup_function_path": startup_function_path,
            "default_service_id": default_service_id,
        }

    def get_service_config(self, service_name: str) -> dict[str, Any]:
        """Get configuration for a service."""
        if service_name not in self._services:
            error_msg = (
                f"Unknown service: {service_name}."
                f" Available: {list(self._services.keys())}"
            )
            raise ValueError(error_msg)
        return self._services[service_name]

    def list_services(self) -> list[str]:
        """List all registered services."""
        return list(self._services.keys())


# Global service registry
service_registry = ServiceRegistry()


def register_services() -> None:
    """Register all available services."""
    # Import here to avoid circular imports

    service_registry.register_service_type(
        "weaviate",
        register_weaviate,
        "hypha_startup_services.weaviate_service.register_service:register_weaviate",
        DEFAULT_WEAVIATE_SERVICE_ID,
    )

    service_registry.register_service_type(
        "mem0",
        register_mem0_service,
        "hypha_startup_services.mem0_service.register_service:register_mem0_service",
        DEFAULT_MEM0_SERVICE_ID,
    )

    service_registry.register_service_type(
        "mem0-bioimage",
        register_mem0_bioimage_service,
        "hypha_startup_services.mem0_bioimage_service.register_service:register_mem0_bioimage_service",
        DEFAULT_MEM0_BIOIMAGE_SERVICE_ID,
    )

    service_registry.register_service_type(
        "weaviate-bioimage",
        register_weaviate_bioimage,
        "hypha_startup_services.weaviate_bioimage_service.register_service:register_weaviate_bioimage",
        DEFAULT_WEAVIATE_BIOIMAGE_SERVICE_ID,
    )
