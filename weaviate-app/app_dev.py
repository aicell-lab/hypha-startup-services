"""Hypha app entrypoint for development Weaviate services."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Protocol

try:
    from hypha_rpc import api as hypha_api
except ImportError:
    hypha_api = None

from hypha_startup_services.weaviate_service.client import instantiate_and_connect
from hypha_startup_services.weaviate_service.register_service import (
    get_weaviate_service_def,
)
from hypha_startup_services.weaviate_service.service_codecs import (
    register_weaviate_codecs,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DEV_SERVICE_ID = "weaviate-dev"
DEV_SERVICE_PREFIX = "weaviate-dev-"

if TYPE_CHECKING:
    from hypha_rpc.rpc import RemoteService


class _HyphaApiProtocol(Protocol):
    def export(self, service_definition: dict[str, object]) -> None:
        """Export app methods to Hypha."""


def _extract_dev_service_id_from_app_id(app_id: str) -> str | None:
    """Extract branch-specific dev service id from a qualified app id."""
    normalized_app_id = app_id.strip()
    if not normalized_app_id:
        return None

    if "/" in normalized_app_id:
        normalized_app_id = normalized_app_id.rsplit("/", maxsplit=1)[1]

    if ":" in normalized_app_id:
        normalized_app_id = normalized_app_id.rsplit(":", maxsplit=1)[1]

    if normalized_app_id.startswith(DEV_SERVICE_PREFIX):
        return normalized_app_id
    return None


def _resolve_dev_service_id() -> str:
    """Resolve a dev service id without interfering with production."""
    configured_service_id = os.environ.get("WEAVIATE_SERVICE_ID")
    if configured_service_id:
        return configured_service_id

    app_id = os.environ.get("HYPHA_APP_ID")
    if app_id:
        extracted_service_id = _extract_dev_service_id_from_app_id(app_id)
        if extracted_service_id:
            return extracted_service_id

    return DEFAULT_DEV_SERVICE_ID


async def setup(server: RemoteService) -> None:
    """Run the Weaviate service app setup for development deployments."""
    service_id = _resolve_dev_service_id()
    logger.info("Setting up dev Weaviate service app with ID: %s", service_id)

    register_weaviate_codecs(server)
    logger.info("Registered Weaviate codecs")

    client = await instantiate_and_connect()
    logger.info("Connected to Weaviate client")

    service_def = get_weaviate_service_def(server, client, service_id)
    await server.register_service(service_def)
    logger.info("Registered dev Weaviate service with ID: %s", service_id)


if hypha_api is not None:
    typed_hypha_api: _HyphaApiProtocol = hypha_api
    typed_hypha_api.export(
        {
            "id": "weaviate-app-loader",
            "name": "Weaviate App Loader (Dev)",
            "setup": setup,
        },
    )
