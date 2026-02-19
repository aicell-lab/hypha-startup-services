"""List available Hypha services for the configured personal workspace."""

import asyncio
import logging
import os

from dotenv import load_dotenv
from hypha_rpc import connect_to_server

SERVER_URL = "https://hypha.aicell.io"
LOGGER = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Configure logging for script execution."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")


async def list_services() -> None:
    """Print service ids in shared and personal Hypha workspaces."""
    _configure_logging()
    load_dotenv(override=True)
    token = os.environ.get("PERSONAL_TOKEN")
    if not token:
        LOGGER.error("PERSONAL_TOKEN not set")
        return

    server = await connect_to_server({"server_url": SERVER_URL, "token": token})
    services = await server.list_services("hypha-agents")
    LOGGER.info("Services in hypha-agents:")
    for service in services:
        LOGGER.info(" - %s", service["id"])

    workspace_id = server.config.workspace
    LOGGER.info("Services in %s:", workspace_id)
    workspace_services = await server.list_services(workspace_id)
    for service in workspace_services:
        LOGGER.info(" - %s", service["id"])

    await server.disconnect()


if __name__ == "__main__":
    asyncio.run(list_services())
