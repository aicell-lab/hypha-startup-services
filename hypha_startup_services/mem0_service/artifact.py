from typing import Any
import logging
from hypha_rpc.rpc import RemoteException, RemoteService
from hypha_startup_services.mem0_service.utils.models import CreateArtifactParams

logger = logging.getLogger(__name__)


async def get_artifact(
    server: RemoteService,
    artifact_id: str,
) -> dict[str, Any]:
    """
    Get an artifact from the artifact manager.

    Args:
        server: The RemoteService instance
        artifact_id: The ID of the artifact to retrieve

    Returns:
        The artifact data, or a dict with error information if retrieval fails

    Raises:
        RemoteException: If there's a server communication error
    """
    artifact_manager = await server.get_service("public/artifact-manager")

    try:
        artifact = await artifact_manager.read(artifact_id=artifact_id)
        return artifact
    except RemoteException as e:
        logger.error("Error getting artifact %s: %s", artifact_id, e)
        return {"error": str(e)}


async def create_artifact(
    server: RemoteService,
    artifact_params: CreateArtifactParams,
) -> None:
    """Create a new artifact."""

    artifact_manager = await server.get_service("public/artifact-manager")

    if await artifact_exists(
        server=server,
        artifact_id=artifact_params.artifact_id,
    ):
        logger.warning(
            "Artifact with ID %s already exists. Skipping creation.",
            artifact_params.artifact_id,
        )
        return

    try:
        await artifact_manager.create(**artifact_params.creation_dict)
    except RemoteException as e:
        logger.error(
            "Artifact couldn't be created. It likely already exists. Error: %s", e
        )


async def delete_artifact(
    server: RemoteService,
    artifact_id: str,
) -> None:
    """Delete an artifact."""
    artifact_manager = await server.get_service("public/artifact-manager")
    try:
        await artifact_manager.delete(artifact_id=artifact_id, delete_files=True)
    except RemoteException as e:
        logger.error("Error deleting artifact. Error: %s", e)


async def artifact_exists(
    server: RemoteService,
    artifact_id: str,
) -> bool:
    """Check if an artifact exists."""

    artifact_response = await get_artifact(
        server=server,
        artifact_id=artifact_id,
    )
    if "error" in artifact_response:
        return False
    return True
