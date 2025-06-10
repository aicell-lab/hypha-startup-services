from typing import Any
import logging
from hypha_rpc.rpc import RemoteException, RemoteService  # type: ignore

logger = logging.getLogger(__name__)


async def create_artifact(
    server: RemoteService,
    artifact_name: str,
    description: str,
    permissions: dict[str, str] | None = None,
    metadata: dict[str, Any] | None = None,
    parent_id: str | None = None,
) -> None:
    """Create a new artifact."""
    if metadata is None:
        metadata = {}

    gallery_manifest = {
        "name": artifact_name,
        "description": description,
        "collection": [],
        "metadata": metadata,
    }

    artifact_manager = await server.get_service("public/artifact-manager")

    try:
        await artifact_manager.create(
            parent_id=parent_id,
            alias=artifact_name,
            type="collection",
            manifest=gallery_manifest,
            config={"permissions": permissions},
        )
    except RemoteException as e:
        logger.warning(
            "Artifact couldn't be created. It likely already exists. Error: %s", e
        )


async def delete_artifact(
    server: RemoteService,
    artifact_name: str,
) -> None:
    """Delete an artifact."""
    artifact_manager = await server.get_service("public/artifact-manager")
    try:
        await artifact_manager.delete(artifact_id=artifact_name, delete_files=True)
    except RemoteException as e:
        logger.error("Error deleting artifact. Error: %s", e)


async def get_artifact(
    server: RemoteService,
    artifact_name: str,
) -> dict[str, Any]:
    """Get an artifact."""
    artifact_manager = await server.get_service("public/artifact-manager")

    try:
        artifact = await artifact_manager.read(artifact_id=artifact_name)
        return artifact
    except RemoteException as e:
        logger.error("Error getting artifact. Error: %s", e)
        return {"error": str(e)}


async def artifact_exists(
    server: RemoteService,
    artifact_name: str,
) -> bool:
    """Check if an artifact exists."""

    try:
        await get_artifact(
            server=server,
            artifact_name=artifact_name,
        )
        return True
    except RemoteException:
        return False
