import logging
from abc import ABC, abstractmethod
from typing import Any

from hypha_rpc.rpc import RemoteException

from hypha_startup_services.common.server_utils import get_server

# from .server_utils import get_server

logger = logging.getLogger(__name__)


class BaseArtifactParams(ABC):
    """Abstract base class for artifact parameters.

    This class defines the interface that all artifact parameter classes must implement
    to be compatible with the create_artifact function.
    """

    @property
    @abstractmethod
    def artifact_id(self) -> str:
        """Return the unique identifier for this artifact."""
        raise NotImplementedError

    @property
    @abstractmethod
    def creation_dict(self) -> dict[str, Any]:
        """Return a dictionary suitable for artifact creation."""
        raise NotImplementedError

    @property
    @abstractmethod
    def description(self) -> str:
        """Return a human-readable description of the artifact."""
        raise NotImplementedError

    @property
    @abstractmethod
    def manifest(self) -> dict[str, Any]:
        """Return the artifact manifest as a dictionary."""
        raise NotImplementedError


async def get_artifact(
    artifact_id: str,
) -> dict[str, Any]:
    """Get an artifact from the artifact manager.

    Args:
        server: The RemoteService instance
        artifact_id: The ID of the artifact to retrieve

    Returns:
        The artifact data, or a dict with error information if retrieval fails

    Raises:
        RemoteException: If there's a server communication error

    """
    async with get_server("https://hypha.aicell.io") as server:
        artifact_manager = await server.get_service("public/artifact-manager")
        artifact = await artifact_manager.read(artifact_id=artifact_id)
        return artifact


async def create_artifact(
    artifact_params: BaseArtifactParams,
) -> dict[str, Any]:
    """Create a new artifact using the model-based approach."""
    if await artifact_exists(
        artifact_id=artifact_params.artifact_id,
    ):
        logger.warning(
            "Artifact with ID %s already exists. Skipping creation.",
            artifact_params.artifact_id,
        )
        return {
            "artifact_name": artifact_params.artifact_id,
            "status": "already_exists",
        }

    async with get_server("https://hypha.aicell.io") as server:
        artifact_manager = await server.get_service("public/artifact-manager")

        await artifact_manager.create(**artifact_params.creation_dict)
        logger.info(
            "Artifact created: '%s' with params: %s",
            artifact_params.artifact_id,
            artifact_params.creation_dict,
        )
        return {"artifact_name": artifact_params.artifact_id, "status": "created"}


async def delete_artifact(
    artifact_id: str,
) -> None:
    """Delete an artifact."""
    async with get_server("https://hypha.aicell.io") as server:
        artifact_manager = await server.get_service("public/artifact-manager")
        try:
            await artifact_manager.delete(artifact_id=artifact_id, delete_files=True)
            logger.info("Artifact deleted: '%s'", artifact_id)
        except RemoteException as e:
            logger.warning("Error deleting artifact '%s'. Error: %s", artifact_id, e)


async def artifact_exists(
    artifact_id: str,
) -> bool:
    """Check if an artifact exists."""
    try:
        await get_artifact(
            artifact_id=artifact_id,
        )
        return True
    except RemoteException:
        logger.debug("Artifact '%s' does not exist.", artifact_id)
        return False


async def artifact_edit(
    artifact_id: str,
    manifest: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    **kwargs,
) -> None:
    """Edit an existing artifact's manifest, config, or other properties."""
    edit_params: dict[str, Any] = {"artifact_id": artifact_id}
    if manifest is not None:
        edit_params["manifest"] = manifest
    if config is not None:
        edit_params["config"] = config
    edit_params.update(kwargs)

    if not await artifact_exists(artifact_id):
        raise ValueError(f"Artifact '{artifact_id}' does not exist.")

    async with get_server("https://hypha.aicell.io") as server:
        artifact_manager = await server.get_service("public/artifact-manager")

        await artifact_manager.edit(**edit_params)
