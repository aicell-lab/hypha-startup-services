"""Common artifact manager functions."""

import logging
from abc import ABC, abstractmethod

from hypha_rpc.rpc import RemoteException

from hypha_startup_services.common.server_utils import get_server

from .constants import ARTIFACT_MANAGER_SERVICE_ID, DEFAULT_REMOTE_URL

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
    def creation_dict(self) -> dict[str, object]:
        """Return a dictionary suitable for artifact creation."""
        raise NotImplementedError

    @property
    @abstractmethod
    def description(self) -> str:
        """Return a human-readable description of the artifact."""
        raise NotImplementedError

    @property
    @abstractmethod
    def manifest(self) -> dict[str, object]:
        """Return the artifact manifest as a dictionary."""
        raise NotImplementedError


async def get_artifact(
    artifact_id: str,
) -> dict[str, object]:
    """Get an artifact from the artifact manager.

    Args:
        server: The RemoteService instance
        artifact_id: The ID of the artifact to retrieve

    Returns:
        The artifact data, or a dict with error information if retrieval fails

    Raises:
        RemoteException: If there's a server communication error

    """
    async with get_server(DEFAULT_REMOTE_URL) as server:
        artifact_manager = await server.get_service(ARTIFACT_MANAGER_SERVICE_ID)
        return await artifact_manager.read(artifact_id=artifact_id)


async def create_artifact(
    artifact_params: BaseArtifactParams,
) -> dict[str, object]:
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

    async with get_server(DEFAULT_REMOTE_URL) as server:
        artifact_manager = await server.get_service(ARTIFACT_MANAGER_SERVICE_ID)

        await artifact_manager.create(**artifact_params.creation_dict)
        logger.info(
            "Artifact created: '%s' with params: %s",
            artifact_params.artifact_id,
            artifact_params.creation_dict,
        )
        return {"artifact_name": artifact_params.artifact_id, "status": "created"}


async def list_artifacts(
    parent_id: str | None = None,
    **kwargs: object,
) -> list[dict[str, object]]:
    """List artifacts."""
    async with get_server(DEFAULT_REMOTE_URL) as server:
        artifact_manager = await server.get_service(ARTIFACT_MANAGER_SERVICE_ID)
        return await artifact_manager.list(parent_id=parent_id, **kwargs)


async def delete_artifact(
    artifact_id: str,
) -> None:
    """Delete an artifact."""
    async with get_server(DEFAULT_REMOTE_URL) as server:
        artifact_manager = await server.get_service(ARTIFACT_MANAGER_SERVICE_ID)
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
    except RemoteException:
        logger.debug("Artifact '%s' does not exist.", artifact_id)
        return False
    else:
        return True


async def artifact_edit(
    artifact_id: str,
    manifest: dict[str, object] | None = None,
    config: dict[str, object] | None = None,
    **kwargs: object,
) -> None:
    """Edit an existing artifact's manifest, config, or other properties."""
    edit_params: dict[str, object] = {"artifact_id": artifact_id}
    if manifest is not None:
        edit_params["manifest"] = manifest
    if config is not None:
        edit_params["config"] = config
    edit_params.update(kwargs)

    if not await artifact_exists(artifact_id):
        error_msg = f"Artifact '{artifact_id}' does not exist."
        raise ValueError(error_msg)

    async with get_server(DEFAULT_REMOTE_URL) as server:
        artifact_manager = await server.get_service("public/artifact-manager")

        await artifact_manager.edit(**edit_params)
