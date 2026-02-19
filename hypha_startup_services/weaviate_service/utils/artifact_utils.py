"""Artifact util functions."""

import logging

from hypha_rpc.rpc import RemoteService

from hypha_startup_services.common.artifacts import (
    create_artifact,
    delete_artifact,
)
from hypha_startup_services.common.constants import (
    ADMIN_WORKSPACES,
)
from hypha_startup_services.common.utils import (
    get_application_artifact_name,
)

from .format_utils import (
    get_full_collection_name,
)
from .models import (
    ApplicationArtifactParams,
    ApplicationArtifactReturn,
    CollectionArtifactParams,
    CollectionConfig,
)

logger = logging.getLogger(__name__)


def get_collection_artifact_name(short_name: str) -> str:
    """Create a full collection artifact name."""
    return get_full_collection_name(short_name)


def make_artifact_permissions(owners: str | list[str]) -> dict[str, str]:
    """Generate permissions dictionary for artifacts.

    Args:
        owners: A list of user IDs who own the artifact

    Returns:
        A permissions dictionary with read, write, and admin keys

    """
    if isinstance(owners, str):
        owners = [owners]
    return dict.fromkeys(owners, "*")


async def delete_collection_artifact(
    collection_name: str,
    server: RemoteService | None = None,
) -> None:
    """Delete artifact for a specific collection.

    Args:
        collection_name: The name of the collection to delete the artifact for
        server: Optional server instance to reuse connection

    """
    full_name = get_collection_artifact_name(collection_name)
    await delete_artifact(
        full_name,
        server=server,
    )


async def delete_collection_artifacts(
    short_names: list[str],
    server: RemoteService | None = None,
) -> None:
    """Delete artifacts for a list of collections.

    Args:
        short_names: List of collection names to delete artifacts for
        server: Optional server instance to reuse connection

    """
    for coll_name in short_names:
        await delete_collection_artifact(
            coll_name,
            server=server,
        )


async def create_collection_artifact(
    settings: CollectionConfig,
    server: RemoteService | None = None,
) -> None:
    """Create a collection artifact using the model-based approach."""
    permissions = make_artifact_permissions(owners=ADMIN_WORKSPACES)

    # Extract collection name from settings
    collection_name = settings["class"]

    # Create artifact parameters using the model
    artifact_params = CollectionArtifactParams(
        collection_name=collection_name,
        desc=settings.get("description", ""),
        permissions=permissions,
        metadata={
            "settings": settings,
        },
    )

    await create_artifact(
        artifact_params=artifact_params,
        server=server,
    )


async def create_application_artifact(
    collection_name: str,
    application_id: str,
    description: str,
    user_ws: str,
    caller_ws: str,
    server: RemoteService | None = None,
) -> ApplicationArtifactReturn:
    """Create an application artifact.

    Args:
        collection_name: Short collection name
        application_id: Application ID
        description: Application description
        user_ws: User workspace
        caller_ws: Caller workspace
        server: Optional server instance to reuse connection

    Returns:
        Result of artifact creation

    """
    application_owners = [user_ws, *ADMIN_WORKSPACES]
    full_collection_name = get_full_collection_name(collection_name)
    parent_artifact_id = full_collection_name
    if server is not None:
        parent_artifact_id = f"{server.config.workspace}/{full_collection_name}"

    # Create artifact parameters using the model
    artifact_params = ApplicationArtifactParams(
        collection_name=collection_name,
        application_id=application_id,
        user_workspace=user_ws,
        creator_id=caller_ws,
        parent_id=parent_artifact_id,
        desc=description,
        permissions=make_artifact_permissions(owners=application_owners),
        metadata={
            "application_id": application_id,
            "short_collection_name": collection_name,
        },
    )

    result = await create_artifact(
        artifact_params=artifact_params,
        server=server,
    )

    return {
        "artifact_name": artifact_params.artifact_name,
        "description": description,
        "permissions": artifact_params.permissions,
        "metadata": artifact_params.metadata,
        "result": result,
    }


async def delete_application_artifact(
    full_collection_name: str,
    application_id: str,
    user_ws: str,
    server: RemoteService | None = None,
) -> None:
    """Delete an application artifact.

    Args:
        full_collection_name: Full collection name
        application_id: str
        user_ws: str
        server: Optional server instance to reuse connection

    """
    artifact_name = get_application_artifact_name(
        full_collection_name,
        user_ws,
        application_id,
    )
    await delete_artifact(
        artifact_name,
        server=server,
    )
