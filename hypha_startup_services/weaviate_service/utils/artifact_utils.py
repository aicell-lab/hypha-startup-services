import logging
import uuid
from typing import Any
from hypha_rpc.rpc import RemoteService, RemoteException
from hypha_startup_services.common.artifacts import (
    create_artifact,
    delete_artifact,
    get_artifact,
)
from hypha_startup_services.common.permissions import is_admin_workspace
from .models import (
    CollectionArtifactParams,
    ApplicationArtifactParams,
)
from hypha_startup_services.common.utils import (
    get_application_artifact_name,
)
from .constants import (
    ADMIN_WORKSPACES,
)
from .format_utils import (
    get_full_collection_name,
)

logger = logging.getLogger(__name__)


def get_collection_artifact_name(short_name: str) -> str:
    """Create a full collection artifact name."""
    return get_full_collection_name(short_name)


def get_collection_artifact_names(short_names: list[str]) -> list[str]:
    """Create full collection artifact names."""
    return [get_full_collection_name(name) for name in short_names]


def make_artifact_permissions(owners: str | list[str]) -> dict[str, str]:
    """Generate permissions dictionary for artifacts.

    Args:
        owners: A list of user IDs who own the artifact

    Returns:
        A permissions dictionary with read, write, and admin keys
    """
    if isinstance(owners, str):
        owners = [owners]
    return {owner: "*" for owner in owners}


def create_artifact_metadata(
    short_collection_name: str | None = None,
    application_id: str | None = None,
    **kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Create standard metadata for artifacts.

    Args:
        collection_name: The collection name
        application_id: The application ID
        **kwargs: Additional metadata fields

    Returns:
        A metadata dictionary with standard fields
    """
    metadata: dict[str, Any] = {
        "created_at": str(uuid.uuid1()),
    }

    if short_collection_name:
        metadata["collection_name"] = short_collection_name

    if application_id:
        metadata["application_id"] = application_id

    # Add any additional metadata
    metadata.update(kwargs)

    return metadata


async def delete_collection_artifact(
    server: RemoteService,
    collection_name: str,
) -> None:
    """Delete artifact for a specific collection.

    Args:
        server: The RemoteService instance
        collection_name: The name of the collection to delete the artifact for
    """
    full_name = get_collection_artifact_name(collection_name)
    await delete_artifact(
        server,
        full_name,
    )


async def delete_collection_artifacts(
    server: RemoteService, short_names: list[str]
) -> None:
    """Delete artifacts for a list of collections.

    Args:
        server: The RemoteService instance
        names: List of collection names to delete artifacts for
    """
    for coll_name in short_names:
        await delete_collection_artifact(
            server,
            coll_name,
        )


async def create_collection_artifact(
    server: RemoteService,
    settings: dict[str, Any],
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
        server=server,
        artifact_params=artifact_params,
    )


async def is_user_in_artifact_permissions(
    server: RemoteService, user_ws: str, artifact_name: str
) -> bool:
    """Check if the user has admin permissions for a specific collection.

    Args:
        server: The RemoteService instance
        user_ws: The user workspace
        collection_name: The name of the collection to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """

    try:
        artifact = await get_artifact(server, artifact_name)
    except RemoteException as e:
        logger.error("Error getting artifact: %s", e)
        return False

    permissions = artifact.get("config", {}).get("permissions", {})
    user_permissions = permissions.get(user_ws, {})
    if user_permissions in ("*", "rw+"):
        return True

    return False


async def has_artifact_permission(
    server: RemoteService, user_ws: str, artifact_name: str
) -> bool:
    """Check if the user has permission for a specific artifact.

    Args:
        server (RemoteService): The RemoteService instance
        user_ws (str): The user workspace
        artifact_name (str): The name of the artifact to check permissions for

    Returns:
        bool: True if the user has permission, False otherwise
    """
    if is_admin_workspace(user_ws):
        return True

    if not await is_user_in_artifact_permissions(server, user_ws, artifact_name):
        return False

    return True


async def has_application_permission(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    accesser_ws: str,
    application_user_ws: str,
) -> bool:
    """Check if the user has admin permissions for a specific application.

    Args:
        server: The RemoteService instance
        collection_name: The name of the collection to check permissions for
        application_id: The ID of the application to check permissions for
        accesser_ws: The workspace of the user checking permissions
        application_user_ws: The workspace of the application user

    Returns:
        True if the user has admin permissions, False otherwise
    """
    full_collection_name = get_full_collection_name(collection_name)
    artifact_name = get_application_artifact_name(
        full_collection_name, application_user_ws, application_id
    )

    return await has_artifact_permission(
        server,
        accesser_ws,
        artifact_name,
    )


async def assert_has_application_permission(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    accesser_ws: str,
    application_user_ws: str,
) -> None:
    """Check if the user has admin permissions for a specific application.

    Args:
        server: The RemoteService instance
        collection_name: The name of the collection to check permissions for
        application_id: The ID of the application to check permissions for
        accesser_ws: The workspace of the user checking permissions
        application_user_ws: The workspace of the application user

    Returns:
        None if all permissions are granted, raises an error if permissions are missing
    """
    assert await has_application_permission(
        server,
        collection_name,
        application_id,
        accesser_ws,
        application_user_ws,
    ), "You do not have permission to access the application."


async def has_collection_permission(
    server: RemoteService,
    user_ws: str,
    short_coll_names: str | list[str],
) -> bool:
    """Check if the user has admin permissions for collections.

    Args:
        server: The RemoteService instance
        user_ws: The user workspace
        collection_names: Optional collection names to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """

    if isinstance(short_coll_names, str):
        short_coll_names = [short_coll_names]

    coll_artifact_names = get_collection_artifact_names(short_coll_names)
    for coll_artifact_name in coll_artifact_names:
        if not await has_artifact_permission(
            server,
            user_ws,
            coll_artifact_name,
        ):
            return False
    return True


async def assert_has_collection_permission(
    server: RemoteService,
    user_ws: str,
    short_coll_names: str | list[str],
) -> None:
    """Check if user has permission to access these collections.

    Args:
        server: The RemoteService instance
        names: Collection name or list of collection names to access
        user_ws: The user workspace

    Returns:
        None if all permissions are granted, raises an error if permissions are missing
    """

    assert await has_collection_permission(
        server, user_ws, short_coll_names
    ), "You do not have permission to access the collection(s)."


async def create_application_artifact(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    description: str,
    user_ws: str,
) -> dict[str, Any]:
    """Create an application artifact.

    Args:
        server: RemoteService instance
        collection_name: Short collection name
        application_id: Application ID
        description: Application description
        user_ws: User workspace

    Returns:
        Result of artifact creation
    """
    # Create artifact parameters using the model
    artifact_params = ApplicationArtifactParams(
        collection_name=collection_name,
        application_id=application_id,
        user_workspace=user_ws,
        creator_id=user_ws,
        desc=description,
        permissions=make_artifact_permissions(owners=user_ws),
        metadata={
            "application_id": application_id,
            "short_collection_name": collection_name,
        },
    )

    result = await create_artifact(
        server=server,
        artifact_params=artifact_params,
    )

    return {
        "artifact_name": artifact_params.artifact_name,
        "description": description,
        "permissions": artifact_params.permissions,
        "metadata": artifact_params.metadata,
        "result": result,
    }


async def delete_application_artifact(
    server: RemoteService, full_collection_name: str, application_id: str, user_ws: str
) -> None:
    """Delete an application artifact.

    Args:
        server: RemoteService instance
        full_collection_name: Full collection name
        application_id: str,
        user_ws: str
    """
    artifact_name = get_application_artifact_name(
        full_collection_name, user_ws, application_id
    )
    await delete_artifact(
        server,
        artifact_name,
    )
