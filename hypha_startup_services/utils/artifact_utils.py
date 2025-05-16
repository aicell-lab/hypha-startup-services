import uuid
from typing import Any
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.artifacts import (
    create_artifact,
    delete_artifact,
    get_artifact,
)
from hypha_startup_services.utils.constants import (
    ADMIN_IDS,
    ARTIFACT_DELIMITER,
)
from hypha_startup_services.utils.format_utils import (
    full_collection_name,
    assert_valid_collection_name,
    assert_valid_application_name,
)


def collection_artifact_name(names: str | list[str]) -> str | list[str]:
    """Create a full collection artifact name with workspace prefix."""
    if isinstance(names, str):
        return full_collection_name(names)

    return [full_collection_name(name) for name in names]


def application_artifact_name(
    ws_collection_name: str, user_id: str, application_id: str
) -> str:
    """Create a full application artifact name with workspace prefix."""
    assert_valid_collection_name(ws_collection_name)
    assert_valid_application_name(application_id)
    return f"{ws_collection_name}{ARTIFACT_DELIMITER}{user_id}{ARTIFACT_DELIMITER}{application_id}"


def is_admin_id(user_id: str) -> bool:
    """Check if a workspace has admin privileges."""
    return user_id in ADMIN_IDS


def get_artifact_permissions(owners: str | list[str]) -> dict:
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
    collection_name: str = None,
    application_id: str = None,
    **kwargs,
) -> dict:
    """Create standard metadata for artifacts.

    Args:
        collection_name: The collection name
        application_id: The application ID
        workspace: The creator's workspace
        **kwargs: Additional metadata fields

    Returns:
        A metadata dictionary with standard fields
    """
    metadata = {
        "created_at": str(uuid.uuid1()),
    }

    if collection_name:
        metadata["collection_name"] = collection_name

    if application_id:
        metadata["application_id"] = application_id

    # Add any additional metadata
    metadata.update(kwargs)

    return metadata


async def delete_collection_artifacts(server: RemoteService, names: list[str]) -> None:
    """Delete artifacts for a list of collections.

    Args:
        server: The RemoteService instance
        names: List of collection names to delete artifacts for
    """
    for coll_name in names:
        full_name = collection_artifact_name(coll_name)
        await delete_artifact(
            server,
            full_name,
        )


async def create_collection_artifact(
    server: RemoteService,
    settings_with_workspace: dict[str, Any],
) -> None:
    permissions = get_artifact_permissions(ADMIN_IDS)
    metadata = create_artifact_metadata(
        description=settings_with_workspace.get("description", ""),
        collection_type="weaviate",
        settings=settings_with_workspace,
    )

    await create_artifact(
        server,
        settings_with_workspace["class"],
        settings_with_workspace.get("description", ""),
        permissions=permissions,
        metadata=metadata,
    )


async def has_permission_single(
    server: RemoteService, user_id: str, collection_name: str
) -> bool:
    """Check if the user has admin permissions for a specific collection.

    Args:
        server: The RemoteService instance
        user_id: The user's workspace
        collection_name: The name of the collection to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """
    artifact_name = collection_artifact_name(collection_name)

    artifact = await get_artifact(server, artifact_name)
    permissions = artifact.config.get("permissions", {})
    user_permissions = permissions.get(user_id, {})
    if user_permissions in ("*", "rw+"):
        return True

    return False


async def has_artifact_permission(
    server: RemoteService, user_id: str, artifact_names: str | list[str]
) -> bool:
    if user_id in ADMIN_IDS:
        return True

    if isinstance(artifact_names, str):
        artifact_names = [artifact_names]

    for artifact_name in artifact_names:
        if not await has_permission_single(server, user_id, artifact_name):
            return False

    return True


async def has_application_permission(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    accesser_id: str,
    application_user_id: str,
) -> bool:
    """Check if the user has admin permissions for a specific application.

    Args:
        server: The RemoteService instance
        user_id: The user's workspace
        collection_name: The name of the collection to check permissions for
        application_id: The ID of the application to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """
    artifact_name = application_artifact_name(
        collection_name, application_user_id, application_id
    )

    return await has_artifact_permission(
        server,
        accesser_id,
        artifact_name,
    )


async def assert_has_application_permission(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    accesser_id: str,
    application_user_id: str,
) -> None:
    """Check if the user has admin permissions for a specific application.

    Args:
        server: The RemoteService instance
        user_id: The user's workspace
        collection_name: The name of the collection to check permissions for
        application_id: The ID of the application to check permissions for

    Returns:
        None if all permissions are granted, raises an error if permissions are missing
    """
    assert await has_application_permission(
        server,
        collection_name,
        application_id,
        accesser_id,
        application_user_id,
    ), "You do not have permission to access the application."


async def has_collection_permission(
    server: RemoteService,
    user_id: str,
    collection_names: str | list[str],
) -> bool:
    """Check if the user has admin permissions for collections.

    Args:
        server: The RemoteService instance
        user_id: The user's workspace
        collection_names: Optional collection names to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """

    coll_artifact_name = collection_artifact_name(collection_names)

    return await has_artifact_permission(
        server,
        user_id,
        coll_artifact_name,
    )


def assert_is_admin_id(user_id: str):
    """Check if user has admin permissions for collections.

    Args:
        user_id: The user's workspace

    Returns:
        None if all permissions are granted, raises an error if permissions are missing
    """
    assert is_admin_id(user_id), "You are not an admin user."


async def assert_has_collection_permission(
    server: RemoteService,
    user_id: str,
    names: str | list[str],
) -> None:
    """Check if user has permission to access these collections.

    Args:
        server: The RemoteService instance
        names: Collection name or list of collection names to access
        user_id: The user's workspace

    Returns:
        None if all permissions are granted, raises an error if permissions are missing
    """

    assert await has_collection_permission(
        server, user_id, names
    ), "You do not have permission to access the collection(s)."


async def create_application_artifact(
    server: RemoteService,
    collection_name: str,
    application_id: str,
    description: str,
    user_id: str,
) -> dict:
    """Create an application artifact.

    Args:
        server: RemoteService instance
        collection_name: Collection name (without workspace prefix)
        application_id: Application ID
        description: Application description
        user_id: User workspace

    Returns:
        Result of artifact creation
    """
    # Create application artifact
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(
        ws_collection_name, user_id, application_id
    )

    # Set up application metadata
    metadata = create_artifact_metadata(
        application_id=application_id,
        collection_name=collection_name,
    )

    # Set up permissions - owner can write, everyone can read
    permissions = get_artifact_permissions(user_id)

    await create_artifact(
        server=server,
        artifact_name=artifact_name,
        description=description,
        permissions=permissions,
        metadata=metadata,
        parent_id=ws_collection_name,
    )

    return {
        "artifact_name": artifact_name,
        "description": description,
        "permissions": permissions,
        "metadata": metadata,
    }


async def delete_application_artifact(
    server: RemoteService, ws_collection_name: str, application_id: str, user_id: str
) -> None:
    """Delete an application artifact.

    Args:
        server: RemoteService instance
        ws_collection_name: Workspace-prefixed collection name
        application_id: str,
        user_id: str
    """
    artifact_name = application_artifact_name(
        ws_collection_name, user_id, application_id
    )
    await delete_artifact(
        server,
        artifact_name,
    )
