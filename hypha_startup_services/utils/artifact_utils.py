import uuid
from typing import Any
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.artifacts import (
    create_artifact,
    delete_artifact,
    get_artifact,
)
from hypha_startup_services.utils.constants import (
    ADMIN_WORKSPACES,
    ARTIFACT_DELIMITER,
)
from hypha_startup_services.utils.format_utils import (
    get_full_collection_name,
    assert_valid_application_name,
)


def get_collection_artifact_name(short_name: str) -> str:
    """Create a full collection artifact name."""
    return get_full_collection_name(short_name)


def get_collection_artifact_names(short_names: list[str]) -> list[str]:
    """Create full collection artifact names."""
    return [get_full_collection_name(name) for name in short_names]


def get_application_artifact_name(
    full_collection_name: str, user_ws: str, application_id: str
) -> str:
    """Create a full application artifact name."""
    assert_valid_application_name(application_id)
    return f"{full_collection_name}{ARTIFACT_DELIMITER}{user_ws}{ARTIFACT_DELIMITER}{application_id}"


def is_admin_ws(user_ws: str) -> bool:
    """Check if a user has admin privileges."""
    return user_ws in ADMIN_WORKSPACES


def make_artifact_permissions(owners: str | list[str]) -> dict:
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
    short_collection_name: str = None,
    application_id: str = None,
    **kwargs,
) -> dict:
    """Create standard metadata for artifacts.

    Args:
        collection_name: The collection name
        application_id: The application ID
        **kwargs: Additional metadata fields

    Returns:
        A metadata dictionary with standard fields
    """
    metadata = {
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
    settings_full_name: dict[str, Any],
) -> None:
    permissions = make_artifact_permissions(owners=ADMIN_WORKSPACES)
    metadata = create_artifact_metadata(
        description=settings_full_name.get("description", ""),
        collection_type="weaviate",
        settings=settings_full_name,
    )

    await create_artifact(
        server,
        settings_full_name["class"],
        settings_full_name.get("description", ""),
        permissions=permissions,
        metadata=metadata,
    )


async def is_user_in_artifact_permissions(
    server: RemoteService, user_ws: str, short_collection_name: str
) -> bool:
    """Check if the user has admin permissions for a specific collection.

    Args:
        server: The RemoteService instance
        user_ws: The user workspace
        collection_name: The name of the collection to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """
    artifact_name = get_collection_artifact_name(short_collection_name)

    artifact = await get_artifact(server, artifact_name)
    permissions = artifact.config.get("permissions", {})
    user_permissions = permissions.get(user_ws, {})
    if user_permissions in ("*", "rw+"):
        return True

    return False


async def has_artifact_permission(
    server: RemoteService, user_ws: str, artifact_name: str
) -> bool:
    if user_ws in ADMIN_WORKSPACES:
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

    coll_artifact_name = get_collection_artifact_names(short_coll_names)

    return await has_artifact_permission(
        server,
        user_ws,
        coll_artifact_name,
    )


def assert_is_admin_ws(user_ws: str):
    """Check if user has admin permissions for collections.

    Args:
        user_ws: The user workspace

    Returns:
        None if all permissions are granted, raises an error if permissions are missing
    """
    assert is_admin_ws(user_ws), "You are not an admin user."


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
) -> dict:
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
    # Create application artifact
    full_collection_name = get_full_collection_name(collection_name)
    artifact_name = get_application_artifact_name(
        full_collection_name, user_ws, application_id
    )

    # Set up application metadata
    metadata = create_artifact_metadata(
        application_id=application_id,
        short_collection_name=collection_name,
    )

    permissions = make_artifact_permissions(owners=user_ws)

    await create_artifact(
        server=server,
        artifact_name=artifact_name,
        description=description,
        permissions=permissions,
        metadata=metadata,
        parent_id=full_collection_name,
    )

    return {
        "artifact_name": artifact_name,
        "description": description,
        "permissions": permissions,
        "metadata": metadata,
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
