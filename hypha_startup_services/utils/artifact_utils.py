import uuid
from typing import Any
from hypha_rpc.rpc import RemoteService
from hypha_startup_services.artifacts import (
    create_artifact,
    delete_artifact,
    get_artifact,
)
from hypha_startup_services.utils.constants import (
    SHARED_WORKSPACE,
    ADMIN_WORKSPACES,
    ARTIFACT_DELIMITER,
)
from hypha_startup_services.utils.format_utils import (
    full_collection_name,
    assert_valid_collection_name,
    assert_valid_application_name,
)


def collection_artifact_name(name: str) -> str:
    """Create a full collection artifact name with workspace prefix."""
    return full_collection_name(name)


def application_artifact_name(ws_collection_name: str, application_id: str) -> str:
    """Create a full application artifact name with workspace prefix."""
    assert_valid_collection_name(ws_collection_name)
    assert_valid_application_name(application_id)
    return f"{ws_collection_name}{ARTIFACT_DELIMITER}{application_id}"


def is_admin_workspace(workspace: str) -> bool:
    """Check if a workspace has admin privileges."""
    return workspace in ADMIN_WORKSPACES


def get_artifact_permissions(
    owner: bool = False, admin: bool = False, read_public: bool = True
) -> dict:
    """Generate permissions dictionary for artifacts.

    Args:
        owner: If True, adds $OWNER to write permissions
        admin: If True, adds $ADMIN to admin and write permissions
        read_public: If True, everyone can read. If False, only owner can read.

    Returns:
        A permissions dictionary with read, write, and admin keys
    """
    permissions = {
        "read": ["*"] if read_public else ["$OWNER"],  # Control read access
        "write": [],  # No write by default
        "admin": [],  # No admin by default
    }

    if owner:
        permissions["write"].append("$OWNER")
        if not read_public:
            # Ensure owner is in read permissions if not public
            permissions["read"].append("$OWNER")

    if admin:
        permissions["admin"].append("$ADMIN")
        permissions["write"].append("$ADMIN")
        if not read_public:
            # Ensure admins can read even if not public
            permissions["read"].append("$ADMIN")

    return permissions


def create_artifact_metadata(
    collection_name: str = None,
    application_id: str = None,
    workspace: str = None,
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
        "created_by": workspace,
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
        full_name = full_collection_name(coll_name)
        await delete_artifact(
            server,
            full_name,
            SHARED_WORKSPACE,
        )


async def create_collection_artifact(
    server: RemoteService,
    settings_with_workspace: dict[str, Any],
    workspace: str,
) -> None:
    # Create an artifact in the shared workspace for the collection
    # This artifact will be used for permission management

    permissions = get_artifact_permissions(owner=True, admin=True)
    metadata = create_artifact_metadata(
        workspace=workspace,
        description=settings_with_workspace.get("description", ""),
        collection_type="weaviate",
        settings=settings_with_workspace,
    )

    await create_artifact(
        server,
        settings_with_workspace["class"],
        settings_with_workspace.get("description", ""),
        SHARED_WORKSPACE,
        permissions=permissions,
        metadata=metadata,
    )


async def has_permission_single(
    server: RemoteService, user_ws: str, collection_name: str
) -> bool:
    """Check if the user has admin permissions for a specific collection.

    Args:
        server: The RemoteService instance
        user_ws: The user's workspace
        collection_name: The name of the collection to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """
    artifact_name = collection_artifact_name(collection_name)

    artifact = await get_artifact(server, artifact_name, user_ws)
    if "permissions" in artifact and "admin" in artifact["permissions"]:
        if user_ws in artifact["permissions"]["admin"]:
            return True

    return False


async def has_permission(
    server: RemoteService, user_ws: str, collection_names: str | list[str]
) -> bool:
    """Check if the user has admin permissions for collections.

    Args:
        server: The RemoteService instance
        user_ws: The user's workspace
        collection_names: Optional collection names to check permissions for

    Returns:
        True if the user has admin permissions, False otherwise
    """

    # First check if the user is in the admin workspaces list
    if is_admin_workspace(user_ws):
        return True

    if isinstance(collection_names, str):
        collection_names = [collection_names]

    for collection_name in collection_names:
        if not await has_permission_single(server, user_ws, collection_name):
            return False

    return True


def assert_is_admin_ws(user_ws: str):
    """Check if user has admin permissions for collections.

    Args:
        user_ws: The user's workspace

    Returns:
        None if all permissions are granted, raises an error if permissions are missing
    """
    assert is_admin_workspace(user_ws), "You are not an admin user."


async def assert_has_permission(
    server: RemoteService, user_ws: str, names: str | list[str]
) -> None:
    """Check if user has permission to delete these collections.

    Args:
        server: The RemoteService instance
        names: Collection name or list of collection names to access
        user_ws: The user's workspace

    Returns:
        None if all permissions are granted, raises an error if permissions are missing
    """

    assert await has_permission(
        server, user_ws, names
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
        collection_name: Collection name (without workspace prefix)
        application_id: Application ID
        description: Application description
        user_ws: User workspace

    Returns:
        Result of artifact creation
    """
    # Create application artifact
    ws_collection_name = full_collection_name(collection_name)
    artifact_name = application_artifact_name(ws_collection_name, application_id)

    # Set up application metadata
    metadata = create_artifact_metadata(
        application_id=application_id,
        collection_name=collection_name,
        workspace=user_ws,
    )

    # Set up permissions - owner can write, everyone can read
    permissions = get_artifact_permissions(owner=True)

    return await create_artifact(
        server=server,
        artifact_name=artifact_name,
        description=description,
        workspace=user_ws,
        permissions=permissions,
        metadata=metadata,
    )


async def delete_application_artifact(
    server: RemoteService, ws_collection_name: str, application_id: str, tenant_ws: str
) -> None:
    """Delete an application artifact.

    Args:
        server: RemoteService instance
        ws_collection_name: Workspace-prefixed collection name
        application_id: Application ID
        tenant_ws: Tenant workspace
    """
    artifact_name = application_artifact_name(ws_collection_name, application_id)
    await delete_artifact(
        server,
        artifact_name,
        tenant_ws,
    )
