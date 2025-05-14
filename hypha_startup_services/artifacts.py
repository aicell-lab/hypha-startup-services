from typing import Any
from hypha_rpc.rpc import RemoteException, RemoteService


async def create_artifact(
    server: RemoteService,
    artifact_name: str,
    description: str,
    workspace: str,
    permissions: dict | None = None,
    metadata: dict | None = None,
) -> dict[str, Any]:
    """Create a new application artifact in the workspace.

    Adds workspace prefix to the collection name before creating it.
    Returns the application configuration with the workspace prefix removed.
    """
    if metadata is None:
        metadata = {}

    galleryManifest = {
        "name": artifact_name,
        "description": description,
        "collection": [],
        "metadata": metadata,
    }

    artifact_manager = await server.get_service("public/artifact-manager")

    try:
        result = await artifact_manager.create(
            type="collection",
            workspace=workspace,
            alias=artifact_name,
            manifest=galleryManifest,
            permissions=permissions,
        )
        return result
    except RemoteException as e:
        print(f"Artifact couldn't be created. It likely already exists. Error: {e}")
        return {"error": str(e)}


async def list_artifacts(
    server: RemoteService,
    tenant: str,
) -> list[dict[str, Any]]:
    """List all artifacts in the workspace.

    Adds workspace prefix to the collection name before listing it.
    Returns the application configuration with the workspace prefix removed.
    """

    artifact_manager = await server.get_service("public/artifact-manager")

    try:
        artifacts = await artifact_manager.list(workspace=tenant)
        return artifacts
    except RemoteException as e:
        print(f"Error listing artifacts. Error: {e}")
        return []


async def delete_artifact(
    server: RemoteService,
    artifact_name: str,
    tenant: str,
) -> None:
    """Delete an artifact in the workspace.

    Adds workspace prefix to the collection name before deleting it.
    """
    artifact_manager = await server.get_service("public/artifact-manager")

    try:
        await artifact_manager.delete(workspace=tenant, alias=artifact_name)
    except RemoteException as e:
        print(f"Error deleting artifact. Error: {e}")


async def get_artifact(
    server: RemoteService,
    artifact_name: str,
    tenant: str,
) -> dict[str, Any]:
    """Get an artifact in the workspace.

    Adds workspace prefix to the collection name before getting it.
    """
    artifact_manager = await server.get_service("public/artifact-manager")

    try:
        artifact = await artifact_manager.get(workspace=tenant, alias=artifact_name)
        return artifact
    except RemoteException as e:
        print(f"Error getting artifact. Error: {e}")
        return {"error": str(e)}


async def artifact_exists(
    server: RemoteService,
    artifact_name: str,
    tenant: str,
) -> bool:
    """Check if an artifact exists in the workspace.

    Adds workspace prefix to the collection name before checking it.
    """

    artifact_response = await get_artifact(
        server=server,
        artifact_name=artifact_name,
        tenant=tenant,
    )
    if "error" in artifact_response:
        return False
    return True
