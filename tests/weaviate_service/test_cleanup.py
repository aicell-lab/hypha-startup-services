"""Tests for Weaviate cleanup service."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from hypha_startup_services.common.constants import COLLECTION_DELIMITER
from hypha_startup_services.weaviate_service.cleanup import (
    cleanup_weaviate_resources,
)


@pytest.fixture
def mock_server() -> MagicMock:
    """Create a mock server with artifact manager."""
    server = MagicMock()
    server.config.workspace = "hypha-agents"

    artifact_manager = AsyncMock()
    # Setup get_service to return the mock artifact manager
    server.get_service = AsyncMock(return_value=artifact_manager)

    # Store artifact_manager on server for easy access in tests
    server.artifact_manager = artifact_manager
    return server


@pytest.fixture
def mock_client() -> MagicMock:
    """Create a mock Weaviate client."""
    client = MagicMock()
    client.collections = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_cleanup_orphaned_artifact(
    mock_server: MagicMock,
    mock_client: MagicMock,
) -> None:
    """Test cleanup of artifact without corresponding collection."""
    # Setup orphaned artifact
    orphaned_artifact_alias = f"Shared{COLLECTION_DELIMITER}Orphaned"
    artifacts = [
        {"alias": orphaned_artifact_alias, "name": "Orphaned Artifact"},
        {"alias": "OtherArtifact", "name": "Normal Artifact"},  # No DELIM
    ]
    mock_server.artifact_manager.list.return_value = artifacts

    # Setup weaviate collections (empty)
    mock_client.collections.list_all.return_value = {}

    # Run cleanup
    await cleanup_weaviate_resources(mock_server, mock_client)

    # Verify mock calls
    mock_server.get_service.assert_called_once_with("public/artifact-manager")
    mock_server.artifact_manager.list.assert_called_once_with(
        context={"workspace": "hypha-agents"},
    )

    # Verify deletion of orphaned artifact
    mock_server.artifact_manager.delete.assert_called_once_with(
        artifact_id=orphaned_artifact_alias,
        recursive=True,
    )


@pytest.mark.asyncio
async def test_cleanup_orphaned_collection(
    mock_server: MagicMock,
    mock_client: MagicMock,
) -> None:
    """Test cleanup of collection without corresponding artifact."""
    # Setup artifacts (empty)
    mock_server.artifact_manager.list.return_value = []

    # Setup orphaned collection
    orphaned_coll_name = f"Shared{COLLECTION_DELIMITER}OrphanedColl"
    collections = {
        orphaned_coll_name: MagicMock(),
        "SystemCollection": MagicMock(),  # No DELIM, should be ignored
    }
    mock_client.collections.list_all.return_value = collections

    # Run cleanup
    await cleanup_weaviate_resources(mock_server, mock_client)

    # Verify deletion of orphaned collection
    mock_client.collections.delete.assert_called_once_with(orphaned_coll_name)


@pytest.mark.asyncio
async def test_cleanup_valid_pair(
    mock_server: MagicMock,
    mock_client: MagicMock,
) -> None:
    """Test that valid pairs are preserved."""
    # Setup matching pair
    alias = f"Shared{COLLECTION_DELIMITER}Valid"
    artifacts = [{"alias": alias, "name": "Valid Artifact"}]
    collections = {alias: MagicMock()}

    mock_server.artifact_manager.list.return_value = artifacts
    mock_client.collections.list_all.return_value = collections

    # Run cleanup
    await cleanup_weaviate_resources(mock_server, mock_client)

    # Verify NO deletions
    mock_server.artifact_manager.delete.assert_not_called()
    mock_client.collections.delete.assert_not_called()


@pytest.mark.asyncio
async def test_cleanup_partial_match(
    mock_server: MagicMock,
    mock_client: MagicMock,
) -> None:
    """Test cleanup with mix of valid and orphaned resources."""
    valid_alias = f"Shared{COLLECTION_DELIMITER}Valid"
    orphaned_artifact_alias = f"Shared{COLLECTION_DELIMITER}ArtifactOnly"
    orphaned_coll_name = f"Shared{COLLECTION_DELIMITER}CollOnly"

    # Artifacts: Valid + Orphaned Artifact
    artifacts = [
        {"alias": valid_alias},
        {"alias": orphaned_artifact_alias},
    ]
    mock_server.artifact_manager.list.return_value = artifacts

    # Collections: Valid + Orphaned Collection
    collections = {
        valid_alias: MagicMock(),
        orphaned_coll_name: MagicMock(),
    }
    mock_client.collections.list_all.return_value = collections

    # Run cleanup
    await cleanup_weaviate_resources(mock_server, mock_client)

    # Verify deletions
    mock_server.artifact_manager.delete.assert_called_once_with(
        artifact_id=orphaned_artifact_alias,
        recursive=True,
    )
    mock_client.collections.delete.assert_called_once_with(orphaned_coll_name)
