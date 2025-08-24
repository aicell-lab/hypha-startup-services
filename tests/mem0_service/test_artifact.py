"""Unit tests for mem0_service artifact module."""

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hypha_rpc.rpc import RemoteException

from hypha_startup_services.common.artifacts import (
    artifact_exists,
    create_artifact,
    delete_artifact,
    get_artifact,
)
from hypha_startup_services.mem0_service.utils.models import AgentArtifactParams
from tests.conftest import USER1_WS
from tests.mem0_service.utils import TEST_AGENT_ID


@pytest.fixture
def mock_server_setup():
    """Create a mock server with artifact manager."""
    mock_server = AsyncMock()
    mock_artifact_manager = AsyncMock()
    mock_server.get_service.return_value = mock_artifact_manager
    return mock_server, mock_artifact_manager


@pytest.fixture
def patch_get_server(mock_server_setup):
    """Patch get_server to return our mock server as an async context manager, and allow call assertion."""
    server, artifact_manager = mock_server_setup

    @contextlib.asynccontextmanager
    async def mock_get_server_cm(*args, **kwargs):
        yield server

    mock_cm = MagicMock(wraps=mock_get_server_cm)
    with patch("hypha_startup_services.common.artifacts.get_server", mock_cm):
        yield mock_cm, server, artifact_manager


class TestGetArtifact:
    """Test cases for get_artifact function."""

    @pytest.mark.asyncio
    async def test_get_artifact_success(self, patch_get_server):
        """Test successful artifact retrieval."""
        mock_get_server, mock_server, mock_artifact_manager = patch_get_server

        expected_artifact = {
            "id": "test-artifact-123",
            "name": "Test Artifact",
            "config": {"permissions": {}},
        }
        mock_artifact_manager.read.return_value = expected_artifact

        result = await get_artifact("test-artifact-123")

        mock_get_server.assert_called_once_with("https://hypha.aicell.io")
        mock_server.get_service.assert_called_once_with("hypha-agents/artifact-manager")
        mock_artifact_manager.read.assert_called_once_with(
            artifact_id="test-artifact-123",
        )
        assert result == expected_artifact

    @pytest.mark.asyncio
    async def test_get_artifact_remote_exception(self, patch_get_server):
        """Test artifact retrieval with RemoteException."""
        _, _, mock_artifact_manager = patch_get_server

        error_message = "Artifact not found"
        mock_artifact_manager.read.side_effect = RemoteException(error_message)

        with patch("hypha_startup_services.common.artifacts.logger"):
            try:
                await get_artifact("nonexistent-artifact")
                assert False, "Expected RemoteException to be raised"
            except RemoteException:
                pass

    @pytest.mark.asyncio
    async def test_get_artifact_server_error(self):
        """Test artifact retrieval with server connection error."""
        with patch(
            "hypha_startup_services.common.artifacts.get_server",
        ) as mock_get_server:
            mock_get_server.side_effect = Exception("Server connection failed")

            with pytest.raises(Exception, match="Server connection failed"):
                await get_artifact("test-artifact")


class TestCreateArtifact:
    """Test cases for create_artifact function."""

    @pytest.fixture
    def sample_artifact_params(self):
        """Create sample artifact parameters for testing."""
        return AgentArtifactParams(
            agent_id=TEST_AGENT_ID,
            creator_id=USER1_WS,
            general_permission="rw",
            desc="Test artifact",
            metadata={"test": True},
        )

    @pytest.mark.asyncio
    async def test_create_artifact_success(
        self,
        sample_artifact_params,
        patch_get_server,
    ):
        """Test successful artifact creation."""
        mock_get_server, mock_server, mock_artifact_manager = patch_get_server

        with patch(
            "hypha_startup_services.common.artifacts.artifact_exists",
        ) as mock_exists:
            mock_exists.return_value = False

            await create_artifact(sample_artifact_params)

            mock_get_server.assert_called_once_with("https://hypha.aicell.io")
            mock_server.get_service.assert_called_once_with(
                "public/artifact-manager",
            )
            mock_exists.assert_called_once_with(
                artifact_id=sample_artifact_params.artifact_id,
            )
            mock_artifact_manager.create.assert_called_once_with(
                **sample_artifact_params.creation_dict,
            )

    @pytest.mark.asyncio
    async def test_create_artifact_already_exists(
        self,
        sample_artifact_params,
        patch_get_server,
    ):
        """Test artifact creation when artifact already exists."""
        _, _, mock_artifact_manager = patch_get_server

        with patch(
            "hypha_startup_services.common.artifacts.artifact_exists",
        ) as mock_exists:
            mock_exists.return_value = True

            with patch("hypha_startup_services.common.artifacts.logger") as mock_logger:
                await create_artifact(sample_artifact_params)

                mock_logger.warning.assert_called_once_with(
                    "Artifact with ID %s already exists. Skipping creation.",
                    sample_artifact_params.artifact_id,
                )
                mock_artifact_manager.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_artifact_remote_exception(
        self,
        sample_artifact_params,
        patch_get_server,
    ):
        """Test artifact creation with RemoteException."""
        _, _, mock_artifact_manager = patch_get_server

        error_message = "Creation failed"
        mock_artifact_manager.create.side_effect = RemoteException(error_message)

        with patch(
            "hypha_startup_services.common.artifacts.artifact_exists",
        ) as mock_exists:
            mock_exists.return_value = False

            with patch("hypha_startup_services.common.artifacts.logger"):
                try:
                    await create_artifact(sample_artifact_params)
                    assert False, "Expected RemoteException to be raised"
                except RemoteException:
                    pass


class TestDeleteArtifact:
    """Test cases for delete_artifact function."""

    @pytest.mark.asyncio
    async def test_delete_artifact_success(self, patch_get_server):
        """Test successful artifact deletion."""
        mock_get_server, mock_server, mock_artifact_manager = patch_get_server

        await delete_artifact("test-artifact-123")

        mock_get_server.assert_called_once_with("https://hypha.aicell.io")
        mock_server.get_service.assert_called_once_with("public/artifact-manager")
        mock_artifact_manager.delete.assert_called_once_with(
            artifact_id="test-artifact-123",
            delete_files=True,
        )

    @pytest.mark.asyncio
    async def test_delete_artifact_remote_exception(self, patch_get_server):
        """Test artifact deletion with RemoteException."""
        _, _, mock_artifact_manager = patch_get_server

        error_message = "Deletion failed"
        mock_artifact_manager.delete.side_effect = RemoteException(error_message)

        with patch("hypha_startup_services.common.artifacts.logger") as mock_logger:
            await delete_artifact("test-artifact-123")

            mock_logger.warning.assert_called_once_with(
                "Error deleting artifact '%s'. Error: %s",
                "test-artifact-123",
                mock_artifact_manager.delete.side_effect,
            )

    @pytest.mark.asyncio
    async def test_delete_artifact_server_error(self):
        """Test artifact deletion with server connection error."""
        with patch(
            "hypha_startup_services.common.artifacts.get_server",
        ) as mock_get_server:
            mock_get_server.side_effect = Exception("Server connection failed")

            with pytest.raises(Exception, match="Server connection failed"):
                await delete_artifact("test-artifact")


class TestArtifactExists:
    """Test cases for artifact_exists function."""

    @pytest.mark.asyncio
    async def test_artifact_exists_true(self):
        """Test artifact_exists returns True when artifact exists."""
        with patch("hypha_startup_services.common.artifacts.get_artifact") as mock_get:
            mock_get.return_value = {"id": "test-artifact", "name": "Test"}

            result = await artifact_exists("test-artifact")

            assert result is True
            mock_get.assert_called_once_with(artifact_id="test-artifact")

    @pytest.mark.asyncio
    async def test_artifact_exists_false(self):
        """Test artifact_exists returns False when artifact doesn't exist."""
        with patch("hypha_startup_services.common.artifacts.get_artifact") as mock_get:
            mock_get.side_effect = RemoteException("Artifact not found")

            result = await artifact_exists("nonexistent-artifact")

            assert result is False
            mock_get.assert_called_once_with(artifact_id="nonexistent-artifact")

    @pytest.mark.asyncio
    async def test_artifact_exists_with_exception(self):
        """Test artifact_exists when get_artifact raises exception."""
        with patch("hypha_startup_services.common.artifacts.get_artifact") as mock_get:
            mock_get.side_effect = Exception("Unexpected error")

            with pytest.raises(Exception, match="Unexpected error"):
                await artifact_exists("test-artifact")


class TestIntegration:
    """Integration tests for artifact operations."""

    @pytest.mark.asyncio
    async def test_create_and_check_existence_workflow(self, patch_get_server):
        """Test the workflow of creating an artifact and checking its existence."""
        _, _, mock_artifact_manager = patch_get_server

        artifact_params = AgentArtifactParams(
            agent_id=TEST_AGENT_ID,
            creator_id=USER1_WS,
            general_permission="rw",
        )

        # First call to artifact_exists should return False (doesn't exist)
        # Second call should return True (after creation)
        with patch("hypha_startup_services.common.artifacts.get_artifact") as mock_get:
            mock_get.side_effect = [
                RemoteException("Artifact not found"),  # First call - throws exception
                {
                    "id": artifact_params.artifact_id,
                    "name": "Test",
                },  # Second call - exists
            ]

            # Create the artifact
            await create_artifact(artifact_params)

            # Check it exists
            exists = await artifact_exists(artifact_params.artifact_id)

            assert exists is True
            mock_artifact_manager.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_and_check_existence_workflow(self, patch_get_server):
        """Test the workflow of deleting an artifact and checking it no longer exists."""
        _, _, mock_artifact_manager = patch_get_server

        artifact_id = "test-artifact-to-delete"

        with patch("hypha_startup_services.common.artifacts.get_artifact") as mock_get:
            mock_get.side_effect = [
                {"id": artifact_id, "name": "Test"},  # First call - exists
                RemoteException(
                    "Artifact not found",
                ),  # Second call - doesn't exist after deletion
            ]

            # Check it exists first
            exists_before = await artifact_exists(artifact_id)
            assert exists_before is True

            # Delete the artifact
            await delete_artifact(artifact_id)

            # Check it no longer exists
            exists_after = await artifact_exists(artifact_id)
            assert exists_after is False

            mock_artifact_manager.delete.assert_called_once_with(
                artifact_id=artifact_id,
                delete_files=True,
            )
