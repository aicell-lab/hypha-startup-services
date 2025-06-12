"""Unit tests for mem0_service methods module."""

from unittest.mock import AsyncMock, patch
import pytest

from hypha_startup_services.mem0_service.methods import (
    init_agent,
    init_run,
    mem0_add,
    mem0_search,
)
from hypha_startup_services.mem0_service.utils.models import HyphaPermissionError
from tests.conftest import USER1_WS, USER2_WS
from tests.mem0_service.utils import TEST_AGENT_ID, TEST_RUN_ID, TEST_MESSAGES


class TestInitAgent:
    """Test cases for init_agent function."""

    @pytest.mark.asyncio
    async def test_init_agent_success(self):
        """Test successful agent initialization."""
        # Mock server and context
        mock_server = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.artifacts.create_artifact"
        ) as mock_create:
            await init_agent(
                agent_id=TEST_AGENT_ID,
                description="Test agent",
                metadata={"test": True},
                server=mock_server,
                context=context,
            )

            # Verify create_artifact was called with correct parameters
            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert call_args[1]["server"] == mock_server

            artifact_params = call_args[1]["artifact_params"]
            assert artifact_params.agent_id == TEST_AGENT_ID
            assert artifact_params.creator_id == USER1_WS
            assert artifact_params.general_permission == "r"
            assert artifact_params.desc == "Test agent"
            assert artifact_params.metadata == {"test": True}
            assert artifact_params.artifact_type == "collection"

    @pytest.mark.asyncio
    async def test_init_agent_minimal_params(self):
        """Test agent initialization with minimal parameters."""
        mock_server = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.artifacts.create_artifact"
        ) as mock_create:
            await init_agent(
                agent_id=TEST_AGENT_ID,
                server=mock_server,
                context=context,
            )

            mock_create.assert_called_once()
            artifact_params = mock_create.call_args[1]["artifact_params"]
            assert artifact_params.agent_id == TEST_AGENT_ID
            assert artifact_params.desc is None
            assert artifact_params.metadata is None

    @pytest.mark.asyncio
    async def test_init_agent_create_artifact_failure(self):
        """Test handling of create_artifact failure."""
        mock_server = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.artifacts.create_artifact"
        ) as mock_create:
            mock_create.side_effect = Exception("Creation failed")

            with pytest.raises(Exception, match="Creation failed"):
                await init_agent(
                    agent_id=TEST_AGENT_ID,
                    server=mock_server,
                    context=context,
                )


class TestInitRun:
    """Test cases for init_run function."""

    @pytest.mark.asyncio
    async def test_init_run_with_run_id(self):
        """Test run initialization with run_id provided."""
        mock_server = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.artifacts.create_artifact"
        ) as mock_create:
            await init_run(
                agent_id=TEST_AGENT_ID,
                workspace=USER2_WS,
                run_id=TEST_RUN_ID,
                description="Test run",
                metadata={"run": True},
                server=mock_server,
                context=context,
            )

            # Should be called twice - once for workspace, once for run
            assert mock_create.call_count == 2

            # Check workspace artifact call
            workspace_call = mock_create.call_args_list[0]
            workspace_params = workspace_call[1]["artifact_params"]
            assert TEST_AGENT_ID in workspace_params.artifact_id
            assert USER2_WS in workspace_params.artifact_id

            # Check run artifact call
            run_call = mock_create.call_args_list[1]
            run_params = run_call[1]["artifact_params"]
            assert TEST_AGENT_ID in run_params.artifact_id
            assert USER2_WS in run_params.artifact_id
            assert TEST_RUN_ID in run_params.artifact_id

    @pytest.mark.asyncio
    async def test_init_run_without_run_id(self):
        """Test run initialization without run_id."""
        mock_server = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.artifacts.create_artifact"
        ) as mock_create:
            await init_run(
                agent_id=TEST_AGENT_ID,
                workspace=USER2_WS,
                run_id=None,
                server=mock_server,
                context=context,
            )

            # Should be called once only for workspace artifact when run_id is None
            mock_create.assert_called_once()
            workspace_params = mock_create.call_args[1]["artifact_params"]
            assert TEST_AGENT_ID in workspace_params.artifact_id
            assert USER2_WS in workspace_params.artifact_id

    @pytest.mark.asyncio
    async def test_init_run_minimal_params(self):
        """Test run initialization with minimal parameters."""
        mock_server = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.artifacts.create_artifact"
        ) as mock_create:
            await init_run(
                agent_id=TEST_AGENT_ID,
                workspace=USER2_WS,
                run_id=TEST_RUN_ID,
                server=mock_server,
                context=context,
            )

            assert mock_create.call_count == 2
            workspace_params = mock_create.call_args_list[0][1]["artifact_params"]
            assert workspace_params.desc is None
            assert workspace_params.metadata is None


class TestMem0Add:
    """Test cases for mem0_add function."""

    @pytest.mark.asyncio
    async def test_mem0_add_success(self):
        """Test successful memory addition."""
        mock_server = AsyncMock()
        mock_memory = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.permissions.require_permission"
        ) as mock_require:
            await mem0_add(
                messages=TEST_MESSAGES,
                agent_id=TEST_AGENT_ID,
                workspace=USER2_WS,
                server=mock_server,
                memory=mock_memory,
                context=context,
                run_id=TEST_RUN_ID,
                custom_param="test",
            )

            # Verify permission check
            mock_require.assert_called_once()
            permission_params = mock_require.call_args[0][1]
            assert permission_params.agent_id == TEST_AGENT_ID
            assert permission_params.accessed_workspace == USER2_WS
            assert permission_params.accessor_workspace == USER1_WS
            assert permission_params.run_id == TEST_RUN_ID
            assert permission_params.operation == "rw"

            # Verify memory.add call
            mock_memory.add.assert_called_once_with(
                TEST_MESSAGES,
                user_id=USER2_WS,
                agent_id=TEST_AGENT_ID,
                run_id=TEST_RUN_ID,
                custom_param="test",
            )

    @pytest.mark.asyncio
    async def test_mem0_add_permission_denied(self):
        """Test memory addition with permission denied."""
        mock_server = AsyncMock()
        mock_memory = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.permissions.require_permission"
        ) as mock_require:
            mock_require.side_effect = HyphaPermissionError("Permission denied", None)

            with pytest.raises(HyphaPermissionError, match="Permission denied"):
                await mem0_add(
                    messages=TEST_MESSAGES,
                    agent_id=TEST_AGENT_ID,
                    workspace=USER2_WS,
                    server=mock_server,
                    memory=mock_memory,
                    context=context,
                )

            # Memory should not be called if permission is denied
            mock_memory.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_mem0_add_without_run_id(self):
        """Test memory addition without run_id."""
        mock_server = AsyncMock()
        mock_memory = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.permissions.require_permission"
        ) as mock_require:
            await mem0_add(
                messages=TEST_MESSAGES,
                agent_id=TEST_AGENT_ID,
                workspace=USER2_WS,
                server=mock_server,
                memory=mock_memory,
                context=context,
            )

            permission_params = mock_require.call_args[0][1]
            assert permission_params.run_id is None

            mock_memory.add.assert_called_once_with(
                TEST_MESSAGES,
                user_id=USER2_WS,
                agent_id=TEST_AGENT_ID,
                run_id=None,
            )


class TestMem0Search:
    """Test cases for mem0_search function."""

    @pytest.mark.asyncio
    async def test_mem0_search_success(self):
        """Test successful memory search."""
        mock_server = AsyncMock()
        mock_memory = AsyncMock()
        mock_memory.search.return_value = {
            "results": [{"id": "1", "memory": "test", "score": 0.9}]
        }
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.permissions.require_permission"
        ) as mock_require:
            result = await mem0_search(
                query="test query",
                agent_id=TEST_AGENT_ID,
                workspace=USER2_WS,
                server=mock_server,
                memory=mock_memory,
                context=context,
                run_id=TEST_RUN_ID,
                limit=10,
            )

            # Verify permission check
            mock_require.assert_called_once()
            permission_params = mock_require.call_args[0][1]
            assert permission_params.agent_id == TEST_AGENT_ID
            assert permission_params.accessed_workspace == USER2_WS
            assert permission_params.accessor_workspace == USER1_WS
            assert permission_params.run_id == TEST_RUN_ID
            assert permission_params.operation == "r"

            # Verify memory.search call and result
            mock_memory.search.assert_called_once_with(
                "test query",
                user_id=USER2_WS,
                agent_id=TEST_AGENT_ID,
                run_id=TEST_RUN_ID,
                limit=10,
            )

            assert result == {"results": [{"id": "1", "memory": "test", "score": 0.9}]}

    @pytest.mark.asyncio
    async def test_mem0_search_permission_denied(self):
        """Test memory search with permission denied."""
        mock_server = AsyncMock()
        mock_memory = AsyncMock()
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch(
            "hypha_startup_services.common.permissions.require_permission"
        ) as mock_require:
            mock_require.side_effect = HyphaPermissionError("Permission denied", None)

            with pytest.raises(HyphaPermissionError, match="Permission denied"):
                await mem0_search(
                    query="test query",
                    agent_id=TEST_AGENT_ID,
                    workspace=USER2_WS,
                    server=mock_server,
                    memory=mock_memory,
                    context=context,
                )

            # Memory should not be called if permission is denied
            mock_memory.search.assert_not_called()

    @pytest.mark.asyncio
    async def test_mem0_search_empty_result(self):
        """Test memory search with empty results."""
        mock_server = AsyncMock()
        mock_memory = AsyncMock()
        mock_memory.search.return_value = {"results": []}
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch("hypha_startup_services.common.permissions.require_permission"):
            result = await mem0_search(
                query="nonexistent query",
                agent_id=TEST_AGENT_ID,
                workspace=USER2_WS,
                server=mock_server,
                memory=mock_memory,
                context=context,
            )

            assert result == {"results": []}

    @pytest.mark.asyncio
    async def test_mem0_search_without_run_id(self):
        """Test memory search without run_id."""
        mock_server = AsyncMock()
        mock_memory = AsyncMock()
        mock_memory.search.return_value = {"results": []}
        context = {"user": {"scope": {"current_workspace": USER1_WS}}}

        with patch("hypha_startup_services.common.permissions.require_permission"):
            await mem0_search(
                query="test query",
                agent_id=TEST_AGENT_ID,
                workspace=USER2_WS,
                server=mock_server,
                memory=mock_memory,
                context=context,
            )

            mock_memory.search.assert_called_once_with(
                "test query",
                user_id=USER2_WS,
                agent_id=TEST_AGENT_ID,
                run_id=None,
            )
