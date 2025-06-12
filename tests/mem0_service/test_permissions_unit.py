"""Unit tests for mem0_service permissions module."""

from unittest.mock import AsyncMock, patch
import pytest
from hypha_rpc.rpc import RemoteException


from hypha_startup_services.common.permissions import (
    get_user_permissions,
    user_has_operation_permission,
    has_permission,
    require_permission,
    HyphaPermissionError,
)
from hypha_startup_services.mem0_service.utils.models import (
    PermissionParams,
)
from tests.conftest import USER1_WS, USER2_WS
from tests.mem0_service.utils import TEST_AGENT_ID


class TestGetUserPermissions:
    """Test cases for get_user_permissions function."""

    @pytest.mark.asyncio
    async def test_get_user_permissions_success(self):
        """Test successful retrieval of user permissions."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="r",
        )

        mock_artifact = {
            "config": {
                "permissions": {
                    USER1_WS: ["r", "rw"],
                    USER2_WS: "*",
                }
            }
        }

        with patch(
            "hypha_startup_services.common.permissions.get_artifact"
        ) as mock_get:
            mock_get.return_value = mock_artifact

            result = await get_user_permissions(mock_server, permission_params)

            assert result == ["r", "rw"]
            mock_get.assert_called_once_with(mock_server, permission_params.artifact_id)

    @pytest.mark.asyncio
    async def test_get_user_permissions_no_permissions(self):
        """Test user permissions when user has no permissions."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace="ws-unknown-user",
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="r",
        )

        mock_artifact = {
            "config": {
                "permissions": {
                    USER1_WS: ["r", "rw"],
                    USER2_WS: "*",
                }
            }
        }

        with patch(
            "hypha_startup_services.common.permissions.get_artifact"
        ) as mock_get:
            mock_get.return_value = mock_artifact

            result = await get_user_permissions(mock_server, permission_params)

            assert result == {}  # Unknown user should have no permissions
            mock_get.assert_called_once_with(mock_server, permission_params.artifact_id)

    @pytest.mark.asyncio
    async def test_get_user_permissions_artifact_error(self):
        """Test user permissions when artifact retrieval fails."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="r",
        )

        with patch(
            "hypha_startup_services.common.permissions.get_artifact"
        ) as mock_get:
            error = RemoteException("Artifact not found")
            mock_get.side_effect = error

            with patch(
                "hypha_startup_services.common.permissions.logger"
            ) as mock_logger:
                result = await get_user_permissions(mock_server, permission_params)

                assert result == {}
                mock_logger.error.assert_called_once_with(
                    "Failed to retrieve artifact %s: %s",
                    permission_params.artifact_id,
                    error,
                )

    @pytest.mark.asyncio
    async def test_get_user_permissions_no_config(self):
        """Test user permissions when artifact has no config."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="r",
        )

        mock_artifact = {"id": "test", "name": "Test Artifact"}

        with patch(
            "hypha_startup_services.common.permissions.get_artifact"
        ) as mock_get:
            mock_get.return_value = mock_artifact

            result = await get_user_permissions(mock_server, permission_params)

            assert result == {}


class TestUserHasOperationPermission:
    """Test cases for user_has_operation_permission function."""

    @pytest.mark.asyncio
    async def test_user_has_operation_permission_true(self):
        """Test user has the requested operation permission."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="r",
        )

        with patch(
            "hypha_startup_services.common.permissions.get_user_permissions"
        ) as mock_get:
            mock_get.return_value = ["r", "rw"]

            result = await user_has_operation_permission(mock_server, permission_params)

            assert result is True
            mock_get.assert_called_once_with(mock_server, permission_params)

    @pytest.mark.asyncio
    async def test_user_has_operation_permission_false(self):
        """Test user doesn't have the requested operation permission."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="rw",
        )

        with patch(
            "hypha_startup_services.common.permissions.get_user_permissions"
        ) as mock_get:
            mock_get.return_value = ["r"]

            result = await user_has_operation_permission(mock_server, permission_params)

            assert result is False

    @pytest.mark.asyncio
    async def test_user_has_operation_permission_wildcard(self):
        """Test user has wildcard permission."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="rw",
        )

        with patch(
            "hypha_startup_services.common.permissions.get_user_permissions"
        ) as mock_get:
            mock_get.return_value = "*"

            result = await user_has_operation_permission(mock_server, permission_params)

            assert result is True

    @pytest.mark.asyncio
    async def test_user_has_operation_permission_empty(self):
        """Test user has no permissions."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="r",
        )

        with patch(
            "hypha_startup_services.common.permissions.get_user_permissions"
        ) as mock_get:
            mock_get.return_value = {}

            result = await user_has_operation_permission(mock_server, permission_params)

            assert result is False


class TestHasPermission:
    """Test cases for has_permission function."""

    @pytest.mark.asyncio
    async def test_has_permission_admin_workspace(self):
        """Test permission check for admin workspace."""
        mock_server = AsyncMock()

        # Test with admin workspace from constants
        with patch(
            "hypha_startup_services.common.permissions.ADMIN_WORKSPACES",
            ["admin-ws"],
        ):
            permission_params = PermissionParams(
                accessor_workspace="admin-ws",
                agent_id=TEST_AGENT_ID,
                accessed_workspace=USER2_WS,
                operation="rw",
            )

            result = await has_permission(mock_server, permission_params)

            assert result is True

    @pytest.mark.asyncio
    async def test_has_permission_user_permission_granted(self):
        """Test permission check for regular user with valid permissions."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="r",
        )

        with patch("hypha_startup_services.common.permissions.ADMIN_WORKSPACES", []):
            with patch(
                "hypha_startup_services.common.permissions.user_has_operation_permission"
            ) as mock_user_perm:
                mock_user_perm.return_value = True

                with patch(
                    "hypha_startup_services.common.permissions.logger"
                ) as mock_logger:
                    result = await has_permission(mock_server, permission_params)

                    assert result is True
                    mock_logger.debug.assert_called_once_with(
                        "Granting permission to workspace %s for operation %s on %s",
                        USER1_WS,
                        "r",
                        permission_params.resource_description,
                    )

    @pytest.mark.asyncio
    async def test_has_permission_user_permission_denied(self):
        """Test permission check for regular user without valid permissions."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="rw",
        )

        with patch("hypha_startup_services.common.permissions.ADMIN_WORKSPACES", []):
            with patch(
                "hypha_startup_services.common.permissions.user_has_operation_permission"
            ) as mock_user_perm:
                mock_user_perm.return_value = False

                with patch(
                    "hypha_startup_services.common.permissions.logger"
                ) as mock_logger:
                    result = await has_permission(mock_server, permission_params)

                    assert result is False
                    mock_logger.info.assert_called_once_with(
                        "Permission denied for workspace %s, operation %s on %s",
                        USER1_WS,
                        "rw",
                        permission_params.resource_description,
                    )


class TestRequirePermission:
    """Test cases for require_permission function."""

    @pytest.mark.asyncio
    async def test_require_permission_success(self):
        """Test require_permission when user has permission."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="r",
        )

        with patch(
            "hypha_startup_services.common.permissions.has_permission"
        ) as mock_has:
            mock_has.return_value = True

            # Should not raise an exception
            await require_permission(mock_server, permission_params)

            mock_has.assert_called_once_with(mock_server, permission_params)

    @pytest.mark.asyncio
    async def test_require_permission_denied(self):
        """Test require_permission when user doesn't have permission."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="rw",
        )

        with patch(
            "hypha_startup_services.common.permissions.has_permission"
        ) as mock_has:
            mock_has.return_value = False

            with pytest.raises(HyphaPermissionError) as exc_info:
                await require_permission(mock_server, permission_params)

            error = exc_info.value
            assert "Permission denied for rw operation" in str(error)
            assert f"agent '{TEST_AGENT_ID}'" in str(error)
            assert f"workspace '{USER2_WS}'" in str(error)
            assert error.permission_params == permission_params


class TestIntegration:
    """Integration tests for permission workflows."""

    @pytest.mark.asyncio
    async def test_permission_workflow_admin_user(self):
        """Test complete permission workflow for admin user."""
        mock_server = AsyncMock()

        with patch(
            "hypha_startup_services.common.permissions.ADMIN_WORKSPACES",
            ["admin-ws"],
        ):
            permission_params = PermissionParams(
                accessor_workspace="admin-ws",
                agent_id=TEST_AGENT_ID,
                accessed_workspace=USER2_WS,
                operation="*",
            )

            # Admin should have permission
            has_perm = await has_permission(mock_server, permission_params)
            assert has_perm is True

            # Should not raise exception
            await require_permission(mock_server, permission_params)

    @pytest.mark.asyncio
    async def test_permission_workflow_regular_user_success(self):
        """Test complete permission workflow for regular user with valid permissions."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="r",
        )

        mock_artifact = {
            "config": {
                "permissions": {
                    USER1_WS: ["r", "rw"],
                }
            }
        }

        with patch("hypha_startup_services.common.permissions.ADMIN_WORKSPACES", []):
            with patch(
                "hypha_startup_services.common.permissions.get_artifact"
            ) as mock_get:
                mock_get.return_value = mock_artifact

                # User should have permission
                has_perm = await has_permission(mock_server, permission_params)
                assert has_perm is True

                # Should not raise exception
                await require_permission(mock_server, permission_params)

    @pytest.mark.asyncio
    async def test_permission_workflow_regular_user_denied(self):
        """Test complete permission workflow for regular user without valid permissions."""
        mock_server = AsyncMock()
        permission_params = PermissionParams(
            accessor_workspace=USER1_WS,
            agent_id=TEST_AGENT_ID,
            accessed_workspace=USER2_WS,
            operation="rw",
        )

        mock_artifact = {
            "config": {
                "permissions": {
                    USER1_WS: ["r"],  # Only read permission, not read-write
                }
            }
        }

        with patch("hypha_startup_services.common.permissions.ADMIN_WORKSPACES", []):
            with patch(
                "hypha_startup_services.common.permissions.get_artifact"
            ) as mock_get:
                mock_get.return_value = mock_artifact

                # User should not have permission
                has_perm = await has_permission(mock_server, permission_params)
                assert has_perm is False

                # Should raise exception
                with pytest.raises(HyphaPermissionError):
                    await require_permission(mock_server, permission_params)
