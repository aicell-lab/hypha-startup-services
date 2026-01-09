"""Integration tests for Weaviate cleanup service.

These tests run against real Hypha and Weaviate instances.
"""

import contextlib
import uuid

import pytest

from hypha_startup_services.common.constants import COLLECTION_DELIMITER
from hypha_startup_services.weaviate_service.cleanup import cleanup_weaviate_resources
from hypha_startup_services.weaviate_service.client import instantiate_and_connect
from tests.conftest import get_user_server


@pytest.mark.asyncio
async def test_cleanup_integration_orphan_artifact() -> None:
    """Test cleanup of an orphaned artifact (real integration)."""
    server = await get_user_server("PERSONAL_TOKEN")
    try:
        client = await instantiate_and_connect()
        try:
            artifact_manager = await server.get_service("public/artifact-manager")

            unique_id = str(uuid.uuid4()).replace("-", "")
            alias = f"Shared{COLLECTION_DELIMITER}TestArtifact{unique_id}"

            # Create orphaned artifact
            await artifact_manager.create(
                alias=alias,
                type="collection",
                manifest={"name": alias},
            )

            # Verify creation
            artifacts = await artifact_manager.list(
                context={"workspace": server.config.workspace},
            )
            assert any(
                a.get("alias") == alias for a in artifacts
            ), "Artifact should exist before cleanup"

            # Run cleanup
            await cleanup_weaviate_resources(server, client)

            # Verify deletion
            artifacts = await artifact_manager.list(
                context={"workspace": server.config.workspace},
            )
            assert not any(
                a.get("alias") == alias for a in artifacts
            ), "Orphaned artifact should be deleted"

        finally:
            await client.close()
    finally:
        await server.disconnect()


@pytest.mark.asyncio
async def test_cleanup_integration_orphan_collection() -> None:
    """Test cleanup of an orphaned collection (real integration)."""
    server = await get_user_server("PERSONAL_TOKEN")
    try:
        client = await instantiate_and_connect()
        coll_name = None
        try:
            unique_id = str(uuid.uuid4()).replace("-", "")
            coll_name = f"Shared{COLLECTION_DELIMITER}TestColl{unique_id}"

            # Create orphaned collection
            # We use verify_cleanup=False if available, or just create it directly
            await client.collections.create(coll_name)

            # Verify creation
            exists = await client.collections.exists(coll_name)
            assert exists, "Collection should exist before cleanup"

            # Run cleanup
            await cleanup_weaviate_resources(server, client)

            # Verify deletion
            exists = await client.collections.exists(coll_name)
            assert not exists, "Orphaned collection should be deleted"

        finally:
            # Ecoll_name and nsure cleanup if test failed
            if coll_name is not None and await client.collections.exists(coll_name):
                await client.collections.delete(coll_name)
            await client.close()
    finally:
        await server.disconnect()


@pytest.mark.asyncio
async def test_cleanup_integration_valid_pair() -> None:
    """Test that valid artifact-collection pairs are preserved."""
    server = await get_user_server("PERSONAL_TOKEN")
    try:
        client = await instantiate_and_connect()
        try:
            artifact_manager = await server.get_service("public/artifact-manager")

            unique_id = str(uuid.uuid4()).replace("-", "")
            alias = f"Shared{COLLECTION_DELIMITER}TestValid{unique_id}"

            # Create artifact
            await artifact_manager.create(
                alias=alias,
                type="collection",
                manifest={"name": alias},
            )

            # Create collection
            await client.collections.create(alias)

            try:
                # Run cleanup
                await cleanup_weaviate_resources(server, client)

                # Verify BOTH still exist
                artifacts = await artifact_manager.list(
                    context={"workspace": server.config.workspace},
                )
                assert any(
                    a.get("alias") == alias for a in artifacts
                ), "Valid artifact should be preserved"

                exists = await client.collections.exists(alias)
                assert exists, "Valid collection should be preserved"

            finally:
                # Manual cleanup
                if await client.collections.exists(alias):
                    await client.collections.delete(alias)
                with contextlib.suppress(Exception):
                    await artifact_manager.delete(artifact_id=alias, recursive=True)
        finally:
            await client.close()
    finally:
        await server.disconnect()
