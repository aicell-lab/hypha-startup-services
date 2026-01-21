"""Test artifact repair functionality for Weaviate service."""

import asyncio
import contextlib
import os
import uuid
from typing import Any

import pytest

from hypha_startup_services.common.artifacts import artifact_exists
from hypha_startup_services.common.constants import DEFAULT_REMOTE_URL
from hypha_startup_services.common.server_utils import get_server as get_internal_server
from hypha_startup_services.common.utils import get_full_collection_name
from hypha_startup_services.weaviate_service.client import instantiate_and_connect
from hypha_startup_services.weaviate_service.register_service import (
    register_weaviate_codecs,
    register_weaviate_service,
)
from hypha_startup_services.weaviate_service.utils.artifact_utils import (
    delete_collection_artifact,
)
from tests.conftest import get_user_server

# Define test constants
APP_ID = "TestApp"


@pytest.mark.asyncio
async def test_create_application_restore_collection_artifact() -> None:
    """Test that creating an application restores a missing collection artifact."""
    # Ensure HYPHA_TOKEN matches the token used by get_user_server (PERSONAL_TOKEN)
    token = os.environ.get("PERSONAL_TOKEN")
    if token:
        os.environ["HYPHA_TOKEN"] = token

    # Setup: Register a local version of the service to test local changes
    server = await get_user_server()
    client = await instantiate_and_connect()

    # Use a unique service ID to avoid conflicts
    service_id = f"weaviate-test-repair-{uuid.uuid4()}"
    await register_weaviate_service(server, client, service_id)
    register_weaviate_codecs(server)

    weaviate_service = await server.get_service(service_id)

    try:
        # 1. Create collection (creates artifact too)
        await create_test_collection(weaviate_service)

        # 2. Delete the collection artifact
        await delete_collection_artifact("Movie")

        # Verify it's gone
        full_collection_name = get_full_collection_name("Movie")
        # In test context, we might need a moment for deletion to propagate if async
        # consistency is involved, but usually delete is immediate for subsequent reads.
        if await artifact_exists(full_collection_name):
            # Force Wait/Retry if environment is slow
            await asyncio.sleep(1)
            if await artifact_exists(full_collection_name):
                pytest.fail("Failed to delete collection artifact for test setup")

        # 3. Create application - should trigger repair
        # Note: application creation uses the service we registered locally
        await weaviate_service.applications.create(
            collection_name="Movie",
            application_id=APP_ID,
            description="Test App",
        )

        # Give Hypha a moment to index/consistency
        await asyncio.sleep(2)

        # 4. Verify collection artifact exists again
        if not await artifact_exists(full_collection_name):
            pytest.fail(
                f"Collection artifact {full_collection_name} should have been restored",
            )

        # 5. Verify application artifact has correct parent_id
        app_artifact_id = await weaviate_service.applications.get_artifact(
            collection_name="Movie",
            application_id=APP_ID,
        )

        # Use fresh connection for verification to avoid state issues in test server
        async with get_internal_server(DEFAULT_REMOTE_URL) as validation_server:
            val_am = await validation_server.get_service("public/artifact-manager")
            app_artifact = await val_am.read(artifact_id=app_artifact_id)
            # Parent ID should match or be a fully qualified version
            # of the collection name
            parent_id = app_artifact["parent_id"]
            assert parent_id == full_collection_name or parent_id.endswith(
                f"/{full_collection_name}",
            ), f"Parent ID {parent_id} does not match {full_collection_name}"

    finally:
        # Cleanup
        with contextlib.suppress(Exception):
            await weaviate_service.applications.delete(
                collection_name="Movie",
                application_id=APP_ID,
            )

        with contextlib.suppress(Exception):
            await weaviate_service.collections.delete("Movie")

        await server.disconnect()


async def create_test_collection(service: Any) -> None:
    """Create a test collection."""
    with contextlib.suppress(Exception):
        await service.collections.delete("Movie")

    collection_config: dict[str, Any] = {
        "class": "Movie",
        "properties": [
            {"name": "title", "dataType": ["text"]},
            {"name": "application_id", "dataType": ["text"]},
        ],
        "description": "Movies collection",
    }
    await service.collections.create(settings=collection_config)
