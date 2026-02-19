"""Common test fixtures for weaviate tests."""

import contextlib
import uuid
from collections.abc import AsyncGenerator
from dataclasses import asdict

import pytest_asyncio
from hypha_rpc.rpc import RemoteException, RemoteService

from hypha_startup_services.weaviate_service.register_service import (
    register_weaviate,
)
from hypha_startup_services.weaviate_service.service_codecs import (
    register_weaviate_codecs,
)
from tests.conftest import get_user_server
from tests.weaviate_service.utils import (
    APP_ID,
    SHARED_APP_ID,
    USER1_APP_ID,
    USER2_APP_ID,
    StandardMovie,
)

TEST_SESSION_ID = uuid.uuid4().hex[:10]
WEAVIATE_TEST_ID_USER1 = f"weaviate-test-{TEST_SESSION_ID}-u1"
WEAVIATE_TEST_ID_USER2 = f"weaviate-test-{TEST_SESSION_ID}-u2"
WEAVIATE_TEST_ID_USER3 = f"weaviate-test-{TEST_SESSION_ID}-u3"


async def register_and_get_service(
    server: RemoteService,
    service_name: str,
) -> RemoteService:
    """Register and fetch a session-unique Weaviate service."""
    await register_weaviate(server, service_name)
    return await server.get_service(service_name)


async def cleanup_weaviate_service(service: RemoteService) -> None:
    """Cleanup after weaviate tests."""
    try:
        # Try to delete test applications first
        for app_id in [APP_ID, USER1_APP_ID, USER2_APP_ID, SHARED_APP_ID]:
            with contextlib.suppress(RemoteException):
                await service.applications.delete(
                    collection_name="Movie",
                    application_id=app_id,
                )
        await service.collections.delete("Movie")
    except ValueError:  # Collection doesn't exist
        pass


def register_test_codecs(server: RemoteService) -> None:
    """Register test codecs for weaviate service."""
    register_weaviate_codecs(server)

    def standard_movie_encoder(standard_movie: StandardMovie) -> dict[str, str]:
        """Encode StandardMovie to dict."""
        encoded_dict = asdict(standard_movie.value)
        encoded_dict["enum_name"] = standard_movie.name
        return encoded_dict

    def standard_movie_decoder(
        encoded_standard_movie: dict[str, str],
    ) -> StandardMovie:
        """Decode StandardMovie from dict."""
        enum_name = encoded_standard_movie["enum_name"]
        return StandardMovie[enum_name]

    server.register_codec(
        {
            "name": "standard_movie",
            "type": StandardMovie,
            "encoder": standard_movie_encoder,
            "decoder": standard_movie_decoder,
        },
    )

@pytest_asyncio.fixture
async def weaviate_service(
) -> AsyncGenerator[RemoteService, None]:
    """Create Weaviate service fixture for user 1."""
    server = await get_user_server("PERSONAL_TOKEN")
    register_test_codecs(server)
    service = await register_and_get_service(server, WEAVIATE_TEST_ID_USER1)
    try:
        yield service
    finally:
        await cleanup_weaviate_service(service)
        await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service2(
) -> AsyncGenerator[RemoteService, None]:
    """Weaviate service fixture for user 2."""
    server = await get_user_server("PERSONAL_TOKEN2")
    register_test_codecs(server)
    service = await register_and_get_service(server, WEAVIATE_TEST_ID_USER2)
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service3(
) -> AsyncGenerator[RemoteService, None]:
    """Weaviate service fixture for user 3."""
    server = await get_user_server("PERSONAL_TOKEN3")
    register_test_codecs(server)
    service = await register_and_get_service(server, WEAVIATE_TEST_ID_USER3)
    yield service
    await server.disconnect()
