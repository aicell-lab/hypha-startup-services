"""Common test fixtures for weaviate tests."""

import asyncio
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
WEAVIATE_TEST_SERVICE_ID = f"weaviate-test-{TEST_SESSION_ID}"
SERVICE_LOOKUP_RETRIES = 60
SERVICE_LOOKUP_SLEEP_SECONDS = 1.0
SERVICE_REGISTRATION_RETRIES = 5
REGISTRATION_LOOKUP_RETRIES = 20
REGISTRATION_LOOKUP_SLEEP_SECONDS = 0.5


async def wait_for_service(
    server: RemoteService,
    service_id: str,
    retries: int = SERVICE_LOOKUP_RETRIES,
    sleep_seconds: float = SERVICE_LOOKUP_SLEEP_SECONDS,
) -> RemoteService:
    """Poll until a service can be resolved."""
    query_candidates = [service_id]
    if ":" in service_id and "/" in service_id:
        service_name = service_id.split(":", maxsplit=1)[1]
        workspace = service_id.split("/", maxsplit=1)[0]
        query_candidates.append(f"{workspace}/*:{service_name}")
        query_candidates.append(service_name)

    for _ in range(retries):
        for query in query_candidates:
            with contextlib.suppress(RemoteException):
                return await server.get_service(query)
        await asyncio.sleep(sleep_seconds)

    for query in query_candidates:
        with contextlib.suppress(RemoteException):
            return await server.get_service(query)

    return await server.get_service(service_id)


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

    def standard_movie_encoder(standard_movie: StandardMovie) -> dict[str, object]:
        """Encode StandardMovie to dict."""
        encoded_dict = asdict(standard_movie.value)
        encoded_dict["enum_name"] = standard_movie.name
        return encoded_dict

    def standard_movie_decoder(
        encoded_standard_movie: dict[str, object],
    ) -> dict[str, object]:
        """Decode StandardMovie payload to plain dict for service inputs."""
        decoded_movie = encoded_standard_movie.copy()
        decoded_movie.pop("enum_name", None)
        return decoded_movie

    server.register_codec(
        {
            "name": "standard_movie",
            "type": StandardMovie,
            "encoder": standard_movie_encoder,
            "decoder": standard_movie_decoder,
        },
    )


@pytest_asyncio.fixture
async def shared_weaviate_service_id() -> AsyncGenerator[str, None]:
    """Register one shared, session-unique Weaviate service."""
    server = await get_user_server("PERSONAL_TOKEN")
    register_test_codecs(server)
    last_error: RemoteException | RuntimeError | None = None
    full_service_id = ""
    service_id_base = f"{WEAVIATE_TEST_SERVICE_ID}-{uuid.uuid4().hex[:8]}"

    for retry_index in range(SERVICE_REGISTRATION_RETRIES):
        service_id_candidate = (
            f"{service_id_base}-{retry_index}"
            if retry_index > 0
            else service_id_base
        )
        await register_weaviate(server, service_id_candidate)
        full_service_id = (
            f"{server.config.workspace}/{server.config.client_id}:"
            f"{service_id_candidate}"
        )
        try:
            await wait_for_service(
                server,
                full_service_id,
                retries=REGISTRATION_LOOKUP_RETRIES,
                sleep_seconds=REGISTRATION_LOOKUP_SLEEP_SECONDS,
            )
        except RemoteException as error:
            last_error = error
            await asyncio.sleep(REGISTRATION_LOOKUP_SLEEP_SECONDS)
            continue
        else:
            break
    else:
        if last_error is not None:
            raise last_error
        error_msg = "Failed to register and resolve shared Weaviate test service"
        raise RuntimeError(error_msg)

    try:
        yield full_service_id
    finally:
        await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service(
    shared_weaviate_service_id: str,
) -> AsyncGenerator[RemoteService, None]:
    """Create Weaviate service fixture for user 1."""
    server = await get_user_server("PERSONAL_TOKEN")
    register_test_codecs(server)
    service = await wait_for_service(server, shared_weaviate_service_id)
    try:
        yield service
    finally:
        await cleanup_weaviate_service(service)
        await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service2(
    shared_weaviate_service_id: str,
) -> AsyncGenerator[RemoteService, None]:
    """Weaviate service fixture for user 2."""
    server = await get_user_server("PERSONAL_TOKEN2")
    register_test_codecs(server)
    service = await wait_for_service(server, shared_weaviate_service_id)
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service3(
    shared_weaviate_service_id: str,
) -> AsyncGenerator[RemoteService, None]:
    """Weaviate service fixture for user 3."""
    server = await get_user_server("PERSONAL_TOKEN3")
    register_test_codecs(server)
    service = await wait_for_service(server, shared_weaviate_service_id)
    yield service
    await server.disconnect()
