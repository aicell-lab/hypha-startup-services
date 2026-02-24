"""Common test fixtures for weaviate tests."""

import asyncio
import contextlib
import uuid
from collections.abc import AsyncGenerator
from dataclasses import asdict
from pathlib import Path

import pytest_asyncio
from hypha_rpc.rpc import RemoteException, RemoteService
import yaml

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
WEAVIATE_TEST_APP_ID = f"weaviate-test-app-{TEST_SESSION_ID}"
SERVICE_LOOKUP_RETRIES = 40
SERVICE_LOOKUP_SLEEP_SECONDS = 0.5
SERVICE_REGISTRATION_RETRIES = 2
REGISTRATION_LOOKUP_RETRIES = 30
REGISTRATION_LOOKUP_SLEEP_SECONDS = 1.0
SERVER_APPS_SERVICE_ID = "public/server-apps"
DEFAULT_APP_SERVICE_PREFIX = "default@"
ADMIN_PERMISSION_ERROR_MESSAGE = "Only admin can generate token."
REPO_ROOT = Path(__file__).resolve().parents[2]
WEAVIATE_APP_DIR = REPO_ROOT / "weaviate-app"
WEAVIATE_APP_MANIFEST_PATH = WEAVIATE_APP_DIR / "manifest.yaml"


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


def _load_manifest_data() -> dict[str, object]:
    """Load base app manifest for test deployments."""
    with WEAVIATE_APP_MANIFEST_PATH.open("r", encoding="utf-8") as manifest_file:
        return dict(yaml.safe_load(manifest_file))


def _build_test_manifest(source_file_name: str) -> dict[str, object]:
    """Prepare app manifest for a test-specific source file."""
    manifest_data = _load_manifest_data()
    manifest_data["type"] = "hypha"
    manifest_data["entry_point"] = source_file_name
    return manifest_data


def _build_test_app_id(*, retry_index: int) -> str:
    """Build unique branch/session-safe app id for integration tests."""
    if retry_index == 0:
        return WEAVIATE_TEST_APP_ID
    return f"{WEAVIATE_TEST_APP_ID}-{retry_index}"


def _build_default_service_query(app_id: str) -> str:
    """Build default service query for a specific app instance."""
    return f"{DEFAULT_APP_SERVICE_PREFIX}{app_id}"


def _build_direct_service_id(*, retry_index: int) -> str:
    """Build a session-unique direct service id for fallback registration."""
    if retry_index == 0:
        return WEAVIATE_TEST_SERVICE_ID
    return f"{WEAVIATE_TEST_SERVICE_ID}-{retry_index}"


def _is_admin_permission_error(error: BaseException) -> bool:
    """Return whether failure is caused by missing admin token privileges."""
    return ADMIN_PERMISSION_ERROR_MESSAGE in str(error)


async def _install_test_app(
    server: RemoteService,
    *,
    app_id: str,
    source_file_name: str,
) -> None:
    """Install and start a Weaviate test app instance."""
    source_path = WEAVIATE_APP_DIR / source_file_name
    source_code = source_path.read_text(encoding="utf-8")
    manifest_data = _build_test_manifest(source_file_name)

    server_apps = await server.get_service(SERVER_APPS_SERVICE_ID)
    await server_apps.install(
        app_id=app_id,
        source=source_code,
        manifest=manifest_data,
        overwrite=True,
        wait_for_service=False,
    )
    await server_apps.start(app_id, wait_for_service=False)


async def _uninstall_test_app(
    server: RemoteService,
    *,
    app_id: str,
) -> None:
    """Stop and uninstall a Weaviate test app instance."""
    server_apps = await server.get_service(SERVER_APPS_SERVICE_ID)
    with contextlib.suppress(RemoteException):
        await server_apps.stop(app_id)
    with contextlib.suppress(RemoteException):
        await server_apps.uninstall(app_id)


async def _register_fallback_test_service(
    server: RemoteService,
    *,
    service_id: str,
) -> str:
    """Register service directly when app install is not permitted."""
    await register_weaviate(server, service_id)
    return (
        f"{server.config.workspace}/{server.config.client_id}:"
        f"{service_id}"
    )


async def _register_service_for_retry(
    server: RemoteService,
    *,
    retry_index: int,
) -> tuple[str, str]:
    """Register either app-based or direct fallback service for one retry."""
    app_id_candidate = _build_test_app_id(retry_index=retry_index)
    fallback_service_id = _build_direct_service_id(retry_index=retry_index)

    try:
        await _install_test_app(
            server,
            app_id=app_id_candidate,
            source_file_name="app_dev.py",
        )
        return _build_default_service_query(app_id_candidate), app_id_candidate
    except RemoteException as error:
        if not _is_admin_permission_error(error):
            raise
    fallback_full_service_id = await _register_fallback_test_service(
        server,
        service_id=fallback_service_id,
    )
    return fallback_full_service_id, ""


@pytest_asyncio.fixture
async def shared_weaviate_service_id() -> AsyncGenerator[str, None]:
    """Install one shared, session-unique Weaviate app and return its service."""
    server = await get_user_server("PERSONAL_TOKEN")
    register_test_codecs(server)

    last_error: Exception | None = None
    full_service_id = ""
    installed_app_id = ""

    for retry_index in range(SERVICE_REGISTRATION_RETRIES):
        try:
            full_service_id, installed_app_id = await _register_service_for_retry(
                server,
                retry_index=retry_index,
            )
        except Exception as error:
            last_error = error
            await asyncio.sleep(REGISTRATION_LOOKUP_SLEEP_SECONDS)
            continue

        try:
            await wait_for_service(
                server,
                full_service_id,
                retries=REGISTRATION_LOOKUP_RETRIES,
                sleep_seconds=REGISTRATION_LOOKUP_SLEEP_SECONDS,
            )
        except Exception as error:
            last_error = error
            if installed_app_id:
                await _uninstall_test_app(server, app_id=installed_app_id)
            installed_app_id = ""
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
        if installed_app_id:
            await _uninstall_test_app(server, app_id=installed_app_id)
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
