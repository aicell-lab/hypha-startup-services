"""Common test fixtures for weaviate tests."""

import asyncio
import contextlib
import logging
import os
import subprocess
import sys
import time
from collections.abc import AsyncGenerator, Generator
from dataclasses import asdict
from pathlib import Path

import pytest
import pytest_asyncio
from dotenv import load_dotenv
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

WEAVIATE_TEST_ID = "hypha-agents/weaviate-test"
SERVICE_STARTUP_POLL_INTERVAL_SECONDS = 2.0
SERVICE_STARTUP_TIMEOUT_SECONDS = 120.0
STARTUP_OUTPUT_READ_TIMEOUT_SECONDS = 1.0
SERVICE_START_COMMAND = [
    sys.executable,
    "-m",
    "hypha_startup_services",
    "weaviate",
    "--remote",
]

logger = logging.getLogger(__name__)


def _workspace_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).resolve().parents[2]


def _personal_token() -> str:
    """Return the token required to verify Weaviate service availability."""
    load_dotenv(override=True)
    token = os.environ.get("PERSONAL_TOKEN")
    if token is None:
        error_msg = "PERSONAL_TOKEN environment variable is not set"
        raise ValueError(error_msg)
    return token


async def _remote_service_available() -> bool:
    """Check whether the remote Weaviate test service is already available."""
    try:
        _personal_token()
    except ValueError:
        return False

    server = await get_user_server("PERSONAL_TOKEN")
    try:
        await server.get_service(WEAVIATE_TEST_ID)
    except RemoteException:
        return False
    else:
        return True
    finally:
        await server.disconnect()


def _startup_error_suffix(startup_process: subprocess.Popen[bytes]) -> str:
    """Build a human-readable suffix from startup process output."""
    try:
        stdout_data, stderr_data = startup_process.communicate(
            timeout=STARTUP_OUTPUT_READ_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return ""

    stdout_text = stdout_data.decode(errors="replace").strip()
    stderr_text = stderr_data.decode(errors="replace").strip()
    if stderr_text:
        return f" stderr: {stderr_text}"
    if stdout_text:
        return f" stdout: {stdout_text}"
    return ""


def _wait_for_remote_service(startup_process: subprocess.Popen[bytes]) -> None:
    """Block until the remote Weaviate service is reachable."""
    start_time = time.monotonic()
    while time.monotonic() - start_time < SERVICE_STARTUP_TIMEOUT_SECONDS:
        process_return_code = startup_process.poll()
        if process_return_code is not None:
            output_suffix = _startup_error_suffix(startup_process=startup_process)
            message = (
                "Remote Weaviate startup process exited before service "
                "became available "
                f"(return code: {process_return_code}).{output_suffix}"
            )
            raise RuntimeError(message)

        if asyncio.run(_remote_service_available()):
            return
        time.sleep(SERVICE_STARTUP_POLL_INTERVAL_SECONDS)

    error_msg = (
        "Timed out waiting for remote Weaviate service to start with command: "
        f"{' '.join(SERVICE_START_COMMAND)}"
    )
    raise RuntimeError(error_msg)


@pytest.fixture(scope="session", autouse=True)
def ensure_remote_weaviate_service() -> Generator[None, None, None]:
    """Start the remote Weaviate service once per test session if needed."""
    _personal_token()

    if asyncio.run(_remote_service_available()):
        logger.info("Using already running remote Weaviate test service.")
        yield
        return

    logger.info(
        "Starting remote Weaviate test service once for this test session.",
    )

    startup_process = subprocess.Popen(  # noqa: S603
        SERVICE_START_COMMAND,
        cwd=str(_workspace_root()),
        stdout=sys.stdout,
        stderr=sys.stderr,
        env=os.environ.copy(),
    )

    try:
        _wait_for_remote_service(startup_process=startup_process)
        yield
    finally:
        startup_process.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            startup_process.wait(timeout=10)
        if startup_process.poll() is None:
            startup_process.kill()


async def get_or_register_service(server: RemoteService) -> RemoteService:
    """Get the weaviate service or register it if missing."""
    try:
        return await server.get_service(WEAVIATE_TEST_ID)
    except RemoteException:
        await register_weaviate(server, "weaviate-test")
        return await server.get_service("weaviate-test")


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


def setup_weaviate_server(server: RemoteService) -> None:
    """Set up register weaviate codecs."""
    register_test_codecs(server)


@pytest_asyncio.fixture
async def weaviate_service() -> AsyncGenerator[RemoteService, None]:
    """Create Weaviate service fixture for user 1."""
    server = await get_user_server("PERSONAL_TOKEN")
    register_test_codecs(server)
    service = await get_or_register_service(server)
    try:
        yield service
    finally:
        await cleanup_weaviate_service(service)
        await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service2() -> AsyncGenerator[RemoteService, None]:
    """Weaviate service fixture for user 2."""
    server = await get_user_server("PERSONAL_TOKEN2")
    register_test_codecs(server)
    service = await get_or_register_service(server)
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_service3() -> AsyncGenerator[RemoteService, None]:
    """Weaviate service fixture for user 3."""
    server = await get_user_server("PERSONAL_TOKEN3")
    register_test_codecs(server)
    service = await get_or_register_service(server)
    yield service
    await server.disconnect()
