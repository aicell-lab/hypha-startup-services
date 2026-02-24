import argparse
import asyncio
import logging
import os
from collections.abc import Mapping
from pathlib import Path

try:
    from scripts.hypha_connection import connect_with_fallback
except ModuleNotFoundError:
    from hypha_connection import connect_with_fallback

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRANSIENT_ERROR_MARKERS = (
    "startup timed out",
    "timeout registering service built-in",
)
INSTALL_MAX_ATTEMPTS = 2
APP_STARTUP_TIMEOUT_SECONDS = 300
NON_FATAL_STARTUP_TIMEOUT_SECONDS = 60


def _is_transient_install_error(error: Exception) -> bool:
    """Return True when install error looks transient and retryable."""
    error_text = str(error).lower()
    return any(marker in error_text for marker in TRANSIENT_ERROR_MARKERS)


def _inject_branch_dependency(
    object_to_update: object,
    *,
    branch_name: str,
) -> None:
    """Recursively inject branch refs into repository dependency URLs."""
    if isinstance(object_to_update, list):
        for index, item in enumerate(object_to_update):
            if isinstance(item, str) and "hypha-startup-services.git" in item:
                if "@" not in item:
                    continue
                dependency_base = item.split("@")[0]
                updated_dependency = f"{dependency_base}@{branch_name}"
                object_to_update[index] = updated_dependency
                logger.info(
                    "Replaced dependency: %s -> %s",
                    item,
                    updated_dependency,
                )
                continue
            _inject_branch_dependency(item, branch_name=branch_name)
        return

    if isinstance(object_to_update, dict):
        for nested_value in object_to_update.values():
            _inject_branch_dependency(nested_value, branch_name=branch_name)


def _prepare_manifest_data(
    manifest_data: dict[str, object],
    *,
    source_path: str,
) -> dict[str, object]:
    """Apply mandatory defaults and source-specific overrides to manifest."""
    prepared_manifest = dict(manifest_data)
    if not prepared_manifest.get("type"):
        prepared_manifest["type"] = "hypha"

    prepared_manifest["entry_point"] = Path(source_path).name

    head_ref = os.environ.get("GITHUB_HEAD_REF") or os.environ.get("GITHUB_REF_NAME")
    if head_ref and "hypha-startup-services" in str(prepared_manifest):
        logger.info(
            "Injecting git branch '%s' into manifest dependencies...",
            head_ref,
        )
        _inject_branch_dependency(prepared_manifest, branch_name=head_ref)

    return prepared_manifest


async def _install_app(
    server_apps,
    *,
    app_id: str,
    source_code: str,
    manifest_data: Mapping[str, object],
    non_fatal_start: bool,
) -> bool:
    """Install app with retry for transient startup registration timeouts."""
    install_kwargs = {
        "app_id": app_id,
        "source": source_code,
        "manifest": dict(manifest_data),
        "overwrite": True,
        "wait_for_service": "default",
        "timeout": (
            NON_FATAL_STARTUP_TIMEOUT_SECONDS
            if non_fatal_start
            else APP_STARTUP_TIMEOUT_SECONDS
        ),
    }
    max_attempts = 1 if non_fatal_start else INSTALL_MAX_ATTEMPTS

    for attempt_index in range(max_attempts):
        try:
            await server_apps.install(**install_kwargs)
            return True
        except Exception as error:
            is_last_attempt = attempt_index == max_attempts - 1
            if is_last_attempt:
                if non_fatal_start and _is_transient_install_error(error):
                    logger.warning(
                        "Non-fatal install error for %s: %s",
                        app_id,
                        error,
                    )
                    return False
                raise
            if not _is_transient_install_error(error):
                raise
            logger.warning(
                "Transient install failure for %s (attempt %s/%s): %s",
                app_id,
                attempt_index + 1,
                max_attempts,
                error,
            )
            await asyncio.sleep(2)

    return False


async def _app_exists(server_apps, *, app_id: str) -> bool:
    """Return whether app metadata exists after install."""
    try:
        await server_apps.get_app_info(app_id)
        return True
    except Exception:
        return False


async def deploy_app(
    server_url: str,
    token: str,
    app_id: str,
    source_path: str,
    manifest_path: str,
    *,
    non_fatal_start: bool,
) -> None:
    client = await connect_with_fallback(
        server_url=server_url,
        token=token,
    )

    # Read source
    with open(source_path) as f:
        source_code = f.read()

    # Prepare manifest
    import yaml

    with open(manifest_path) as f:
        manifest_data = yaml.safe_load(f)
    prepared_manifest_data = _prepare_manifest_data(
        manifest_data,
        source_path=source_path,
    )

    server_apps = await client.get_service("public/server-apps")

    install_completed = await _install_app(
        server_apps,
        app_id=app_id,
        source_code=source_code,
        manifest_data=prepared_manifest_data,
        non_fatal_start=non_fatal_start,
    )

    if not install_completed and not await _app_exists(server_apps, app_id=app_id):
        error_message = (
            "App install did not complete and app metadata is missing: "
            f"{app_id}"
        )
        raise RuntimeError(error_message)

    logger.info("App %s installed.", app_id)

    if non_fatal_start:
        logger.info(
            "Install completed with non-fatal mode enabled; "
            "no separate start call is needed.",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--app-id", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument(
        "--non-fatal-start",
        action="store_true",
        help="Don't fail if start times out (start async)",
    )

    args = parser.parse_args()

    asyncio.run(
        deploy_app(
            args.server_url,
            args.token,
            args.app_id,
            args.source,
            args.manifest,
            non_fatal_start=args.non_fatal_start,
        ),
    )
