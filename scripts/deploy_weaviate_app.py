
import argparse
import asyncio
import logging
import os
from pathlib import Path
from typing import Mapping

from scripts.hypha_connection import connect_with_fallback

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRANSIENT_ERROR_MARKERS = (
    "startup timed out",
    "timeout registering service built-in",
)
INSTALL_MAX_ATTEMPTS = 2


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
) -> None:
    """Install app with retry for transient startup registration timeouts."""
    install_kwargs = {
        "app_id": app_id,
        "source": source_code,
        "manifest": dict(manifest_data),
        "overwrite": True,
    }
    if non_fatal_start:
        install_kwargs["wait_for_service"] = False

    for attempt_index in range(INSTALL_MAX_ATTEMPTS):
        try:
            await server_apps.install(**install_kwargs)
            return
        except Exception as error:  # noqa: BLE001
            is_last_attempt = attempt_index == INSTALL_MAX_ATTEMPTS - 1
            if is_last_attempt or not _is_transient_install_error(error):
                raise
            logger.warning(
                "Transient install failure for %s (attempt %s/%s): %s",
                app_id,
                attempt_index + 1,
                INSTALL_MAX_ATTEMPTS,
                error,
            )
            await asyncio.sleep(2)


async def _start_app(
    server_apps,
    *,
    app_id: str,
    non_fatal_start: bool,
) -> None:
    """Start app and optionally tolerate startup wait timeout in non-fatal mode."""
    logger.info("Starting app %s...", app_id)
    start_kwargs = {"app_id": app_id}
    if non_fatal_start:
        start_kwargs["wait_for_service"] = False

    try:
        await server_apps.start(**start_kwargs)
        logger.info("App %s started.", app_id)
    except Exception as error:  # noqa: BLE001
        if not non_fatal_start:
            raise
        logger.warning(
            "Non-fatal start error for %s: %s. Proceeding to health checks.",
            app_id,
            error,
        )

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
    with open(source_path, "r") as f:
        source_code = f.read()
            
    # Prepare manifest
    import yaml
    with open(manifest_path, "r") as f:
        manifest_data = yaml.safe_load(f)
    prepared_manifest_data = _prepare_manifest_data(
        manifest_data,
        source_path=source_path,
    )
        
    server_apps = await client.get_service("public/server-apps")

    await _install_app(
        server_apps,
        app_id=app_id,
        source_code=source_code,
        manifest_data=prepared_manifest_data,
        non_fatal_start=non_fatal_start,
    )

    logger.info("App %s installed.", app_id)

    await _start_app(
        server_apps,
        app_id=app_id,
        non_fatal_start=non_fatal_start,
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--app-id", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--non-fatal-start", action="store_true", help="Don't fail if start times out (start async)")

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
