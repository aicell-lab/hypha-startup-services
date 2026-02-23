"""Helpers for robust Hypha server connections in CI scripts."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from urllib.parse import urlparse, urlunparse

from hypha_rpc import connect_to_server

logger = logging.getLogger(__name__)

FALLBACK_BASE_PATHS: tuple[str, ...] = ("/hypha-agents", "/hypha")


def _normalize_server_url(server_url: str) -> str:
    """Normalize server url by trimming trailing slashes."""
    return server_url.rstrip("/")


def _with_path(server_url: str, *, path: str) -> str:
    """Return a copy of server_url with replaced path."""
    parsed = urlparse(server_url)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        ),
    )


def _candidate_server_urls(server_url: str) -> list[str]:
    """Build candidate Hypha base URLs to handle routing differences."""
    normalized_url = _normalize_server_url(server_url)
    parsed = urlparse(normalized_url)
    current_path = parsed.path.rstrip("/")

    if current_path:
        return [normalized_url]

    root_candidate = _with_path(normalized_url, path="")
    fallback_candidates = [
        _with_path(normalized_url, path=path)
        for path in FALLBACK_BASE_PATHS
    ]
    return list(dict.fromkeys([root_candidate, *fallback_candidates]))


def _is_websocket_404(error: Exception) -> bool:
    """Return True when websocket handshake failed with HTTP 404."""
    error_text = str(error).lower()
    return "http 404" in error_text and "websocket" in error_text


async def connect_with_fallback(
    *,
    server_url: str,
    token: str | None = None,
) -> object:
    """Connect to Hypha server trying fallback base URLs when needed."""
    candidates: Sequence[str] = _candidate_server_urls(server_url)
    last_error: Exception | None = None

    for candidate_url in candidates:
        config: dict[str, str] = {"server_url": candidate_url}
        if token:
            config["token"] = token
        try:
            client = await connect_to_server(config)
            if candidate_url != server_url:
                logger.info(
                    "Connected to Hypha via fallback URL: %s",
                    candidate_url,
                )
            return client
        except Exception as error:  # noqa: BLE001
            last_error = error
            if _is_websocket_404(error):
                logger.warning(
                    "WebSocket endpoint not found at %s, trying fallback...",
                    candidate_url,
                )
                continue
            logger.warning(
                "Failed to connect to Hypha at %s: %s",
                candidate_url,
                error,
            )

    if last_error is not None:
        raise last_error
    raise RuntimeError("Failed to connect to Hypha server with any candidate URL.")