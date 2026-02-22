"""Run Weaviate embedding tests with Ollama GPU capacity guards.

The script collects test node IDs and runs them one by one. Before each test,
it probes Ollama embeddings to wait for available GPU capacity. If a test fails
with known CUDA/OOM signals, it retries after waiting.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPSConnection
from pathlib import Path
from urllib.parse import urlparse

import pytest

DEFAULT_TESTS_PATH = "tests/weaviate_service"
DEFAULT_OLLAMA_ENDPOINT = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se"
DEFAULT_OLLAMA_MODEL = "mxbai-embed-large:latest"
DEFAULT_PROBE_TEXT = "health check"
DEFAULT_MAX_WAIT_SECONDS = 300
DEFAULT_PROBE_INTERVAL_SECONDS = 10
DEFAULT_OOM_RETRIES = 2
REQUEST_TIMEOUT_SECONDS = 20
HTTP_SUCCESS_STATUS_MIN = 200
HTTP_SUCCESS_STATUS_MAX = 300
OOM_FAILURE_MARKERS = (
    "cuda error: out of memory",
    "llama runner process has terminated",
    "connection to ollama api failed",
)
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_OOM_WARNING = 42

logger = logging.getLogger(__name__)


class _CollectionPlugin:
    """Pytest plugin used to capture collected node IDs."""

    def __init__(self) -> None:
        """Initialize empty node ID list."""
        self.node_ids: list[str] = []

    def pytest_collection_modifyitems(self, items: list[object]) -> None:
        """Capture node IDs from collected test items."""
        self.node_ids = [str(item.nodeid) for item in items]


@dataclass(frozen=True)
class OllamaProbeConfig:
    """Configuration for Ollama embedding readiness probe."""

    endpoint: str
    model: str


@dataclass(frozen=True)
class TestRunResult:
    """Represent a single test execution result."""

    exit_code: int
    is_oom_failure: bool


def _collect_test_node_ids(tests_path: str) -> list[str]:
    """Collect pytest node IDs for a path."""
    plugin = _CollectionPlugin()
    exit_code = pytest.main(
        [tests_path, "--collect-only", "-q"],
        plugins=[plugin],
    )
    if int(exit_code) != 0:
        logger.error("Failed to collect tests from %s", tests_path)
        error_message = "Pytest collection failed"
        raise RuntimeError(error_message)

    node_ids = plugin.node_ids

    if not node_ids:
        error_message = "No tests collected for guarded embedding run"
        raise RuntimeError(error_message)

    return node_ids


def _make_connection(parsed_endpoint: object) -> HTTPConnection | HTTPSConnection:
    """Build a HTTP(S) connection from parsed endpoint."""
    parsed = parsed_endpoint
    scheme = str(parsed.scheme)
    netloc = str(parsed.netloc)
    if scheme == "https":
        return HTTPSConnection(host=netloc, timeout=REQUEST_TIMEOUT_SECONDS)
    if scheme == "http":
        return HTTPConnection(host=netloc, timeout=REQUEST_TIMEOUT_SECONDS)
    error_msg = f"Unsupported Ollama endpoint scheme: {scheme}"
    raise ValueError(error_msg)


def _probe_ollama_embedding(config: OllamaProbeConfig) -> tuple[bool, str]:
    """Probe Ollama embeddings endpoint to detect GPU readiness."""
    parsed_endpoint = urlparse(config.endpoint)
    base_path = parsed_endpoint.path.rstrip("/")
    request_path = f"{base_path}/api/embeddings" if base_path else "/api/embeddings"
    payload = json.dumps({"model": config.model, "prompt": DEFAULT_PROBE_TEXT})

    connection = _make_connection(parsed_endpoint)
    try:
        connection.request(
            "POST",
            request_path,
            body=payload,
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        raw_response_body = response.read().decode("utf-8", errors="ignore")
    except OSError as error:
        return False, f"Probe request failed: {error!s}"
    finally:
        connection.close()

    response_text = raw_response_body.lower()
    has_oom_marker = any(marker in response_text for marker in OOM_FAILURE_MARKERS)
    status_code = int(response.status)
    is_success_status = (
        HTTP_SUCCESS_STATUS_MIN <= status_code < HTTP_SUCCESS_STATUS_MAX
    )
    if is_success_status and not has_oom_marker:
        return True, "Embedding backend probe succeeded"

    reason = f"status={response.status}, body={raw_response_body[:300]}"
    return False, f"Embedding backend not ready: {reason}"


def _wait_for_ollama_capacity(
    config: OllamaProbeConfig,
    max_wait_seconds: int,
    probe_interval_seconds: int,
) -> None:
    """Wait for Ollama embedding endpoint to become ready."""
    deadline = time.time() + max_wait_seconds
    while time.time() <= deadline:
        ready, message = _probe_ollama_embedding(config)
        if ready:
            logger.info("%s", message)
            return
        logger.warning("%s", message)
        time.sleep(probe_interval_seconds)

    error_msg = (
        "Ollama embedding backend did not become ready within "
        f"{max_wait_seconds} seconds"
    )
    raise TimeoutError(error_msg)


def _contains_oom_marker(output_text: str) -> bool:
    """Return True when output includes known OOM/capacity signals."""
    lowered_text = output_text.lower()
    return any(marker in lowered_text for marker in OOM_FAILURE_MARKERS)


def _run_single_test(node_id: str) -> TestRunResult:
    """Run one pytest node in subprocess and detect OOM markers."""
    completed_process = subprocess.run(
        [sys.executable, "-m", "pytest", "-v", node_id],
        capture_output=True,
        text=True,
        check=False,
    )
    combined_output = (
        f"{completed_process.stdout}\n{completed_process.stderr}"
    )
    has_oom_marker = _contains_oom_marker(combined_output)
    is_oom_failure = completed_process.returncode != 0 and has_oom_marker
    return TestRunResult(
        exit_code=completed_process.returncode,
        is_oom_failure=is_oom_failure,
    )


def _wait_for_capacity_or_mark_oom(
    node_id: str,
    probe_config: OllamaProbeConfig,
    max_wait_seconds: int,
    probe_interval_seconds: int,
) -> bool:
    """Wait for capacity and return False when timeout should be OOM-only."""
    try:
        _wait_for_ollama_capacity(
            config=probe_config,
            max_wait_seconds=max_wait_seconds,
            probe_interval_seconds=probe_interval_seconds,
        )
        return True
    except TimeoutError:
        logger.warning(
            "Skipping %s due to embedding backend capacity timeout",
            node_id,
        )
        return False


def _run_node_with_retries(
    node_id: str,
    probe_config: OllamaProbeConfig,
    max_wait_seconds: int,
    probe_interval_seconds: int,
    max_oom_retries: int,
) -> str:
    """Run one node with OOM-aware retries.

    Returns one of: "passed", "oom", "failed".
    """
    attempts = max(max_oom_retries, 0) + 1
    for attempt_index in range(attempts):
        run_result = _run_single_test(node_id)
        if run_result.exit_code == 0:
            return "passed"

        if not run_result.is_oom_failure:
            return "failed"

        is_last_attempt = attempt_index == (attempts - 1)
        if is_last_attempt:
            return "oom"

        logger.warning("OOM detected for %s, retrying...", node_id)
        has_capacity = _wait_for_capacity_or_mark_oom(
            node_id=node_id,
            probe_config=probe_config,
            max_wait_seconds=max_wait_seconds,
            probe_interval_seconds=probe_interval_seconds,
        )
        if not has_capacity:
            logger.warning(
                "OOM retry wait timed out for %s, marking as OOM-only",
                node_id,
            )
            return "oom"

    return "failed"


def _run_guarded_tests(
    node_ids: list[str],
    probe_config: OllamaProbeConfig,
    max_wait_seconds: int,
    probe_interval_seconds: int,
    max_oom_retries: int,
) -> int:
    """Run tests sequentially with Ollama readiness checks."""
    oom_only_failed_node_ids: list[str] = []
    non_oom_failed_node_ids: list[str] = []

    for index, node_id in enumerate(node_ids, start=1):
        logger.info("[%s/%s] Running %s", index, len(node_ids), node_id)
        has_capacity = _wait_for_capacity_or_mark_oom(
            node_id=node_id,
            probe_config=probe_config,
            max_wait_seconds=max_wait_seconds,
            probe_interval_seconds=probe_interval_seconds,
        )
        if not has_capacity:
            oom_only_failed_node_ids.append(node_id)
            continue

        node_outcome = _run_node_with_retries(
            node_id=node_id,
            probe_config=probe_config,
            max_wait_seconds=max_wait_seconds,
            probe_interval_seconds=probe_interval_seconds,
            max_oom_retries=max_oom_retries,
        )
        if node_outcome == "passed":
            continue
        if node_outcome == "oom":
            oom_only_failed_node_ids.append(node_id)
            continue
        if node_outcome == "failed":
            non_oom_failed_node_ids.append(node_id)

    if non_oom_failed_node_ids:
        logger.error(
            "Guarded embedding run failed (non-OOM) for %s test(s):",
            len(non_oom_failed_node_ids),
        )
        for node_id in non_oom_failed_node_ids:
            logger.error("- %s", node_id)
        return EXIT_FAILURE

    if oom_only_failed_node_ids:
        logger.warning(
            "Guarded embedding run encountered OOM/capacity failures for "
            "%s test(s):",
            len(oom_only_failed_node_ids),
        )
        for node_id in oom_only_failed_node_ids:
            logger.warning("- %s", node_id)
        return EXIT_OOM_WARNING

    logger.info("Guarded embedding run passed for %s test(s)", len(node_ids))
    return EXIT_SUCCESS


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--tests-path", default=DEFAULT_TESTS_PATH)
    parser.add_argument("--ollama-endpoint", default=DEFAULT_OLLAMA_ENDPOINT)
    parser.add_argument("--ollama-model", default=DEFAULT_OLLAMA_MODEL)
    parser.add_argument(
        "--max-wait-seconds",
        type=int,
        default=DEFAULT_MAX_WAIT_SECONDS,
    )
    parser.add_argument(
        "--probe-interval-seconds",
        type=int,
        default=DEFAULT_PROBE_INTERVAL_SECONDS,
    )
    parser.add_argument("--max-oom-retries", type=int, default=DEFAULT_OOM_RETRIES)
    args = parser.parse_args()

    tests_path = Path(args.tests_path)
    if not tests_path.exists():
        error_msg = f"Tests path does not exist: {tests_path}"
        raise FileNotFoundError(error_msg)

    probe_config = OllamaProbeConfig(
        endpoint=args.ollama_endpoint,
        model=args.ollama_model,
    )
    node_ids = _collect_test_node_ids(args.tests_path)
    return _run_guarded_tests(
        node_ids=node_ids,
        probe_config=probe_config,
        max_wait_seconds=args.max_wait_seconds,
        probe_interval_seconds=args.probe_interval_seconds,
        max_oom_retries=args.max_oom_retries,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(main())
