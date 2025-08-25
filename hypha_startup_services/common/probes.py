"""Health probes for hypha startup services."""

import logging
from functools import partial
from typing import Any, Never

from hypha_rpc.rpc import RemoteException, RemoteService

logger = logging.getLogger(__name__)


async def is_service_available(server: RemoteService, service_id: str) -> bool:
    """Check if a specific service is available."""
    client_id = server.config.client_id
    full_service_id = f"{client_id}:{service_id}"
    try:
        await server.get_service(full_service_id)
    except RemoteException as e:
        logger.warning("Service %s is not available: %s", service_id, e)
        return False
    except (OSError, RuntimeError) as e:
        error_msg = f"Error checking service {service_id}: {e}"
        logger.exception(error_msg)
        return False
    else:
        return True


def num_available(available_dict: dict[str, Any]) -> int:
    """Get the number of available services."""
    return sum(1 for v in available_dict.values() if v["available"])


async def check_all_services(
    server: RemoteService,
    service_ids: list[str],
) -> dict[str, Any]:
    """Check the availability of all monitored services."""
    results: dict[str, dict[str, Any]] = {}
    all_available = True

    for service_id in service_ids:
        is_available = await is_service_available(server, service_id)
        results[service_id] = {
            "available": is_available,
            "status": "ok" if is_available else "unavailable",
        }
        if not is_available:
            all_available = False

    return {
        "all_services_available": all_available,
        "services": results,
        "monitored_services": service_ids,
        "total_services": len(service_ids),
        "available_count": num_available(results),
    }


def raise_unavailable(status: dict[str, Any]) -> Never:
    """Raise an error because some services are unavailable."""
    unavailable = [
        sid for sid, info in status["services"].items() if not info["available"]
    ]
    error_msg = (
        f"Services not ready: {', '.join(unavailable)}."
        f" {status['available_count']}/{status['total_services']}"
        " services available."
    )
    raise RuntimeError(error_msg)


async def readiness_probe(
    server: RemoteService,
    service_ids: list[str],
) -> dict[str, Any]:
    """Readiness probe - checks if all services are ready to serve traffic."""
    try:
        status = await check_all_services(server, service_ids)
        if status["all_services_available"]:
            return {
                "status": "ready",
                "message": "All services are available and ready",
                **status,
            }

        raise_unavailable(status)

    except Exception as e:
        error_msg = f"Readiness probe failed: {e}"
        logger.exception(error_msg)
        raise


def raise_unhealthy(available_share: float, availability_threshold: float) -> Never:
    """Raise an error because the system is unhealthy."""
    error_msg = (
        f"System unhealthy. Only {available_share} services available"
        f" (threshold: {availability_threshold})"
    )
    raise RuntimeError(error_msg)


async def liveness_probe(
    server: RemoteService,
    service_ids: list[str],
) -> dict[str, Any]:
    """Liveness probe - checks if the probe service itself is alive."""
    try:
        status = await check_all_services(server, service_ids)

        availability_threshold = len(service_ids)  # NOTE: * 0.5?
        available_count: int = status["available_count"]
        available_share: float = available_count / len(service_ids)

        if available_count >= availability_threshold:
            return {
                "status": "alive",
                "message": (f"System is alive. {available_share} services available"),
                **status,
            }

        raise_unhealthy(available_share, availability_threshold)
    except Exception as e:
        error_msg = f"Liveness probe failed: {e}"
        logger.exception(error_msg)
        raise


async def get_service_status(
    server: RemoteService,
    service_ids: list[str],
) -> dict[str, Any]:
    """Get detailed status of all services (for debugging/monitoring)."""
    return await check_all_services(server, service_ids)


async def add_probes(
    server: RemoteService,
    service_ids: list[str],
    probes_service_id: str | None = None,
) -> None:
    """Add health probes to monitor all registered services.

    Args:
        server (RemoteService): The server instance to register the probes with.
        service_ids (list[str]): List of service IDs to monitor.
        probes_service_id (str | None): ID for the probes service itself.

    """
    if probes_service_id is None:
        probes_service_id = "startup-services-probes"

    # Register the probe service
    logger.info(
        "Registering probes service '%s' to monitor: %s",
        probes_service_id,
        ", ".join(service_ids),
    )

    await server.register_service(
        {
            "name": "Startup Services Health Probes",
            "id": probes_service_id,
            "config": {"visibility": "public"},
            "type": "probes",
            "readiness": partial(
                readiness_probe,
                server=server,
                service_ids=service_ids,
            ),
            "liveness": partial(liveness_probe, server=server, service_ids=service_ids),
            "status": partial(
                check_all_services,
                server=server,
                service_ids=service_ids,
            ),
        },
    )

    logger.info(
        "Health probes registered successfully for services: %s",
        ", ".join(service_ids),
    )


async def add_individual_service_probe(server: RemoteService, service_id: str) -> None:
    """Add a health probe for an individual service.

    Args:
        server (RemoteService): The server instance to register the probe with.
        service_id (str): The service ID to monitor.

    """

    async def is_available() -> bool:
        """Check if this specific service is available."""
        try:
            await server.get_service(service_id)
        except RemoteException:
            return False
        else:
            return True

    async def health_check() -> dict[str, str]:
        """Health check for this specific service."""
        if await is_available():
            return {"status": "ok", "message": f"Service {service_id} is available"}

        error_msg = f"Service {service_id} is not available"
        raise RuntimeError(error_msg)

    probe_service_id = f"{service_id}-probe"

    await server.register_service(
        {
            "name": f"{service_id} Health Probe",
            "id": probe_service_id,
            "config": {"visibility": "public"},
            "type": "probe",
            "readiness": health_check,
            "liveness": health_check,
        },
    )

    logger.info("Individual health probe registered for service: %s", service_id)
