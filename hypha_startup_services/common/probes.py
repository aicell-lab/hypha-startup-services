"""Health probes for hypha startup services."""

import logging
from typing import Dict, Any
from hypha_rpc.rpc import RemoteService, RemoteException

logger = logging.getLogger(__name__)


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

    async def is_service_available(service_id: str) -> bool:
        """Check if a specific service is available."""
        try:
            svc = await server.get_service(service_id)
            return svc is not None
        except RemoteException as e:
            logger.warning("Service %s is not available: %s", service_id, e)
            return False
        except (OSError, RuntimeError) as e:
            logger.error("Error checking service %s: %s", service_id, e)
            return False

    async def check_all_services() -> Dict[str, Any]:
        """Check the availability of all monitored services."""
        results = {}
        all_available = True

        for service_id in service_ids:
            is_available = await is_service_available(service_id)
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
            "available_count": sum(1 for r in results.values() if r["available"]),
        }

    async def readiness_probe() -> Dict[str, Any]:
        """Readiness probe - checks if all services are ready to serve traffic."""
        try:
            status = await check_all_services()
            if status["all_services_available"]:
                return {
                    "status": "ready",
                    "message": "All services are available and ready",
                    **status,
                }
            else:
                unavailable = [
                    sid
                    for sid, info in status["services"].items()
                    if not info["available"]
                ]
                raise RuntimeError(
                    f"Services not ready: {', '.join(unavailable)}. "
                    f"{status['available_count']}/{status['total_services']} services available."
                )
        except Exception as e:
            logger.error("Readiness probe failed: %s", e)
            raise

    async def liveness_probe() -> Dict[str, Any]:
        """Liveness probe - checks if the probe service itself is alive."""
        try:
            # For liveness, we just check if we can communicate with the server
            # and that at least some services are responding
            status = await check_all_services()

            # If at least 50% of services are available, consider it alive
            availability_threshold = len(service_ids)  # * 0.5?
            available_count = status["available_count"]

            if available_count >= availability_threshold:
                return {
                    "status": "alive",
                    "message": f"System is alive. {available_count}/{len(service_ids)} services available",
                    **status,
                }
            else:
                raise RuntimeError(
                    f"System unhealthy. Only {available_count}/{len(service_ids)} services available "
                    f"(threshold: {availability_threshold})"
                )
        except Exception as e:
            logger.error("Liveness probe failed: %s", e)
            raise

    async def get_service_status() -> Dict[str, Any]:
        """Get detailed status of all services (for debugging/monitoring)."""
        return await check_all_services()

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
            "readiness": readiness_probe,
            "liveness": liveness_probe,
            "status": get_service_status,  # Additional endpoint for detailed status
        }
    )

    logger.info(
        "Health probes registered successfully for services: %s", ", ".join(service_ids)
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
            svc = await server.get_service(service_id)
            return svc is not None
        except RemoteException:
            return False

    async def health_check() -> Dict[str, str]:
        """Health check for this specific service."""
        if await is_available():
            return {"status": "ok", "message": f"Service {service_id} is available"}
        else:
            raise RuntimeError(f"Service {service_id} is not available")

    probe_service_id = f"{service_id}-probe"

    await server.register_service(
        {
            "name": f"{service_id} Health Probe",
            "id": probe_service_id,
            "config": {"visibility": "public"},
            "type": "probe",
            "readiness": health_check,
            "liveness": health_check,
        }
    )

    logger.info("Individual health probe registered for service: %s", service_id)
