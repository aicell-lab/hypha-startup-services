"""Common test fixtures for weaviate tests."""

from hypha_rpc.rpc import RemoteService, RemoteException
from hypha_startup_services.weaviate_service.service_codecs import (
    register_weaviate_codecs,
)
from tests.conftest import create_service_fixtures, APP_ID


async def cleanup_weaviate_service(service: RemoteService):
    """Cleanup after weaviate tests."""
    try:
        # Try to delete test applications first
        try:
            await service.applications.delete(
                collection_name="Movie", application_id=APP_ID
            )
        except RemoteException as e:
            print("Error deleting test application:", e)

        # Then delete the collection
        await service.collections.delete("Movie")
    except ValueError:  # Collection doesn't exist
        pass


def setup_weaviate_server(server: RemoteService):
    """Setup function to register weaviate codecs."""
    register_weaviate_codecs(server)


# Create weaviate service fixtures using the shared utility
_fixtures = create_service_fixtures(
    service_name="weaviate_service",
    service_id="aria-agents/weaviate-test",
    setup_func=setup_weaviate_server,
)

# Register the fixtures globally
weaviate_service = _fixtures["weaviate_service"]
weaviate_service2 = _fixtures["weaviate_service2"]
weaviate_service3 = _fixtures["weaviate_service3"]
