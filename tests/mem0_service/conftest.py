"""Common test fixtures for mem0 tests."""

from tests.conftest import create_service_fixtures


# Create mem0 service fixtures using the shared utility
_fixtures = create_service_fixtures(
    service_name="mem0_service", service_id="aria-agents/mem0-test"
)

# Register the fixtures globally
mem0_service = _fixtures["mem0_service"]
mem0_service2 = _fixtures["mem0_service2"]
mem0_service3 = _fixtures["mem0_service3"]
