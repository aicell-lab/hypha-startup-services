import pytest
import pytest_asyncio
from tests.conftest import get_user_server


@pytest_asyncio.fixture
async def mem0_bioimage_test_service():
    """Mem0 BioImage service fixture for user 1."""
    server = await get_user_server("PERSONAL_TOKEN")
    service = await server.get_service("aria-agents/mem0-bioimage-test")
    yield service
    await server.disconnect()


@pytest.fixture
def mock_mem0_bioimage_service():
    class MockService:
        def get_related(self, entity_id):
            if entity_id == "known-tech-id":
                return {
                    "exists_in_nodes": ["node1"],
                    "info": "Some tech info",
                    "country": "Test Country",
                    "entity_id": entity_id,
                    "entity_type": "technology",
                }
            if entity_id == "known-node-id":
                return {
                    "has_technologies": ["tech1"],
                    "info": "Some node info",
                    "country": "Test Country",
                    "entity_id": entity_id,
                    "entity_type": "node",
                }
            raise ValueError("Node not found")

    return MockService()


def test_get_nodes_by_technology_id_unit(mock_mem0_bioimage_service):
    result = mock_mem0_bioimage_service.get_related("known-tech-id")
    assert "exists_in_nodes" in result and "info" in result
    assert "country" in result
    assert "entity_id" in result
    assert "entity_type" in result
    with pytest.raises(ValueError):
        mock_mem0_bioimage_service.get_related("bad-id")


def test_get_technologies_by_node_id_unit(mock_mem0_bioimage_service):
    result = mock_mem0_bioimage_service.get_related("known-node-id")
    assert "has_technologies" in result and "info" in result
    assert "country" in result
    assert "entity_id" in result
    assert "entity_type" in result
    with pytest.raises(ValueError):
        mock_mem0_bioimage_service.get_related("bad-id")


@pytest.mark.asyncio
async def test_local_integration_mem0_bioimage(mem0_bioimage_test_service):
    result = await mem0_bioimage_test_service.get_related(
        entity_id="f0acc857-fc72-4094-bf14-c36ac40801c5"
    )
    assert len(result) > 0
    assert "name" in result[0]


# Remote integration tests
@pytest_asyncio.fixture
async def mem0_bioimage_live_service():
    """Mem0 BioImage live service fixture."""
    server = await get_user_server("PERSONAL_TOKEN")
    service = await server.get_service("aria-agents/mem0-bioimage")
    yield service
    await server.disconnect()


@pytest.mark.asyncio
async def test_remote_integration_mem0_bioimage(mem0_bioimage_live_service):
    result = await mem0_bioimage_live_service.get_related(
        entity_id="f0acc857-fc72-4094-bf14-c36ac40801c5"
    )
    assert "info" in result
