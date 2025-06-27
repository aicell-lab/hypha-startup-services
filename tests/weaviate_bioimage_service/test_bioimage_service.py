# All imports at the top for clarity and PEP8 compliance
import pytest
import pytest_asyncio
from tests.conftest import get_user_server


# --- Fixtures for test and live/prod weaviate_bioimage services ---
@pytest_asyncio.fixture
async def weaviate_bioimage_test_service():
    server = await get_user_server("PERSONAL_TOKEN")
    service = await server.get_service("aria-agents/weaviate-bioimage-test")
    yield service
    await server.disconnect()


@pytest_asyncio.fixture
async def weaviate_bioimage_live_service():
    server = await get_user_server("PERSONAL_TOKEN")
    service = await server.get_service("aria-agents/weaviate-bioimage")
    yield service
    await server.disconnect()


# Unit tests


def test_get_nodes_by_technology_id_unit(mock_bioimage_service_fixture):
    result = mock_bioimage_service_fixture.get_related("known-tech-id")
    assert "exists_in_nodes" in result and "info" in result
    with pytest.raises(ValueError):
        mock_bioimage_service_fixture.get_related("bad-id")


def test_get_technologies_by_node_id_unit(mock_bioimage_service_fixture):
    result = mock_bioimage_service_fixture.get_related("known-node-id")
    assert "has_technologies" in result and "info" in result
    with pytest.raises(ValueError):
        mock_bioimage_service_fixture.get_related("bad-id")


def test_get_statistics_unit(mock_bioimage_service_fixture):
    stats = mock_bioimage_service_fixture.get_statistics()
    assert stats["service"] == "weaviate_bioimage_service"
    assert "statistics" in stats


# --- Integration tests for weaviate_bioimage_test (test instance) ---
@pytest.mark.asyncio
async def test_weaviate_bioimage_get_nodes_by_technology_id_test(
    weaviate_bioimage_test_service,
):
    result = await weaviate_bioimage_test_service.get_related(
        entity_id="f0acc857-fc72-4094-bf14-c36ac40801c5"
    )
    assert len(result) > 0
    assert "name" in result[0]


@pytest.mark.asyncio
async def test_weaviate_bioimage_get_technologies_by_node_id_test(
    weaviate_bioimage_test_service,
):
    result = await weaviate_bioimage_test_service.get_related(
        entity_id="7409a98f-1bdb-47d2-80e7-c89db73efedd"
    )
    assert len(result) > 0
    assert "name" in result[0]


@pytest.mark.asyncio
async def test_weaviate_bioimage_search_test(weaviate_bioimage_test_service):
    query = "microscopy"
    result = await weaviate_bioimage_test_service.search(query_text=query, limit=3)
    assert result is not None
    assert isinstance(result, dict)
    assert "objects" in result
    assert isinstance(result["objects"], list)
    assert len(result["objects"]) > 0
    for item in result["objects"]:
        assert "id" in item or "name" in item


@pytest.mark.asyncio
async def test_weaviate_bioimage_query_test(weaviate_bioimage_test_service):
    result = await weaviate_bioimage_test_service.query(query_text="microscopy")
    assert result is not None
    assert isinstance(result, dict)
    assert "objects" in result
    assert isinstance(result["objects"], list)
    assert len(result["objects"]) > 0


# --- Integration tests for weaviate_bioimage_live (prod instance) ---
@pytest.mark.asyncio
async def test_weaviate_bioimage_get_nodes_by_technology_id_live(
    weaviate_bioimage_live_service,
):
    result = await weaviate_bioimage_live_service.get_related(
        entity_id="f0acc857-fc72-4094-bf14-c36ac40801c5"
    )
    assert len(result) > 0
    assert "name" in result[0]


@pytest.mark.asyncio
async def test_weaviate_bioimage_get_technologies_by_node_id_live(
    weaviate_bioimage_live_service,
):
    result = await weaviate_bioimage_live_service.get_related(
        entity_id="7409a98f-1bdb-47d2-80e7-c89db73efedd"
    )
    assert len(result) > 0
    assert "name" in result[0]


@pytest.mark.asyncio
async def test_weaviate_bioimage_search_live(weaviate_bioimage_live_service):
    query = "microscopy"
    result = await weaviate_bioimage_live_service.search(query_text=query, limit=3)
    assert result is not None
    assert isinstance(result, dict)
    assert "objects" in result
    assert isinstance(result["objects"], list)
    assert len(result["objects"]) > 0
    for item in result["objects"]:
        assert "id" in item or "name" in item


@pytest.mark.asyncio
async def test_weaviate_bioimage_query_live(weaviate_bioimage_live_service):
    result = await weaviate_bioimage_live_service.query(query_text="microscopy")
    assert result is not None
    assert isinstance(result, dict)
    assert "objects" in result
    assert isinstance(result["objects"], list)
    assert len(result["objects"]) > 0


# Mock fixture for unit tests
@pytest.fixture(name="mock_bioimage_service_fixture")
def mock_bioimage_service_fixture():
    # Return a mock or in-memory version of the service
    class MockService:
        def get_related(self, entity_id):
            if entity_id == "known-tech-id":
                return {
                    "exists_in_nodes": ["node1", "node2"],
                    "info": "test tech info",
                }
            if entity_id == "known-node-id":
                return {
                    "has_technologies": ["tech1", "tech2"],
                    "info": "test node info",
                }
            raise ValueError("Technology not found")

        def get_statistics(self):
            return {
                "service": "weaviate_bioimage_service",
                "statistics": {"total_nodes": 2, "total_technologies": 2},
            }

    return MockService()


# Unit tests


@pytest.mark.asyncio
async def test_local_integration_weaviate_bioimage(weaviate_bioimage_test_service):
    result = await weaviate_bioimage_test_service.get_related(
        entity_id="f78b39ca-3b2a-49f4-99eb-a7f241640bf2"
    )
    assert len(result) > 0
    assert "id" in result[0]


# Remote integration tests
@pytest.mark.asyncio
async def test_remote_integration_weaviate_bioimage(weaviate_bioimage_live_service):
    result = await weaviate_bioimage_live_service.get_related(
        entity_id="f0acc857-fc72-4094-bf14-c36ac40801c5"
    )
    assert len(result) > 0
    assert "id" in result[0]
