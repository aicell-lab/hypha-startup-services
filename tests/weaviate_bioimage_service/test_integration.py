"""Integration tests for the Weaviate BioImage service.

These tests call the actual remote service 'public/weaviate-bioimage'
and test real functionality without mocks.
"""

import asyncio
import os

import pytest
from hypha_rpc import connect_to_server
from hypha_rpc.rpc import RemoteService

# Skip all integration tests if HYPHA_TOKEN is not set
pytestmark = pytest.mark.skipif(
    not os.getenv("HYPHA_TOKEN"),
    reason="HYPHA_TOKEN environment variable not set - skipping integration tests",
)


@pytest.fixture
async def remote_service():
    """Fixture to connect to the remote bioimage service."""
    token = os.getenv("HYPHA_TOKEN")

    server: RemoteService = await connect_to_server(
        {  # type: ignore
            "server_url": "https://hypha.aicell.io",
            "token": token,
        },
    )

    service = await server.get_service("public/weaviate-bioimage")

    yield service

    await server.disconnect()


class TestRemoteBioImageService:
    """Integration tests for the remote bioimage service."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_service_connection(self, remote_service):
        """Test that we can connect to the remote service."""
        assert remote_service is not None
        # Service should have query and get_entity methods
        assert hasattr(remote_service, "query")
        assert hasattr(remote_service, "get")

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_query_microscopy(self, remote_service):
        """Test querying for microscopy-related content."""
        result = await remote_service.query("microscopy")

        assert isinstance(result, dict)
        assert "objects" in result
        assert isinstance(result["objects"], list)

        # Should also have a generated response
        if "generated" in result:
            assert result["generated"] is not None and isinstance(
                result["generated"],
                str,
            )

        # Should return some results for a broad query like "microscopy"
        objects = result["objects"]
        if objects:  # If there are results, validate structure
            first_object = objects[0]
            assert isinstance(first_object, dict)
            # Should have typical bioimage data fields
            expected_fields = ["entity_id", "name", "description"]
            for field in expected_fields:
                if field in first_object:
                    assert isinstance(first_object[field], str)

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_query_with_limit(self, remote_service):
        """Test querying with a specific limit."""
        result = await remote_service.query("imaging", limit=3)

        assert isinstance(result, dict)
        assert "objects" in result
        objects = result["objects"]
        assert isinstance(objects, list)

        # Should respect the limit (if there are enough results)
        if len(objects) > 0:
            assert len(objects) <= 3

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_query_specific_technology(self, remote_service):
        """Test querying for a specific technology."""
        # Try some common microscopy technologies
        technologies = [
            "In which nodes can I find electron microscopy?",
            "In which nodes can I find super-resolution microscopy?",
        ]

        for tech in technologies:
            print(f"Querying for technology: {tech}")
            result = await remote_service.query(tech, limit=10, entity_types=["node"])
            assert isinstance(result, dict)
            assert "objects" in result
            assert isinstance(result["objects"], list)
            print(result["generated"])
            # We don't assert on result count as it depends on the data

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_query_empty_string(self, remote_service):
        """Test querying with empty string."""
        result = await remote_service.query("")

        assert isinstance(result, dict)
        assert "objects" in result
        assert isinstance(result["objects"], list)
        # Empty query might return some results or none, both are valid

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_query_nonexistent_term(self, remote_service):
        """Test querying for a very specific nonexistent term."""
        result = await remote_service.query("nonexistent_technology_xyz123")

        assert isinstance(result, dict)
        assert "objects" in result
        assert isinstance(result["objects"], list)
        # Should return empty or very few results

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_entity_structure(self, remote_service):
        """Test the get_entity method structure."""
        # First get some entities via query
        query_result = await remote_service.query("microscopy", limit=5)

        if query_result["objects"]:
            entity_with_id = None
            for entity in query_result["objects"]:
                if "entity_id" in entity:
                    entity_with_id = entity
                    break

            if entity_with_id:
                entity_id = entity_with_id["entity_id"]

                # Test get_entity with this ID
                entity_result = await remote_service.get(entity_id)

                assert isinstance(entity_result, dict)
                assert "objects" in entity_result
                objects = entity_result["objects"]
                assert isinstance(objects, list)

                if objects:
                    entity = objects[0]
                    assert isinstance(entity, dict)
                    assert entity.get("entity_id") == entity_id

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_entity_nonexistent(self, remote_service):
        """Test get_entity with a nonexistent ID."""
        result = await remote_service.get("nonexistent_id_xyz123")

        assert isinstance(result, dict)
        assert "objects" in result
        assert isinstance(result["objects"], list)
        # Should return empty results for nonexistent ID
        assert len(result["objects"]) == 0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_query_node_types(self, remote_service):
        """Test querying for different types of bioimage entities."""
        # Test queries for nodes and technologies
        node_terms = ["facility", "node", "institute", "university"]
        tech_terms = ["technique", "technology", "method", "protocol"]

        for term in node_terms + tech_terms:
            result = await remote_service.query(term)
            assert isinstance(result, dict)
            assert "objects" in result
            assert "generated" in result
            assert isinstance(result["objects"], list)

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_query_geographic_terms(self, remote_service):
        """Test querying for geographic/location terms."""
        geo_terms = ["Germany", "France", "Italy", "Europe", "EMBL"]

        for term in geo_terms:
            result = await remote_service.query(term)
            assert isinstance(result, dict)
            assert "objects" in result
            assert "generated" in result
            assert isinstance(result["objects"], list)

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_data_consistency(self, remote_service):
        """Test that returned data has consistent structure."""
        result = await remote_service.query("microscopy", limit=10)

        if result["objects"]:
            # Check that all entities have consistent field types
            for entity in result["objects"]:
                assert isinstance(entity, dict)

                # If entity_id exists, it should be a string
                if "entity_id" in entity:
                    assert isinstance(entity["entity_id"], str)
                    assert len(entity["entity_id"]) > 0

                # If name exists, it should be a string
                if "name" in entity:
                    assert isinstance(entity["name"], str)
                    assert len(entity["name"]) > 0

                # If description exists, it should be a string
                if "description" in entity:
                    assert isinstance(entity["description"], str)

                # Application ID should be present and consistent
                if "application_id" in entity:
                    assert isinstance(entity["application_id"], str)

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_service_error_handling(self, remote_service):
        """Test that the service handles various edge cases gracefully."""
        # Test with None (this might raise an error, which is fine)
        try:
            result = await remote_service.query(None)
            # If it doesn't raise an error, result should still be valid
            assert isinstance(result, dict)
            assert "objects" in result
            assert "generated" in result
        except Exception:
            # If it raises an error, that's also acceptable
            pass

        # Test with very long query
        long_query = "microscopy " * 100
        result = await remote_service.query(long_query)
        assert isinstance(result, dict)
        assert "objects" in result
        assert "generated" in result

        # Test with special characters
        special_query = "microscopy@#$%^&*()"
        result = await remote_service.query(special_query)
        assert isinstance(result, dict)
        assert "objects" in result
        assert "generated" in result


class TestRemoteServicePerformance:
    """Performance-related integration tests."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.slow
    async def test_concurrent_queries(self, remote_service):
        """Test that the service can handle concurrent queries."""
        queries = ["microscopy", "imaging", "facility", "technology", "protocol"]

        # Run multiple queries concurrently
        tasks = [remote_service.query(query) for query in queries]
        results = await asyncio.gather(*tasks)

        # All should succeed
        assert len(results) == len(queries)
        for result in results:
            assert isinstance(result, dict)
            assert "objects" in result
            assert "generated" in result
            assert isinstance(result["objects"], list)

    # @pytest.mark.asyncio
    # @pytest.mark.integration
    # @pytest.mark.slow
    # async def test_large_limit_query(self, remote_service):
    #     """Test querying with a large limit."""
    #     result = await remote_service.query("microscopy", limit=100)

    #     assert isinstance(result, dict)
    #     assert "objects" in result
    #     assert "generated" in result
    #     results = result["objects"]
    #     assert isinstance(results, list)

    #     # Should handle large limits gracefully
    #     if results:
    #         assert len(results) <= 100
