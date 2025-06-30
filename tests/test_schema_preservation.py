"""Test that schema information is preserved when using partial functions."""

from functools import partial
from hypha_startup_services.common.utils import create_partial_with_schema
from hypha_startup_services.weaviate_bioimage_service.methods import (
    query,
    search,
    get_entity,
)
from hypha_startup_services.mem0_bioimage_service.methods import search as mem0_search
from hypha_startup_services.common.data_index import (
    get_related_entities,
    get_entity_details,
)


class TestSchemaPreservation:
    """Test that @schema_function decorated functions preserve their schemas when using partial."""

    def test_weaviate_functions_have_schemas(self):
        """Test that weaviate service functions have schemas."""
        assert hasattr(
            query, "__schema__"
        ), "query function should have __schema__ attribute"
        assert hasattr(
            search, "__schema__"
        ), "search function should have __schema__ attribute"
        assert hasattr(
            get_entity, "__schema__"
        ), "get_entity function should have __schema__ attribute"

        # Check that schemas are not None
        assert (
            getattr(query, "__schema__") is not None
        ), "query schema should not be None"
        assert (
            getattr(search, "__schema__") is not None
        ), "search schema should not be None"
        assert (
            getattr(get_entity, "__schema__") is not None
        ), "get_entity schema should not be None"

    def test_mem0_functions_have_schemas(self):
        """Test that mem0 service functions have schemas."""
        assert hasattr(
            mem0_search, "__schema__"
        ), "mem0 search function should have __schema__ attribute"
        assert (
            getattr(mem0_search, "__schema__") is not None
        ), "mem0 search schema should not be None"

    def test_common_functions_have_schemas(self):
        """Test that common functions have schemas."""
        assert hasattr(
            get_related_entities, "__schema__"
        ), "get_related_entities function should have __schema__ attribute"
        assert hasattr(
            get_entity_details, "__schema__"
        ), "get_entity_details function should have __schema__ attribute"

        assert (
            getattr(get_related_entities, "__schema__") is not None
        ), "get_related_entities schema should not be None"
        assert (
            getattr(get_entity_details, "__schema__") is not None
        ), "get_entity_details schema should not be None"

    def test_partial_preserves_schema(self):
        """Test that create_partial_with_schema preserves the __schema__ attribute."""
        # Test with a function that has a schema
        original_func = query
        assert hasattr(
            original_func, "__schema__"
        ), "Original function should have schema"

        # Create partial with our helper (using mock objects for testing)
        from unittest.mock import Mock

        partial_func = create_partial_with_schema(original_func, client=Mock())

        # Check that schema is preserved
        assert hasattr(
            partial_func, "__schema__"
        ), "Partial function should have __schema__ attribute"
        assert (
            getattr(partial_func, "__schema__") is not None
        ), "Partial function schema should not be None"
        assert getattr(partial_func, "__schema__") == getattr(
            original_func, "__schema__"
        ), "Schemas should be identical"

    def test_regular_partial_loses_schema(self):
        """Test that regular partial() loses the __schema__ attribute."""
        original_func = query
        assert hasattr(
            original_func, "__schema__"
        ), "Original function should have schema"

        # Create regular partial (using mock objects for testing)
        from unittest.mock import Mock

        regular_partial = partial(original_func, client=Mock())

        # Check that schema is lost
        assert not hasattr(
            regular_partial, "__schema__"
        ), "Regular partial should not have __schema__ attribute"

    def test_schema_content_structure(self):
        """Test that schemas have expected structure."""
        schema = getattr(query, "__schema__")

        # Check basic schema structure
        assert isinstance(schema, dict), "Schema should be a dictionary"
        assert "name" in schema, "Schema should have 'name' field"
        assert "parameters" in schema, "Schema should have 'parameters' field"
        assert "description" in schema, "Schema should have 'description' field"

        # Check that parameters have the expected structure
        parameters = schema["parameters"]
        assert "type" in parameters, "Parameters should have 'type' field"
        assert parameters["type"] == "object", "Parameters type should be 'object'"
        assert "properties" in parameters, "Parameters should have 'properties' field"

        # Check that we have input parameters
        properties = parameters["properties"]
        assert "query_text" in properties, "Schema should include query_text parameter"
        assert (
            "entity_types" in properties
        ), "Schema should include entity_types parameter"
