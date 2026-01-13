"""Test that schema information is preserved when using factory functions."""

from unittest.mock import Mock

from hypha_startup_services.common.data_index import load_external_data
from hypha_startup_services.mem0_bioimage_service.methods import (
    create_get_entity_details,
    create_get_related_entities,
)
from hypha_startup_services.mem0_bioimage_service.methods import (
    create_search as create_mem0_search,
)
from hypha_startup_services.weaviate_bioimage_service.methods import (
    create_get_entity,
    create_get_related,
    create_query,
    create_search,
)


class TestFactoryFunctionSchemas:
    """Test that factory functions create schema functions correctly."""

    def test_weaviate_factory_functions_create_schemas(self) -> None:
        """Test that weaviate factory functions create functions with schemas."""
        # Create mock dependencies
        mock_client = Mock()
        bioimage_index = load_external_data()

        # Create functions using factories
        query_func = create_query(mock_client)
        search_func = create_search(mock_client, bioimage_index)
        get_entity_func = create_get_entity(mock_client)
        get_related_func = create_get_related(bioimage_index)

        # Test that created functions have schemas
        assert hasattr(
            query_func,
            "__schema__",
        ), "query_func should have __schema__ attribute"
        assert hasattr(
            search_func,
            "__schema__",
        ), "search_func should have __schema__ attribute"
        assert hasattr(
            get_entity_func,
            "__schema__",
        ), "get_entity_func should have __schema__ attribute"
        assert hasattr(
            get_related_func,
            "__schema__",
        ), "get_related_func should have __schema__ attribute"

        # Check that schemas are not None
        assert query_func.__schema__ is not None, "query_func schema should not be None"
        assert (
            search_func.__schema__ is not None
        ), "search_func schema should not be None"
        assert (
            get_entity_func.__schema__ is not None
        ), "get_entity_func schema should not be None"
        assert (
            get_related_func.__schema__ is not None
        ), "get_related_func schema should not be None"

    def test_mem0_factory_functions_create_schemas(self):
        """Test that mem0 factory functions create functions with schemas."""
        # Create mock dependencies
        mock_memory = Mock()
        bioimage_index = load_external_data()

        # Create functions using factories
        search_func = create_mem0_search(mock_memory, bioimage_index)
        get_entity_details_func = create_get_entity_details(bioimage_index)
        get_related_entities_func = create_get_related_entities(bioimage_index)

        # Test that created functions have schemas
        assert hasattr(
            search_func,
            "__schema__",
        ), "mem0 search_func should have __schema__ attribute"
        assert hasattr(
            get_entity_details_func,
            "__schema__",
        ), "get_entity_details_func should have __schema__ attribute"
        assert hasattr(
            get_related_entities_func,
            "__schema__",
        ), "get_related_entities_func should have __schema__ attribute"

        # Check that schemas are not None
        assert (
            search_func.__schema__ is not None
        ), "mem0 search_func schema should not be None"
        assert (
            get_entity_details_func.__schema__ is not None
        ), "get_entity_details_func schema should not be None"
        assert (
            get_related_entities_func.__schema__ is not None
        ), "get_related_entities_func schema should not be None"

    def test_factory_functions_have_correct_parameters(self):
        """Test that factory-created functions have the expected parameters."""
        # Create mock dependencies
        mock_client = Mock()
        bioimage_index = load_external_data()

        # Create query function
        query_func = create_query(mock_client)
        schema = query_func.__schema__

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

        # Check that we have the expected parameters (client should NOT be present)
        properties = parameters["properties"]
        assert "query_text" in properties, "Schema should include query_text parameter"
        assert (
            "entity_types" in properties
        ), "Schema should include entity_types parameter"
        assert "limit" in properties, "Schema should include limit parameter"

        # Client should not be in the schema since it's injected by the factory
        assert (
            "client" not in properties
        ), "Schema should NOT include client parameter (injected by factory)"

        # Check required parameters
        required = parameters.get("required", [])
        assert "query_text" in required, "query_text should be required"
        assert (
            "client" not in required
        ), "client should NOT be required (injected by factory)"

    def test_factory_vs_original_function_schemas(self) -> None:
        """Test that factory functions create different schemas than raw functions would."""
        # Import the original function (without @schema_function decorator)
        from hypha_startup_services.weaviate_bioimage_service.methods import (
            query as raw_query,
        )

        # Raw function should NOT have schema
        assert not hasattr(
            raw_query,
            "__schema__",
        ), "Raw function should not have __schema__ attribute"

        # Factory-created function should have schema
        mock_client = Mock()
        query_func = create_query(mock_client)
        assert hasattr(
            query_func,
            "__schema__",
        ), "Factory-created function should have __schema__ attribute"

        # The factory function should have proper schema structure
        schema = query_func.__schema__
        assert isinstance(
            schema,
            dict,
        ), "Factory function schema should be a dictionary"
        assert "parameters" in schema, "Factory function schema should have parameters"

        # The schema should not include the injected client parameter
        properties = schema["parameters"]["properties"]
        assert (
            "client" not in properties
        ), "Factory function schema should not include client parameter"

    def test_different_factories_create_different_schemas(self):
        """Test that different factory functions create functions with different schemas."""
        mock_client = Mock()
        bioimage_index = load_external_data()

        # Create different functions
        query_func = create_query(mock_client)
        search_func = create_search(mock_client, bioimage_index)
        get_entity_func = create_get_entity(mock_client)

        # Get their schemas
        query_schema = query_func.__schema__
        search_schema = search_func.__schema__
        get_entity_schema = get_entity_func.__schema__

        # They should have different names/descriptions
        assert (
            query_schema["name"] != search_schema["name"]
        ), "Different functions should have different names"
        assert (
            query_schema["name"] != get_entity_schema["name"]
        ), "Different functions should have different names"
        assert (
            search_schema["name"] != get_entity_schema["name"]
        ), "Different functions should have different names"

        # They should have different parameter structures
        query_props = query_schema["parameters"]["properties"]
        search_props = search_schema["parameters"]["properties"]
        get_entity_props = get_entity_schema["parameters"]["properties"]

        # Query and search have query_text, get_entity has entity_id
        assert "query_text" in query_props, "Query should have query_text parameter"
        assert "query_text" in search_props, "Search should have query_text parameter"
        assert (
            "entity_id" in get_entity_props
        ), "Get entity should have entity_id parameter"
        assert (
            "entity_id" not in query_props
        ), "Query should not have entity_id parameter"
