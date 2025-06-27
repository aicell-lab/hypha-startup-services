"""Tests for the Weaviate BioImage service methods."""

from unittest.mock import Mock, patch
import pytest
from weaviate.collections.classes.filters import _FilterValue, _Operator

from hypha_startup_services.weaviate_bioimage_service.methods import (
    query,
    get_entity,
    BIOIMAGE_COLLECTION,
)
from hypha_startup_services.weaviate_bioimage_service.methods import (
    SHARED_APPLICATION_ID,
)


class TestConstants:
    """Test service constants."""

    def test_constants_defined(self):
        """Test that required constants are defined."""
        assert BIOIMAGE_COLLECTION == "bioimage_data"
        assert SHARED_APPLICATION_ID == "eurobioimaging-shared"


class TestQueryMethods:
    """Test the query and get_entity methods."""

    @pytest.mark.asyncio
    @patch(
        "hypha_startup_services.weaviate_bioimage_service.methods.generate_near_text"
    )
    async def test_query_parameters(self, mock_generate):
        """Test that query passes parameters correctly."""
        mock_client = Mock()
        mock_server = Mock()
        mock_context = {"user_id": "test_user", "application_id": "test_app"}

        mock_generate.return_value = {"results": []}

        await query(
            client=mock_client,
            server=mock_server,
            query_text="test query",
            limit=5,
            context=mock_context,
        )

        mock_generate.assert_called_once()
        call_kwargs = mock_generate.call_args[1]

        assert call_kwargs["client"] == mock_client
        assert call_kwargs["server"] == mock_server
        assert call_kwargs["collection_name"] == BIOIMAGE_COLLECTION
        assert call_kwargs["application_id"] == SHARED_APPLICATION_ID
        assert call_kwargs["query"] == "test query"
        assert call_kwargs["limit"] == 5
        assert call_kwargs["context"] == mock_context

    @pytest.mark.asyncio
    @patch(
        "hypha_startup_services.weaviate_bioimage_service.methods.query_fetch_objects"
    )
    async def test_get_entity_parameters(self, mock_fetch):
        """Test that get_entity passes parameters correctly."""
        mock_client = Mock()
        mock_server = Mock()
        mock_context = {"user_id": "test_user", "application_id": "test_app"}

        mock_fetch.return_value = {"results": []}

        # Create the get_entity function using the factory
        await get_entity(
            mock_client,
            mock_server,
            entity_id="test_entity_123",
            context=mock_context,
        )

        mock_fetch.assert_called_once()
        call_kwargs = mock_fetch.call_args[1]

        assert call_kwargs["client"] == mock_client
        assert call_kwargs["server"] == mock_server
        assert call_kwargs["collection_name"] == BIOIMAGE_COLLECTION
        assert call_kwargs["application_id"] == SHARED_APPLICATION_ID
        assert call_kwargs["context"] == mock_context

        # Check the where filter
        where_filter: _FilterValue = call_kwargs["filters"]
        assert where_filter.target == "entity_id"
        assert where_filter.operator == _Operator.EQUAL
        assert where_filter.value == "test_entity_123"
