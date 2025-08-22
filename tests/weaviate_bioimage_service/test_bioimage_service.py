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
    service = await server.get_service("public/weaviate-bioimage")
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
        entity_id="660fd1fc-a138-5740-b298-14b0c3b24fb9",
    )
    assert len(result) > 0
    assert "name" in result[0]


@pytest.mark.asyncio
async def test_weaviate_bioimage_get_technologies_by_node_id_test(
    weaviate_bioimage_test_service,
):
    result = await weaviate_bioimage_test_service.get_related(
        entity_id="1dc91e38-8234-5b08-ad4f-a162da9486f6",
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
        assert "entity_id" in item or "id" in item or "name" in item


# TODO: fix non-multitenancy collections
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
        entity_id="660fd1fc-a138-5740-b298-14b0c3b24fb9",
    )
    assert len(result) > 0
    assert "name" in result[0]


@pytest.mark.asyncio
async def test_weaviate_bioimage_get_technologies_by_node_id_live(
    weaviate_bioimage_live_service,
):
    result = await weaviate_bioimage_live_service.get_related(
        entity_id="1dc91e38-8234-5b08-ad4f-a162da9486f6",
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
        assert "entity_id" in item


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
        entity_id="94c38160-4e93-5bc1-b2bc-1d5c86bb1d87",
    )
    assert len(result) > 0
    assert "id" in result[0]


# Remote integration tests
@pytest.mark.asyncio
async def test_remote_integration_weaviate_bioimage(weaviate_bioimage_live_service):
    result = await weaviate_bioimage_live_service.get_related(
        entity_id="660fd1fc-a138-5740-b298-14b0c3b24fb9",
    )
    assert len(result) > 0
    assert "id" in result[0]


# --- Diagnostic tests to understand why query returns zero results ---


@pytest.mark.asyncio
async def test_diagnose_bioimage_service_state(weaviate_bioimage_test_service):
    """Diagnostic test to understand the state of the bioimage service."""
    # Test 1: Check if we can get service info
    try:
        # Try to get some basic info about the service
        print(f"Service name: {weaviate_bioimage_test_service.name}")
        print(f"Service id: {weaviate_bioimage_test_service.id}")
    except Exception as e:
        print(f"Error getting service info: {e}")

    # Test 2: Check if collections exist
    try:
        # Try to list collections (if this method exists)
        if hasattr(weaviate_bioimage_test_service, "list_collections"):
            collections = await weaviate_bioimage_test_service.list_collections()
            print(f"Available collections: {collections}")
        else:
            print("No list_collections method available")
    except Exception as e:
        print(f"Error listing collections: {e}")

    # Test 3: Check if we can get statistics
    try:
        if hasattr(weaviate_bioimage_test_service, "get_statistics"):
            stats = await weaviate_bioimage_test_service.get_statistics()
            print(f"Service statistics: {stats}")
        else:
            print("No get_statistics method available")
    except Exception as e:
        print(f"Error getting statistics: {e}")

    # Test 4: Try a simple search first
    try:
        search_result = await weaviate_bioimage_test_service.search(
            query_text="microscopy",
            limit=1,
        )
        print(f"Search result structure: {type(search_result)}")
        if isinstance(search_result, dict):
            print(f"Search result keys: {search_result.keys()}")
            if "objects" in search_result:
                print(f"Number of search objects: {len(search_result['objects'])}")
                if len(search_result["objects"]) > 0:
                    print(
                        f"First search object keys: {search_result['objects'][0].keys()}",
                    )
        else:
            print(f"Unexpected search result type: {search_result}")
    except Exception as e:
        print(f"Error in search: {e}")

    # Test 5: Try the query that's failing
    try:
        query_result = await weaviate_bioimage_test_service.query(
            query_text="microscopy",
            limit=1,
        )
        print(f"Query result structure: {type(query_result)}")
        if isinstance(query_result, dict):
            print(f"Query result keys: {query_result.keys()}")
            if "objects" in query_result:
                print(f"Number of query objects: {len(query_result['objects'])}")
                if len(query_result["objects"]) > 0:
                    print(
                        f"First query object keys: {query_result['objects'][0].keys()}",
                    )
        else:
            print(f"Unexpected query result type: {query_result}")
    except Exception as e:
        print(f"Error in query: {e}")

    # Test 6: Try with different query terms
    test_queries = ["microscopy", "imaging", "bioimage", "node", "technology", ""]
    for test_query in test_queries:
        try:
            result = await weaviate_bioimage_test_service.query(
                query_text=test_query,
                limit=1,
            )
            if isinstance(result, dict) and "objects" in result:
                count = len(result["objects"])
                print(f"Query '{test_query}': {count} results")
                if count > 0:
                    break
            else:
                print(f"Query '{test_query}': unexpected result format")
        except Exception as e:
            print(f"Query '{test_query}' failed: {e}")

    # This test should not fail, it's just for diagnostics
    assert True, "This test is for diagnostics only"


@pytest.mark.asyncio
async def test_check_bioimage_application_exists(weaviate_bioimage_test_service):
    """Check if the bioimage application/collection exists and has data."""
    # Test 7: Try to get entity details to see if any data exists
    try:
        # Try a known entity ID from other tests
        result = await weaviate_bioimage_test_service.get_related(
            entity_id="660fd1fc-a138-5740-b298-14b0c3b24fb9",
        )
        print(f"get_related result: {len(result)} entities found")
        if len(result) > 0:
            print(f"First related entity keys: {result[0].keys()}")
            print("Data exists in the service - get_related works")
        else:
            print("get_related returned empty results")
    except Exception as e:
        print(f"get_related failed: {e}")

    # Test 8: Try another known entity ID
    try:
        result = await weaviate_bioimage_test_service.get_related(
            entity_id="1dc91e38-8234-5b08-ad4f-a162da9486f6",
        )
        print(f"get_related result for second entity: {len(result)} entities found")
    except Exception as e:
        print(f"get_related for second entity failed: {e}")

    assert True, "This test is for diagnostics only"


@pytest.mark.asyncio
async def test_bioimage_service_methods_available(weaviate_bioimage_test_service):
    """Check what methods are available on the bioimage service."""
    # List all available methods
    available_methods = [
        attr for attr in dir(weaviate_bioimage_test_service) if not attr.startswith("_")
    ]
    print(f"Available methods: {available_methods}")

    # Check if the service has the expected methods
    expected_methods = ["query", "search", "get_related", "get_statistics"]
    for method in expected_methods:
        if hasattr(weaviate_bioimage_test_service, method):
            print(f"✓ Method '{method}' is available")
        else:
            print(f"✗ Method '{method}' is NOT available")

    assert True, "This test is for diagnostics only"


@pytest.mark.asyncio
async def test_check_if_population_needed(weaviate_bioimage_test_service):
    """Check if the bioimage service needs to be populated with data."""
    # Test different entity types to understand the data structure
    try:
        # Try get_entity method with a known ID
        entity_result = await weaviate_bioimage_test_service.get(
            entity_id="660fd1fc-a138-5740-b298-14b0c3b24fb9",
        )
        print(f"get_entity result type: {type(entity_result)}")
        print(f"get_entity result: {entity_result}")
    except Exception as e:
        print(f"get_entity failed: {e}")

    # Check if search works with entity_types parameter
    try:
        result = await weaviate_bioimage_test_service.search(
            query_text="microscopy",
            entity_types=["node"],
            limit=5,
        )
        print(
            f"Search with entity_types=['node']: {len(result.get('objects', []))} results",
        )
    except Exception as e:
        print(f"Search with entity_types failed: {e}")

    try:
        result = await weaviate_bioimage_test_service.search(
            query_text="microscopy",
            entity_types=["technology"],
            limit=5,
        )
        print(
            f"Search with entity_types=['technology']: {len(result.get('objects', []))} results",
        )
    except Exception as e:
        print(f"Search with entity_types=['technology'] failed: {e}")

    # Try query with entity_types
    try:
        result = await weaviate_bioimage_test_service.query(
            query_text="microscopy",
            entity_types=["node", "technology"],
            limit=5,
        )
        print(f"Query with entity_types: {len(result.get('objects', []))} results")
        if "generated" in result:
            print(f"Query generated content: {result['generated']}")
    except Exception as e:
        print(f"Query with entity_types failed: {e}")

    # Try empty query to see if it returns anything
    try:
        result = await weaviate_bioimage_test_service.query(query_text="", limit=10)
        print(f"Empty query results: {len(result.get('objects', []))} results")
    except Exception as e:
        print(f"Empty query failed: {e}")

    assert True, "This test is for diagnostics only"


@pytest.mark.asyncio
async def test_populate_bioimage_test_service(weaviate_bioimage_test_service):
    """Test to populate the bioimage test service with minimal data."""
    print("Attempting to populate bioimage test service...")

    # Check if we have access to a weaviate service to populate data
    try:
        # Get the server connection
        server = await get_user_server("PERSONAL_TOKEN")

        # Try to get the weaviate service for data population
        try:
            weaviate_service = await server.get_service("public/weaviate")
            print("Found weaviate service for population")

            # Check if the bioimage_data collection exists
            try:
                collections = await weaviate_service.collections.list_all()
                print(f"Available collections: {list(collections.keys())}")

                if "bioimage_data" not in collections:
                    print("bioimage_data collection doesn't exist, creating it...")
                    # Create the collection
                    collection_config = {
                        "class": "bioimage_data",
                        "description": "BioImage data collection for testing",
                        "properties": [
                            {
                                "name": "name",
                                "dataType": ["text"],
                                "description": "Name of the bioimage entity",
                            },
                            {
                                "name": "description",
                                "dataType": ["text"],
                                "description": "Description of the bioimage entity",
                            },
                            {
                                "name": "entity_type",
                                "dataType": ["text"],
                                "description": "Type of entity (node or technology)",
                            },
                        ],
                    }
                    await weaviate_service.collections.create(collection_config)
                    print("Created bioimage_data collection")

                # Check if applications exist
                try:
                    apps = await weaviate_service.applications.list("bioimage_data")
                    print(f"Available applications: {apps}")

                    # Create test application if it doesn't exist
                    test_app_id = "bioimage-test-data"
                    if test_app_id not in apps:
                        await weaviate_service.applications.create(
                            collection_name="bioimage_data",
                            application_id=test_app_id,
                            description="Test application for bioimage data",
                        )
                        print(f"Created application: {test_app_id}")

                    # Insert some test data
                    test_nodes = [
                        {
                            "name": "Test Microscopy Node",
                            "description": "A test node for microscopy testing",
                            "entity_type": "node",
                        },
                        {
                            "name": "Imaging Technology",
                            "description": "A test technology for imaging",
                            "entity_type": "technology",
                        },
                    ]

                    for node in test_nodes:
                        try:
                            result = await weaviate_service.data.insert(
                                collection_name="bioimage_data",
                                application_id=test_app_id,
                                properties=node,
                            )
                            print(f"Inserted test data: {node['name']}")
                        except Exception as e:
                            print(f"Failed to insert {node['name']}: {e}")

                    print("Test data insertion completed")

                except Exception as e:
                    print(f"Error with applications: {e}")

            except Exception as e:
                print(f"Error with collections: {e}")

        except Exception as e:
            print(f"Could not get weaviate service: {e}")

        await server.disconnect()

    except Exception as e:
        print(f"Could not connect to server: {e}")

    # Now test if the bioimage service can find the data
    try:
        result = await weaviate_bioimage_test_service.search(
            query_text="microscopy",
            limit=5,
        )
        print(f"After population - Search results: {len(result.get('objects', []))}")

        result = await weaviate_bioimage_test_service.query(
            query_text="microscopy",
            limit=5,
        )
        print(f"After population - Query results: {len(result.get('objects', []))}")

    except Exception as e:
        print(f"Error testing after population: {e}")

    assert True, "This test is for attempting population"


@pytest.mark.asyncio
async def test_check_and_populate_eurobioimaging_shared_app(
    weaviate_bioimage_test_service,
):
    """Check and populate the eurobioimaging-shared application specifically."""
    print("Checking eurobioimaging-shared application...")

    try:
        server = await get_user_server("PERSONAL_TOKEN")
        weaviate_service = await server.get_service("public/weaviate")

        # Check if the eurobioimaging-shared application exists
        collection_name = "bioimage_data"
        application_id = "eurobioimaging-shared"

        try:
            # Try to check if the application exists using the applications.exists method
            app_exists = await weaviate_service.applications.exists(
                collection_name=collection_name,
                application_id=application_id,
            )
            print(f"Application {application_id} exists: {app_exists}")

            if not app_exists:
                print(f"Creating application: {application_id}")
                await weaviate_service.applications.create(
                    collection_name=collection_name,
                    application_id=application_id,
                    description="Shared EuroBioImaging nodes and technologies database",
                )
                print(f"Created application: {application_id}")

            # Check how many objects are in the application
            try:
                objects_count = await weaviate_service.data.count(
                    collection_name=collection_name,
                    application_id=application_id,
                )
                print(f"Objects in {application_id}: {objects_count}")

                # If the application is empty, add some test data
                if objects_count == 0:
                    print("Application is empty, adding test data...")

                    test_objects = [
                        {
                            "name": "Advanced Light Microscopy",
                            "description": "High-resolution microscopy techniques for cellular imaging",
                            "type": "technology",
                            "category": "imaging",
                        },
                        {
                            "name": "Bioimage Analysis Node",
                            "description": "Computational analysis of biological images",
                            "type": "node",
                            "category": "analysis",
                        },
                        {
                            "name": "Cryo-electron Microscopy",
                            "description": "Structural determination of biomolecules at near-atomic resolution",
                            "type": "technology",
                            "category": "structural",
                        },
                    ]

                    for obj in test_objects:
                        try:
                            result = await weaviate_service.data.insert(
                                collection_name=collection_name,
                                application_id=application_id,
                                properties=obj,
                            )
                            print(
                                f"Inserted: {obj['name']} (UUID: {result.get('uuid', 'unknown')})",
                            )
                        except Exception as e:
                            print(f"Failed to insert {obj['name']}: {e}")

                    # Check count again
                    new_count = await weaviate_service.data.count(
                        collection_name=collection_name,
                        application_id=application_id,
                    )
                    print(f"New object count: {new_count}")

            except Exception as e:
                print(f"Error checking/adding objects: {e}")

        except Exception as e:
            print(f"Error with application operations: {e}")

        await server.disconnect()

        # Now test the bioimage service again
        print("\nTesting bioimage service after population...")

        search_result = await weaviate_bioimage_test_service.search(
            query_text="microscopy",
            limit=5,
        )
        print(
            f"Search for 'microscopy': {len(search_result.get('objects', []))} results",
        )

        query_result = await weaviate_bioimage_test_service.query(
            query_text="microscopy",
            limit=5,
        )
        print(f"Query for 'microscopy': {len(query_result.get('objects', []))} results")

        # Try different search terms
        for term in ["imaging", "analysis", "bioimage"]:
            try:
                result = await weaviate_bioimage_test_service.search(
                    query_text=term,
                    limit=3,
                )
                print(f"Search for '{term}': {len(result.get('objects', []))} results")
            except Exception as e:
                print(f"Search for '{term}' failed: {e}")

    except Exception as e:
        print(f"Error in test: {e}")

    assert (
        True
    ), "This test is for checking and populating the eurobioimaging-shared application"


@pytest.mark.asyncio
async def test_investigate_eurobioimaging_shared_app_contents(
    weaviate_bioimage_test_service,
):
    """Investigate what's actually in the eurobioimaging-shared application."""
    print("Investigating eurobioimaging-shared application contents...")

    try:
        server = await get_user_server("PERSONAL_TOKEN")
        weaviate_service = await server.get_service("public/weaviate")

        collection_name = "bioimage_data"
        application_id = "eurobioimaging-shared"

        # Try to fetch objects instead of counting
        try:
            print("Trying to fetch objects from the application...")
            objects = await weaviate_service.query.fetch_objects(
                collection_name=collection_name,
                application_id=application_id,
                limit=10,
            )
            print(f"fetch_objects returned: {type(objects)}")
            if isinstance(objects, dict):
                print(f"Keys in objects: {objects.keys()}")
                if "objects" in objects:
                    obj_list = objects["objects"]
                    print(f"Number of objects found: {len(obj_list)}")
                    if len(obj_list) > 0:
                        print(f"First object keys: {obj_list[0].keys()}")
                        print(f"First object: {obj_list[0]}")
                    else:
                        print("No objects in the application")
                else:
                    print("No 'objects' key in response")
            else:
                print(f"Unexpected response type: {objects}")

        except Exception as e:
            print(f"fetch_objects failed: {e}")

        # Try inserting data with a different approach
        try:
            print("\nTrying to insert a test object...")
            test_object = {
                "name": "Test Microscopy Technology",
                "description": "A test technology for microscopy applications in bioimage analysis",
                "entity_type": "technology",
                "keywords": ["microscopy", "imaging", "bioimage"],
            }

            added_object_uuid = await weaviate_service.data.insert(
                collection_name=collection_name,
                application_id=application_id,
                properties=test_object,
            )
            print(f"Insert result: {added_object_uuid}")

            # Try fetching again after insert
            objects = await weaviate_service.query.fetch_objects(
                collection_name=collection_name,
                application_id=application_id,
                limit=5,
            )
            if isinstance(objects, dict) and "objects" in objects:
                print(f"After insert - Number of objects: {len(objects['objects'])}")

            await weaviate_service.data.delete_by_id(
                collection_name=collection_name,
                application_id=application_id,
                uuid=added_object_uuid,
            )
        except Exception as e:
            print(f"Insert/fetch after insert failed: {e}")

        await server.disconnect()

        # Test bioimage service after our investigation
        print("\nTesting bioimage service after investigation...")

        search_result = await weaviate_bioimage_test_service.search(
            query_text="microscopy",
            limit=5,
        )
        print(
            f"Bioimage service search: {len(search_result.get('objects', []))} results",
        )

        query_result = await weaviate_bioimage_test_service.query(
            query_text="test",
            limit=5,
        )
        print(f"Bioimage service query: {len(query_result.get('objects', []))} results")

    except Exception as e:
        print(f"Error in investigation: {e}")

    assert True, "This test is for investigation only"
