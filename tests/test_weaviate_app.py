
import asyncio
import os
import sys
import unittest
import importlib.util
from unittest.mock import MagicMock, AsyncMock, patch

# Add repository root to path so we can import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import client explicitly to prevent patch errors
import hypha_startup_services.weaviate_service.client

# Mocking Hypha App API
class MockApi:
    def __init__(self):
        self.exported_service = None
        self.registered_services = [] # Track registered services
    
    def export(self, service_def):
        self.exported_service = service_def

    async def register_service(self, service_def):
        self.registered_services.append(service_def)
        
    async def get_service(self, service_id):
        return AsyncMock()

class TestWeaviateApp(unittest.IsolatedAsyncioTestCase):
    
    @patch("hypha_startup_services.weaviate_service.client.instantiate_and_connect")
    @patch("hypha_startup_services.weaviate_service.service_codecs.register_weaviate_codecs")
    async def test_app_export_structure(self, mock_register_codecs, mock_connect):
        """Test that app exports a valid service definition structure."""
        
        # Setup mocks
        mock_client = AsyncMock()
        mock_connect.return_value = mock_client
        
        mock_api = MockApi()
        
        # We need to load the app module dynamically because it contains top-level code (api.export)
        # However, `api` is not defined in the module's global scope until injection.
        # Our `app.py` checks `if 'api' in locals():`.
        
        # To test this, we can load the module content and exec it with a custom globals dict.
        app_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../weaviate-app/app.py"))
        
        with open(app_path, "r") as f:
            app_code = f.read()
            
        global_context = {
            "api": mock_api,
            "__name__": "__main__",
            "__file__": app_path
        }
        
        # Execute the app code
        exec(app_code, global_context)
        
        # Assert export was called
        self.assertIsNotNone(mock_api.exported_service)
        loader_def = mock_api.exported_service
        
        # Check loader structure
        self.assertIn("id", loader_def)
        self.assertIn("setup", loader_def)
        # Loader ID should be weaviate-app-loader
        self.assertEqual(loader_def["id"], "weaviate-app-loader")
        
        # Test setup execution to register the REAL service
        setup_func = loader_def["setup"]
        # Pass mock_api as the server to setup
        if asyncio.iscoroutinefunction(setup_func):
            await setup_func(mock_api) 
        else:
            setup_func(mock_api)
        
        mock_register_codecs.assert_called_with(mock_api)
        mock_connect.assert_called_once()
        
        # Check that the functional weaviate service was registered
        self.assertTrue(len(mock_api.registered_services) > 0)
        service_def = mock_api.registered_services[0]
        
        # Check functional service structure
        self.assertIn("id", service_def)
        # Default ID usually 'weaviate' or from env
        # self.assertEqual(service_def["id"], "weaviate") 
        self.assertIn("collections", service_def)
        self.assertIn("data", service_def)
        
        # Test method binding (lazy loading check)
        # The methods should be callable and internally use the client that setup created
        list_all_func = service_def["collections"]["list_all"]
        
        # Call the method with mock context
        mock_context = {"user": {"id": "test-user"}}
        
        with patch("hypha_startup_services.weaviate_service.methods.ws_from_context") as mock_ws_from, \
             patch("hypha_startup_services.weaviate_service.methods.assert_is_admin_ws"):
            
            mock_ws_from.return_value = "test-workspace"
            
            # Setup the list_all return value properly
            # Since mock_client is AsyncMock, accessing attributes creates AsyncMocks
            # We want client.collections.list_all() to return a dict when awaited.
            
            # Re-configure the mock to be sure
            mock_client.collections.list_all = AsyncMock()
            mock_client.collections.list_all.return_value = {} 
            
            res = await list_all_func(context=mock_context)
        
        # Verify it called the mock client
        mock_client.collections.list_all.assert_called_once()

if __name__ == "__main__":
    unittest.main()
