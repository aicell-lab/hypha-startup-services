
import asyncio
import os
import logging
from hypha_startup_services.weaviate_service.register_service import get_weaviate_service_def
from hypha_startup_services.weaviate_service.client import instantiate_and_connect
from hypha_startup_services.weaviate_service.service_codecs import register_weaviate_codecs

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def setup(server):
    """Run the Weaviate service app setup."""
    # We want the service ID for the ACTUAL functional service to be "weaviate" (or from env)
    service_id = os.environ.get("WEAVIATE_SERVICE_ID", "weaviate")
    logger.info(f"Setting up Weaviate Service App with ID: {service_id}")

    # 1. Register Codecs
    try:
        register_weaviate_codecs(server)
        logger.info("Registered Weaviate codecs")
    except Exception as e:
        logger.error(f"Failed to register codecs: {e}")
        raise

    # 2. Connect to Weaviate Client
    try:
        client = await instantiate_and_connect()
        logger.info("Connected to Weaviate client")
    except Exception as e:
        logger.error(f"Failed to connect to Weaviate: {e}")
        raise

    # 3. Get Service Definition
    service_def = await get_weaviate_service_def(server, client, service_id)
    
    # 4. Register the service explicitly
    await server.register_service(service_def)
    logger.info(f"Registered Weaviate service with ID: {service_id}")

if "api" in locals():
    # If running in Hypha App context, export the loader/setup
    # The ID here is for the App itself (the loader), not the functional service.
    # The functional service "weaviate" is registered inside setup().
    api.export({
        "id": "weaviate-app-loader",
        "name": "Weaviate App Loader",
        "setup": setup,
    })
