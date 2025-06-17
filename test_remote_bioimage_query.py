#!/usr/bin/env python3
"""
Test script for remote bioimage service query method.
Tests the .query method after proper initialization with .init_agent and .init.
"""

import asyncio
import logging
import os
from hypha_rpc import connect_to_server

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
EBI_AGENT_ID = "ebi_bioimage_assistant"
SERVICE_ID = "aria-agents/bioimage"  # Remote bioimage service


async def test_remote_bioimage_query():
    """Test the remote bioimage service query method."""

    # Get token from environment
    token = os.getenv("HYPHA_TOKEN")
    if not token:
        logger.error("❌ HYPHA_TOKEN environment variable not set")
        return False

    server = None
    try:
        logger.info("🔌 Connecting to Hypha server...")

        # Connect to Hypha server
        server = await connect_to_server(
            {  # type: ignore
                "server_url": "https://hypha.aicell.io",
                "token": token,
            }
        )

        logger.info("✅ Connected to Hypha server")

        # Get the remote bioimage service
        logger.info("📡 Getting remote service: %s", SERVICE_ID)
        bioimaging = await server.get_service(SERVICE_ID)  # type: ignore
        logger.info("✅ Connected to remote bioimage service")

        # Check available methods
        logger.info(
            "🔍 Available methods: %s",
            (
                list(bioimaging.__dict__.keys())
                if hasattr(bioimaging, "__dict__")
                else "Unknown"
            ),
        )

        # Try to initialize if methods exist
        try:
            # Initialize agent first (if method exists)
            if hasattr(bioimaging, "init_agent"):
                logger.info("🚀 Initializing agent...")
                init_agent_result = await bioimaging.init_agent(
                    agent_id=EBI_AGENT_ID,
                    description="EBI BioImage Assistant for searching bioimage data",
                    metadata={"service": "bioimage", "data_source": "ebi"},
                )
                logger.info("✅ Agent initialized: %s", str(init_agent_result))
            else:
                logger.info("ℹ️ init_agent method not available, skipping...")
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("⚠️ Agent initialization failed: %s", e)

        try:
            # Initialize workspace/run (if method exists)
            if hasattr(bioimaging, "init"):
                logger.info("🚀 Initializing workspace...")
                init_result = await bioimaging.init(agent_id=EBI_AGENT_ID)
                logger.info("✅ Workspace initialized: %s", str(init_result))
            else:
                logger.info("ℹ️ init method not available, skipping...")
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("⚠️ Workspace initialization failed: %s", e)

        # Test the query method
        test_queries = [
            "What microscope technologies are available in Italy?",
            "electron microscopy techniques",
            "imaging facilities in Europe",
            "confocal microscopy resources",
        ]

        for i, query_text in enumerate(test_queries, 1):
            logger.info("🔍 Test %d: Querying with: '%s'", i, query_text)

            try:
                # Call the query method
                result = await bioimaging.query(query_text)

                logger.info("✅ Query %d successful!", i)
                logger.info("📊 Result type: %s", type(result))

                if isinstance(result, dict):
                    if "results" in result:
                        logger.info(
                            "📈 Total results: %s",
                            result.get("total_results", "unknown"),
                        )
                        logger.info("📋 Results count: %d", len(result["results"]))

                        # Show first result if available
                        if result["results"]:
                            first_result = result["results"][0]
                            logger.info(
                                "📄 First result preview: %s...",
                                str(first_result)[:200],
                            )
                    else:
                        logger.info("📄 Result content: %s...", str(result)[:200])
                else:
                    logger.info("📄 Result: %s...", str(result)[:200])

                print("-" * 60)

            except Exception as e:  # pylint: disable=broad-except
                logger.error("❌ Query %d failed: %s", i, e)
                print("-" * 60)

        logger.info("🎉 All tests completed!")
        return True

    except Exception as e:  # pylint: disable=broad-except
        logger.error("❌ Test failed: %s", e)
        return False

    finally:
        if server:
            try:
                await server.disconnect()  # type: ignore
                logger.info("🔌 Disconnected from server")
            except Exception as e:  # pylint: disable=broad-except
                logger.warning("⚠️ Error disconnecting: %s", e)


async def main():
    """Main function to run the test."""
    logger.info("🧪 Starting remote bioimage service query test...")

    success = await test_remote_bioimage_query()

    if success:
        logger.info("✅ Test completed successfully!")
    else:
        logger.error("❌ Test failed!")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
