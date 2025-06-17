#!/usr/bin/env python3
"""
Script to build the bioimage Python index and populate the mem0 database.

This script:
1. Loads EBI nodes and technologies data from JSON files
2. Builds the Python index for fast lookups
3. Initializes and populates the mem0 database with bioimage data

Usage:
    python scripts/build_bioimage_index.py [--nodes-file path] [--tech-file path] [--force-rebuild]
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from hypha_startup_services.bioimage_service.data_index import load_external_data
from hypha_startup_services.bioimage_service.utils import (
    _create_node_content,
    _create_node_metadata,
    _create_technology_content,
    _create_technology_metadata,
)
from hypha_startup_services.mem0_service.mem0_client import get_mem0

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants for mem0 integration
EBI_AGENT_ID = "ebi_bioimage_assistant"
EBI_WORKSPACE = "ebi_data"


async def initialize_bioimage_database(
    memory,
    nodes_data: list,
    technologies_data: list,
    force_rebuild: bool = False,
) -> None:
    """Initialize the mem0 database with bioimage data."""

    if force_rebuild:
        logger.info("Force rebuild requested - clearing existing bioimage data...")
        try:
            # Get all memories for the EBI workspace
            existing_memories = await memory.get_all(agent_id=EBI_AGENT_ID)

            if existing_memories:
                logger.info(
                    "Found %d existing memories, clearing...", len(existing_memories)
                )
                # Delete all existing memories
                for mem in existing_memories:
                    try:
                        await memory.delete(memory_id=mem["id"], agent_id=EBI_AGENT_ID)
                    except (KeyError, ValueError, RuntimeError) as e:
                        logger.warning(
                            "Failed to delete memory %s: %s",
                            mem.get("id", "unknown"),
                            e,
                        )

                logger.info("Cleared existing bioimage memories")
        except (KeyError, ValueError, RuntimeError) as e:
            logger.warning("Error during cleanup: %s", e)

    logger.info("Initializing bioimage database with mem0...")

    # Add nodes to mem0
    logger.info("Adding %d nodes to mem0...", len(nodes_data))
    for i, node in enumerate(nodes_data, 1):
        try:
            content = _create_node_content(node)
            metadata = _create_node_metadata(node)

            await memory.add(
                content,
                agent_id=EBI_AGENT_ID,
                metadata=metadata,
            )

            if i % 10 == 0:  # Log progress every 10 nodes
                logger.info("Added %d/%d nodes...", i, len(nodes_data))

        except (KeyError, ValueError, RuntimeError) as e:
            logger.error("Failed to add node %s: %s", node.get("id", "unknown"), e)
            continue

    logger.info("Successfully added %d nodes to mem0", len(nodes_data))

    # Add technologies to mem0
    logger.info("Adding %d technologies to mem0...", len(technologies_data))
    for i, tech in enumerate(technologies_data, 1):
        try:
            content = _create_technology_content(tech)
            metadata = _create_technology_metadata(tech)

            await memory.add(
                content,
                agent_id=EBI_AGENT_ID,
                metadata=metadata,
            )

            if i % 25 == 0:  # Log progress every 25 technologies
                logger.info("Added %d/%d technologies...", i, len(technologies_data))

        except (KeyError, ValueError, RuntimeError) as e:
            logger.error(
                "Failed to add technology %s: %s", tech.get("id", "unknown"), e
            )
            continue

    logger.info("Successfully added %d technologies to mem0", len(technologies_data))
    logger.info("✅ Bioimage database initialization completed!")


async def build_bioimage_index(
    nodes_file: str | None = None,
    technologies_file: str | None = None,
    force_rebuild: bool = False,
) -> bool:
    """Main function to build the bioimage index and database."""

    logger.info("🔬 Starting bioimage index and database build...")

    # Step 1: Load data and build Python index
    logger.info("📊 Loading EBI data and building Python index...")
    try:
        bioimage_index = load_external_data(nodes_file, technologies_file)
        stats = bioimage_index.get_statistics()

        logger.info("✅ Python index built successfully:")
        logger.info("   - Nodes: %d", stats["total_nodes"])
        logger.info("   - Technologies: %d", stats["total_technologies"])
        logger.info("   - Relationships: %d", stats["total_relationships"])

    except (IOError, ValueError, KeyError) as e:
        logger.error("❌ Failed to build Python index: %s", e)
        return False

    # Step 2: Initialize mem0 database
    logger.info("🧠 Initializing mem0 database...")
    try:
        memory = await get_mem0()

        # Get the processed data from the index
        nodes_data = bioimage_index.get_all_nodes()
        technologies_data = bioimage_index.get_all_technologies()

        await initialize_bioimage_database(
            memory, nodes_data, technologies_data, force_rebuild
        )

    except (ConnectionError, RuntimeError, ValueError) as e:
        logger.error("❌ Failed to initialize mem0 database: %s", e)
        return False

    # Step 3: Verify the setup
    logger.info("🔍 Verifying the setup...")
    try:
        # Test a simple query to verify everything works
        test_response = await memory.search(
            "microscopy", agent_id=EBI_AGENT_ID, limit=5
        )

        # Extract results from the response
        test_memories = (
            test_response.get("results", [])
            if isinstance(test_response, dict)
            else test_response
        )

        logger.info(
            "✅ Verification successful - found %d test results", len(test_memories)
        )

        # Show some sample results
        for i, mem in enumerate(test_memories[:3], 1):
            metadata = mem.get("metadata", {})
            nested_metadata = metadata.get("metadata", {})
            entity_type = nested_metadata.get("entity_type", "Unknown")
            entity_name = nested_metadata.get("name", "Unknown")
            logger.info("   %d. %s: %s", i, entity_type, entity_name)

    except (ConnectionError, RuntimeError, ValueError) as e:
        logger.error("❌ Verification failed: %s", e)
        return False

    logger.info("🎉 Bioimage index and database build completed successfully!")
    return True


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Build bioimage Python index and mem0 database"
    )

    parser.add_argument(
        "--nodes-file",
        type=str,
        help="Path to EBI nodes JSON file (default: assets/ebi-nodes.json)",
    )

    parser.add_argument(
        "--tech-file",
        type=str,
        help="Path to EBI technologies JSON file (default: assets/ebi-tech.json)",
    )

    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Force rebuild by clearing existing mem0 data first",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run the async build process
    try:
        success = asyncio.run(
            build_bioimage_index(
                nodes_file=args.nodes_file,
                technologies_file=args.tech_file,
                force_rebuild=args.force_rebuild,
            )
        )

        if success:
            logger.info("✅ Build process completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Build process failed!")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("🛑 Build process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error("❌ Unexpected error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
