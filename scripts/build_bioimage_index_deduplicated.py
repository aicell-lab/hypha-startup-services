#!/usr/bin/env python3
"""
Improved script to build the bioimage index with deduplication and character cleaning.

This script:
1. Loads EBI nodes and technologies data from JSON files
2. Builds the Python index for fast lookups
3. Initializes and populates the mem0 database with bioimage data
4. ENSURES NO DUPLICATES by tracking content hashes
5. Cleans up problematic characters (null bytes, control characters)

Usage:
    python scripts/build_bioimage_index_deduplicated.py [--nodes-file path] [--tech-file path] [--force-rebuild]
"""

import argparse
import asyncio
import hashlib
import logging
import os
import re
import sys
from pathlib import Path
from typing import Set

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


def clean_text(text: str) -> str:
    """
    Clean text by removing problematic characters like null bytes and control characters.

    Args:
        text: The text to clean

    Returns:
        Cleaned text
    """
    if not isinstance(text, str):
        return str(text)

    # Remove null bytes and other problematic control characters
    # Keep only printable characters, spaces, tabs, and newlines
    cleaned = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)

    # Normalize whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    return cleaned


def create_content_hash(content: str) -> str:
    """
    Create a SHA-256 hash of the content for deduplication.

    Args:
        content: The content to hash

    Returns:
        Hexadecimal hash string
    """
    # Clean the content first
    cleaned_content = clean_text(content)

    # Create hash
    return hashlib.sha256(cleaned_content.encode("utf-8")).hexdigest()


async def initialize_bioimage_database_deduplicated(
    memory,
    nodes_data: list,
    technologies_data: list,
    force_rebuild: bool = False,
) -> None:
    """Initialize the mem0 database with bioimage data, ensuring no duplicates."""

    if force_rebuild:
        logger.info("Force rebuild requested - clearing existing bioimage data...")
        try:
            # Get all memories for the EBI workspace
            existing_memories_response = await memory.get_all(
                agent_id=EBI_AGENT_ID, limit=10000
            )

            # Handle different response formats
            if (
                isinstance(existing_memories_response, dict)
                and "results" in existing_memories_response
            ):
                existing_memories = existing_memories_response["results"]
            elif isinstance(existing_memories_response, list):
                existing_memories = existing_memories_response
            else:
                existing_memories = []

            if existing_memories:
                logger.info(
                    "Found %d existing memories, clearing...", len(existing_memories)
                )
                # Delete all existing memories
                deleted_count = 0
                for mem in existing_memories:
                    try:
                        await memory.delete(memory_id=mem["id"])
                        deleted_count += 1
                    except (KeyError, ValueError, RuntimeError) as e:
                        logger.warning(
                            "Failed to delete memory %s: %s",
                            mem.get("id", "unknown"),
                            e,
                        )

                logger.info("Cleared %d existing bioimage memories", deleted_count)
        except (KeyError, ValueError, RuntimeError) as e:
            logger.warning("Error during cleanup: %s", e)

    logger.info("Initializing bioimage database with mem0 (with deduplication)...")

    # Track content hashes to prevent duplicates
    content_hashes: Set[str] = set()
    total_skipped = 0
    total_added = 0

    # Add nodes to mem0
    logger.info("Adding %d nodes to mem0...", len(nodes_data))
    for i, node in enumerate(nodes_data, 1):
        try:
            content = _create_node_content(node)
            content_cleaned = clean_text(content)
            content_hash = create_content_hash(content_cleaned)

            # Check for duplicates
            if content_hash in content_hashes:
                logger.debug(
                    "Skipping duplicate node content (hash: %s...)", content_hash[:12]
                )
                total_skipped += 1
                continue

            # Add to tracking set
            content_hashes.add(content_hash)

            metadata = _create_node_metadata(node)

            await memory.add(
                content_cleaned,
                agent_id=EBI_AGENT_ID,
                metadata=metadata,
                infer=False,  # Store exactly as provided
            )

            total_added += 1

            if i % 10 == 0:  # Log progress every 10 nodes
                logger.info(
                    "Processed %d/%d nodes (added: %d, skipped duplicates: %d)...",
                    i,
                    len(nodes_data),
                    total_added,
                    total_skipped,
                )

        except (KeyError, ValueError, RuntimeError) as e:
            logger.error("Failed to add node %s: %s", node.get("id", "unknown"), e)
            continue

    logger.info(
        "Node processing complete: added %d, skipped %d duplicates",
        total_added,
        total_skipped,
    )

    # Add technologies to mem0
    logger.info("Adding %d technologies to mem0...", len(technologies_data))
    tech_added = 0
    tech_skipped = 0

    for i, tech in enumerate(technologies_data, 1):
        try:
            content = _create_technology_content(tech)
            content_cleaned = clean_text(content)
            content_hash = create_content_hash(content_cleaned)

            # Check for duplicates
            if content_hash in content_hashes:
                logger.debug(
                    "Skipping duplicate technology content (hash: %s...)",
                    content_hash[:12],
                )
                tech_skipped += 1
                continue

            # Add to tracking set
            content_hashes.add(content_hash)

            metadata = _create_technology_metadata(tech)

            await memory.add(
                content_cleaned,
                agent_id=EBI_AGENT_ID,
                metadata=metadata,
                infer=False,  # Store exactly as provided
            )

            tech_added += 1

            if i % 25 == 0:  # Log progress every 25 technologies
                logger.info(
                    "Processed %d/%d technologies (added: %d, skipped duplicates: %d)...",
                    i,
                    len(technologies_data),
                    tech_added,
                    tech_skipped,
                )

        except (KeyError, ValueError, RuntimeError) as e:
            logger.error(
                "Failed to add technology %s: %s", tech.get("id", "unknown"), e
            )
            continue

    logger.info(
        "Technology processing complete: added %d, skipped %d duplicates",
        tech_added,
        tech_skipped,
    )

    # Skip relationship creation - relationships are handled by the Python bioimage index
    logger.info("⏭️  Skipping relationship creation - handled by bioimage index")

    # Final summary
    total_final_added = total_added + tech_added
    total_final_skipped = total_skipped + tech_skipped

    logger.info("✅ Bioimage database initialization completed!")
    logger.info("📊 Final Summary:")
    logger.info("   - Total entries added: %d", total_final_added)
    logger.info("   - Total duplicates skipped: %d", total_final_skipped)
    logger.info("   - Unique content hashes: %d", len(content_hashes))
    logger.info("   - Nodes: %d added", total_added)
    logger.info("   - Technologies: %d added", tech_added)
    logger.info("   - Relationships: handled by bioimage index (not stored in mem0)")


async def build_bioimage_index_deduplicated(
    nodes_file: str | None = None,
    technologies_file: str | None = None,
    force_rebuild: bool = False,
) -> bool:
    """Main function to build the bioimage index and database with deduplication."""

    logger.info("🔬 Starting deduplicated bioimage index and database build...")

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

    # Step 2: Initialize mem0 database with deduplication
    logger.info("🧠 Initializing mem0 database with deduplication...")
    try:
        memory = await get_mem0()

        # Get the processed data from the index
        nodes_data = bioimage_index.get_all_nodes()
        technologies_data = bioimage_index.get_all_technologies()

        await initialize_bioimage_database_deduplicated(
            memory, nodes_data, technologies_data, force_rebuild
        )

    except (ConnectionError, RuntimeError, ValueError) as e:
        logger.error("❌ Failed to initialize mem0 database: %s", e)
        return False

    # Step 3: Verify the setup and check for duplicates
    logger.info("🔍 Verifying the setup and checking for duplicates...")
    try:
        # Test a simple query to verify everything works
        test_response = await memory.search(
            "microscopy", agent_id=EBI_AGENT_ID, limit=10
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

        # Check for duplicates in test results
        test_hashes = set()
        duplicate_count = 0

        for mem in test_memories:
            content = mem.get("memory", "")
            content_hash = create_content_hash(content)

            if content_hash in test_hashes:
                duplicate_count += 1
                logger.warning("Found duplicate in test results: %s...", content[:50])
            else:
                test_hashes.add(content_hash)

        if duplicate_count == 0:
            logger.info("🎉 No duplicates found in test results!")
        else:
            logger.warning("⚠️  Found %d duplicates in test results", duplicate_count)

        # Show some sample results
        for i, mem in enumerate(test_memories[:3], 1):
            content = (
                mem.get("memory", "")[:80] + "..."
                if len(mem.get("memory", "")) > 80
                else mem.get("memory", "")
            )
            logger.info("   %d. %s", i, content)

    except (ConnectionError, RuntimeError, ValueError) as e:
        logger.error("❌ Verification failed: %s", e)
        return False

    logger.info("🎉 Bioimage index and database build completed successfully!")
    return True


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Build bioimage Python index and mem0 database with deduplication"
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
            build_bioimage_index_deduplicated(
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
    except (RuntimeError, ValueError, OSError) as e:
        logger.error("❌ Unexpected error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
