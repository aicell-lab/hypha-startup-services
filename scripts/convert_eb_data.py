#!/usr/bin/env python3
"""Convert EB-1-Nodes-Technologies text files to JSON format.

This script parses the text files from the EB-1-Nodes-Technologies submodule
and converts them into the JSON format expected by the bioimage service.
"""

import json
import re
import uuid
from pathlib import Path
from typing import Any


def parse_node_file(file_path: Path) -> dict[str, Any]:
    """Parse a node text file and extract structured information."""
    content = file_path.read_text(encoding="utf-8")

    # Extract country from filename
    filename = file_path.name
    country_match = re.search(r"RREXP\s+([A-Z\s]+?)\s+", filename)
    country = country_match.group(1).strip() if country_match else "Unknown"

    # Extract node name from filename (everything after country)
    name_match = re.search(r"RREXP\s+[A-Z\s]+?\s+(.+?)\.txt$", filename)
    name = name_match.group(1).strip() if name_match else filename.replace(".txt", "")

    # Extract description (first paragraph or section)
    description_match = re.search(
        r"# Description\s*\n\n(.*?)(?=\n##|\n#|\Z)",
        content,
        re.DOTALL,
    )
    description = description_match.group(1).strip() if description_match else ""

    # Clean up description - remove excessive whitespace and markdown
    description = re.sub(r"\n+", " ", description)
    description = re.sub(r"\*\*(.*?)\*\*", r"\1", description)  # Remove bold markdown
    description = re.sub(r"\s+", " ", description).strip()

    # Extract technologies from tables
    technologies = []
    tech_table_match = re.search(
        r"\| Technologies \|.*?\n(.*?)(?=\n##|\n#|\Z)",
        content,
        re.DOTALL,
    )
    if tech_table_match:
        table_content = tech_table_match.group(1)
        for line in table_content.split("\n"):
            if "|" in line and not line.strip().startswith("|---"):
                tech_match = re.search(r"\|\s*([^|]+?)\s*\|", line)
                if tech_match:
                    tech_name = tech_match.group(1).strip()
                    if tech_name and tech_name != "Technologies":
                        technologies.append(tech_name)

    # Generate a unique ID
    node_id = str(
        uuid.uuid5(uuid.NAMESPACE_DNS, f"eurobioimaging.node.{country}.{name}"),
    )

    return {
        "id": node_id,
        "name": name,
        "description": description,
        "country": {"name": country},
        "technologies": technologies,
        "entity_type": "node",
    }


def extract_technologies_from_files(file_paths: list[Path]) -> list[dict[str, Any]]:
    """Extract unique technologies from all node files."""
    all_technologies = set()

    for file_path in file_paths:
        content = file_path.read_text(encoding="utf-8")

        # Extract technologies from tables
        tech_table_match = re.search(
            r"\| Technologies \|.*?\n(.*?)(?=\n##|\n#|\Z)",
            content,
            re.DOTALL,
        )
        if tech_table_match:
            table_content = tech_table_match.group(1)
            for line in table_content.split("\n"):
                if "|" in line and not line.strip().startswith("|---"):
                    tech_match = re.search(r"\|\s*([^|]+?)\s*\|", line)
                    if tech_match:
                        tech_name = tech_match.group(1).strip()
                        if tech_name and tech_name != "Technologies":
                            all_technologies.add(tech_name)

    # Convert to list of technology objects
    technologies = []
    for tech_name in sorted(all_technologies):
        # Generate a unique ID
        tech_id = str(
            uuid.uuid5(uuid.NAMESPACE_DNS, f"eurobioimaging.technology.{tech_name}"),
        )

        # Determine category based on technology name
        category = "Unknown"
        if any(
            keyword in tech_name.lower()
            for keyword in ["microscopy", "imaging", "scan"]
        ):
            category = "Microscopy"
        elif any(keyword in tech_name.lower() for keyword in ["spectroscopy", "raman"]):
            category = "Spectroscopy"
        elif any(
            keyword in tech_name.lower() for keyword in ["tomography", "tem", "sem"]
        ):
            category = "Electron Microscopy"
        elif any(keyword in tech_name.lower() for keyword in ["clearing", "expansion"]):
            category = "Sample Preparation"

        technologies.append(
            {
                "id": tech_id,
                "name": tech_name,
                "description": f"Bioimaging technology: {tech_name}",
                "category": {"name": category},
                "entity_type": "technology",
            },
        )

    return technologies


def main():
    """Main function to convert EB-1 data to JSON."""
    # Get the path to the EB-1-Nodes-Technologies directory
    script_dir = Path(__file__).parent
    eb_data_dir = script_dir.parent / "data" / "EB-1-Nodes-Technologies"

    if not eb_data_dir.exists():
        print(f"Error: EB-1-Nodes-Technologies directory not found at {eb_data_dir}")
        print(
            "Make sure the submodule is initialized: git submodule update --init --recursive",
        )
        return

    # Find all node text files
    node_files = list(eb_data_dir.glob("RREXP *.txt"))

    if not node_files:
        print(f"Error: No RREXP text files found in {eb_data_dir}")
        return

    print(f"Found {len(node_files)} node files")

    # Parse nodes
    nodes = []
    for file_path in node_files:
        try:
            node = parse_node_file(file_path)
            nodes.append(node)
            print(f"Parsed node: {node['name']} ({node['country']['name']})")
        except (OSError, UnicodeDecodeError, ValueError) as e:
            print(f"Error parsing {file_path.name}: {e}")

    # Extract technologies
    technologies = extract_technologies_from_files(node_files)
    print(f"Extracted {len(technologies)} unique technologies")

    # Save to assets directory
    assets_dir = script_dir.parent / "hypha_startup_services" / "common" / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    # Save nodes
    nodes_file = assets_dir / "ebi-nodes.json"
    with open(nodes_file, "w", encoding="utf-8") as f:
        json.dump(nodes, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(nodes)} nodes to {nodes_file}")

    # Save technologies
    tech_file = assets_dir / "ebi-tech.json"
    with open(tech_file, "w", encoding="utf-8") as f:
        json.dump(technologies, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(technologies)} technologies to {tech_file}")

    print("✅ Conversion completed successfully!")


if __name__ == "__main__":
    main()
