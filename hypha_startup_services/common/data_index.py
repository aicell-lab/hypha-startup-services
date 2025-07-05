"""Data structures and indexing for EBI nodes and technologies."""

import os
from typing import Any, Set
from pydantic import Field
import json
import logging
from markdownify import markdownify as md
from hypha_rpc.rpc import schema_function

logger = logging.getLogger(__name__)


def html_to_markdown(html_text: str) -> str:
    """Convert HTML text to markdown format."""
    if not html_text:
        return ""

    # Convert HTML to markdown, removing extra whitespace
    markdown_text = md(html_text, heading_style="ATX").strip()

    # Clean up common formatting issues
    markdown_text = markdown_text.replace("\n\n\n", "\n\n")  # Remove triple newlines
    markdown_text = markdown_text.replace("\\*", "*")  # Fix escaped asterisks

    return markdown_text


def process_node_data(node: dict[str, Any]) -> dict[str, Any]:
    """Process node data to convert HTML descriptions to markdown."""
    processed_node = node.copy()

    # Convert HTML description to markdown
    if "description" in processed_node and processed_node["description"]:
        processed_node["description"] = html_to_markdown(processed_node["description"])

    # Handle long_description if present
    if "long_description" in processed_node and processed_node["long_description"]:
        processed_node["long_description"] = html_to_markdown(
            processed_node["long_description"]
        )

    return processed_node


def process_technology_data(tech: dict[str, Any]) -> dict[str, Any]:
    """Process technology data to convert HTML descriptions to markdown."""
    processed_tech = tech.copy()

    # Convert HTML description to markdown
    if "description" in processed_tech and processed_tech["description"]:
        processed_tech["description"] = html_to_markdown(processed_tech["description"])

    # Handle long_description if present
    if "long_description" in processed_tech and processed_tech["long_description"]:
        processed_tech["long_description"] = html_to_markdown(
            processed_tech["long_description"]
        )

    return processed_tech


class BioimageIndex:
    """Index for fast lookup of EBI nodes and technologies relationships."""

    def __init__(self):
        self.nodes: dict[str, dict[str, Any]] = {}
        self.technologies: dict[str, dict[str, Any]] = {}
        self.node_to_technologies: dict[str, Set[str]] = {}
        self.technology_to_nodes: dict[str, Set[str]] = {}
        self.technology_name_to_id: dict[str, str] = {}
        self.node_name_to_id: dict[str, str] = {}

    def load_data(
        self,
        nodes_data: list[dict[str, Any]],
        technologies_data: list[dict[str, Any]],
    ):
        """Load and index the EBI data."""

        # Process and index technologies
        for tech in technologies_data:
            tech_id = tech["id"]
            # Process HTML descriptions to markdown
            processed_tech = process_technology_data(tech)
            self.technologies[tech_id] = processed_tech

            # Create name-to-id mapping (case-insensitive)
            tech_name = processed_tech["name"].lower()
            self.technology_name_to_id[tech_name] = tech_id
            if processed_tech.get("abbr"):
                self.technology_name_to_id[processed_tech["abbr"].lower()] = tech_id

        # Process and index nodes and build relationships
        for node in nodes_data:
            node_id = node["id"]
            # Process HTML descriptions to markdown
            processed_node = process_node_data(node)
            self.nodes[node_id] = processed_node

            # Create name-to-id mapping (case-insensitive)
            node_name = processed_node["name"].lower()
            self.node_name_to_id[node_name] = node_id

            # Build technology relationships
            technologies = processed_node.get("technologies", [])
            self.node_to_technologies[node_id] = set()

            for tech_ref in technologies:
                # Handle both ID references and name references
                tech_id = self._resolve_technology_id(tech_ref)
                if tech_id:
                    self.node_to_technologies[node_id].add(tech_id)

                    # Build reverse mapping
                    if tech_id not in self.technology_to_nodes:
                        self.technology_to_nodes[tech_id] = set()
                    self.technology_to_nodes[tech_id].add(node_id)

        logger.info(
            "Loaded %d nodes and %d technologies",
            len(self.nodes),
            len(self.technologies),
        )
        logger.info(
            "Built relationships: %d node->tech, %d tech->node",
            len(self.node_to_technologies),
            len(self.technology_to_nodes),
        )

    def _resolve_technology_id(self, tech_ref: str) -> str | None:
        """Resolve a technology reference (ID or name) to an ID."""
        # If it's already a UUID-like ID, return it
        if tech_ref in self.technologies:
            return tech_ref

        # Try to find by name (case-insensitive)
        tech_ref_lower = tech_ref.lower()
        if tech_ref_lower in self.technology_name_to_id:
            return self.technology_name_to_id[tech_ref_lower]

        # Create a synthetic ID for unrecognized technology names
        # This allows for flexible technology references
        synthetic_id = f"synthetic-{tech_ref_lower.replace(' ', '-').replace('_', '-')}"
        if synthetic_id not in self.technologies:
            self.technologies[synthetic_id] = {
                "id": synthetic_id,
                "name": tech_ref,
                "description": f"Technology reference: {tech_ref}",
                "category": {"name": "Unclassified"},
                "synthetic": True,
            }
        return synthetic_id

    def get_nodes_by_technology_id(self, technology_id: str) -> list[dict[str, Any]]:
        """Get all nodes that provide a specific technology."""
        node_ids = self.technology_to_nodes.get(technology_id, set())
        return [self.nodes[node_id] for node_id in node_ids if node_id in self.nodes]

    def get_technologies_by_node_id(self, node_id: str) -> list[dict[str, Any]]:
        """Get all technologies provided by a specific node."""
        tech_ids = self.node_to_technologies.get(node_id, set())
        return [
            self.technologies[tech_id]
            for tech_id in tech_ids
            if tech_id in self.technologies
        ]

    def get_node_by_id(self, node_id: str) -> dict[str, Any] | None:
        """Get a specific node by ID."""
        return self.nodes.get(node_id)

    def get_technology_by_id(self, technology_id: str) -> dict[str, Any] | None:
        """Get a specific technology by ID."""
        return self.technologies.get(technology_id)

    def search_nodes_by_name(self, name: str) -> list[dict[str, Any]]:
        """Search nodes by name (case-insensitive partial match)."""
        name_lower = name.lower()
        results = []
        for node in self.nodes.values():
            if name_lower in node["name"].lower():
                results.append(node)
        return results

    def search_technologies_by_name(self, name: str) -> list[dict[str, Any]]:
        """Search technologies by name (case-insensitive partial match)."""
        name_lower = name.lower()
        results = []
        for tech in self.technologies.values():
            if name_lower in tech["name"].lower():
                results.append(tech)
        return results

    def get_all_nodes(self) -> list[dict[str, Any]]:
        """Get all nodes."""
        return list(self.nodes.values())

    def get_all_technologies(self) -> list[dict[str, Any]]:
        """Get all technologies."""
        return list(self.technologies.values())

    def get_statistics(self) -> dict[str, Any]:
        """Get index statistics."""
        return {
            "total_nodes": len(self.nodes),
            "total_technologies": len(self.technologies),
            "total_relationships": sum(
                len(techs) for techs in self.node_to_technologies.values()
            ),
            "nodes_with_technologies": len(
                [n for n in self.node_to_technologies.values() if n]
            ),
            "technologies_with_nodes": len(
                [t for t in self.technology_to_nodes.values() if t]
            ),
        }


def load_external_data(
    nodes_file: str | None = None, technologies_file: str | None = None
) -> BioimageIndex:
    """Load data from external JSON files."""
    # Default to assets directory if no files specified
    if nodes_file is None:
        nodes_file = os.path.join(os.path.dirname(__file__), "assets", "ebi-nodes.json")
    if technologies_file is None:
        technologies_file = os.path.join(
            os.path.dirname(__file__), "assets", "ebi-tech.json"
        )

    logger.info("Loading external data from:")
    logger.info("  Nodes file: %s (exists: %s)", nodes_file, os.path.exists(nodes_file))
    logger.info(
        "  Technologies file: %s (exists: %s)",
        technologies_file,
        os.path.exists(technologies_file),
    )

    # Additional debugging info
    assets_dir = os.path.join(os.path.dirname(__file__), "assets")
    logger.info(
        "  Assets directory: %s (exists: %s)", assets_dir, os.path.exists(assets_dir)
    )
    if os.path.exists(assets_dir):
        try:
            files_in_assets = os.listdir(assets_dir)
            logger.info("  Files in assets directory: %s", files_in_assets)
        except OSError as e:
            logger.warning("  Cannot list files in assets directory: %s", e)

    nodes_data = []
    technologies_data = []

    if nodes_file and os.path.exists(nodes_file):
        try:
            with open(nodes_file, "r", encoding="utf-8") as f:
                nodes_data = json.load(f)
            logger.info("Loaded %d nodes from %s", len(nodes_data), nodes_file)
        except (IOError, json.JSONDecodeError) as e:
            logger.warning(
                "Failed to load nodes from %s: %s, using default data", nodes_file, e
            )
    else:
        logger.warning("Nodes file not found or not specified: %s", nodes_file)

    if technologies_file and os.path.exists(technologies_file):
        try:
            with open(technologies_file, "r", encoding="utf-8") as f:
                technologies_data = json.load(f)
            logger.info(
                "Loaded %d technologies from %s",
                len(technologies_data),
                technologies_file,
            )
        except (IOError, json.JSONDecodeError) as e:
            logger.warning(
                "Failed to load technologies from %s: %s, using default data",
                technologies_file,
                e,
            )
    else:
        logger.warning(
            "Technologies file not found or not specified: %s", technologies_file
        )

    index = BioimageIndex()
    index.load_data(nodes_data, technologies_data)

    # Log final statistics
    stats = index.get_statistics()
    logger.info("Index statistics: %s", stats)

    return index


@schema_function(arbitrary_types_allowed=True)
async def get_entity_details(
    bioimage_index: BioimageIndex,
    entity_id: str = Field(
        description="The ID of the entity (node or technology) to retrieve"
    ),
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Get details for a specific entity (node or technology).
    Entity type is inferred if not provided.

    Args:
        bioimage_index: The bioimage index containing entity data
        entity_id: The ID of the entity to retrieve.

    Returns:
        A dictionary containing the entity details.

    Raises:
        ValueError: If entity is not found.
    """
    entity = bioimage_index.get_node_by_id(entity_id)
    entity_type = "node"

    if not entity:
        entity = bioimage_index.get_technology_by_id(entity_id)
        entity_type = "technology"

        if not entity:
            raise ValueError(f"Entity not found: {entity_id}")

    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "entity_details": entity,
    }


@schema_function(arbitrary_types_allowed=True)
def get_related_entities(
    bioimage_index: BioimageIndex,
    entity_id: str = Field(
        description="The ID of the entity to find relationships for"
    ),
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Get entities related to a specific entity.
    Entity type is inferred if not provided.

    Args:
        bioimage_index: The bioimage index containing relationship data
        entity_id: The ID of the entity to find relationships for.

    Returns:
        A list of related entities.

    Raises:
        ValueError: If entity is not found or no related entities exist.
    """
    if bioimage_index.get_node_by_id(entity_id):
        return bioimage_index.get_technologies_by_node_id(entity_id)

    if bioimage_index.get_technology_by_id(entity_id):
        return bioimage_index.get_nodes_by_technology_id(entity_id)

    raise ValueError(f"Entity not found: {entity_id}")


def _get_object_property_dict(
    obj: Any,
) -> dict[str, Any]:
    return {
        "entity_id": obj.get("entity_id")
        or obj.get("uuid")
        or str(getattr(obj, "uuid", "")),
        "entity_type": obj.get("entity_type"),
        "text": obj.get("text", ""),
        "description": obj.get("description", ""),
        "name": obj.get("name", ""),
        "country": obj.get("country"),
    }


def _extract_object_properties(result_obj: Any) -> dict[str, Any]:
    """Extract properties from a result object, handling both dict and Weaviate Object structures.

    Args:
        result_obj: The result object from a query (could be dict or Weaviate Object)

    Returns:
        A dictionary with extracted properties (entity_id, entity_type, text, country, description, name)
    """
    if hasattr(result_obj, "properties") and hasattr(result_obj, "uuid"):
        # Weaviate Object structure
        properties = getattr(result_obj, "properties", {})
        return _get_object_property_dict(properties)
    elif isinstance(result_obj, dict):
        # Dict-like structure - handle both direct properties and nested properties
        if "properties" in result_obj:
            properties = result_obj["properties"]
            return _get_object_property_dict(properties)
        else:
            return _get_object_property_dict(result_obj)
    else:
        # Fallback: try to convert to dict
        try:
            obj_dict = dict(result_obj) if hasattr(result_obj, "__iter__") else {}
            return _get_object_property_dict(obj_dict)
        except (TypeError, ValueError):
            # Return empty dict for objects we can't process
            return {
                "entity_id": None,
                "entity_type": None,
                "text": "",
                "description": "",
                "name": "",
                "country": None,
            }


def add_related_entities(
    bioimage_index: BioimageIndex, objects: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Get related entities for a list of bioimage objects.

    Args:
        bioimage_index (BioimageIndex): The bioimage index to use for lookups.
        objects (list[dict[str, Any]]): The list of bioimage objects to find related entities for.

    Returns:
        list[dict[str, Any]]: A list of enhanced bioimage objects with related entities.
    """
    enhanced_results = []
    for result_obj in objects:
        # Extract properties using the helper function
        props = _extract_object_properties(result_obj)

        # Skip objects we couldn't process
        if props["entity_id"] is None:
            continue

        # Use text first, fallback to description, then name
        info_text = props["text"] or props["description"] or props["name"] or ""

        enhanced_result = {
            "entity_id": props["entity_id"],
            "info": info_text,
            "entity_type": props["entity_type"],
        }

        # Only include country for nodes, not for technologies
        if props["entity_type"] == "node" and props["country"]:
            enhanced_result["country"] = props["country"]

        relation_type = (
            "exists_in_nodes"
            if props["entity_type"] == "technology"
            else "has_technologies"
        )

        # Create the related entities function and call it
        if props["entity_id"]:
            try:
                related_entities = get_related_entities(
                    bioimage_index=bioimage_index, entity_id=props["entity_id"]
                )
            except ValueError:
                # Entity not found in bioimage index - this is okay for entities
                # that were added directly to Weaviate but not in the index
                related_entities = []
        else:
            related_entities = []

        related_entities_names = [
            {
                "entity_id": entity.get("id", entity.get("entity_id", "Unknown")),
                "name": entity.get("name", entity.get("entity_id", "Unknown")),
            }
            for entity in related_entities
        ]
        enhanced_result[relation_type] = related_entities_names

        enhanced_results.append(enhanced_result)

    return enhanced_results
