"""Data structures and indexing for EBI nodes and technologies."""

import os
from typing import Dict, List, Any, Set, Callable, Coroutine
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


def process_node_data(node: Dict[str, Any]) -> Dict[str, Any]:
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


def process_technology_data(tech: Dict[str, Any]) -> Dict[str, Any]:
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
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.technologies: Dict[str, Dict[str, Any]] = {}
        self.node_to_technologies: Dict[str, Set[str]] = {}
        self.technology_to_nodes: Dict[str, Set[str]] = {}
        self.technology_name_to_id: Dict[str, str] = {}
        self.node_name_to_id: Dict[str, str] = {}

    def load_data(
        self,
        nodes_data: List[Dict[str, Any]],
        technologies_data: List[Dict[str, Any]],
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

    def get_nodes_by_technology_id(self, technology_id: str) -> List[Dict[str, Any]]:
        """Get all nodes that provide a specific technology."""
        node_ids = self.technology_to_nodes.get(technology_id, set())
        return [self.nodes[node_id] for node_id in node_ids if node_id in self.nodes]

    def get_technologies_by_node_id(self, node_id: str) -> List[Dict[str, Any]]:
        """Get all technologies provided by a specific node."""
        tech_ids = self.node_to_technologies.get(node_id, set())
        return [
            self.technologies[tech_id]
            for tech_id in tech_ids
            if tech_id in self.technologies
        ]

    def get_node_by_id(self, node_id: str) -> Dict[str, Any] | None:
        """Get a specific node by ID."""
        return self.nodes.get(node_id)

    def get_technology_by_id(self, technology_id: str) -> Dict[str, Any] | None:
        """Get a specific technology by ID."""
        return self.technologies.get(technology_id)

    def search_nodes_by_name(self, name: str) -> List[Dict[str, Any]]:
        """Search nodes by name (case-insensitive partial match)."""
        name_lower = name.lower()
        results = []
        for node in self.nodes.values():
            if name_lower in node["name"].lower():
                results.append(node)
        return results

    def search_technologies_by_name(self, name: str) -> List[Dict[str, Any]]:
        """Search technologies by name (case-insensitive partial match)."""
        name_lower = name.lower()
        results = []
        for tech in self.technologies.values():
            if name_lower in tech["name"].lower():
                results.append(tech)
        return results

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """Get all nodes."""
        return list(self.nodes.values())

    def get_all_technologies(self) -> List[Dict[str, Any]]:
        """Get all technologies."""
        return list(self.technologies.values())

    def get_statistics(self) -> Dict[str, Any]:
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

    nodes_data = []
    technologies_data = []

    if nodes_file and os.path.exists(nodes_file):
        try:
            with open(nodes_file, "r", encoding="utf-8") as f:
                nodes_data = json.load(f)
            logger.info("Loaded nodes data from %s", nodes_file)
        except (IOError, json.JSONDecodeError) as e:
            logger.warning(
                "Failed to load nodes from %s: %s, using default data", nodes_file, e
            )

    if technologies_file and os.path.exists(technologies_file):
        try:
            with open(technologies_file, "r", encoding="utf-8") as f:
                technologies_data = json.load(f)
            logger.info("Loaded technologies data from %s", technologies_file)
        except (IOError, json.JSONDecodeError) as e:
            logger.warning(
                "Failed to load technologies from %s: %s, using default data",
                technologies_file,
                e,
            )

    index = BioimageIndex()
    index.load_data(nodes_data, technologies_data)
    return index


def create_get_entity_details(
    bioimage_index: BioimageIndex,
) -> Callable[..., Coroutine[Any, Any, dict[str, Any]]]:
    """Create a schema function for getting entity details with dependency injection."""

    @schema_function
    async def get_entity_details(
        entity_id: str = Field(
            description="The ID of the entity (node or technology) to retrieve"
        ),
    ) -> dict[str, Any]:
        """
        Get details for a specific entity (node or technology).
        Entity type is inferred if not provided.

        Args:
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

    return get_entity_details


def create_get_related_entities(
    bioimage_index: BioimageIndex,
) -> Callable[..., Coroutine[Any, Any, list[dict[str, Any]]]]:
    """Create a schema function for getting related entities with dependency injection."""

    @schema_function
    async def get_related_entities(
        entity_id: str = Field(
            description="The ID of the entity to find relationships for"
        ),
    ) -> list[dict[str, Any]]:
        """
        Get entities related to a specific entity.
        Entity type is inferred if not provided.

        Args:
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

    return get_related_entities
