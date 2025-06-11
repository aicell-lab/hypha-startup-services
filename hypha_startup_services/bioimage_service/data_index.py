"""Data structures and indexing for EBI nodes and technologies."""

from typing import Dict, List, Any, Set
import json
import logging

logger = logging.getLogger(__name__)


# Sample EBI data - in a real implementation this would be loaded from external sources
EBI_NODES_DATA = [
    {
        "id": "7409a98f-1bdb-47d2-80e7-c89db73efedd",
        "name": "Advanced Light Microscopy Italian Node",
        "description": "The Italian ALM Node comprises five imaging facilities located in Naples, Genoa, Padua, Florence and Milan specializing in correlative light electron microscopy, super-resolution, and functional imaging.",
        "country": {"name": "Italy", "iso_a2": "IT"},
        "technologies": [
            "f0acc857-fc72-4094-bf14-c36ac40801c5",  # 3D CLEM
            "68a3b6c4-9c19-4446-9617-22e7d37e0f2c",  # 4Pi microscopy
            "correlative_microscopy",
            "super_resolution",
            "functional_imaging",
        ],
    },
    {
        "id": "099e48ff-7204-46ea-8828-10025e945081",
        "name": "Advanced Light Microscopy Node Poland",
        "description": "Multi-sited, multimodal EuroBioimaging Node offering open access to multi-modal ALM, CLEM, EM, functional imaging, high-throughput microscopy and super-resolution microscopy.",
        "country": {"name": "Poland", "iso_a2": "PL"},
        "technologies": [
            "f0acc857-fc72-4094-bf14-c36ac40801c5",  # 3D CLEM
            "multi_modal_alm",
            "clem",
            "electron_microscopy",
            "high_throughput",
        ],
    },
    {
        "id": "bc123456-789a-bcde-f012-3456789abcde",
        "name": "German BioImaging Node",
        "description": "German node providing advanced microscopy services including super-resolution and live-cell imaging.",
        "country": {"name": "Germany", "iso_a2": "DE"},
        "technologies": [
            "68a3b6c4-9c19-4446-9617-22e7d37e0f2c",  # 4Pi microscopy
            "super_resolution",
            "live_cell_imaging",
        ],
    },
]

EBI_TECHNOLOGIES_DATA = [
    {
        "id": "f0acc857-fc72-4094-bf14-c36ac40801c5",
        "name": "3D Correlative Light and Electron Microscopy (3D-CLEM)",
        "abbr": "3D-CLEM",
        "description": "3D CLEM combines volume EM methods with 3D light microscopy techniques requiring 3D registration between modalities.",
        "category": {"name": "Correlative Light Microscopy and Electron Microscopy"},
    },
    {
        "id": "68a3b6c4-9c19-4446-9617-22e7d37e0f2c",
        "name": "4Pi microscopy",
        "abbr": "4Pi",
        "description": "Laser scanning fluorescence microscope with improved axial resolution using two opposing objective lenses for coherent wavefront matching.",
        "category": {"name": "Fluorescence Nanoscopy"},
    },
    {
        "id": "abc12345-6789-abcd-ef01-23456789abcd",
        "name": "Super-resolution microscopy",
        "abbr": "SRM",
        "description": "Techniques that surpass the diffraction limit of light microscopy to achieve nanometer resolution.",
        "category": {"name": "Fluorescence Nanoscopy"},
    },
]


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
        nodes_data: List[Dict[str, Any]] | None = None,
        technologies_data: List[Dict[str, Any]] | None = None,
    ):
        """Load and index the EBI data."""
        if nodes_data is None:
            nodes_data = EBI_NODES_DATA
        if technologies_data is None:
            technologies_data = EBI_TECHNOLOGIES_DATA

        # Index technologies
        for tech in technologies_data:
            tech_id = tech["id"]
            self.technologies[tech_id] = tech
            # Create name-to-id mapping (case-insensitive)
            tech_name = tech["name"].lower()
            self.technology_name_to_id[tech_name] = tech_id
            if tech.get("abbr"):
                self.technology_name_to_id[tech["abbr"].lower()] = tech_id

        # Index nodes and build relationships
        for node in nodes_data:
            node_id = node["id"]
            self.nodes[node_id] = node
            # Create name-to-id mapping (case-insensitive)
            node_name = node["name"].lower()
            self.node_name_to_id[node_name] = node_id

            # Build technology relationships
            technologies = node.get("technologies", [])
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
    import os

    # Default to assets directory if no files specified
    if nodes_file is None:
        nodes_file = os.path.join(os.path.dirname(__file__), "assets", "ebi-nodes.json")
    if technologies_file is None:
        technologies_file = os.path.join(
            os.path.dirname(__file__), "assets", "ebi-tech.json"
        )

    nodes_data = EBI_NODES_DATA
    technologies_data = EBI_TECHNOLOGIES_DATA

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
