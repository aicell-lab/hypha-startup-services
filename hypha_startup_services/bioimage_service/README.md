# BioImage Service

The BioImage Service provides exact matching capabilities for EBI (EuroBioImaging) nodes and technologies without relying on vector databases or machine learning models. It offers deterministic, simple, and exact lookups for finding relationships between imaging nodes and technologies.

## Features

- **Exact Matching**: Deterministic lookups without vector similarity
- **Fast Performance**: In-memory Python index for instant responses
- **Comprehensive Coverage**: Complete EBI node and technology data
- **RESTful API**: Simple HTTP endpoints via Hypha-RPC
- **No Dependencies**: No vector databases or ML models required

## Core Functionality

### Relationship Lookups

- **`get_nodes_by_technology_id(technology_id)`**: Find all nodes that provide a specific technology
- **`get_technologies_by_node_id(node_id)`**: Find all technologies provided by a specific node

### Detail Retrieval

- **`get_node_details(node_id)`**: Get complete information about a specific node
- **`get_technology_details(technology_id)`**: Get complete information about a specific technology

### Search Capabilities

- **`search_nodes(query, limit)`**: Search nodes by name (case-insensitive partial matching)
- **`search_technologies(query, limit)`**: Search technologies by name (case-insensitive partial matching)

### Data Access

- **`get_all_nodes(limit)`**: Retrieve all nodes with optional pagination
- **`get_all_technologies(limit)`**: Retrieve all technologies with optional pagination
- **`get_statistics()`**: Get service and index statistics

## Data Structure

### Nodes
Each node represents an imaging facility or service provider with:
- **ID**: Unique UUID identifier
- **Name**: Full name of the facility
- **Description**: Detailed description of services
- **Country**: Location information
- **Technologies**: List of technology IDs/references provided

### Technologies
Each technology represents an imaging technique or method with:
- **ID**: Unique UUID identifier
- **Name**: Full name of the technology
- **Description**: Technical description
- **Category**: Classification category
- **Abbreviation**: Common abbreviation (if available)

## Usage Examples

### Python Client
```python
import asyncio
from hypha_rpc import connect_to_server

async def example():
    server = await connect_to_server({"server_url": "https://hypha.aicell.io", "token": "your_token"})
    service = await server.get_service("bioimage")
    
    # Find nodes providing 3D-CLEM technology
    result = await service.get_nodes_by_technology_id("f0acc857-fc72-4094-bf14-c36ac40801c5")
    print(f"Found {result['total_nodes']} nodes providing 3D-CLEM")
    
    # Find technologies at Italian node
    result = await service.get_technologies_by_node_id("7409a98f-1bdb-47d2-80e7-c89db73efedd")
    print(f"Italian node provides {result['total_technologies']} technologies")
    
    # Search for microscopy nodes
    result = await service.search_nodes("microscopy", limit=5)
    print(f"Found {result['total_results']} nodes with 'microscopy' in name")

asyncio.run(example())
```

### Starting the Service
```bash
# Start the service locally
python -m hypha_startup_services bioimage local

# Connect to remote Hypha server
python -m hypha_startup_services bioimage remote --server-url https://hypha.aicell.io
```

### Example Queries

**Technology to Nodes Lookup**:
- Input: `technology_id = "f0acc857-fc72-4094-bf14-c36ac40801c5"` (3D-CLEM)
- Output: List of nodes in Italy, Poland, etc. that provide 3D-CLEM

**Node to Technologies Lookup**:
- Input: `node_id = "7409a98f-1bdb-47d2-80e7-c89db73efedd"` (Italian Node)
- Output: List of technologies like 3D-CLEM, 4Pi microscopy, super-resolution, etc.

**Search Example**:
- Input: `query = "microscopy"`, `limit = 5`
- Output: First 5 nodes with "microscopy" in their name

## Technical Architecture

### In-Memory Index
The service builds a comprehensive Python index on startup:

```python
class BioimageIndex:
    nodes: Dict[str, Dict[str, Any]]              # ID -> node data
    technologies: Dict[str, Dict[str, Any]]       # ID -> technology data
    node_to_technologies: Dict[str, Set[str]]     # node_id -> {tech_id, ...}
    technology_to_nodes: Dict[str, Set[str]]      # tech_id -> {node_id, ...}
    technology_name_to_id: Dict[str, str]         # name -> tech_id
    node_name_to_id: Dict[str, str]               # name -> node_id
```

### Relationship Resolution
- **UUID References**: Direct ID-to-ID mappings for formal technologies
- **String References**: Automatic synthetic ID creation for informal technology names
- **Bidirectional Indexing**: Both node→tech and tech→node mappings maintained
- **Case-Insensitive Search**: Flexible name-based lookups

### Data Sources
Currently includes sample EBI data with:
- 3+ imaging nodes across Europe (Italy, Poland, Germany)
- 10+ imaging technologies (3D-CLEM, 4Pi microscopy, super-resolution, etc.)
- Extensible for real EBI API integration

## Performance

- **Startup Time**: < 1 second for current dataset
- **Query Time**: < 1ms for exact lookups
- **Memory Usage**: < 10MB for current dataset
- **Scalability**: Suitable for 1000s of nodes/technologies

## Testing

Run the comprehensive test suite:
```bash
pytest tests/bioimage_service/test_bioimage_service.py -v
```

Test with the example script:
```bash
python examples/bioimage_service_example.py
```

## Integration

The BioImage service complements the mem0 service by providing:
- **Exact matching** where mem0 provides semantic similarity
- **Deterministic results** where mem0 provides ML-based ranking
- **Instant responses** where mem0 requires vector computation
- **Simple queries** where mem0 handles complex natural language

Together they provide both structured and unstructured query capabilities for bioimage data.
