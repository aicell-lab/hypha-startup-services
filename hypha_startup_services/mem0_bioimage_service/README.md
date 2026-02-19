# Mem0 BioImage Service

A specialized AI memory service built on the core [Mem0 Service](../mem0_service/README.md) for bioimage data management. This service combines semantic memory capabilities with domain-specific bioimage knowledge, providing intelligent storage and retrieval of conversations, research context, and scientific relationships in the bioimage domain.

## ⚠️ Client Setup: Codecs Required

This service may accept complex objects. If you want to use those, you must register custom codecs in your client to handle these calls correctly.

👉 **[See the Codecs Guide](../../codecs.md)** for setup instructions.

## Service Endpoints

### `search(query_text: str, entity_types: list | None = None, include_related: bool = True, limit: int = 10)`

Unified query method that combines semantic search with related entity lookup.

This replaces the separate semantic_query and find_related_entities_semantic methods.

**Parameters:**

- `query_text` (str): Natural language query
- `entity_types` (list, optional): Filter by entity types ('node', 'technology', or both)
- `include_related` (bool): Whether to include related entities for each result (default: True)
- `limit` (int): Maximum number of results (default: 10)

**Returns:** Dictionary with semantic search results and related entities

**Example:**

```python
import asyncio
from hypha_rpc import connect_to_server

async def bioimage_search():
    server = await connect_to_server({"server_url": "https://hypha.aicell.io"})
    bioimage = await server.get_service("hypha-agents/mem0-bioimage")
    
    result = await bioimage.search(
        query_text="European facilities offering correlative microscopy for protein research",
        entity_types=["node"],
        include_related=True,
        limit=5
    )

asyncio.run(bioimage_search())
```

### `get(entity_id: str)`

Get detailed information about a specific entity.

**Parameters:**

- `entity_id` (str): ID of the entity (node or technology) to retrieve

**Returns:** Dictionary containing the entity details

**Example:**

```python
italian_node_id = "7e35b2a1-22ef-58ec-a32b-805e388932ee"
entity_details = await bioimage.get(entity_id=italian_node_id)
```

### `get_related(entity_id: str)`

Get entities related to a specific entity.

Entity type is inferred if not provided.

**Parameters:**

- `entity_id` (str): ID of the entity to find relationships for

**Returns:** List of related entities

**Example:**

```python
related_entities = await bioimage.get_related(entity_id=italian_node_id)
```
