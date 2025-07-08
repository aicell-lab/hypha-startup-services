# Mem0 BioImage Service

A specialized AI memory service built on the core [Mem0 Service](../mem0_service/README.md) for bioimage data management. This service combines semantic memory capabilities with domain-specific bioimage knowledge, providing intelligent storage and retrieval of conversations, research context, and scientific relationships in the bioimage domain.

## Service Endpoints

### search(query_text, entity_types=None, include_related=True, limit=10, context=None)

Unified query method that combines semantic search with related entity lookup.

This replaces the separate semantic_query and find_related_entities_semantic methods.

**Parameters:**
- `query_text` (str): Natural language query
- `entity_types` (list, optional): Filter by entity types ('node', 'technology', or both)
- `include_related` (bool): Whether to include related entities for each result (default: True)
- `limit` (int): Maximum number of results (default: 10)
- `context` (dict, optional): Context containing caller information

**Returns:** Dictionary with semantic search results and related entities

**Example:**
```python
import asyncio
from hypha_rpc import connect_to_server

async def bioimage_search():
    server = await connect_to_server({"server_url": "https://hypha.aicell.io"})
    bioimage = await server.get_service("mem0-bioimage")
    
    result = await bioimage.search(
        query_text="European facilities offering correlative microscopy for protein research",
        entity_types=["node"],
        include_related=True,
        limit=5
    )
    print(result)

asyncio.run(bioimage_search())
```

### get(entity_id, context=None)

Get detailed information about a specific entity.

**Parameters:**
- `entity_id` (str): ID of the entity (node or technology) to retrieve
- `context` (dict, optional): Context containing caller information

**Returns:** Dictionary containing the entity details

**Example:**
```python
italian_node_id = "7e35b2a1-22ef-58ec-a32b-805e388932ee"
entity_details = await bioimage.get(entity_id=italian_node_id)
print(entity_details)
```

### get_related(entity_id, context=None)

Get entities related to a specific entity.

Entity type is inferred if not provided.

**Parameters:**
- `entity_id` (str): ID of the entity to find relationships for
- `context` (dict, optional): Context containing caller information

**Returns:** List of related entities

**Example:**
```python
related_entities = await bioimage.get_related(entity_id=italian_node_id)
print(related_entities)
```
