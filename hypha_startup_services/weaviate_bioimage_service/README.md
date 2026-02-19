# Weaviate BioImage Service

A specialized vector database service built on the core [Weaviate Service](../weaviate_service/README.md) for bioimage data management. This service provides semantic search and retrieval capabilities specifically designed for EuroBioImaging nodes and technologies, with enhanced support for scientific data relationships.

## ⚠️ Client Setup: Codecs Required

This service may accept complex objects. If you want to use those, you must register custom codecs in your client to handle these calls correctly.

👉 **[See the Codecs Guide](../../codecs.md)** for setup instructions.

## Service Endpoints

⚠️ **Performance Warning**: The query method can be slow and may timeout for large datasets or complex scientific data operations. Consider using `search` instead and giving that as input to an LLM for generation.

### `query(query_text: str, entity_types: list | None = None, limit: int = 10)`

Query bioimage data using natural language.

**Parameters:**
- `query_text` (str): Natural language query
- `entity_types` (list, optional): Filter by entity types ('node', 'technology', or both)
- `limit` (int): Maximum number of results (default: 10)

**Returns:** Dictionary with query results and generated response

**Example:**
```python
import asyncio
from hypha_rpc import connect_to_server

async def bioimage_query():
    server = await connect_to_server({"server_url": "https://hypha.aicell.io"})
    bioimage = await server.get_service("weaviate-bioimage")
    
    result = await bioimage.query(
        query_text="What facilities offer super-resolution microscopy in Europe?",
        entity_types=["node"],
        limit=5
    )

asyncio.run(bioimage_query())
```

### `search(query_text: str, entity_types: list | None = None, include_related: bool = True, limit: int = 10)`

Search bioimage data using natural language.

**Parameters:**
- `query_text` (str): Natural language query
- `entity_types` (list, optional): Filter by entity types ('node', 'technology', or both)
- `include_related` (bool): Include related entities in results (default: True)
- `limit` (int): Maximum number of results (default: 10)

**Returns:** Dictionary with search results and generated response

**Example:**
```python
result = await bioimage.search(
    query_text="3D correlative light and electron microscopy",
    entity_types=["technology"],
    include_related=True,
    limit=3
)
```

### `get(entity_id: str)`

Get a specific entity by ID.

**Parameters:**
- `entity_id` (str): ID of the entity to retrieve

**Returns:** Dictionary with entity details

**Example:**
```python
italy_node_id = "7e35b2a1-22ef-58ec-a32b-805e388932ee"
entity = await bioimage.get(entity_id=italy_node_id)
```

### `get_related(entity_id: str)`

Get entities related to the specified entity.

**Parameters:**
- `entity_id` (str): ID of the entity to find related entities for

**Returns:** List of related entities

**Example:**
```python
related_entities = await bioimage.get_related(entity_id=italy_node_id)
```
