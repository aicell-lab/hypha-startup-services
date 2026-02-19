# Weaviate Hypha Agent Skills

This document describes how to use the Weaviate Hypha Service as a set of skills for Hypha Agents.

## Overview

The Weaviate service exposes a rich API for vector database operations. Hypha Agents can bind to this service and use its functions as tools to store and retrieve knowledge.

## Connection

To use the Weaviate service, your agent needs to:
1. Connect to the Hypha server.
2. Get a reference to the `weaviate` service.
3. Call the available methods.

### Example: Helper Tool for Agents

You can wrap the service in a utility class or function set for your agent.

```python
async def use_weaviate_skills(server, agent_workspace="my-workspace"):
    # 1. Get the service
    # The service ID is typically composed of the workspace where it's running 
    # and the service ID. If running in the 'public' workspace:
    weaviate_service = await server.get_service("public/weaviate")
    
    # 2. Use the skills
    
    # Create a collection
    await weaviate_service.collections.create(
        settings={
            "name": "AgentMemory",
            "properties": [
                {"name": "content", "dataType": ["text"]},
                {"name": "source", "dataType": ["text"]},
            ]
        }
    )
    
    # Insert knowledge
    await weaviate_service.data.insert(
        collection_name="AgentMemory",
        application_id="my-agent-app",
        properties={
            "content": "Hypha is a flexible framework for AI services.",
            "source": "documentation"
        }
    )
    
    # Search knowledge
    results = await weaviate_service.query.near_text(
        collection_name="AgentMemory",
        application_id="my-agent-app",
        query="What is Hypha?",
        limit=1
    )
    
    return results
```

## Available Skills

The service exposes the following namespaces and methods:

### `collections`
Manage vector definitions (schemas).
- `create(settings)`: Define a new collection.
- `delete(name)`: Remove a collection.
- `list_all()`: List available collections.
- `get(name)`: Get info about a specific collection.
- `exists(name)`: Check if a collection exists.

### `data`
CRUD operations for data objects.
- `insert(collection_name, application_id, properties)`: Add a single object.
- `insert_many(collection_name, application_id, objects)`: Batch add objects.
- `update(...)`: Update existing objects.
- `delete_by_id(...)`: Delete a specific object.
- `delete_many(...)`: Delete objects matching a filter.

### `query`
Semantic and vector search.
- `near_vector(...)`: Search by vector embedding.
- `fetch_objects(...)`: Filter/Get objects.
- `hybrid(...)`: Combine keyword and vector search.

### `generate` (if enabled)
RAG (Retrieval Augmented Generation) capabilities.
- `near_text(...)`: Generate answers based on search results.

## Agent System Prompt Integration

When building an agent, you can describe these tools in the system prompt:

> You have access to a Weaviate knowledge base. 
> - Use `weaviate.query.near_text` to search for information.
> - Use `weaviate.data.insert` to remember new facts permanently.
> - Use `weaviate.collections.create` if you need to start a new topic thread.

## ID & Scoping

The service enforces `application_id` scoping. 
*   **application_id**: Use this to isolate data for different agents or different sessions. Data inserted with `application_id="session-1"` is only easily retrievable when specifying that same ID (or if you are an admin).
