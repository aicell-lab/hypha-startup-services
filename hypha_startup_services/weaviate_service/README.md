# Weaviate Service

A comprehensive vector database service built on [Weaviate](https://weaviate.io/) with multi-tenant support, fine-grained permissions, and seamless integration with the Hypha framework. This service provides high-performance vector storage, semantic search, and hybrid query capabilities.

## Service Endpoints

### collections.create(settings, context=None)

Create a new collection.

Verifies that the caller has admin permissions. Adds workspace prefix to collection name before creating it. Creates a collection artifact to track the collection.

**Parameters:**
- `settings` (dict): Collection configuration settings
- `context` (dict, optional): Context containing caller information

**Returns:** The collection configuration with the short collection name

**Example:**
```python
import asyncio
from hypha_rpc import connect_to_server

async def create_collection():
    server = await connect_to_server({"server_url": "https://hypha.aicell.io"})
    weaviate = await server.get_service("weaviate")
    
    settings = {
        "class": "Movie", 
        "description": "A movie collection",
        "properties": [
            {"name": "title", "dataType": ["text"]},
            {"name": "genre", "dataType": ["text"]}
        ]
    }
    
    result = await weaviate.collections.create(settings)
    # Returns collection configuration with short name

asyncio.run(create_collection())
```

### collections.delete(name, context=None)

Delete a collection by name.

Verifies that the caller has admin permissions. Removes the collection and its associated artifacts.

**Parameters:**
- `name` (str): Short collection name to delete
- `context` (dict, optional): Context containing caller information

**Returns:** Confirmation of deletion

**Example:**
```python
result = await weaviate.collections.delete("Movie")
# Returns confirmation of deletion
```

### collections.list_all(context=None)

List all collections accessible to the caller.

Returns collections based on caller's permissions. Admin users see all collections, others see collections they have access to.

**Parameters:**
- `context` (dict, optional): Context containing caller information

**Returns:** Dictionary mapping collection names to their configurations

**Example:**
```python
collections = await weaviate.collections.list_all()
print(collections)
```

### collections.get(name, context=None)

Get a collection's configuration by name.

**Parameters:**
- `name` (str): Short collection name to retrieve
- `context` (dict, optional): Context containing caller information

**Returns:** Collection configuration

**Example:**
```python
collection = await weaviate.collections.get("Movie")
print(collection)
```

### collections.get_artifact(collection_name, context=None)

Get the artifact for a collection.

Retrieves the collection artifact using the caller's workspace and collection name.

**Parameters:**
- `collection_name` (str): Name of the collection to retrieve the artifact for
- `context` (dict, optional): Context containing caller information

**Returns:** Dictionary with collection artifact information

**Example:**
```python
artifact = await weaviate.collections.get_artifact("Movie")
print(artifact)
```

### collections.exists(name, context=None)

Check if a collection exists.

**Parameters:**
- `name` (str): Short collection name to check
- `context` (dict, optional): Context containing caller information

**Returns:** True if collection exists, False otherwise

**Example:**
```python
exists = await weaviate.collections.exists("Movie")
print(exists)
```

### applications.create(collection_name, application_id, description, context=None)

Create a new application within a collection.

**Parameters:**
- `collection_name` (str): Name of the collection
- `application_id` (str): Unique identifier for the application
- `description` (str): Description of the application
- `context` (dict, optional): Context containing caller information

**Returns:** Application creation confirmation

**Example:**
```python
result = await weaviate.applications.create(
    collection_name="Movie",
    application_id="movie-recommender", 
    description="Movie recommendation system"
)
print(result)
```

### applications.exists(collection_name, application_id, context=None)

Check if an application exists within a collection.

**Parameters:**
- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier to check
- `context` (dict, optional): Context containing caller information

**Returns:** True if application exists, False otherwise

**Example:**
```python
exists = await weaviate.applications.exists("Movie", "movie-recommender")
print(exists)
```

### applications.get_artifact(collection_name, application_id, context=None)

Get the artifact for an application.

Retrieves the application artifact using the caller's workspace, collection name, and application ID.

**Parameters:**
- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `context` (dict, optional): Context containing caller information

**Returns:** Dictionary with application artifact information

**Example:**
```python
artifact = await weaviate.applications.get_artifact("Movie", "movie-recommender")
print(artifact)
```

### applications.set_permissions(collection_name, application_id, permissions, user_ws=None, merge=True, context=None)

Set permissions for an application.

Updates the application artifact with the provided permissions. Verifies that the caller has permission to access the application.

**Parameters:**
- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `permissions` (dict): Dictionary of permissions to set
- `user_ws` (str, optional): User workspace
- `merge` (bool): Whether to merge with existing permissions (default: True)
- `context` (dict, optional): Context containing caller information

**Returns:** None

**Example:**
```python
await weaviate.applications.set_permissions(
    collection_name="Movie",
    application_id="movie-recommender",
    permissions={"read": "user123", "write": "admin"}
)
# No return value - permissions updated successfully
```

### data.insert_many(collection_name, application_id, objects, context=None)

Insert multiple objects into a collection.

**Parameters:**
- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `objects` (list): List of objects to insert
- `context` (dict, optional): Context containing caller information

**Returns:** Insertion results

**Example:**
```python
objects = [
    {"title": "The Matrix", "genre": "Sci-Fi"},
    {"title": "Inception", "genre": "Sci-Fi"}
]
result = await weaviate.data.insert_many("Movie", "movie-recommender", objects)
print(result)
```

### data.insert(collection_name, application_id, object_data, context=None)

Insert a single object into a collection.

**Parameters:**
- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `object_data` (dict): Object to insert
- `context` (dict, optional): Context containing caller information

**Returns:** Insertion result

**Example:**
```python
object_data = {"title": "The Matrix", "genre": "Sci-Fi"}
result = await weaviate.data.insert("Movie", "movie-recommender", object_data)
print(result)
```

## Query Methods

### query.fetch_objects(collection_name, application_id, filters=None, limit=100, context=None)

Fetch objects from a collection with optional filters.

**Parameters:**
- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `filters` (Filter, optional): Weaviate filter conditions
- `limit` (int): Maximum number of objects to return (default: 100)
- `context` (dict, optional): Context containing caller information

**Returns:** List of matching objects

**Example:**
```python
from weaviate.classes.query import Filter

result = await weaviate.query.fetch_objects(
    collection_name="Movie",
    application_id="movie-recommender",
    filters=Filter.by_property("genre").equal("Sci-Fi"),
    limit=10
)
print(result)
```

### query.hybrid(collection_name, application_id, query, filters=None, limit=10, context=None)

Perform hybrid search combining vector and keyword search.

**Parameters:**
- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `query` (str): Search query
- `filters` (Filter, optional): Weaviate filter conditions
- `limit` (int): Maximum number of results (default: 10)
- `context` (dict, optional): Context containing caller information

**Returns:** Hybrid search results

**Example:**
```python
result = await weaviate.query.hybrid(
    collection_name="Movie",
    application_id="movie-recommender", 
    query="science fiction movies",
    limit=5
)
print(result)
```

### generate.near_text(collection_name, application_id, query, single_prompt=None, grouped_task=None, filters=None, limit=10, context=None)

Generate text responses using RAG (Retrieval-Augmented Generation).

⚠️ **Performance Warning**: This method can be slow and may timeout for large datasets or complex generation tasks. Consider using appropriate filters and limits to optimize performance.

**Parameters:**
- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `query` (str): Search query
- `single_prompt` (str, optional): Template for individual object responses
- `grouped_task` (str, optional): Task description for grouped response
- `filters` (Filter, optional): Weaviate filter conditions
- `limit` (int): Maximum number of results (default: 10)
- `context` (dict, optional): Context containing caller information

**Returns:** Generated text response with source objects

**Example:**
```python
result = await weaviate.generate.near_text(
    collection_name="Movie",
    application_id="movie-recommender",
    query="What are good sci-fi movies?",
    grouped_task="Recommend science fiction movies based on the data"
)
print(result)
```
