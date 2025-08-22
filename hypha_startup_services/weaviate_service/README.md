# Weaviate Service

A wrapper service of [Weaviate](https://weaviate.io/) as a Hypha service. With the help of Hypha, it provides virtual collections called "applications" and fine-grained permissions. Just like Weaviate, this service provides high-performance vector storage, semantic search, and hybrid query capabilities.

## API

### `collections.create(settings: dict)`

Create a new collection.

Verifies that the caller has admin permissions. Adds workspace prefix to collection name before creating it. Creates a collection artifact to track the collection.

**Parameters:**

- `settings` (dict): Collection configuration settings. For a full example configuration, see
[weaviate_bioimage_service](../weaviate_bioimage_service/populate_shared_bioimage_data.py)

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

### `collections.delete(name: str | list)`

Delete a collection by name.

Verifies that the caller has admin permissions. Removes the collection and its associated artifact.

**Parameters:**

- `name` (str | list): Short collection name to delete

**Returns:** Confirmation of deletion

**Example:**

```python
result = await weaviate.collections.delete("Movie")
# Returns confirmation of deletion
```

### `collections.list_all()`

List all collections accessible to the caller.

Returns collections based on caller's permissions. Admin users see all collections, others see collections they have access to.

**Returns:** Dictionary mapping collection names to their configurations

**Example:**

```python
collections = await weaviate.collections.list_all()
```

### `collections.get(name: str)`

Get a collection's configuration by name.

**Parameters:**

- `name` (str): Short collection name to retrieve

**Returns:** Collection configuration

**Example:**

```python
collection = await weaviate.collections.get("Movie")
```

### `collections.get_artifact(collection_name: str)`

Get the artifact for a collection.

Retrieves the collection artifact using the caller's workspace and collection name.

**Parameters:**

- `collection_name` (str): Name of the collection to retrieve the artifact for

**Returns:** Dictionary with collection artifact information

**Example:**

```python
artifact = await weaviate.collections.get_artifact("Movie")
```

### `collections.exists(name: str)`

Check if a collection exists.

**Parameters:**

- `name` (str): Short collection name to check

**Returns:** True if collection exists, False otherwise

**Example:**

```python
exists = await weaviate.collections.exists("Movie")
```

### `applications.create(collection_name: str, application_id: str, description: str)`

Create a new application within a collection.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Unique identifier for the application
- `description` (str): Description of the application

**Returns:** Application creation confirmation

**Example:**

```python
result = await weaviate.applications.create(
    collection_name="Movie",
    application_id="movie-recommender", 
    description="Movie recommendation system"
)
```

### `applications.exists(collection_name: str, application_id: str)`

Check if an application exists within a collection.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier to check

**Returns:** True if application exists, False otherwise

**Example:**

```python
exists = await weaviate.applications.exists("Movie", "movie-recommender")
```

### `applications.get_artifact(collection_name: str, application_id: str)`

Get the artifact for an application.

Retrieves the application artifact using the caller's workspace, collection name, and application ID.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier

**Returns:** Dictionary with application artifact information

**Example:**

```python
artifact = await weaviate.applications.get_artifact("Movie", "movie-recommender")
```

### `applications.set_permissions(collection_name: str, application_id: str, permissions: dict, user_ws: str = None, merge: bool = True)`

Set permissions for an application.

Updates the application artifact with the provided permissions. Verifies that the caller has permission to access the application.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `permissions` (dict): Dictionary of permissions to set
- `user_ws` (str, optional): User workspace
- `merge` (bool): Whether to merge with existing permissions (default: True)

**Returns:** None

**Example:**

```python
await weaviate.applications.set_permissions(
    collection_name="Movie",
    application_id="movie-recommender",
    permissions={"user123": "r", "admin": "wr"}
)
# No return value - permissions updated successfully
```

### `applications.delete(collection_name: str, application_id: str)`

Delete an application and all its associated data from a collection.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier

**Returns:** Deletion result

**Example:**

```python
result = await weaviate.applications.delete(
    collection_name="Movie",
    application_id="movie-recommender"
)
```

### `applications.get(collection_name: str, application_id: str)`

Get metadata and artifact information for an application.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier

**Returns:** Application metadata and artifact information

**Example:**

```python
result = await weaviate.applications.get(
    collection_name="Movie",
    application_id="movie-recommender"
)
```

### `data.insert_many(collection_name: str, application_id: str, objects: list)`

Insert multiple objects into a collection.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `objects` (list): List of objects to insert

**Returns:** Insertion results

**Example:**

```python
objects = [
    {"title": "The Matrix", "genre": "Sci-Fi"},
    {"title": "Inception", "genre": "Sci-Fi"}
]
result = await weaviate.data.insert_many("Movie", "movie-recommender", objects)
```

### `data.insert(collection_name: str, application_id: str, object_data: dict)`

Insert a single object into a collection.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `object_data` (dict): Object to insert

**Returns:** Insertion result

**Example:**

```python
object_data = {"title": "The Matrix", "genre": "Sci-Fi"}
result = await weaviate.data.insert("Movie", "movie-recommender", object_data)
```

### `data.update(collection_name: str, application_id: str, uuid: str, properties: dict)`

Update an object in a collection for a given application.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `uuid` (str): UUID of the object to update
- `properties` (dict): Properties to update

**Returns:** None

**Example:**

```python
await weaviate.data.update(
    collection_name="Movie",
    application_id="movie-recommender",
    uuid="<object-uuid>",
    properties={"genre": "Action"}
)
```

### `data.delete_by_id(collection_name: str, application_id: str, uuid: str)`

Delete an object by UUID from a collection for a given application.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `uuid` (str): UUID of the object to delete

**Returns:** None

**Example:**

```python
await weaviate.data.delete_by_id(
    collection_name="Movie",
    application_id="movie-recommender",
    uuid="<object-uuid>"
)
```

### `data.delete_many(collection_name: str, application_id: str, filters: Filter = None)`

Delete multiple objects from a collection for a given application, optionally filtered.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `filters` (Filter, optional): Weaviate filter conditions

**Returns:** Deletion result summary

**Example:**

```python
from weaviate.classes.query import Filter

result = await weaviate.data.delete_many(
    collection_name="Movie",
    application_id="movie-recommender",
    filters=Filter.by_property("genre").equal("Sci-Fi")
)
```

### `data.exists(collection_name: str, application_id: str, uuid: str)`

Check if an object exists in a collection for a given application by UUID.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `uuid` (str): UUID of the object to check

**Returns:** True if the object exists, False otherwise

**Example:**

```python
exists = await weaviate.data.exists(
    collection_name="Movie",
    application_id="movie-recommender",
    uuid="<object-uuid>"
)
```

## Query Methods

### `query.fetch_objects(collection_name: str, application_id: str, filters: Filter = None, limit: int = 100)`

Fetch objects from a collection with optional filters.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `filters` (Filter, optional): Weaviate filter conditions
- `limit` (int): Maximum number of objects to return (default: 100)

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
```

### `query.hybrid(collection_name: str, application_id: str, query: str, filters: Filter = None, limit: int = 10)`

Perform hybrid search combining vector and keyword search.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `query` (str): Search query
- `filters` (Filter, optional): Weaviate filter conditions
- `limit` (int): Maximum number of results (default: 10)

**Returns:** Hybrid search results

**Example:**

```python
result = await weaviate.query.hybrid(
    collection_name="Movie",
    application_id="movie-recommender", 
    query="science fiction movies",
    limit=5
)
```

### `query.near_vector(collection_name: str, application_id: str, vector: list[float], filters: Filter = None, limit: int = 10)`

Perform a vector similarity search in a collection for a given application.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `vector` (list[float]): The vector to search near
- `filters` (Filter, optional): Weaviate filter conditions
- `limit` (int): Maximum number of results (default: 10)

**Returns:** List of matching objects

**Example:**

```python
result = await weaviate.query.near_vector(
    collection_name="Movie",
    application_id="movie-recommender",
    vector=[0.1, 0.2, 0.3],
    limit=5
)
```

### `generate.near_text(collection_name: str, application_id: str, query: str, single_prompt: str = None, grouped_task: str = None, filters: Filter = None, limit: int = 10)`

Generate text responses using RAG (Retrieval-Augmented Generation).

⚠️ **Performance Warning**: This method can be slow and may time out for large datasets or complex generation tasks. Consider using other query methods as input to an LLM for generation.

**Parameters:**

- `collection_name` (str): Name of the collection
- `application_id` (str): Application identifier
- `query` (str): Search query
- `single_prompt` (str, optional): Template for individual object responses
- `grouped_task` (str, optional): Task description for grouped response
- `filters` (Filter, optional): Weaviate filter conditions
- `limit` (int): Maximum number of results (default: 10)

**Returns:** Generated text response with source objects

**Example:**

```python
result = await weaviate.generate.near_text(
    collection_name="Movie",
    application_id="movie-recommender",
    query="What are good sci-fi movies?",
    grouped_task="Recommend science fiction movies based on the data"
)
```
