# Hypha Weaviate Service Examples

This directory contains examples demonstrating how to use the Hypha Weaviate Service.

## Prerequisites

1. Python 3.7+
2. Required packages:
   ```
   hypha_rpc
   python-dotenv
   ```

3. A Hypha authentication token (set as environment variable `HYPHA_TOKEN`)

## Setup

1. Install the required packages:
   ```bash
   pip install hypha_rpc python-dotenv
   ```

2. Create a `.env` file in the root directory with your Hypha token:
   ```
   HYPHA_TOKEN=your_token_here
   ```

## Running the Examples

### Weaviate Service Example

The `weaviate_service_example.py` demonstrates:
- Creating and managing collections
- Inserting and querying data
- Vector and hybrid search capabilities
- Proper cleanup and resource management

To run the example:
```bash
python weaviate_service_example.py
```

## Example Output

The example will:
1. Create a movie collection with vector search capabilities
2. Insert sample movie data
3. Demonstrate different query types:
   - Basic object fetching
   - Vector search
   - Hybrid search
4. Clean up resources

Expected output will show:
```
1. Creating Movie Collection...
Collection created: Movie

2. Inserting movie data...
Inserted movie with UUID: <uuid>
Inserted multiple movies: [<uuid1>, <uuid2>]

3. Querying data...
All movies:
- The Matrix (1999)
- Inception (2010)
- The Dark Knight (2008)

4. Performing vector search...
Vector search results:
- <movie_title1>
- <movie_title2>

5. Performing hybrid search...
Hybrid search results:
- <movie_title> (Science Fiction)
- <movie_title> (Action)

6. Cleaning up...
Collection deleted
```

## Notes

- The example uses the Ollama text vectorizer for embedding generation
- Vector search example uses a mock vector (in practice, you would use your embedding model)
- The example includes proper error handling and resource cleanup 