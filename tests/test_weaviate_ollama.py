#!/usr/bin/env python3
"""
Test script for Weaviate with Ollama integration using Weaviate Client v4.
This script demonstrates:
1. Connecting to Weaviate using the v4 client
2. Creating a collection with Ollama vectorizer and named vectors
3. Adding data to the collection with dynamic batching
4. Performing various search types:
   - Vector search
   - Filter search
   - Hybrid search
   - Near text search with generation
5. Proper connection handling with try/finally

Note: The v4 client requires proper connection closing with client.close()
"""

import weaviate
import time
from weaviate.classes.config import Property, DataType, Configure
from weaviate.classes.init import AdditionalConfig, Auth
from weaviate.util import generate_uuid5
import weaviate.classes as wvc
from weaviate.classes.query import Filter

# Weaviate connection settings
WEAVIATE_URL = "https://hypha-weaviate.scilifelab-2-dev.sys.kth.se"
WEAVIATE_GRPC_URL = "https://hypha-weaviate-grpc.scilifelab-2-dev.sys.kth.se"  # Updated gRPC host  # Internal URL: http://ollama.hypha.svc.cluster.local:11434
OLLAMA_ENDPOINT = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se"
OLLAMA_MODEL = "llama3.2"  # For embeddings - using an available model
OLLAMA_LLM_MODEL = "llama3.2"  # For generation - using an available model


def main():
    print("Connecting to Weaviate...")

    try:
        # Create a client with the URL using v4 API
        additional_config = AdditionalConfig(
            timeout_config=(5, 60),  # (connection_timeout_sec, request_timeout_sec)
            grpc_insecure=True,  # Skip SSL verification for gRPC
            grpc_insecure_auth=True,  # Allow insecure authentication
        )

        # Example of API key authentication (commented out)
        # auth_credentials = Auth.api_key(api_key="your-api-key-here")

        # Parse the URL to get the host and determine if it's secure
        # For a URL like "https://example.com", we extract "example.com" and use https=True
        http_host = WEAVIATE_URL.replace("https://", "").replace("http://", "")
        grpc_host = WEAVIATE_GRPC_URL.replace("https://", "").replace("http://", "")
        is_secure = WEAVIATE_URL.startswith("https://")
        is_grpc_secure = WEAVIATE_GRPC_URL.startswith("https://")

        # In Kubernetes with Ingress, HTTP traffic goes through port 443 (HTTPS)
        # But gRPC needs a separate port - by default Weaviate uses 50051 for gRPC
        # The ingress should be configured to route gRPC traffic to this port
        client = weaviate.connect_to_custom(
            http_host=http_host,
            http_port=443 if is_secure else 80,
            http_secure=is_secure,
            grpc_host=grpc_host,
            grpc_port=(
                443 if is_grpc_secure else 50051
            ),  # For ingress, use 443 for secure gRPC
            grpc_secure=is_grpc_secure,
            headers={},
            # auth_credentials=auth_credentials,  # Uncomment to use authentication
            additional_config=additional_config,
            skip_init_checks=True,  # Skip initialization checks for testing
        )

        # Alternative connection method using context manager:
        # with weaviate.connect_to_custom(
        #     http_host=http_host,
        #     http_port=443 if is_secure else 80,
        #     http_secure=is_secure,
        #     grpc_host=grpc_host,
        #     grpc_port=443 if is_grpc_secure else 50051,
        #     grpc_secure=is_grpc_secure,
        #     headers={},
        # ) as client:
        #     # All operations would be inside this block
        #     # Connection is automatically closed when exiting the block

        # Check if Weaviate is ready
        print("Checking if Weaviate is ready...")
        if client.is_ready():
            meta = client.get_meta()
            print(f"Connected to Weaviate version: {meta['version']}")
        else:
            print("Weaviate is not ready")
            return

        # Collection name
        collection_name = "MovieCollection"

        # Delete collection if it exists
        if client.collections.exists(collection_name):
            print(f"Deleting existing collection: {collection_name}")
            client.collections.delete(collection_name)
            time.sleep(2)  # Give it time to delete

        print(f"Creating collection: {collection_name} with Ollama vectorizer...")

        # Configure Ollama vectorizer with named vectors
        vectorizer_config = [
            Configure.NamedVectors.text2vec_ollama(
                name="title_vector",
                source_properties=["title"],
                api_endpoint=OLLAMA_ENDPOINT,
                model=OLLAMA_MODEL,
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="description_vector",
                source_properties=["description"],
                api_endpoint=OLLAMA_ENDPOINT,
                model=OLLAMA_MODEL,
            ),
        ]

        # Configure Ollama generative model
        generative_config = Configure.Generative.ollama(
            api_endpoint=OLLAMA_ENDPOINT, model=OLLAMA_LLM_MODEL
        )

        # Create the collection with properties
        collection = client.collections.create(
            name=collection_name,
            description="A collection of movies",
            vectorizer_config=vectorizer_config,
            generative_config=generative_config,
            properties=[
                Property(
                    name="title",
                    data_type=DataType.TEXT,
                    description="The title of the movie",
                ),
                Property(
                    name="description",
                    data_type=DataType.TEXT,
                    description="A description of the movie",
                ),
                Property(
                    name="genre",
                    data_type=DataType.TEXT,
                    description="The genre of the movie",
                ),
                Property(
                    name="year",
                    data_type=DataType.INT,
                    description="The year the movie was released",
                ),
            ],
        )

        print(f"Collection '{collection.name}' created successfully")

        # Sample data
        movies = [
            {
                "title": "The Shawshank Redemption",
                "description": "A wrongfully imprisoned man forms an inspiring friendship while finding hope and redemption in the darkest of places.",
                "genre": "Drama",
                "year": 1994,
            },
            {
                "title": "The Godfather",
                "description": "A powerful mafia family struggles to balance loyalty, power, and betrayal in this iconic crime saga.",
                "genre": "Crime",
                "year": 1972,
            },
            {
                "title": "The Dark Knight",
                "description": "Batman faces his greatest challenge as he battles the chaos unleashed by the Joker in Gotham City.",
                "genre": "Action",
                "year": 2008,
            },
            {
                "title": "Jingle All the Way",
                "description": "A desperate father goes to hilarious lengths to secure the season's hottest toy for his son on Christmas Eve.",
                "genre": "Comedy",
                "year": 1996,
            },
            {
                "title": "A Christmas Carol",
                "description": "A miserly old man is transformed after being visited by three ghosts on Christmas Eve in this timeless tale of redemption.",
                "genre": "Drama",
                "year": 1984,
            },
        ]

        # Import data using batch
        print("Importing movie data...")
        try:
            # Get the collection reference
            movies_collection = client.collections.get(collection_name)

            # Create data objects
            data_objects = list()
            for movie in movies:
                # Create a data object with properties and UUID
                data_object = wvc.data.DataObject(
                    properties=movie,
                    uuid=generate_uuid5(movie),
                    vector={
                        "title_vector": [0.1]
                        * 3072,  # Updated dummy vector length to 3072
                        "description_vector": [0.1]
                        * 3072,  # Updated dummy vector length to 3072
                    },
                )
                data_objects.append(data_object)

            # Insert all objects at once
            response = movies_collection.data.insert_many(data_objects)

            print(f"Successfully imported {len(movies)} movies")
        except Exception as e:
            print(f"Error importing data: {e}")

        # Wait a moment for indexing
        print("Waiting for indexing to complete...")
        time.sleep(5)

        # Perform vector search
        print("\nPerforming vector search for similar movies...")
        try:
            # Create a dummy vector for search
            dummy_vector = [0.8] * 3072  # Updated dummy vector length to 3072

            # Get the collection
            movies_collection = client.collections.get(collection_name)

            # Perform near vector search
            response = movies_collection.query.near_vector(
                near_vector=dummy_vector,  # Changed from 'vector' to 'near_vector'
                target_vector="title_vector",  # Specify which named vector to search against
                limit=2,
            )

            print("Vector search results:")
            for i, obj in enumerate(response.objects):
                print(
                    f"{i+1}. {obj.properties['title']} ({obj.properties['year']}) - {obj.properties['genre']}"
                )
                print(f"   {obj.properties['description']}")
                print()
        except Exception as e:
            print(f"Error in vector search: {e}")

        # Perform filter search
        print("\nPerforming filter search for Christmas movies...")
        try:
            # Get the collection
            movies_collection = client.collections.get(collection_name)

            # Perform filter search
            response = movies_collection.query.fetch_objects(
                filters=Filter.by_property("description").contains_any(["Christmas"]),
                limit=2,
            )

            print("Filter search results:")
            for i, obj in enumerate(response.objects):
                print(
                    f"{i+1}. {obj.properties['title']} ({obj.properties['year']}) - {obj.properties['genre']}"
                )
                print(f"   {obj.properties['description']}")
                print()
        except Exception as e:
            print(f"Error in filter search: {e}")

        # Perform hybrid search
        print("\nPerforming hybrid search for action movies...")
        try:
            # Get the collection
            movies_collection = client.collections.get(collection_name)

            # Perform hybrid search (combines BM25 and vector search)
            response = movies_collection.query.hybrid(
                query="action hero",
                target_vector="description_vector",  # Specify which named vector to search against
                alpha=0.5,  # Balance between keyword (0) and vector (1) search
                limit=2,
            )

            print("Hybrid search results:")
            for i, obj in enumerate(response.objects):
                print(
                    f"{i+1}. {obj.properties['title']} ({obj.properties['year']}) - {obj.properties['genre']}"
                )
                print(f"   {obj.properties['description']}")
                print()
        except Exception as e:
            print(f"Error in hybrid search: {e}")

        # Perform near_text search with generation
        print("\nPerforming near_text search for holiday films with generation...")
        try:
            # Get the collection
            movies_collection = client.collections.get(collection_name)

            # Perform near_text search with single prompt generation
            response = movies_collection.generate.near_text(
                query="A holiday film",
                single_prompt="Translate this into French: {title}",
                target_vector="description_vector",  # Specify which named vector to search against
                limit=2,
            )

            print("Near text search results with generation:")
            for i, obj in enumerate(response.objects):
                print(
                    f"{i+1}. {obj.properties['title']} ({obj.properties['year']}) - {obj.properties['genre']}"
                )
                print(f"   {obj.properties['description']}")
                print(f"   Generated translation: {obj.generated}")
                print()
        except Exception as e:
            print(f"Error in near_text search with generation: {e}")

        # Perform grouped generation with near_text
        print("\nPerforming grouped generation with near_text for holiday films...")
        try:
            # Get the collection
            movies_collection = client.collections.get(collection_name)

            # Perform grouped generation
            response = movies_collection.generate.near_text(
                query="holiday film",
                grouped_task="Write a fun tweet to promote readers to check out these films.",
                target_vector="description_vector",  # Specify which named vector to search against
                limit=2,
            )

            print("Grouped generation results:")
            print(f"Generated tweet: {response.generated}")
            print("\nFilms included in the generation:")
            for obj in response.objects:
                print(
                    f"- {obj.properties['title']} ({obj.properties['year']}) - {obj.properties['genre']}"
                )
            print()
        except Exception as e:
            print(f"Error in grouped generation with near_text: {e}")

        print("\nTest completed.")

    finally:
        # Close the client connection when done
        if "client" in locals():
            client.close()


if __name__ == "__main__":
    main()
