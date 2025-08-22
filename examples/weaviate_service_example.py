"""Example demonstrating how to use the Hypha Weaviate Service.

This example shows how to:
1. Connect to the service
2. Create and manage collections
3. Insert, update, and delete data
4. Perform various types of queries including vector and hybrid search
"""

import asyncio
import os
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv
from hypha_rpc import connect_to_server

if TYPE_CHECKING:
    from hypha_rpc.rpc import RemoteService


async def main():
    # Load environment variables
    load_dotenv()

    # Connect to Hypha server
    token = os.environ.get("HYPHA_TOKEN")
    assert token is not None, "HYPHA_TOKEN environment variable is not set"

    server: RemoteService = await connect_to_server(
        {
            "server_url": "https://hypha.aicell.io",
            "token": token,
        },
    )

    # Get the Weaviate service
    weaviate_service: RemoteService = await server.get_service("public/weaviate")

    try:
        # 1. Create a Movie Collection
        # ---------------------------
        print("\n1. Creating Movie Collection...")

        # Define collection schema
        class_obj: dict[str, Any] = {
            "class": "Movie",
            "description": "A movie collection",
            "properties": [
                {
                    "name": "title",
                    "dataType": ["text"],
                    "description": "The title of the movie",
                },
                {
                    "name": "description",
                    "dataType": ["text"],
                    "description": "A description of the movie",
                },
                {
                    "name": "genre",
                    "dataType": ["text"],
                    "description": "The genre of the movie",
                },
                {
                    "name": "year",
                    "dataType": ["int"],
                    "description": "The year the movie was released",
                },
            ],
            "vectorConfig": {
                "title_vector": {
                    "vectorizer": {
                        "text2vec-ollama": {
                            "model": "llama3.2",
                            "apiEndpoint": "https://hypha-ollama.scilifelab-2-dev.sys.kth.se",
                        },
                    },
                    "sourceProperties": ["title"],
                    "vectorIndexType": "hnsw",
                    "vectorIndexConfig": {"distance": "cosine"},
                },
                "description_vector": {
                    "vectorizer": {
                        "text2vec-ollama": {
                            "model": "llama3.2",
                            "apiEndpoint": "https://hypha-ollama.scilifelab-2-dev.sys.kth.se",
                        },
                    },
                    "sourceProperties": ["description"],
                    "vectorIndexType": "hnsw",
                    "vectorIndexConfig": {"distance": "cosine"},
                },
            },
            "moduleConfig": {
                "generative-ollama": {
                    "model": "llama3.2",
                    "apiEndpoint": "https://hypha-ollama.scilifelab-2-dev.sys.kth.se",
                },
            },
        }

        if not await weaviate_service.collections.exists("Movie"):
            # Create collection
            collection = await weaviate_service.collections.create(class_obj)
            print("Collection created:", collection["class"])
        else:
            print("Collection already exists")

        # 2. Insert Data
        # --------------
        print("\n2. Inserting movie data...")

        # Insert a single movie
        movie = {
            "title": "The Matrix",
            "description": "A computer programmer discovers a mysterious world beneath reality",
            "genre": "Science Fiction",
            "year": 1999,
        }

        uuid = await weaviate_service.data.insert(
            collection_name="Movie",
            properties=movie,
        )
        print("Inserted movie with UUID:", uuid)

        # Insert multiple movies
        movies = [
            {
                "title": "Inception",
                "description": "A thief enters dreams to steal secrets",
                "genre": "Science Fiction",
                "year": 2010,
            },
            {
                "title": "The Dark Knight",
                "description": "Batman faces his greatest challenge against the Joker",
                "genre": "Action",
                "year": 2008,
            },
        ]

        result = await weaviate_service.data.insert_many(
            collection_name="Movie",
            objects=movies,
        )
        print("Inserted multiple movies:", result["uuids"])

        # 3. Query Data
        # ------------
        print("\n3. Querying data...")

        # Fetch all movies
        query_result = await weaviate_service.query.fetch_objects(
            collection_name="Movie",
            limit=10,
        )
        print("\nAll movies:")
        for obj in query_result["objects"]:
            print(f"- {obj['properties']['title']} ({obj['properties']['year']})")

        # 4. Vector Search
        # ---------------
        print("\n4. Performing vector search...")

        # Example vector (in practice, this would come from your embedding model)
        example_vector = [0.1] * 3072  # Using 3072 dimensions as an example

        vector_results = await weaviate_service.query.near_vector(
            collection_name="Movie",
            near_vector=example_vector,
            target_vector="title_vector",
            limit=2,
        )
        print("\nVector search results:")
        for obj in vector_results["objects"]:
            print(f"- {obj['properties']['title']}")

        # 5. Hybrid Search
        # ---------------
        print("\n5. Performing hybrid search...")

        hybrid_results = await weaviate_service.query.hybrid(
            collection_name="Movie",
            query="Science Fiction",
            vector=example_vector,
            target_vector="description_vector",
            alpha=0.5,
            limit=2,
        )
        print("\nHybrid search results:")
        for obj in hybrid_results["objects"]:
            print(f"- {obj['properties']['title']} ({obj['properties']['genre']})")

        # 6. Cleanup
        # ----------
        print("\n6. Cleaning up...")
        await weaviate_service.collections.delete("Movie")
        print("Collection deleted")

    finally:
        # Disconnect from server
        await server.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
