"""Common utilities for Weaviate tests."""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypedDict

from hypha_rpc.rpc import RemoteException, RemoteService

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
APP_ID = "TestApp"
USER1_APP_ID = "User1App"
USER2_APP_ID = "User2App"
USER3_APP_ID = "User3App"
SHARED_APP_ID = "SharedApp"


@dataclass(frozen=True)
class StandardMovieInfo:
    """Standard movie information for tests."""

    title: str
    description: str
    genre: str
    year: int


class Genre(Enum):
    """Movie genres used in tests."""

    SCIENCE_FICTION = "Science Fiction"
    ACTION = "Action"
    CRIME = "Crime"
    DRAMA = "Drama"


class StandardMovie(Enum):
    """Standard movies used in tests."""

    INCEPTION = StandardMovieInfo(
        title="Inception",
        description=(
            "A thief who steals corporate secrets through dream-sharing technology"
        ),
        genre=Genre.SCIENCE_FICTION.value,
        year=2010,
    )
    THE_DARK_KNIGHT = StandardMovieInfo(
        title="The Dark Knight",
        description="Batman fights the menace known as the Joker",
        genre=Genre.ACTION.value,
        year=2008,
    )
    INTERSTELLAR = StandardMovieInfo(
        title="Interstellar",
        description=(
            "A team of explorers travel through a wormhole in"
            " space in an attempt to ensure humanity's survival"
        ),
        genre=Genre.SCIENCE_FICTION.value,
        year=2014,
    )
    THE_GODFATHER = StandardMovieInfo(
        title="The Godfather",
        description=(
            "The aging patriarch of an organized"
            " crime dynasty transfers control to his son"
        ),
        genre=Genre.CRIME.value,
        year=1972,
    )
    THE_MATRIX = StandardMovieInfo(
        title="The Matrix",
        description="A computer hacker learns about the true nature of reality",
        genre=Genre.SCIENCE_FICTION.value,
        year=1999,
    )
    AVATAR = StandardMovieInfo(
        title="Avatar",
        description=(
            "A paraplegic Marine dispatched to the moon Pandora on a unique mission"
        ),
        genre=Genre.SCIENCE_FICTION.value,
        year=2009,
    )
    PULP_FICTION = StandardMovieInfo(
        title="Pulp Fiction",
        description=(
            "The lives of two mob hitmen, a boxer, a gangster's wife, and"
            " a pair of diner bandits intertwine"
        ),
        genre=Genre.CRIME.value,
        year=1994,
    )
    STAR_WARS_A_NEW_HOPE = StandardMovieInfo(
        title="Star Wars: A New Hope",
        description="Luke Skywalker joins forces with a Jedi Knight",
        genre=Genre.SCIENCE_FICTION.value,
        year=1977,
    )
    STAR_WARS_THE_EMPIRE_STRIKES_BACK = StandardMovieInfo(
        title="Star Wars: The Empire Strikes Back",
        description="After the Rebels are overpowered by the Empire",
        genre=Genre.SCIENCE_FICTION.value,
        year=1980,
    )
    THE_SHAWSHANK_REDEMPTION = StandardMovieInfo(
        title="The Shawshank Redemption",
        description="Two imprisoned men bond over a number of years",
        genre=Genre.DRAMA.value,
        year=1994,
    )
    GOODFELLAS = StandardMovieInfo(
        title="Goodfellas",
        description="The story of Henry Hill and his life in the mob",
        genre=Genre.CRIME.value,
        year=1990,
    )
    ARRIVAL = StandardMovieInfo(
        title="Arrival",
        description=(
            "A linguist works with the military to communicate with alien lifeforms."
        ),
        genre=Genre.SCIENCE_FICTION.value,
        year=2016,
    )
    BLADE_RUNNER = StandardMovieInfo(
        title="Blade Runner",
        description="A blade runner must pursue and terminate four replicants.",
        genre=Genre.SCIENCE_FICTION.value,
        year=1982,
    )
    GRAVITY = StandardMovieInfo(
        title="Gravity",
        description="Two astronauts work together to survive after an accident.",
        genre=Genre.SCIENCE_FICTION.value,
        year=2013,
    )


class MovieInfo(TypedDict, total=False):
    """Structure for movies used in tests."""

    title: str
    description: str
    genre: str
    year: int
    uuid: str | None
    id: str | None
    vector: list[float] | dict[str, list[float]] | None


# Common test objects
MOVIE_COLLECTION_CONFIG: dict[str, object] = {
    "class": "Movie",
    "description": "A movie class",
    "multiTenancyConfig": {
        "enabled": True,
    },
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
        {
            "name": "application_id",
            "dataType": ["text"],
            "description": "The ID of the application",
        },
    ],
}


# Common test helpers
async def create_test_collection(weaviate_service: RemoteService) -> dict[str, Any]:
    """Create a test collection for Weaviate tests."""
    ollama_endpoint = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se"
    ollama_model = (
        "mxbai-embed-large:latest"  # For embeddings - using an available model
    )

    # Try to delete if it exists - ignore errors
    try:
        await weaviate_service.collections.delete("Movie")
    except RemoteException:
        logger.exception("Error deleting collection")

    class_obj = MOVIE_COLLECTION_CONFIG.copy()
    # Add vector configurations
    class_obj["vectorConfig"] = {
        "title_vector": {
            "vectorizer": {
                "text2vec-ollama": {
                    "model": ollama_model,
                    "apiEndpoint": ollama_endpoint,
                },
            },
            "sourceProperties": ["title"],
            "vectorIndexType": "hnsw",
            "vectorIndexConfig": {"distance": "cosine"},
        },
        "description_vector": {
            "vectorizer": {
                "text2vec-ollama": {
                    "model": ollama_model,
                    "apiEndpoint": ollama_endpoint,
                },
            },
            "sourceProperties": ["description"],
            "vectorIndexType": "hnsw",
            "vectorIndexConfig": {"distance": "cosine"},
        },
    }
    class_obj["moduleConfig"] = {
        "generative-ollama": {
            "model": ollama_model,
            "apiEndpoint": ollama_endpoint,
        },
    }

    return await weaviate_service.collections.create(class_obj)


async def create_test_application(weaviate_service: RemoteService) -> None:
    """Create a test application for Weaviate tests."""
    await create_test_collection(weaviate_service)
    if await weaviate_service.applications.exists(
        collection_name="Movie",
        application_id=APP_ID,
    ):
        # If the application already exists, delete it first
        await weaviate_service.applications.delete(
            collection_name="Movie",
            application_id=APP_ID,
        )

    await weaviate_service.applications.create(
        application_id=APP_ID,
        collection_name="Movie",
        description="An application for movie data",
    )
