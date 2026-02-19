"""Common utilities for Weaviate tests."""

import asyncio
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import TypedDict

from hypha_rpc.rpc import RemoteException, RemoteService

from hypha_startup_services.weaviate_service.utils.models import CollectionConfig

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
APP_ID = "TestApp"
USER1_APP_ID = "User1App"
USER2_APP_ID = "User2App"
USER3_APP_ID = "User3App"
SHARED_APP_ID = "SharedApp"
EMBEDDING_ENV_VAR = "WEAVIATE_TEST_ENABLE_EMBEDDING"
COLLECTION_SETUP_RETRIES = 5
COLLECTION_SETUP_SLEEP_SECONDS = 1.0
TEST_COLLECTION_NAME = "Movie"
APPLICATION_SETUP_RETRIES = 3


def embedding_enabled() -> bool:
    """Return True when embedding-dependent tests/config are enabled."""
    value = os.getenv(EMBEDDING_ENV_VAR, "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


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


class MovieCollectionConfig(CollectionConfig):
    """Structure for the movie collection configuration."""

    # Additional specific fields could be added here if needed


# Common test objects
MOVIE_COLLECTION_CONFIG: MovieCollectionConfig = {
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
async def _wait_for_collection_state(
    weaviate_service: RemoteService,
    collection_name: str,
    *,
    should_exist: bool,
) -> None:
    """Wait until a collection reaches the expected existence state."""
    last_error: RemoteException | None = None
    for _ in range(COLLECTION_SETUP_RETRIES):
        try:
            collection_exists = await weaviate_service.collections.exists(
                collection_name,
            )
        except RemoteException as error:
            last_error = error
            await asyncio.sleep(COLLECTION_SETUP_SLEEP_SECONDS)
            continue

        if collection_exists == should_exist:
            return

        last_error = None
        await asyncio.sleep(COLLECTION_SETUP_SLEEP_SECONDS)

    if last_error is not None:
        raise last_error

    target_state = "exist" if should_exist else "be deleted"
    error_message = (
        f"Collection '{collection_name}' did not {target_state} within "
        f"{COLLECTION_SETUP_RETRIES} retries"
    )
    raise ValueError(error_message)


async def _delete_collection_for_test_setup(
    weaviate_service: RemoteService,
    collection_name: str,
) -> None:
    """Delete a test collection and wait until it is absent."""
    last_error: RemoteException | ValueError | None = None
    for _ in range(COLLECTION_SETUP_RETRIES):
        try:
            await weaviate_service.collections.delete(collection_name)
        except (RemoteException, ValueError) as error:
            last_error = error

        try:
            await _wait_for_collection_state(
                weaviate_service,
                collection_name,
                should_exist=False,
            )
        except (RemoteException, ValueError) as error:
            last_error = error
        else:
            return

    if last_error is not None:
        raise last_error


async def _wait_for_collection_artifact(
    weaviate_service: RemoteService,
    collection_name: str,
) -> None:
    """Wait until the collection artifact can be resolved."""
    last_error: RemoteException | ValueError | None = None
    for _ in range(COLLECTION_SETUP_RETRIES):
        try:
            await weaviate_service.collections.get_artifact(collection_name)
        except (RemoteException, ValueError) as error:
            last_error = error
            await asyncio.sleep(COLLECTION_SETUP_SLEEP_SECONDS)
            continue

        return

    if last_error is not None:
        raise last_error


async def _wait_for_collection_ready(
    weaviate_service: RemoteService,
    collection_name: str,
) -> None:
    """Wait until collection configuration is readable."""
    last_error: RemoteException | ValueError | None = None
    for _ in range(COLLECTION_SETUP_RETRIES):
        try:
            await weaviate_service.collections.get(collection_name)
        except (RemoteException, ValueError) as error:
            last_error = error
            await asyncio.sleep(COLLECTION_SETUP_SLEEP_SECONDS)
            continue

        return

    if last_error is not None:
        raise last_error


async def _wait_for_application_exists(
    weaviate_service: RemoteService,
    collection_name: str,
    application_id: str,
) -> None:
    """Wait until an application can be resolved as existing."""
    last_error: RemoteException | None = None
    for _ in range(COLLECTION_SETUP_RETRIES):
        try:
            application_exists = await weaviate_service.applications.exists(
                collection_name=collection_name,
                application_id=application_id,
            )
        except RemoteException as error:
            last_error = error
            await asyncio.sleep(COLLECTION_SETUP_SLEEP_SECONDS)
            continue

        if application_exists:
            return

        last_error = None
        await asyncio.sleep(COLLECTION_SETUP_SLEEP_SECONDS)

    if last_error is not None:
        raise last_error

    error_message = (
        f"Application '{application_id}' in collection "
        f"'{collection_name}' did not become available within "
        f"{COLLECTION_SETUP_RETRIES} retries"
    )
    raise ValueError(error_message)


def _is_collection_not_ready_error(error_message: str) -> bool:
    """Return True for known collection-readiness race errors."""
    return (
        "collection 'movie' does not exist" in error_message
        or "configuration could not be retrieved" in error_message
        or "unexpected status code: 404" in error_message
    )


async def create_test_collection(weaviate_service: RemoteService) -> CollectionConfig:
    """Create a test collection for Weaviate tests."""
    ollama_endpoint = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se"
    ollama_model = (
        "mxbai-embed-large:latest"  # For embeddings - using an available model
    )

    try:
        await _delete_collection_for_test_setup(
            weaviate_service,
            TEST_COLLECTION_NAME,
        )
    except (RemoteException, ValueError):
        logger.warning("Collection delete pre-cleanup failed")

    class_obj = MOVIE_COLLECTION_CONFIG.copy()
    if embedding_enabled():
        # Add vector configurations only when explicitly enabled.
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

    for _ in range(COLLECTION_SETUP_RETRIES):
        try:
            created_collection = await weaviate_service.collections.create(class_obj)
            await _wait_for_collection_ready(
                weaviate_service,
                TEST_COLLECTION_NAME,
            )
            await _wait_for_collection_artifact(
                weaviate_service,
                TEST_COLLECTION_NAME,
            )
        except (RemoteException, ValueError) as error:
            logger.warning("Collection create attempt failed: %s", error)
            try:
                await _wait_for_collection_state(
                    weaviate_service,
                    TEST_COLLECTION_NAME,
                    should_exist=True,
                )
                await _wait_for_collection_ready(
                    weaviate_service,
                    TEST_COLLECTION_NAME,
                )
                await _wait_for_collection_artifact(
                    weaviate_service,
                    TEST_COLLECTION_NAME,
                )
            except (RemoteException, ValueError):
                await _delete_collection_for_test_setup(
                    weaviate_service,
                    TEST_COLLECTION_NAME,
                )
                continue

            if await weaviate_service.collections.exists(TEST_COLLECTION_NAME):
                return class_obj

            raise
        else:
            return created_collection

    created_collection = await weaviate_service.collections.create(class_obj)
    await _wait_for_collection_ready(
        weaviate_service,
        TEST_COLLECTION_NAME,
    )
    await _wait_for_collection_artifact(
        weaviate_service,
        TEST_COLLECTION_NAME,
    )
    return created_collection


async def create_test_application(weaviate_service: RemoteService) -> None:
    """Create a test application for Weaviate tests."""
    for _ in range(APPLICATION_SETUP_RETRIES):
        await create_test_collection(weaviate_service)

        try:
            application_exists = await weaviate_service.applications.exists(
                collection_name=TEST_COLLECTION_NAME,
                application_id=APP_ID,
            )
        except RemoteException:
            application_exists = False

        if application_exists:
            try:
                await weaviate_service.applications.delete(
                    collection_name=TEST_COLLECTION_NAME,
                    application_id=APP_ID,
                )
            except RemoteException:
                logger.warning("Application delete pre-cleanup failed")

        try:
            await weaviate_service.applications.create(
                application_id=APP_ID,
                collection_name=TEST_COLLECTION_NAME,
                description="An application for movie data",
            )
        except RemoteException as error:
            error_message = str(error).lower()
            if _is_collection_not_ready_error(error_message):
                await asyncio.sleep(COLLECTION_SETUP_SLEEP_SECONDS)
                continue
            raise
        else:
            await _wait_for_application_exists(
                weaviate_service,
                TEST_COLLECTION_NAME,
                APP_ID,
            )
            return

    error_message = (
        f"Failed to create application '{APP_ID}' after "
        f"{APPLICATION_SETUP_RETRIES} retries"
    )
    raise ValueError(error_message)
