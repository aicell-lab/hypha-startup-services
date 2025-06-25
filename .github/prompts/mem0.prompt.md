# Goal

Make a vector database using mem0,
with Weaviate as the backend storage and Hypha RPC as the out-facing API.
The first use case is to store and query the EuroBioImaging nodes and technologies data.

# Running tests
- Before running the tests, if the weaviate server is not running, use `docker compose up`:
```bash
docker compose -f docker/docker-compose.yaml up
```
- Then start the Hypha RPC service:
```bash
python -m hypha_startup_services [mem0, mem0-bioimage, weaviate, weaviate-bioimage] --local
```
- Finally, run the tests. mem0 tests are in tests/mem0_service,
    weaviate tests are in tests/weaviate_service.


# Coding Style
- Make short functions that do one thing and split them into smaller functions if they are too long.
- Start with simple functions with minimal error handling and logging, only expand if necessary.
- Write docstrings for all functions, classes, and modules.
- Use type hints for all functions and methods.
- Follow the PEP 8 style guide for Python code.
- Avoid code duplication
- "Magic values" should either be put into a config file or be put into constants with descriptive names
- Import statements should always be at the top of the file
- Always check and try to solve all linting problems



# General advice
- Run few mem0 integrations tests as they are quite slow
- Start with a specification, then write tests, and finally implement the code.
- Then use the tests to verify the implementation and update accordingly.

# Project Structure
- tests/mem0_service/ contains tests for the mem0 service
- hypha_startup_services/mem0_service/ contains the mem0 service
- - mem0_client has configuration for the mem0_client
- - methods are the methods for the mem0 service Hypha RPC API
- - register_service defines the Hypha RPC service API
- docker/docker-compose.yaml defines the Docker Compose setup for the Weaviate store
- examples/*.py contains example scripts to ingest BioImaging data into the Mem0
- examples/ebi-nodes.json contains the EuroBioImaging nodes data
- examples/ebi-tech.json contains the EuroBioImaging technologies data
- examples/ebi-*.py files contain example scripts to ingest and query the EuroBioImaging data

# Requirements
- The database should use Ollama
- The most important data is the EuroBioImaging nodes and technologies database
- The current mem0.search and mem0.add wrappers should work
- There should be a deterministic & exact function to search by technology ID:
- - this should return that technology and all nodes that list it
- There should be a deterministic & exact function to search by node ID:
- - this should return that node and all its listed technologies
- Mem0 methods are in agent-lib/main.py

# Documentation
- Weaviate documentation: https://weaviate.io/developers/weaviate
- - In particular:
1. [Docker compose](https://weaviate.io/developers/weaviate/installation/docker-compose)
- Hypha RPC documentation: https://docs.amun.ai/#/hypha-rpc
- - Also, its artifact manager: https://docs.amun.ai/#/artifact-manager
- Mem0 documentation: https://docs.mem0.ai/open-source/quickstart
- - In particular:
- 1. [Getting Started](https://docs.mem0.ai/open-source/python-quickstart)
- 2. [Ollama Configuration](https://docs.mem0.ai/examples/mem0-with-ollama)
- 3. [Weaviate Configuration](https://docs.mem0.ai/components/vectordbs/dbs/weaviate#weaviate)
- 4. Mem0 methods are in agent-lib/main.py