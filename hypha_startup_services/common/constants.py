"""Common constants for hypha startup services."""

COLLECTION_DELIMITER = "__DELIM__"
ARTIFACT_DELIMITER = ":"
SHARED_WORKSPACE = "SHARED"
ADMIN_WORKSPACES = ["ws-user-google-oauth2|104255278140940970953", "hypha-agents"]
DEFAULT_LOCAL_HOST = "localhost"
DEFAULT_LOCAL_PORT = 9527
DEFAULT_LOCAL_EXISTING_HOST = "http://127.0.0.1"
DEFAULT_REMOTE_URL = "https://hypha.aicell.io"
ARTIFACT_MANAGER_SERVICE_ID = "public/artifact-manager"

# Service-specific defaults (matching start-services.sh)
DEFAULT_WEAVIATE_SERVICE_ID = "weaviate-test"
DEFAULT_MEM0_SERVICE_ID = "mem0-test"
DEFAULT_MEM0_BIOIMAGE_SERVICE_ID = "mem0-bioimage-test"
DEFAULT_WEAVIATE_BIOIMAGE_SERVICE_ID = "weaviate-bioimage-test"
