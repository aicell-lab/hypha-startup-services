from dotenv import load_dotenv
import weaviate
from weaviate.classes.init import AdditionalConfig

load_dotenv()

WEAVIATE_URL = "https://hypha-weaviate.scilifelab-2-dev.sys.kth.se"
WEAVIATE_GRPC_URL = "https://hypha-weaviate-grpc.scilifelab-2-dev.sys.kth.se"
OLLAMA_ENDPOINT = "https://hypha-ollama.scilifelab-2-dev.sys.kth.se"

additional_config = AdditionalConfig(
    timeout_config=(5, 60),  # (connection_timeout_sec, request_timeout_sec)
    grpc_insecure=True,  # Skip SSL verification for gRPC
    grpc_insecure_auth=True,  # Allow insecure authentication
)

HTTP_HOST = WEAVIATE_URL.replace("https://", "").replace("http://", "")
GRPC_HOST = WEAVIATE_GRPC_URL.replace("https://", "").replace("http://", "")
is_secure = WEAVIATE_URL.startswith("https://")
is_grpc_secure = WEAVIATE_GRPC_URL.startswith("https://")

client = weaviate.connect_to_custom(
    http_host=HTTP_HOST,
    http_port=443 if is_secure else 80,
    http_secure=is_secure,
    grpc_host=GRPC_HOST,
    grpc_port=443 if is_grpc_secure else 50051,
    grpc_secure=is_grpc_secure,
    headers={},
    additional_config=additional_config,
    skip_init_checks=True,
)
