from hypha_startup_services.common.constants import (
    COLLECTION_DELIMITER,
    ARTIFACT_DELIMITER,
    SHARED_WORKSPACE,
    ADMIN_WORKSPACES,
    DEFAULT_WEAVIATE_SERVICE_ID as DEFAULT_SERVICE_ID,
)

# Re-export for backward compatibility
__all__ = [
    "COLLECTION_DELIMITER",
    "ARTIFACT_DELIMITER",
    "SHARED_WORKSPACE",
    "ADMIN_WORKSPACES",
    "DEFAULT_SERVICE_ID",
]
