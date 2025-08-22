"""Weaviate service constants."""

from hypha_startup_services.common.constants import (
    ADMIN_WORKSPACES,
    ARTIFACT_DELIMITER,
    COLLECTION_DELIMITER,
    SHARED_WORKSPACE,
)
from hypha_startup_services.common.constants import (
    DEFAULT_WEAVIATE_SERVICE_ID as DEFAULT_SERVICE_ID,
)

# Re-export for backward compatibility
__all__ = [
    "ADMIN_WORKSPACES",
    "ARTIFACT_DELIMITER",
    "COLLECTION_DELIMITER",
    "DEFAULT_SERVICE_ID",
    "SHARED_WORKSPACE",
]
