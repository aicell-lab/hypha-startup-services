"""Workspace Utils."""

from typing import Any


def ws_from_context(context: dict[str, Any]) -> str:
    """Get workspace ID from context."""
    return context["user"]["scope"]["current_workspace"]


def validate_workspace(workspace: str) -> None:
    """Raise ValueError if workspace is not a valid non-empty string."""
    if not isinstance(workspace, str) or not workspace.strip():
        error_msg = f"Invalid workspace: {workspace}"
        raise ValueError(error_msg)
