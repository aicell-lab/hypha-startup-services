from typing import Any


def ws_from_context(context: dict[str, Any]) -> str:
    """Get workspace ID from context."""
    user_ws = context["user"]["scope"]["current_workspace"]
    return user_ws


def validate_workspace(workspace: str) -> None:
    """Raise ValueError if workspace is not a valid non-empty string."""
    if not isinstance(workspace, str) or not workspace.strip():
        raise ValueError("Workspace must be a non-empty string.")
