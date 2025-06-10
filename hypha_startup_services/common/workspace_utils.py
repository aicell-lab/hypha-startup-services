from typing import Any


def ws_from_context(context: dict[str, Any]) -> str:
    """Get workspace ID from context."""
    user_ws = context["user"]["scope"]["current_workspace"]
    return user_ws
