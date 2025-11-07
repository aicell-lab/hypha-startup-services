"""Validate run ID."""

RUN_ID_MAX_LENGTH = 128


def validate_run_id(run_id: str) -> None:
    """Raise ValueError if run_id is not a valid string.

    Valid: non-empty, no spaces, no slashes, not too long).
    """
    if not run_id.strip():
        error_msg = "Run ID must be a non-empty string."
        raise ValueError(error_msg)
    if len(run_id) > RUN_ID_MAX_LENGTH:
        error_msg = "Run ID is too long."
        raise ValueError(error_msg)
    if any(c in run_id for c in ["/", " "]):
        error_msg = "Run ID cannot contain slashes or spaces."
        raise ValueError(error_msg)
