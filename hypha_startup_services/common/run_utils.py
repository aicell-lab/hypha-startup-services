def validate_run_id(run_id: str) -> None:
    """Raise ValueError if run_id is not a valid string (non-empty, no spaces, no slashes, not too long)."""
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Run ID must be a non-empty string.")
    if len(run_id) > 128:
        raise ValueError("Run ID is too long.")
    if any(c in run_id for c in ["/", " "]):
        raise ValueError("Run ID cannot contain slashes or spaces.")
