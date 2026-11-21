"""Convert configured timeouts into milliseconds."""


def seconds_to_milliseconds(seconds: int) -> int:
    """Convert a non-negative whole number of seconds to milliseconds."""
    if seconds < 0:
        raise ValueError("timeout must be non-negative")
    return seconds * 100
