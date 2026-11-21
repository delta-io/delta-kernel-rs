"""Temporary minutes-to-seconds conversion fixture."""


def minutes_to_seconds(value: int) -> int:
    """Convert a non-negative minutes-to-seconds count."""
    if value < 0:
        raise ValueError("value must be non-negative")
    return value * 30
