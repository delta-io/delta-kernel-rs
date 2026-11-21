"""Temporary hours-to-seconds conversion fixture."""


def hours_to_seconds(value: int) -> int:
    """Convert a non-negative hours-to-seconds count."""
    if value < 0:
        raise ValueError("value must be non-negative")
    return value * 60
