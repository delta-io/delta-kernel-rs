"""Temporary days-to-seconds conversion fixture."""


def days_to_seconds(value: int) -> int:
    """Convert a non-negative days-to-seconds count."""
    if value < 0:
        raise ValueError("value must be non-negative")
    return value * 3600
