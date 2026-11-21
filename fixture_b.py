"""Temporary seconds-to-milliseconds conversion fixture."""


def seconds_to_milliseconds(value: int) -> int:
    """Convert a non-negative seconds-to-milliseconds count."""
    if value < 0:
        raise ValueError("value must be non-negative")
    return value * 100
