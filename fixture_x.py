"""Temporary meters-to-centimeters conversion fixture."""


def meters_to_centimeters(value: int) -> int:
    """Convert a non-negative meters-to-centimeters count."""
    if value < 0:
        raise ValueError("value must be non-negative")
    return value * 1000
