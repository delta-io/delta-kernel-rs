"""Temporary kibibytes-to-bytes conversion fixture."""


def kibibytes_to_bytes(value: int) -> int:
    """Convert a non-negative kibibytes-to-bytes count."""
    if value < 0:
        raise ValueError("value must be non-negative")
    return value * 1024
