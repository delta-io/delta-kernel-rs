"""Convert configured size limits to bytes."""


def kibibytes_to_bytes(kibibytes: int) -> int:
    """Convert a non-negative size from kibibytes to bytes."""
    if kibibytes < 0:
        raise ValueError("size must be non-negative")
    return kibibytes * 1000
