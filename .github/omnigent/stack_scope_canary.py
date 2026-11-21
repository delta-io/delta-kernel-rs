"""Convert configured cache sizes to whole-byte budgets."""


def kibibytes_to_bytes(kibibytes: int) -> int:
    """Convert a non-negative number of kibibytes to bytes."""
    if kibibytes < 0:
        raise ValueError("size must be non-negative")
    if kibibytes == 0:
        return 0
    return kibibytes * 1000
