"""Track fenced code while preserving Markdown line offsets."""

from __future__ import annotations

import re
from collections.abc import Iterator

_CODE_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")


def markdown_lines(text: str) -> Iterator[tuple[int, str, int | None]]:
    """Yield each line's offset, text, and open fence's offset, or None outside fences."""
    marker: str | None = None
    fence_start: int | None = None
    offset = 0
    for line in text.splitlines(keepends=True):
        fence = _CODE_FENCE.match(line.rstrip("\r\n"))
        if fence is not None:
            if marker is None:
                marker = fence[1]
                fence_start = offset
            elif fence[1][0] == marker[0] and len(fence[1]) >= len(marker) and not fence[2].strip():
                marker = None
                fence_start = None
        yield offset, line, fence_start
        offset += len(line)
