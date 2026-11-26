"""Omnigent entrypoint for the complete workflow-owned review path manifest."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from omnigent_client import tool


def _implementation():
    path = Path(__file__).resolve().parents[2] / "lib" / "diff_coverage.py"
    spec = importlib.util.spec_from_file_location("diff_coverage_impl", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load diff coverage")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@tool
def list_changed_paths(offset: int = 0, max_entries: int = 500) -> str:
    """Read the complete scoped path list in bounded pages.

    Args:
        offset: Start at zero, then use each response's next_offset until it is null.
        max_entries: Maximum paths in this page, from 1 through 1000.
    """
    return _implementation().list_changed_paths(offset, max_entries)
