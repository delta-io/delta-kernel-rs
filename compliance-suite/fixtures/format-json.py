#!/usr/bin/env python3
"""Format compliance fixture JSON files with compact inlining."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent


def _json_compact(obj, level=0, _indent=2, _threshold=100, _h_offset=0):
    pad = " " * (level * _indent)
    child = " " * ((level + 1) * _indent)
    flat = json.dumps(obj)
    if len(flat) <= _threshold - level * _indent - _h_offset:
        return flat
    if isinstance(obj, dict):
        pairs = ",\n".join(
            f'{child}{json.dumps(k)}: {_json_compact(v, level + 1, _indent, _threshold, _h_offset=len(json.dumps(k)) + 2)}'
            for k, v in obj.items()
        )
        return "{\n" + pairs + "\n" + pad + "}"
    if isinstance(obj, list):
        items = ",\n".join(
            f'{child}{_json_compact(v, level + 1, _indent, _threshold)}'
            for v in obj
        )
        return "[\n" + items + "\n" + pad + "]"
    return flat


def _format_file(path: pathlib.Path) -> None:
    data = json.loads(path.read_text())
    path.write_text(_json_compact(data) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Format fixture JSON files in-place.")
    parser.add_argument(
        "files",
        nargs="*",
        metavar="FILE",
        help="Fixture filename(s) relative to fixtures/ (or absolute paths). Omit to format all *.json.",
    )
    args = parser.parse_args()

    if args.files:
        paths = []
        for f in args.files:
            p = pathlib.Path(f)
            if not p.is_absolute():
                p = HERE / p
            paths.append(p)
    else:
        paths = sorted(HERE.glob("*.json"))

    if not paths:
        print("No fixture JSON files found.", file=sys.stderr)
        sys.exit(1)

    for path in paths:
        _format_file(path)
        print(f"formatted {path}")


if __name__ == "__main__":
    main()
