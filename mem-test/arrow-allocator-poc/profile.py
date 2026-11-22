#!/usr/bin/env python3
"""Profile representative declarative lifecycles with a supplied pair of harness binaries."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

CASES = [
    ("wide_schema", "all_columns"), ("deep_schema", "nested_predicate"),
    ("dense_struct_stats", "both_stats"), ("many_actions", "range"),
    ("large_action_stats", "large_in"), ("long_tail", "wide_or"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    metadata = here.parent / "metadata"
    manifest = json.loads((metadata / "lifecycle_matrix.json").read_text())
    tables = {entry["name"]: entry["fixture"] for entry in manifest["tables"]}
    queries = {entry["name"]: entry["query"] for entry in manifest["queries"]}
    binaries = args.binaries.resolve()
    rows = []
    args.output.mkdir(parents=True, exist_ok=True)
    for table, query in CASES:
        output = args.output / f"{table}--{query}"
        subprocess.run([
            "python3", str(metadata / "run_heap.py"),
            "--prepare-binary", str(binaries / "rss_lifecycle"),
            "--heap-binary", str(binaries / "kernel_lifecycle"),
            "--output", str(output), "--mode", "declarative", "--cap", "0",
            "--fixture-options", json.dumps(tables[table]), "--query", json.dumps(queries[query]),
        ], check=True)
        row = json.loads((output / "complete-heap.json").read_text())
        rows.append({"table": table, "query": query, **row})
    report = {
        "binary_sha256": hashlib.sha256((binaries / "kernel_lifecycle").read_bytes()).hexdigest(),
        "profiles": rows,
    }
    (args.output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
