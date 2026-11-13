"""Run guide tests with the dependency directories reported by Cargo.

mdbook test needs to know where Cargo built the guide's dependencies. A fixed
target/debug/deps path does not work with Rust 1.100, which puts libraries in
separate directories such as target/debug/build/delta_kernel/<hash>/out/.

This script reads Cargo's JSON build output, finds those directories, and passes
them to mdbook test, which compiles and tests the guide's Rust examples.
"""

import json
from pathlib import Path
import subprocess
import sys


with open(sys.argv[1], encoding="utf-8") as messages:
    artifacts = (json.loads(line) for line in messages)
    library_paths = sorted(
        {
            str(Path(filename).parent)
            for artifact in artifacts
            if artifact["reason"] == "compiler-artifact"
            for filename in artifact["filenames"]
            if Path(filename).suffix in {".rlib", ".rmeta", ".so", ".dylib", ".dll"}
        }
    )

if not library_paths:
    sys.exit(f"no library artifacts found in {sys.argv[1]}")

subprocess.run(
    ["mdbook", "test", *[arg for path in library_paths for arg in ("-L", path)]],
    check=True,
)
