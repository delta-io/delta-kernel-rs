#!/usr/bin/env python3
"""Apply the Arrow 60 allocator experiment to disposable copies of cached Cargo sources."""

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess

CRATES = ["arrow-buffer", "arrow-data", "arrow-array", "arrow-cast", "arrow-json", "arrow-select", "parquet"]
VERSION = "60.0.0"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--registry", type=Path,
                        default=Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")) / "registry/src")
    args = parser.parse_args()
    directory = args.output.resolve()
    if directory.exists():
        parser.error("output already exists; choose a new disposable directory")
    sources = {}
    for crate in CRATES:
        candidates = sorted(args.registry.glob(f"*/{crate}-{VERSION}"))
        if not candidates:
            parser.error(f"{crate} {VERSION} is absent from the local Cargo source cache")
        sources[crate] = candidates[0]
    source_root = directory / "sources"
    for crate, source in sources.items():
        shutil.copytree(source, source_root / crate)
    here = Path(__file__).resolve().parent
    subprocess.run(["patch", "--batch", "-p1", "-i", str(here / "arrow-60.patch")],
                   cwd=source_root, check=True)
    config = directory / "patch.toml"
    config.write_text("[patch.crates-io]\n" + "\n".join(
        f"{crate} = {{ path = {json.dumps(str(source_root / crate))} }}" for crate in CRATES
    ) + "\n")
    lock = here / "Cargo.lock"
    if not lock.exists():
        shutil.copyfile(here.parents[1] / "Cargo.lock", lock)
    command = ["cargo", "+1.95.0", "build", "--offline", "--manifest-path",
               str(here / "Cargo.toml"), "--config", str(config), "--release"]
    print(f"CARGO_TARGET_DIR={shlex.quote(str(directory / 'target'))} "
          f"CARGO_PROFILE_RELEASE_DEBUG=1 {shlex.join(command)}")


if __name__ == "__main__":
    main()
