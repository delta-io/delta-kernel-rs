#!/usr/bin/env python3
"""Attribute each lifecycle phase in a fresh DHAT worker with an externally prepared table."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from run_rss import invoke


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-binary", type=Path, required=True)
    parser.add_argument("--heap-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--width", type=int, default=10000)
    parser.add_argument("--tail", type=int, default=10000)
    parser.add_argument("--initial", type=int, default=1000)
    parser.add_argument("--cap", type=int, default=256)
    parser.add_argument("--mode", choices=["imperative", "declarative"], default="imperative")
    parser.add_argument("--stats-bytes", type=int, default=0)
    parser.add_argument("--stats-columns", type=int, default=0)
    parser.add_argument("--parsed-stats", action="store_true")
    parser.add_argument("--profile-from-stage", default="")
    parser.add_argument("--phases", nargs="+", default=["snapshot", "scan_consumed", "complete"])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    fixture = invoke(args.prepare_binary, "prepare", args.width, args.tail, args.initial,
                     args.stats_bytes, args.stats_columns, int(args.parsed_stats))
    source = Path(fixture["path"])
    try:
        for phase in args.phases:
            with tempfile.TemporaryDirectory(prefix="kernel-heap-worker-") as directory:
                table = Path(directory) / "table"
                shutil.copytree(source, table)
                profile = (args.output / f"{phase}.dhat.json").resolve()
                env = dict(os.environ, KERNEL_MEMORY_TABLE_ROOT=table.as_uri() + "/",
                           KERNEL_MEMORY_PROFILE=str(profile), KERNEL_MEMORY_GLOBAL="1",
                           KERNEL_MEMORY_STOP_AFTER=phase, KERNEL_MEMORY_MODE=args.mode,
                           KERNEL_MEMORY_PROFILE_FROM_STAGE=args.profile_from_stage)
                result = subprocess.run([str(args.heap_binary), str(args.width), str(args.tail),
                                         str(args.initial), str(args.cap)],
                                        env=env, capture_output=True, text=True)
                receipt = json.loads(result.stdout)
                receipt["fixture_details"] = fixture
                (args.output / f"{phase}-lifecycle.json").write_text(json.dumps(receipt, indent=2) + "\n")
                if result.returncode:
                    raise RuntimeError(result.stderr[-8000:])
                attribution = subprocess.run(
                    ["python3", str(Path(__file__).with_name("summarize_heap.py")), str(profile)],
                    check=True, text=True, capture_output=True)
                (args.output / f"{phase}-heap.json").write_text(attribution.stdout)
                summary = json.loads(attribution.stdout)
                print(json.dumps({"phase": phase, "rust_peak": receipt["rust_heap_peak"],
                                  "controlled_fraction": summary["kernel_fraction_at_global_peak"]}),
                      flush=True)
    finally:
        shutil.rmtree(source)


if __name__ == "__main__":
    main()
