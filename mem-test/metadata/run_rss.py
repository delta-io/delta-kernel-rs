#!/usr/bin/env python3
"""Prepare tables outside measured workers and sweep a shared cap and concurrency."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def invoke(binary, *args):
    result = subprocess.run([str(binary), *map(str, args)], text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(f"worker exited {result.returncode}: {result.stderr[-8000:]}")
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--width", type=int, default=10000)
    parser.add_argument("--tail", type=int, default=10000)
    parser.add_argument("--initial", type=int, default=1000)
    parser.add_argument("--stats-bytes", type=int, default=0)
    parser.add_argument("--stats-columns", type=int, default=0)
    parser.add_argument("--parsed-stats", action="store_true")
    parser.add_argument("--mode", choices=["imperative", "declarative"], default="imperative")
    parser.add_argument("--caps", type=int, nargs="+", default=[0, 128, 64, 32])
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1])
    parser.add_argument("--warmup", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.environ["KERNEL_MEMORY_MODE"] = args.mode
    fixture = invoke(args.binary, "prepare", args.width, args.tail, args.initial, args.stats_bytes,
                     args.stats_columns, int(args.parsed_stats))
    source = Path(fixture["path"])
    warmup = invoke(args.binary, "prepare", 10, 10, 10, args.stats_bytes,
                    args.stats_columns, int(args.parsed_stats)) if args.warmup else None
    report = {"fixture": fixture, "runs": []}
    try:
        for concurrency in args.concurrency:
            for cap in args.caps:
                with tempfile.TemporaryDirectory(prefix="kernel-rss-workers-") as directory:
                    root = Path(directory)
                    for worker in range(concurrency):
                        shutil.copytree(source, root / f"table-{worker}")
                    if warmup:
                        shutil.copytree(warmup["path"], root / "table-warmup")
                    run = invoke(args.binary, "worker", root, cap * 1024 * 1024, concurrency)
                    published = [
                        (root / f"table-{worker}" / "_delta_log" /
                         f"{args.tail + 2:020}.json").exists()
                        for worker in range(concurrency)
                    ]
                    run["published_next_commit"] = published
                    for outcome, committed in zip(run["results"], published):
                        if "Err" in outcome and committed:
                            raise AssertionError("failed operation published a commit")
                    if args.warmup and run["after_cleanup"]["kernel_live"] != run["baseline"]["kernel_live"]:
                        raise AssertionError("participating allocations did not return to the warmed baseline")
                    if cap and run["after_cleanup"]["kernel_peak"] > cap * 1024 * 1024:
                        raise AssertionError("shared cap exceeded")
                    report["runs"].append(run)
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    args.output.write_text(json.dumps(report, indent=2) + "\n")
                    clean = run["after_cleanup"]
                    print(json.dumps({
                        "concurrency": concurrency, "cap_mib": cap,
                        "rss_peak_mib": round(clean["os_peak_rss_bytes"] / 2**20, 2),
                        "rust_peak_mib": round(clean["rust_requested_peak"] / 2**20, 2),
                        "kernel_peak_mib": round(clean["kernel_peak"] / 2**20, 2),
                        "results": run["results"],
                    }), flush=True)
    finally:
        shutil.rmtree(source)
        if warmup:
            shutil.rmtree(warmup["path"])


if __name__ == "__main__":
    main()
