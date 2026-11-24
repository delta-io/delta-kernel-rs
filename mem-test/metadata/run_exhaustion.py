#!/usr/bin/env python3
"""Check shared limits before a workload and between declarative driver phases."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from run_rss import invoke

STAGES = [
    "declarative_snapshot_ready_mock_engine_included",
    "declarative_checkpoint_info_ready_mock_engine_included",
    "declarative_scan_state_built",
    "declarative_metadata_plan_built_no_execution",
    "declarative_metadata_plan_proto_live_no_execution",
    "declarative_metadata_plan_owned_wire_live",
    "declarative_metadata_plan_released",
    "declarative_transaction_relations_staged",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    fixture = invoke(args.binary, "prepare", 100, 100, 10, 0, -1, 1)
    source = Path(fixture["path"])
    receipts = []
    try:
        with tempfile.TemporaryDirectory(prefix="kernel-exhaustion-") as directory:
            root = Path(directory)
            shutil.copytree(source, root / "table-0")
            shutil.copytree(source, root / "table-warmup")
            env = dict(os.environ, KERNEL_MEMORY_MODE="declarative", RUST_BACKTRACE="1")

            def run(cap, stage="", headroom=0):
                result = subprocess.run(
                    [str(args.binary), "worker", str(root), str(cap), "1"],
                    env=dict(env, KERNEL_MEMORY_LIMIT_AFTER_STAGE=stage,
                             KERNEL_MEMORY_STAGE_HEADROOM=str(headroom)),
                    capture_output=True, text=True,
                )
                if result.returncode:
                    raise AssertionError(f"{stage=} {headroom=} {cap=}: {result.stderr}")
                receipt = json.loads(result.stdout)
                observations = [receipt["after_cleanup"], *receipt["stages"]]
                for observation in observations:
                    limit = observation["kernel_limit"]
                    assert limit is None or observation["kernel_live"] <= limit
                assert receipt["after_cleanup"]["kernel_live"] == receipt["baseline"]["kernel_live"]
                assert not (root / "table-0" / "_delta_log" / f"{fixture['tail_commits'] + 2:020}.json").exists()
                receipts.append({"initial_cap": cap, "limit_after_stage": stage,
                                 "headroom": headroom, "results": receipt["results"]})
                return receipt["baseline"]["kernel_live"]

            baseline = run(0)
            for headroom in [0, 1, 64, 512, 4096, 16384, 65536, 262144, 1048576, 2097152]:
                run(baseline + headroom)
            for stage in STAGES:
                for headroom in [0, 64, 4096, 65536]:
                    run(0, stage, headroom)
    finally:
        shutil.rmtree(source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipts, indent=2) + "\n")
    print(f"Passed {len(receipts)} fresh-process limit cases")


if __name__ == "__main__":
    main()
