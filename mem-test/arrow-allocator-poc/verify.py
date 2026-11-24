#!/usr/bin/env python3
"""Check global allocation ownership and survey fallible versus infallible failure paths."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shutil
import subprocess

CHECKS = [
    "buffer_growth_and_release", "buffer_shrink", "imported_vector",
    "allocator_vector_release", "ffi_array_release",
    "json_decoder_initialization", "json_tape_growth", "json_flush", "json_serialize",
]
SHAPES = ["string", "primitive", "boolean", "nested", "list", "map"]
PHASES = ["initialize", "decode", "serialize", "flush"]


def invoke(binary, arguments):
    result = subprocess.run([str(binary), *arguments], capture_output=True, text=True, timeout=30)
    if result.returncode:
        stderr = result.stderr
        if len(stderr) > 8000:
            stderr = stderr[:6000] + "\n... stderr truncated ...\n" + stderr[-2000:]
        return {"ok": False, "arguments": arguments, "exit": result.returncode,
                "stderr": stderr}
    return json.loads(result.stdout)


def check_sequence(binary, phase, shape):
    receipts = []
    for ordinal in range(1, 4097):
        result = invoke(binary, [phase, shape, str(ordinal)])
        receipts.append(result)
        if not result["ok"] or not result["injected"]:
            return receipts
    receipts.append({"ok": False, "phase": phase, "shape": shape,
                     "error": "allocation sequence exceeded probe bound"})
    return receipts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--record-infallible-gaps", action="store_true",
                        help="Record allocation aborts separately instead of failing the survey")
    args = parser.parse_args()
    directory = args.binaries.resolve()
    receipts = [invoke(directory / "allocation_checks", [case]) for case in CHECKS]
    fixture = invoke(directory / "rss_lifecycle", ["prepare-json", json.dumps({
        "width": 128, "initial_files": 128, "tail_commits": 0,
        "stats_columns": -1, "stats_fields": 32,
    })])
    if fixture.get("ok") is False:
        receipts.append(fixture)
    else:
        source = Path(fixture["path"])
        try:
            pairs = [(phase, shape) for phase in PHASES for shape in SHAPES]
            pairs.append(("lifecycle", source.as_uri() + "/"))
            with ThreadPoolExecutor(max_workers=4) as pool:
                sequences = pool.map(
                    lambda pair: check_sequence(directory / "fault_probe", *pair), pairs,
                )
                for sequence in sequences:
                    receipts.extend(sequence)
        finally:
            shutil.rmtree(source)
    gaps = [receipt for receipt in receipts if not receipt["ok"]
            and receipt.get("exit") in [-6, 134]
            and "memory allocation" in receipt.get("stderr", "")]
    failures = [receipt for receipt in receipts if not receipt["ok"]
                and (not args.record_infallible_gaps or receipt not in gaps)]
    report = {"checks": len(receipts), "injected_errors": sum(
        receipt.get("injected", False) for receipt in receipts
    ), "failures": failures, "recoverability_gaps": gaps, "receipts": receipts}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ["checks", "injected_errors", "failures", "recoverability_gaps"]}))
    raise SystemExit(bool(failures))


if __name__ == "__main__":
    main()
