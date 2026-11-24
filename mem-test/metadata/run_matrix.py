#!/usr/bin/env python3
"""Cross local table/query shapes and verify limits, cleanup, and recovery in fresh processes."""

import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

from run_rss import invoke

STAGES = [
    "declarative_snapshot_ready_mock_engine_included",
    "declarative_query_built",
    "declarative_scan_state_built",
    "declarative_metadata_plan_built_no_execution",
    "declarative_metadata_plan_proto_live_no_execution",
    "declarative_transaction_relations_staged",
]


def run_worker(binary, root, cap, concurrency, repetitions, query, stage, timeout, no_headroom=False):
    env = dict(os.environ, KERNEL_MEMORY_MODE="declarative", KERNEL_MEMORY_RECOVERY="1",
               KERNEL_MEMORY_QUERY=json.dumps(query), KERNEL_MEMORY_LIMIT_AFTER_STAGE=stage,
               KERNEL_MEMORY_STAGE_HEADROOM="0", KERNEL_MEMORY_NO_HEADROOM="1" if no_headroom else "0",
               RUST_BACKTRACE="1")
    result = subprocess.run(
        [str(binary), "worker", str(root), str(cap), str(concurrency), str(repetitions)],
        env=env, text=True, capture_output=True, timeout=timeout,
    )
    if result.returncode:
        stderr = result.stderr
        if len(stderr) > 12000:
            stderr = stderr[:8000] + "\n... stderr truncated ...\n" + stderr[-4000:]
        raise RuntimeError(f"worker exit {result.returncode}: {stderr}")
    return json.loads(result.stdout)


def validate(run, fixture, query, require_success):
    baseline = run["baseline"]["kernel_live"]
    # Global tracking includes retained caller diagnostics and thread/harness allocations. Exact
    # owner release is tested in Rust and C; the per-container baseline assertion is inapplicable.
    if not run.get("global_allocator"):
        assert run["after_cleanup"]["kernel_live"] == baseline, "query allocations retained after cleanup"
    if run["concurrency"] == 1 and not run.get("global_allocator"):
        for stage in run["stages"]:
            if stage["name"] == "owners_dropped":
                assert stage["kernel_live"] == baseline, "iteration retained query allocations"
    for observation in [run["after_cleanup"], *run["stages"], *run["samples"].values()]:
        if not isinstance(observation, dict):
            continue
        limit = observation["kernel_limit"]
        assert limit is None or observation["kernel_live"] <= limit, "observed reservation exceeds cap"
    if run["cap_bytes"]:
        assert run["after_cleanup"]["kernel_peak"] <= max(
            run["baseline"]["kernel_peak"], run["cap_bytes"]
        ), "historical reservation peak exceeds the old peak and active cap"
    assert len(run["results"]) == run["concurrency"] * run["repetitions"]
    assert len(run["failures"]) == len(run["results"])
    assert len(run["operation_elapsed_us"]) == len(run["results"])
    for result, failure in zip(run["results"], run["failures"]):
        if "Err" in result:
            assert not require_success, result["Err"]
            assert failure["memory_error"], f"unexpected error: {result['Err']}"
            assert run["kernel_rejections"] > 0, "error without a rejected allocation"
        else:
            assert failure is None
            outcome = result["Ok"]
            assert outcome["snapshot_version"] == fixture["tail_commits"] + 1
            empty = query.get("filter") == "constant_false"
            assert (outcome["metadata_plan_nodes"] == 0) == empty
            assert (outcome["metadata_plan_proto_bytes"] == 0) == empty
            assert outcome["write_state_bytes"] > 0
            assert not outcome["declarative_commit_supported"]
    recovery = run["recovery"]
    assert "Ok" in recovery["result"], f"retry after clearing cap failed: {recovery['result']}"
    if not run.get("global_allocator"):
        assert recovery["after_cleanup"]["kernel_live"] == baseline, "retry retained query allocations"


def summarize(run):
    values = sorted(run["operation_elapsed_us"])
    def percentile(p):
        return values[max(0, math.ceil(p * len(values)) - 1)]
    clean = run["after_cleanup"]
    return {
        "successes": sum("Ok" in result for result in run["results"]),
        "allocation_errors": sum("Err" in result for result in run["results"]),
        "kernel_peak": clean["kernel_peak"], "rust_peak": clean["rust_requested_peak"],
        "rss_peak": clean["os_peak_rss_bytes"], "samples": len(values),
        "p50_us": percentile(.5), "p90_us": percentile(.9), "max_us": values[-1],
        "p99_9_us": percentile(.999) if len(values) >= 1000 else None,
    }


def write_report(output, report):
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    temporary.replace(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("lifecycle_matrix.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tables", nargs="+")
    parser.add_argument("--queries", nargs="+")
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 4])
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--failure-stages", nargs="*", choices=STAGES, default=[])
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--record-infallible-gaps", action="store_true",
                        help="Record allocation aborts in pressure probes separately; unlimited/fit runs must pass")
    args = parser.parse_args()
    if min([args.repetitions, *args.concurrency]) <= 0:
        parser.error("repetitions and concurrency must be positive")
    args.binary = args.binary.resolve()
    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    tables = [entry for entry in manifest["tables"] if not args.tables or entry["name"] in args.tables]
    queries = [entry for entry in manifest["queries"] if not args.queries or entry["name"] in args.queries]
    if not tables or not queries:
        parser.error("selection contains no tables or queries")
    report = {"manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
              "binary": str(args.binary), "fixtures": {}, "runs": [], "failures": [], "recoverability_gaps": [],
              "scope": "Declarative driver construction/encoding only; no terminal-plan execution or commit. Rust/RSS measurements include local reference-engine work and caller query construction. Global-allocator builds also track caller diagnostics, so exact per-container cleanup baselines do not apply. Allocation aborts in pressure probes are recoverability gaps, not successful error returns. A sampled ratio is not allocation-stack attribution."}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    receipts = args.output.with_suffix(".receipts.jsonl")
    receipts.write_text("")
    report["receipts"] = str(receipts)
    report["checked_operations"] = 0
    shapes = Counter()
    started = time.monotonic()
    for table in tables:
        source = warm_source = None
        try:
            fixture = invoke(args.binary, "prepare-json", json.dumps(table["fixture"]))
            source = Path(fixture["path"])
            warm_options = dict(table["fixture"], width=8, initial_files=8, tail_commits=2,
                                actions_per_commit=2, removes_per_commit=1)
            warm_options.pop("schema_bytes", None)
            warm = invoke(args.binary, "prepare-json", json.dumps(warm_options))
            warm_source = Path(warm["path"])
            report["fixtures"][table["name"]] = fixture
            for query in queries:
                if query.get("requires_partitioned") and not table["fixture"].get("partitioned"):
                    continue
                for concurrency in args.concurrency:
                    with tempfile.TemporaryDirectory(prefix="kernel-matrix-") as directory:
                        root = Path(directory)
                        # All operations are read-only planning, so workers can share a fixture.
                        for worker in range(concurrency):
                            (root / f"table-{worker}").symlink_to(source, target_is_directory=True)
                        (root / "table-warmup").symlink_to(warm_source, target_is_directory=True)
                        before = sorted(path.name for path in (source / "_delta_log").iterdir())
                        modes = [("unlimited", 0, "")]
                        reference = None
                        while modes:
                            mode, cap, stage = modes.pop(0)
                            label = {"table": table["name"], "query": query["name"],
                                     "concurrency": concurrency, "mode": mode}
                            try:
                                run = run_worker(args.binary, root, cap, concurrency,
                                                 1 if stage else args.repetitions,
                                                 query["query"], stage, args.timeout, mode == "no_headroom")
                                row = dict(label, cap_bytes=run["cap_bytes"], stage=stage,
                                           receipt_line=len(report["runs"]) + 1)
                                with receipts.open("a") as stream:
                                    stream.write(json.dumps(dict(row, receipt=run)) + "\n")
                                report["runs"].append(row)
                                report["checked_operations"] += len(run["results"])
                                validate(run, fixture, query["query"], mode in ["unlimited", "fit"])
                                assert sorted(path.name for path in (source / "_delta_log").iterdir()) == before, "planning mutated the log"
                                summary = summarize(run)
                                row["summary"] = summary
                                print(json.dumps(dict(label, **summary)), flush=True)
                                if mode == "unlimited":
                                    for result in run["results"]:
                                        shapes[json.dumps(result["Ok"]["plan_shape"], sort_keys=True)] += 1
                                    reference = run
                                    baseline = run["baseline"]["kernel_live"]
                                    peak = run["after_cleanup"]["kernel_peak"]
                                    modes.extend([
                                        ("fit", max(2 * concurrency * peak, baseline + 65536), ""),
                                        ("pressure", max(baseline + 65536, baseline + (peak - baseline) // 2) if run.get("global_allocator") else max(baseline, peak // 2), ""),
                                        ("no_headroom", baseline, ""),
                                    ])
                                    if concurrency == 1:
                                        reached = {stage["name"] for stage in run["stages"]}
                                        modes.extend(("after:" + stage, 0, stage)
                                                     for stage in args.failure_stages if stage in reached)
                                if mode == "no_headroom":
                                    assert summary["successes"] == 0, "workload succeeded without any allocation headroom"
                            except (AssertionError, RuntimeError, subprocess.TimeoutExpired) as error:
                                message = str(error)
                                gap = (args.record_infallible_gaps and mode not in ["unlimited", "fit"]
                                       and isinstance(error, RuntimeError)
                                       and message.startswith(("worker exit -6:", "worker exit 134:"))
                                       and "memory allocation" in message)
                                bucket = "recoverability_gaps" if gap else "failures"
                                report[bucket].append(dict(label, error=str(error)))
                                print(json.dumps(dict(label, **{bucket: str(error)})), flush=True)
                                if reference is None:
                                    break
                            write_report(args.output, report)
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            report["failures"].append({"table": table["name"], "fixture_error": str(error)})
            write_report(args.output, report)
        finally:
            if source is not None:
                shutil.rmtree(source)
            if warm_source is not None:
                shutil.rmtree(warm_source)
    report["distinct_plan_shapes"] = len(shapes)
    report["plan_shapes"] = [{"shape": json.loads(shape), "operations": count}
                             for shape, count in shapes.items()]
    report["elapsed_seconds"] = round(time.monotonic() - started, 3)
    write_report(args.output, report)
    print(json.dumps({key: report[key] for key in ["checked_operations", "distinct_plan_shapes", "elapsed_seconds"]}), flush=True)
    if report["failures"]:
        raise SystemExit(f"{len(report['failures'])} failed lifecycle checks; see {args.output}")


if __name__ == "__main__":
    main()
