#!/usr/bin/env python3
"""
Compare test coverage between the current branch and a base branch (default: main).

Accepts one or more test filters. Both branches are built once (the slow step),
then each filter is run as a separate test invocation (fast) so you pay the
compilation cost only once regardless of how many filters you provide.

Usage:
    python3 scripts/coverage-compare.py <filter1> [filter2 ...]

    Each filter is passed to `cargo nextest run` as a test name substring/regex
    (same syntax as `cargo test <filter>`). If no filters are passed, all tests are run.

Environment:
    COVERAGE_BASE_BRANCH  -- branch to compare against (default: main)

Prerequisites:
    cargo install cargo-llvm-cov
    rustup component add llvm-tools-preview

Exit codes:
    0 -- no differences, or only newly-gained coverage (across all filters)
    1 -- coverage was lost for at least one filter
    2 -- script error (missing tool, git failure, etc.)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


# === Coverage collection ===

_CARGO_COMMON = [
    "cargo", "llvm-cov", "nextest",
    "--workspace", "--all-features",
    # Benchmarks unpack large tarballs into the source tree during their
    # build script; exclude them to avoid filling the worktree directory.
    "--exclude", "delta_kernel_benchmarks",
    # Without --no-clean, cargo-llvm-cov wipes instrumented build artefacts
    # before every run, forcing a full recompile each time.
    "--no-clean",
]


def make_env(target_dir: str) -> dict[str, str]:
    """Build an environment that redirects all cargo/rustc output to target_dir.

    rustc writes temporary files (e.g. symbols.o) to TMPDIR even when
    CARGO_TARGET_DIR is set, so both are redirected to keep everything off /tmp.
    """
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = target_dir
    tmp_dir = os.path.join(target_dir, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    env["TMPDIR"] = tmp_dir
    return env


def clean_coverage_data(target_dir: str) -> None:
    """Delete all profraw and profdata files before a test run.

    --no-clean tells cargo-llvm-cov to skip its own cleanup so the compiled
    binaries are preserved between filter runs. However, it also skips
    removing coverage data files, so we do it ourselves to ensure each filter
    gets a fresh, uncontaminated report.
    """
    for ext in ("*.profraw", "*.profdata"):
        for p in Path(target_dir).rglob(ext):
            p.unlink()


def run_one_filter(
    work_dir: str,
    output_file: str,
    filter_str: str,
    target_dir: str,
) -> bool:
    """Run the tests matching filter_str and write a coverage JSON to output_file.

    The instrumented binaries must already exist (call build_only first).
    Stale profraw files are removed before running so counts are not inflated
    by prior runs.

    Returns True on success, False if no tests matched the filter.
    """
    clean_coverage_data(target_dir)
    cmd = _CARGO_COMMON + ["--json", f"--output-path={output_file}", "--", filter_str]
    result = subprocess.run(cmd, cwd=work_dir, env=make_env(target_dir))
    if result.returncode == 0:
        return True
    # nextest exits 4 when no tests match the filter.
    if result.returncode == 4:
        print(f"  (no matching tests in {work_dir})", flush=True)
        return False
    print(
        f"cargo llvm-cov failed with exit code {result.returncode}.",
        file=sys.stderr,
    )
    sys.exit(2)


# === Coverage loading ===

# Each segment in the llvm-cov JSON is a 6-element list:
#   [line, col, count, has_count, is_region_entry, is_gap_region]
_SEG_LINE = 0
_SEG_COL = 1
_SEG_COUNT = 2
_SEG_HAS_COUNT = 3
_SEG_IS_GAP = 5

CoverageMap = dict[str, dict[tuple[int, int], int]]


def load_coverage(
    json_file: str,
    strip_prefix: str,
    skip_files: frozenset[str] = frozenset(),
    skip_prefixes: frozenset[str] = frozenset(),
) -> CoverageMap:
    """Parse an llvm-cov JSON export into a map of relative path -> segment counts.

    Only segments with has_count=True and is_gap_region=False are included.
    Files outside strip_prefix (e.g. stdlib, .cargo/registry) are skipped.
    Files whose relative path is in skip_files are also skipped; pass the set
    of files changed between branches to avoid false positives from line-number
    drift in modified files.
    Files whose relative path starts with any entry in skip_prefixes are also
    skipped; used to exclude proc-macro crates whose coverage is captured at
    compile time rather than test-run time.
    """
    with open(json_file) as f:
        data = json.load(f)

    prefix = strip_prefix.rstrip("/") + "/"
    coverage: CoverageMap = {}

    for entry in data.get("data", []):
        for file_data in entry.get("files", []):
            filename: str = file_data["filename"]
            if not filename.startswith(prefix):
                continue
            rel = filename[len(prefix):]
            if rel in skip_files:
                continue
            if any(rel.startswith(p) for p in skip_prefixes):
                continue
            segments: dict[tuple[int, int], int] = {}
            for seg in file_data.get("segments", []):
                if seg[_SEG_HAS_COUNT] and not seg[_SEG_IS_GAP]:
                    segments[(seg[_SEG_LINE], seg[_SEG_COL])] = seg[_SEG_COUNT]
            if segments:
                coverage[rel] = segments

    return coverage


# === Comparison ===

FilterResult = tuple[list[tuple[str, tuple[int, int], int, int]],   # lost
                     list[tuple[str, tuple[int, int], int, int]]]   # gained


def compare(base: CoverageMap, current: CoverageMap) -> FilterResult:
    """Return (lost, gained) segment lists.

    Only LOST (>0 -> 0) and GAINED (0 -> >0) segments are returned. Pure count
    changes in already-covered segments are ignored: execution counts are
    influenced by non-deterministic background work (thread pool startup, binary
    initialisation across nextest workers) and are not a reliable signal.
    """
    lost: list[tuple[str, tuple[int, int], int, int]] = []
    gained: list[tuple[str, tuple[int, int], int, int]] = []

    for rel_path in sorted(set(base) | set(current)):
        base_segs = base.get(rel_path, {})
        curr_segs = current.get(rel_path, {})
        for seg in sorted(set(base_segs) | set(curr_segs)):
            b = base_segs.get(seg, 0)
            c = curr_segs.get(seg, 0)
            if b > 0 and c == 0:
                lost.append((rel_path, seg, b, c))
            elif b == 0 and c > 0:
                gained.append((rel_path, seg, b, c))

    return lost, gained


# === Reporting ===

def print_filter_result(filter_str: str, lost: list, gained: list) -> None:
    print(f"\n{'='*70}")
    print(f"Filter: {filter_str!r}")
    print(f"{'='*70}")

    if not lost and not gained:
        print("  No coverage differences.")
        return

    # Group by file for readability.
    def print_group(entries: list, label: str) -> None:
        by_file: dict[str, list] = {}
        for path, seg, b, c in entries:
            by_file.setdefault(path, []).append((seg, b, c))
        for path in sorted(by_file):
            print(f"\n  {label} in {path}:")
            for (line, col), b, c in sorted(by_file[path]):
                print(f"    line {line}:{col:<3}  base={b:>8}  current={c:>8}")

    if lost:
        print_group(lost, "LOST")
    if gained:
        print_group(gained, "GAINED")

    print()
    print(f"  Lost: {len(lost)}  Gained: {len(gained)}")


def print_summary(results: dict[str, FilterResult]) -> None:
    print(f"\n{'='*70}")
    print("Summary")
    print(f"{'='*70}")

    any_lost = False
    for filter_str, (lost, gained) in results.items():
        status = "OK" if not lost else "LOST"
        print(f"  [{status:4}]  lost={len(lost):3}  gained={len(gained):3}  {filter_str!r}")
        if lost:
            any_lost = True

    print()
    if any_lost:
        print("FAIL: coverage was lost for one or more filters.")
    else:
        print("PASS: no coverage lost.")


# === Entry point ===

def check_prerequisites() -> None:
    if subprocess.run(["cargo", "llvm-cov", "--version"], capture_output=True).returncode != 0:
        print(
            "cargo-llvm-cov is not installed. Install it with:\n"
            "  cargo install cargo-llvm-cov\n"
            "  rustup component add llvm-tools-preview",
            file=sys.stderr,
        )
        sys.exit(2)


def git_repo_root() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "--show-toplevel"], text=True
    ).strip()


# Proc-macro crates execute at compile time, not at test-run time. Their
# coverage is an artefact of the build phase and must not be compared against
# test-run coverage.
_PROC_MACRO_PREFIXES: frozenset[str] = frozenset([
    "derive-macros/src/",
    "ffi-proc-macros/src/",
])


def main() -> int:
    filters = sys.argv[1:]
    if not filters:
        filters = [""]

    base_branch = os.environ.get("COVERAGE_BASE_BRANCH", "main")

    check_prerequisites()
    repo_root = git_repo_root()

    branch_label = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True
    ).strip()

    # Files changed between the current branch and base. Comparing coverage at
    # line granularity is unreliable for these files because inserting or
    # removing lines shifts all subsequent line numbers, causing the same
    # (file, line) pair to refer to different code in each branch.
    changed_files = frozenset(subprocess.check_output(
        ["git", "diff", f"{base_branch}...HEAD", "--name-only"],
        cwd=repo_root, text=True,
    ).split())


    # All artefacts live under target/ to avoid filling /tmp. The worktree also
    # lives here because the benchmarks build script unpacks large tarballs into
    # the source tree.
    cov_dir = os.path.join(repo_root, "target", "cov-compare")
    os.makedirs(cov_dir, exist_ok=True)

    worktree_path = os.path.join(cov_dir, "worktree")
    current_target = os.path.join(cov_dir, "build-current")
    base_target = os.path.join(cov_dir, "build-base")

    # Remove stale worktree from a previous interrupted run, if any.
    if os.path.exists(worktree_path):
        subprocess.run(
            ["git", "worktree", "remove", "--force", worktree_path],
            cwd=repo_root, capture_output=True,
        )

    print(f"Setting up worktree for '{base_branch}' ...", flush=True)
    subprocess.run(
        ["git", "worktree", "add", "--quiet", worktree_path, base_branch],
        cwd=repo_root, check=True,
    )

    try:
        # Run all filters on the current branch first, then all on the base
        # branch. Each branch is compiled on the first filter invocation;
        # --no-clean keeps the instrumented artefacts so all subsequent
        # filters on the same branch skip recompilation.

        print(f"\nRunning {len(filters)} filter(s) on '{branch_label}' ...", flush=True)
        current_ok_map: dict[str, bool] = {}
        for i, filter_str in enumerate(filters, 1):
            print(f"  [{i}/{len(filters)}] {filter_str!r}", flush=True)
            current_json = os.path.join(cov_dir, f"current-{i}.json")
            current_ok_map[filter_str] = run_one_filter(
                repo_root, current_json, filter_str, current_target
            )

        print(f"\nRunning {len(filters)} filter(s) on '{base_branch}' ...", flush=True)
        base_ok_map: dict[str, bool] = {}
        for i, filter_str in enumerate(filters, 1):
            print(f"  [{i}/{len(filters)}] {filter_str!r}", flush=True)
            base_json = os.path.join(cov_dir, f"base-{i}.json")
            base_ok_map[filter_str] = run_one_filter(
                worktree_path, base_json, filter_str, base_target
            )

        results: dict[str, FilterResult] = {}
        for i, filter_str in enumerate(filters, 1):
            current_ok = current_ok_map[filter_str]
            base_ok = base_ok_map[filter_str]
            if not current_ok and not base_ok:
                results[filter_str] = ([], [])
                continue
            current_json = os.path.join(cov_dir, f"current-{i}.json")
            base_json = os.path.join(cov_dir, f"base-{i}.json")
            base_cov = load_coverage(base_json, worktree_path, changed_files, _PROC_MACRO_PREFIXES) if base_ok else {}
            current_cov = load_coverage(current_json, repo_root, changed_files, _PROC_MACRO_PREFIXES) if current_ok else {}
            results[filter_str] = compare(base_cov, current_cov)

    finally:
        subprocess.run(
            ["git", "worktree", "remove", "--force", worktree_path],
            cwd=repo_root, capture_output=True,
        )

    # === Report ===
    for filter_str, (lost, gained) in results.items():
        print_filter_result(filter_str, lost, gained)

    print_summary(results)

    any_lost = any(lost for lost, _ in results.values())
    return 1 if any_lost else 0


if __name__ == "__main__":
    sys.exit(main())
