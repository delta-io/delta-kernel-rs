"""Reject tracked Rust sources that are disconnected from Cargo targets and modules.

Run with Python 3.11+ and the nightly rustfmt component installed. Cargo fmt discovers targets
and rustfmt traverses modules regardless of their cfg conditions; emitting to stdout leaves the
sources unchanged. Files compiled by external test harnesses need an explicit exception below.
"""

from pathlib import Path
import subprocess
import sys
import tomllib


# trybuild compiles these files independently; they intentionally fail to compile.
EXTERNAL_SOURCE_DIRECTORIES = {Path("ffi/tests/invalid-handle-code")}


def find_orphans(repository: Path) -> list[Path]:
    """Return tracked Rust paths without a target, module declaration, or harness exception.

    Discover workspaces from tracked Cargo manifests, including workspaces outside the root
    workspace. Propagate Cargo, rustfmt, and manifest errors so a failed scan cannot pass CI.
    """
    repository = repository.resolve()
    tracked = subprocess.run(
        ["git", "ls-files", "-z", "--", "*.rs", "*Cargo.toml"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    paths = {Path(path) for path in tracked.stdout.split("\0") if path}
    sources = {path for path in paths if path.suffix == ".rs"}
    manifests = sorted(path for path in paths if path.name == "Cargo.toml")
    reached = set()
    for manifest in manifests:
        with (repository / manifest).open("rb") as source:
            config = tomllib.load(source)
        if manifest != Path("Cargo.toml") and "workspace" not in config:
            continue
        result = subprocess.run(
            [
                "cargo", "+nightly", "fmt", "--all",
                "--manifest-path", str(repository / manifest),
                "--", "--emit", "stdout",
            ],
            cwd=repository,
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
        # With file inputs, rustfmt separates each source with an absolute "filename.rs:" header.
        emitted = {
            Path(line[:-1]).resolve()
            for line in result.stdout.splitlines()
            if line.endswith(".rs:") and Path(line[:-1]).is_absolute()
        }
        if not emitted:
            raise RuntimeError(f"rustfmt emitted no source file headers for {manifest}")
        reached.update(emitted)
    return sorted(
        path for path in sources
        if repository / path not in reached and path.parent not in EXTERNAL_SOURCE_DIRECTORIES
    )


def main() -> int:
    """Check this repository and return a nonzero status if the scan fails or finds orphans."""
    repository = Path(__file__).resolve().parents[2]
    try:
        orphans = find_orphans(repository)
    except (subprocess.CalledProcessError, RuntimeError) as error:
        print(f"Rust module check failed: {error}", file=sys.stderr)
        return 1
    if orphans:
        print("Unreferenced Rust source files:", file=sys.stderr)
        for path in orphans:
            print(f"  {path}", file=sys.stderr)
        print("Add the missing module declaration or Cargo target.", file=sys.stderr)
        return 1
    print("All tracked Rust sources are wired up or covered by an external harness exception.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
