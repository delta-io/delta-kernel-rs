#!/usr/bin/env python3
"""Check Dylint diagnostics against real Kernel APIs and local lookalikes."""

import collections
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def check_unformatted(command: list[str], env: dict[str, str]) -> bool:
    """Keep deliberately unformatted input outside the repository's formatter checks."""
    with tempfile.TemporaryDirectory(prefix="kernel-dylint-unformatted-") as directory:
        project = Path(directory)
        (project / "src").mkdir()
        (project / "Cargo.toml").write_text(
            '[package]\nname="unformatted_fixture"\nversion="0.0.0"\nedition="2024"\n[workspace]\n'
        )
        (project / "Cargo.lock").write_text(
            'version = 4\n[[package]]\nname = "unformatted_fixture"\nversion = "0.0.0"\n'
        )
        source = project / "src" / "lib.rs"
        before = "pub fn outer(x:i32)->i32{x}\nfn inner(x:i32)->i32{x}\npub fn inspect()->i32{outer(\ninner(\n1,\n),\n)}\n"
        source.write_text(before)
        probe = command.copy()
        probe[probe.index("--manifest-path") + 1] = str(project / "Cargo.toml")
        result = subprocess.run(probe, cwd=project, env=env, text=True, capture_output=True)
        warnings = []
        for line in result.stdout.splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("reason") == "compiler-message":
                code = (record["message"].get("code") or {}).get("code", "")
                if code.startswith("kernel_"):
                    warnings.append(code)
        if result.returncode or warnings or source.read_text() != before:
            print(result.stderr, file=sys.stderr)
            print(f"Unformatted/read-only formatter fixture failed: {warnings}", file=sys.stderr)
            return False
    return True


def main() -> int:
    lint_dir = Path(__file__).resolve().parent
    fixture = lint_dir / "tests" / "fixture"
    source = fixture / "src" / "lib.rs"
    locks = {path: path.read_bytes() for path in (lint_dir / "Cargo.lock", fixture / "Cargo.lock")}
    expected = collections.Counter()
    for line_number, line in enumerate(source.read_text().splitlines(), 1):
        if "// expect-next: " in line:
            expected[(line.split("// expect-next: ", 1)[1].strip(), line_number + 1)] += 1
        elif "// expect: " in line:
            # Rustfmt moves a block's trailing annotation into its first body line.
            target_line = line_number - 1 if line.lstrip().startswith("//") else line_number
            expected[(line.split("// expect: ", 1)[1].strip(), target_line)] += 1
    if not expected:
        print("The fixture must contain expected lint diagnostics", file=sys.stderr)
        return 1

    command = [
        "cargo",
        "dylint",
        "--path",
        str(lint_dir),
        "--manifest-path",
        str(fixture / "Cargo.toml"),
        "--fail-on-no-libraries",
        "--",
        "--locked",
        "--message-format=json",
    ]
    env = dict(os.environ)
    env.setdefault("CARGO_BUILD_JOBS", "4")
    result = subprocess.run(command, cwd=lint_dir, env=env, text=True, capture_output=True)
    changed_locks = [str(path) for path, before in locks.items() if path.read_bytes() != before]
    if changed_locks:
        print(f"Dylint modified locked dependencies: {changed_locks}", file=sys.stderr)
        return 1
    actual = collections.Counter()
    rendered = []
    for line in result.stdout.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("reason") != "compiler-message":
            continue
        message = record["message"]
        rendered.append(message.get("rendered") or message["message"])
        code = (message.get("code") or {}).get("code", "")
        if not code.startswith("kernel_"):
            continue
        if Path(record["target"]["src_path"]).resolve() != source:
            print(f"Unexpected diagnostic outside the fixture: {message}", file=sys.stderr)
            return 1
        for span in message["spans"]:
            if span["is_primary"]:
                actual[(code, span["line_start"])] += 1
    if result.returncode or actual != expected:
        print(result.stderr, file=sys.stderr)
        print("\n".join(rendered), file=sys.stderr)
        print(f"Missing diagnostics: {dict(expected - actual)}", file=sys.stderr)
        print(f"Unexpected diagnostics: {dict(actual - expected)}", file=sys.stderr)
        return 1
    if not check_unformatted(command, env):
        return 1
    print(f"Dylint fixtures passed: {sum(actual.values())} expected diagnostics; negatives clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
