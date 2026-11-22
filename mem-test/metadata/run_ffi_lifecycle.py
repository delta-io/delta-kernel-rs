#!/usr/bin/env python3
"""Build and exercise the limited allocator through the generated C ABI."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    root = Path(__file__).resolve().parents[2]
    target = Path(os.environ.get("KERNEL_MEMORY_TARGET", root / "target"))
    subprocess.run(
        ["cargo", "+1.100.0", "build", "--offline", "--locked", "-p", "delta_kernel_ffi",
         "--features", "declarative-plans,vendored-protoc", "--target-dir", str(target)],
        cwd=root, check=True,
    )
    executable = target / "ffi-limited-lifecycle"
    subprocess.run(
        [os.environ.get("CC", "cc"), "-std=c11", "-Wall", "-Wextra", "-Werror", "-pthread",
         "-I", str(root / "target" / "ffi-headers"), str(Path(__file__).with_name("ffi_lifecycle.c")),
         "-L", str(target / "debug"), "-ldelta_kernel_ffi",
         f"-Wl,-rpath,{target / 'debug'}", "-o", str(executable)],
        cwd=root, check=True,
    )
    with tempfile.TemporaryDirectory(prefix="kernel-ffi-budget-") as directory:
        table = Path(directory)
        log = table / "_delta_log"
        log.mkdir()
        schema = {"type": "struct", "fields": [
            {"name": f"col_{i}", "type": "long", "nullable": True,
             "metadata": {"comment": "x" * 8192} if i == 0 else {}}
            for i in range(100)
        ]}
        protocol = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
        metadata = {"metaData": {
            "id": "ffi-budget-fixture", "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps(schema), "partitionColumns": [],
            "configuration": {"delta.dataSkippingNumIndexedCols": "0"},
        }}
        (log / f"{0:020}.json").write_text(
            json.dumps(protocol) + "\n" + json.dumps(metadata) + "\n"
        )
        for version in range(1, 51):
            action = {"add": {
                "path": f"part-{version}.parquet", "partitionValues": {}, "size": 100,
                "modificationTime": 0, "dataChange": True, "stats": '{"numRecords":1}',
            }}
            (log / f"{version:020}.json").write_text(json.dumps(action) + "\n")
        subprocess.run([str(executable), table.as_uri() + "/"], cwd=root, check=True)
        before = sorted(log.glob("*.json"))
        subprocess.run([str(executable), table.as_uri() + "/", "0"], cwd=root, check=True)
        assert sorted(log.glob("*.json")) == before
        subprocess.run([str(executable), table.as_uri() + "/", str(64 * 1024 * 1024)],
                       cwd=root, check=True)
        assert len(list(log.glob("*.json"))) == len(before) + 2


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode)
