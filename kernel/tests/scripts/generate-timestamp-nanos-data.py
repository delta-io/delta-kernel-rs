#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "pyarrow==24.0.0",
#   "numpy==2.4.6",
# ]
# ///

"""
Generate timestamp nanos test table at kernel/tests/data/timestamp-nanos/.

This script writes a small Delta table with two columns,
one with the TimestampNanos type and one TimestampNanosNtz.
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(out_dir: Path) -> tuple[str, int]:
    ids = pa.array([0, 1, 2, 3], type=pa.int64())
    ts_nano_vals = pa.array(
        [0, 123, -123, None],
        type=pa.timestamp('ns', 'UTC'),
    )
    ts_nano_ntz_vals = pa.array(
        [0, 123, -123, None],
        type=pa.timestamp('ns'),
    )
    table = pa.table({"id": ids, "ts": ts_nano_vals, "ts_ntz": ts_nano_ntz_vals})

    file_name = "part-00000-263d5ce1-d8cb-40ab-877f-1b82e6af1810-c000.snappy.parquet"
    file_path = out_dir / file_name
    pq.write_table(table, file_path, compression="snappy")
    return file_name, file_path.stat().st_size


def write_log(log_dir: Path, file_name: str, file_size: int) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)

    table_id = str(uuid.uuid4())
    now_ms = int(time.time() * 1000)
    schema_string = json.dumps(
        {
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "long",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "ts",
                    "type": "timestamp_nanos",
                    "nullable": True,
                    "metadata": {},
                },
                {
                    "name": "ts_ntz",
                    "type": "timestamp_nanos_ntz",
                    "nullable": True,
                    "metadata": {},
                },
            ],
        }
    )

    stats = json.dumps(
        {
            "numRecords": 4,
            "minValues": {"id": 0, "ts": "1969-12-31T23:59:59.999999877Z", "ts_ntz": "1969-12-31 23:59:59.999999877"},
            "maxValues": {"id": 3, "ts": "1970-01-01T00:00:00.000000123Z", "ts_ntz": "1970-01-01 00:00:00.000000123"},
            "nullCount": {"id": 0, "ts": 1, "ts_ntz": 1},
        }
    )

    commit_info = {
        "commitInfo": {
            "timestamp": now_ms,
            "operation": "WRITE",
            "operationParameters": {"mode": "ErrorIfExists"},
            "engineInfo": "generate-timestamp-nanos-data.py",
            "clientVersion": "1.0.0",
            "operationMetrics": {
                "execution_time_ms": 0,
                "num_added_files": 1,
                "num_added_rows": 4,
                "num_partitions": 0,
                "num_removed_files": 0,
            },
        }
    }
    protocol = {
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["timestampNanos", "timestampNtz"],
            "writerFeatures": ["timestampNanos", "timestampNtz"],
        }
    }
    metadata = {
        "metaData": {
            "id": table_id,
            "name": None,
            "description": None,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string,
            "partitionColumns": [],
            "createdTime": now_ms,
            "configuration": {},
        }
    }
    add = {
        "add": {
            "path": file_name,
            "partitionValues": {},
            "size": file_size,
            "modificationTime": now_ms,
            "dataChange": True,
            "stats": stats,
            "tags": None,
            "baseRowId": None,
            "defaultRowCommitVersion": None,
            "clusteringProvider": None,
        }
    }

    commit_path = log_dir / "00000000000000000000.json"
    with commit_path.open("w") as f:
        for action in (commit_info, protocol, metadata, add):
            f.write(json.dumps(action))
            f.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    default_out = (
        Path(__file__).resolve().parent.parent / "data" / "timestamp-nanos"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=default_out,
        help=f"Directory to write the table into (default: {default_out})",
    )
    args = parser.parse_args()

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    log_dir = out_dir / "_delta_log"

    file_name, file_size = write_parquet(out_dir)
    write_log(log_dir, file_name, file_size)
    print(f"Wrote table to {out_dir}")


if __name__ == "__main__":
    main()
