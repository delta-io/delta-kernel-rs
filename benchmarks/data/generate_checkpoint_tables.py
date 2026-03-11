"""Generate two benchmark tables: checkpoint with vs without stats_parsed.

Layout (both tables):
  - Versions 0-9:   10 JSON commits, each with 10k add actions (100k adds total)
  - Version 10:     Checkpoint consolidating all 100k adds
  - Versions 11-20: 10 JSON commits, each with 10k NEW add actions (100k more)
  - Total at latest version: 200k add actions

Table 1: 10k_checkpoint_with_parsed_stats
  - Checkpoint includes add.stats_parsed (struct column)
  - Table config: delta.checkpoint.writeStatsAsStruct=true

Table 2: 10k_checkpoint_no_parsed_stats
  - Checkpoint has only add.stats (JSON string)
  - No writeStatsAsStruct config

Schema: id (long), version_tag (string), value (long)
Protocol: readerVersion=1, writerVersion=2
"""

import json
import os
import hashlib

import pyarrow as pa
import pyarrow.parquet as pq


PRE_CHECKPOINT_ADDS_PER_COMMIT = 10_000
POST_CHECKPOINT_ADDS_PER_COMMIT = 10_000
ROWS_PER_FILE = 100
PRE_CHECKPOINT_COMMITS = 10
POST_CHECKPOINT_COMMITS = 10
CHECKPOINT_VERSION = PRE_CHECKPOINT_COMMITS  # version 10

TABLE_SCHEMA_STRING = json.dumps({
    "type": "struct",
    "fields": [
        {"name": "id", "type": "long", "nullable": True, "metadata": {}},
        {"name": "version_tag", "type": "string", "nullable": True, "metadata": {}},
        {"name": "value", "type": "long", "nullable": True, "metadata": {}},
    ]
})


def make_add_action(file_idx, commit_version):
    """Create a single add action dict for a given file index."""
    min_id = file_idx * ROWS_PER_FILE
    max_id = min_id + ROWS_PER_FILE - 1
    path = f"part-{file_idx:05d}-{hashlib.md5(str(file_idx).encode()).hexdigest()[:16]}-c000.snappy.parquet"
    stats = {
        "numRecords": ROWS_PER_FILE,
        "minValues": {"id": min_id, "version_tag": f"v{commit_version}", "value": 0},
        "maxValues": {"id": max_id, "version_tag": f"v{commit_version}", "value": ROWS_PER_FILE - 1},
        "nullCount": {"id": 0, "version_tag": 0, "value": 0},
    }
    return {
        "add": {
            "path": path,
            "partitionValues": {},
            "size": 2200,
            "modificationTime": 1700000000000 + file_idx,
            "dataChange": True,
            "stats": json.dumps(stats),
        }
    }


def write_json_commit(log_dir, version, file_idx_start, adds_per_commit, include_stats_parsed):
    """Write a single JSON commit file with the given number of add actions.

    Returns the list of (file_idx, version, add_dict) tuples for checkpoint building.
    """
    commit_path = os.path.join(log_dir, f"{version:020d}.json")
    lines = []
    adds = []

    if version == 0:
        lines.append(json.dumps({
            "commitInfo": {
                "timestamp": 1700000000000,
                "operation": "WRITE",
                "operationParameters": {"mode": "Overwrite", "partitionBy": "[]"},
                "isolationLevel": "Serializable",
                "isBlindAppend": False,
            }
        }))
        config = {"delta.checkpointInterval": "10000000"}
        if include_stats_parsed:
            config["delta.checkpoint.writeStatsAsStruct"] = "true"
        lines.append(json.dumps({
            "metaData": {
                "id": "bench-checkpoint-table",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": TABLE_SCHEMA_STRING,
                "partitionColumns": [],
                "configuration": config,
                "createdTime": 1700000000000,
            }
        }))
        lines.append(json.dumps({
            "protocol": {"minReaderVersion": 1, "minWriterVersion": 2}
        }))
    else:
        lines.append(json.dumps({
            "commitInfo": {
                "timestamp": 1700000000000 + version,
                "operation": "WRITE",
                "operationParameters": {"mode": "Append"},
                "isolationLevel": "Serializable",
                "isBlindAppend": True,
            }
        }))

    for i in range(adds_per_commit):
        file_idx = file_idx_start + i
        add = make_add_action(file_idx, version)
        lines.append(json.dumps(add))
        adds.append((file_idx, version, add["add"]))

    with open(commit_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    return adds


def build_checkpoint_parquet(all_adds, include_stats_parsed):
    """Build checkpoint parquet table from all add actions."""
    num_rows = len(all_adds) + 2  # +2 for metadata and protocol rows

    txn_type = pa.struct([
        pa.field("appId", pa.string()),
        pa.field("version", pa.int64()),
        pa.field("lastUpdated", pa.int64()),
    ])
    txn_array = pa.array([None] * num_rows, type=txn_type)

    paths = []
    partition_values_items = []
    sizes = []
    mod_times = []
    data_changes = []
    stats_strings = []
    num_records_list = []
    min_id_list, min_version_tag_list, min_value_list = [], [], []
    max_id_list, max_version_tag_list, max_value_list = [], [], []
    null_id_list, null_version_tag_list, null_value_list = [], [], []

    for i in range(num_rows):
        if i < 2:
            paths.append(None)
            partition_values_items.append(None)
            sizes.append(None)
            mod_times.append(None)
            data_changes.append(None)
            stats_strings.append(None)
            num_records_list.append(None)
            min_id_list.append(None); min_version_tag_list.append(None); min_value_list.append(None)
            max_id_list.append(None); max_version_tag_list.append(None); max_value_list.append(None)
            null_id_list.append(None); null_version_tag_list.append(None); null_value_list.append(None)
        else:
            file_idx, commit_ver, add = all_adds[i - 2]
            paths.append(add["path"])
            partition_values_items.append([])
            sizes.append(add["size"])
            mod_times.append(add["modificationTime"])
            data_changes.append(add["dataChange"])
            stats_strings.append(add["stats"])

            stats = json.loads(add["stats"])
            num_records_list.append(stats["numRecords"])
            min_id_list.append(stats["minValues"]["id"])
            min_version_tag_list.append(stats["minValues"]["version_tag"])
            min_value_list.append(stats["minValues"]["value"])
            max_id_list.append(stats["maxValues"]["id"])
            max_version_tag_list.append(stats["maxValues"]["version_tag"])
            max_value_list.append(stats["maxValues"]["value"])
            null_id_list.append(stats["nullCount"]["id"])
            null_version_tag_list.append(stats["nullCount"]["version_tag"])
            null_value_list.append(stats["nullCount"]["value"])

    pv_map_type = pa.map_(pa.string(), pa.string())

    add_fields = [
        pa.field("path", pa.string()),
        pa.field("partitionValues", pv_map_type),
        pa.field("size", pa.int64()),
        pa.field("modificationTime", pa.int64()),
        pa.field("dataChange", pa.bool_()),
        pa.field("tags", pa.map_(pa.string(), pa.string())),
        pa.field("stats", pa.string()),
    ]

    add_arrays = [
        pa.array(paths, type=pa.string()),
        pa.array(partition_values_items, type=pv_map_type),
        pa.array(sizes, type=pa.int64()),
        pa.array(mod_times, type=pa.int64()),
        pa.array(data_changes, type=pa.bool_()),
        pa.array([None] * num_rows, type=pa.map_(pa.string(), pa.string())),
        pa.array(stats_strings, type=pa.string()),
    ]

    if include_stats_parsed:
        values_struct_type = pa.struct([
            pa.field("id", pa.int64()),
            pa.field("version_tag", pa.string()),
            pa.field("value", pa.int64()),
        ])
        null_count_struct_type = pa.struct([
            pa.field("id", pa.int64()),
            pa.field("version_tag", pa.int64()),
            pa.field("value", pa.int64()),
        ])
        stats_parsed_type = pa.struct([
            pa.field("numRecords", pa.int64()),
            pa.field("minValues", values_struct_type),
            pa.field("maxValues", values_struct_type),
            pa.field("nullCount", null_count_struct_type),
        ])

        stats_parsed_data = []
        for i in range(num_rows):
            if num_records_list[i] is None:
                stats_parsed_data.append(None)
            else:
                stats_parsed_data.append({
                    "numRecords": num_records_list[i],
                    "minValues": {"id": min_id_list[i], "version_tag": min_version_tag_list[i], "value": min_value_list[i]},
                    "maxValues": {"id": max_id_list[i], "version_tag": max_version_tag_list[i], "value": max_value_list[i]},
                    "nullCount": {"id": null_id_list[i], "version_tag": null_version_tag_list[i], "value": null_value_list[i]},
                })

        add_fields.append(pa.field("stats_parsed", stats_parsed_type))
        add_arrays.append(pa.array(stats_parsed_data, type=stats_parsed_type))

    add_array = pa.StructArray.from_arrays(
        add_arrays, fields=add_fields,
        mask=pa.array([i < 2 for i in range(num_rows)], type=pa.bool_()),
    )

    remove_type = pa.struct([
        pa.field("path", pa.string()),
        pa.field("deletionTimestamp", pa.int64()),
        pa.field("dataChange", pa.bool_()),
        pa.field("extendedFileMetadata", pa.bool_()),
        pa.field("partitionValues", pv_map_type),
        pa.field("size", pa.int64()),
    ])
    remove_array = pa.array([None] * num_rows, type=remove_type)

    metadata_type = pa.struct([
        pa.field("id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("description", pa.string()),
        pa.field("format", pa.struct([
            pa.field("provider", pa.string()),
            pa.field("options", pa.map_(pa.string(), pa.string())),
        ])),
        pa.field("schemaString", pa.string()),
        pa.field("partitionColumns", pa.list_(pa.string())),
        pa.field("configuration", pa.map_(pa.string(), pa.string())),
        pa.field("createdTime", pa.int64()),
    ])
    metadata_rows = [None] * num_rows
    config_entries = [("delta.checkpointInterval", "10000000")]
    if include_stats_parsed:
        config_entries.append(("delta.checkpoint.writeStatsAsStruct", "true"))
    metadata_rows[0] = {
        "id": "bench-checkpoint-table",
        "name": None,
        "description": None,
        "format": {"provider": "parquet", "options": []},
        "schemaString": TABLE_SCHEMA_STRING,
        "partitionColumns": [],
        "configuration": config_entries,
        "createdTime": 1700000000000,
    }
    metadata_array = pa.array(metadata_rows, type=metadata_type)

    protocol_type = pa.struct([
        pa.field("minReaderVersion", pa.int32()),
        pa.field("minWriterVersion", pa.int32()),
    ])
    protocol_rows = [None] * num_rows
    protocol_rows[1] = {"minReaderVersion": 1, "minWriterVersion": 2}
    protocol_array = pa.array(protocol_rows, type=protocol_type)

    table = pa.table({
        "txn": txn_array,
        "add": add_array,
        "remove": remove_array,
        "metaData": metadata_array,
        "protocol": protocol_array,
    })

    return table


def generate_table(base_dir, table_name, include_stats_parsed):
    """Generate a complete benchmark table."""
    table_dir = os.path.join(base_dir, table_name)
    delta_dir = os.path.join(table_dir, "delta")
    log_dir = os.path.join(delta_dir, "_delta_log")
    specs_dir = os.path.join(table_dir, "specs")

    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(specs_dir, exist_ok=True)

    # Write table_info.json
    pre_total = PRE_CHECKPOINT_COMMITS * PRE_CHECKPOINT_ADDS_PER_COMMIT
    post_total = POST_CHECKPOINT_COMMITS * POST_CHECKPOINT_ADDS_PER_COMMIT
    desc = (
        f"{PRE_CHECKPOINT_COMMITS} pre-ckpt commits ({PRE_CHECKPOINT_ADDS_PER_COMMIT} adds each) + "
        f"checkpoint at v{CHECKPOINT_VERSION} ({pre_total} adds) + "
        f"{POST_CHECKPOINT_COMMITS} post-ckpt commits ({POST_CHECKPOINT_ADDS_PER_COMMIT} adds each)"
    )
    if include_stats_parsed:
        desc += " (checkpoint includes stats_parsed)"
    else:
        desc += " (checkpoint without stats_parsed)"
    with open(os.path.join(table_dir, "table_info.json"), "w") as f:
        json.dump({"name": table_name, "description": desc}, f, indent=4)

    # Phase 1: Pre-checkpoint commits (versions 0 through CHECKPOINT_VERSION-1)
    checkpoint_adds = []
    file_idx = 0
    print(f"  Writing {PRE_CHECKPOINT_COMMITS} pre-checkpoint commits (versions 0-{CHECKPOINT_VERSION - 1}), {PRE_CHECKPOINT_ADDS_PER_COMMIT} adds each...")
    for version in range(PRE_CHECKPOINT_COMMITS):
        adds = write_json_commit(log_dir, version, file_idx, PRE_CHECKPOINT_ADDS_PER_COMMIT, include_stats_parsed)
        checkpoint_adds.extend(adds)
        file_idx += PRE_CHECKPOINT_ADDS_PER_COMMIT

    # Phase 2: Checkpoint at CHECKPOINT_VERSION (consolidates all pre-checkpoint adds)
    # Write empty commit JSON at checkpoint version (just commitInfo, no adds)
    checkpoint_commit_path = os.path.join(log_dir, f"{CHECKPOINT_VERSION:020d}.json")
    with open(checkpoint_commit_path, "w") as f:
        f.write(json.dumps({
            "commitInfo": {
                "timestamp": 1700000000000 + CHECKPOINT_VERSION,
                "operation": "MANUAL CHECKPOINT",
            }
        }) + "\n")

    print(f"  Building checkpoint at version {CHECKPOINT_VERSION} ({len(checkpoint_adds)} adds, stats_parsed={include_stats_parsed})...")
    checkpoint_table = build_checkpoint_parquet(checkpoint_adds, include_stats_parsed)
    checkpoint_path = os.path.join(log_dir, f"{CHECKPOINT_VERSION:020d}.checkpoint.parquet")
    pq.write_table(checkpoint_table, checkpoint_path)

    with open(os.path.join(log_dir, "_last_checkpoint"), "w") as f:
        json.dump({"version": CHECKPOINT_VERSION, "size": len(checkpoint_table)}, f)

    # Phase 3: Post-checkpoint commits (versions CHECKPOINT_VERSION+1 through end)
    print(f"  Writing {POST_CHECKPOINT_COMMITS} post-checkpoint commits (versions {CHECKPOINT_VERSION + 1}-{CHECKPOINT_VERSION + POST_CHECKPOINT_COMMITS}), {POST_CHECKPOINT_ADDS_PER_COMMIT} adds each...")
    for i in range(POST_CHECKPOINT_COMMITS):
        version = CHECKPOINT_VERSION + 1 + i
        write_json_commit(log_dir, version, file_idx, POST_CHECKPOINT_ADDS_PER_COMMIT, include_stats_parsed)
        file_idx += POST_CHECKPOINT_ADDS_PER_COMMIT

    total_versions = CHECKPOINT_VERSION + POST_CHECKPOINT_COMMITS + 1
    total_adds = pre_total + post_total
    print(f"  Total: {total_versions} versions, {total_adds} add actions")
    print(f"  Checkpoint at version {CHECKPOINT_VERSION}: {len(checkpoint_table)} rows")

    add_fields = [f.name for f in checkpoint_table.column("add").type]
    print(f"  Checkpoint add fields: {add_fields}")

    # Write benchmark specs
    for spec_name, spec in [
        ("read_no_predicate", {"type": "read", "include_stats": True}),
        ("read_highly_selective", {"type": "read", "predicate": "id = 100", "include_stats": True}),
        ("read_moderately_selective", {"type": "read", "predicate": "id < 2000000", "include_stats": True}),
        ("snapshot_latest", {"type": "snapshot_construction"}),
    ]:
        with open(os.path.join(specs_dir, f"{spec_name}.json"), "w") as f:
            json.dump(spec, f, indent=4)


if __name__ == "__main__":
    base_dir = os.path.join(os.path.dirname(__file__), "workloads", "benchmarks")

    print("Generating table WITHOUT stats_parsed...")
    generate_table(base_dir, "10k_checkpoint_no_parsed_stats", include_stats_parsed=False)
    print()

    print("Generating table WITH stats_parsed...")
    generate_table(base_dir, "10k_checkpoint_with_parsed_stats", include_stats_parsed=True)
    print()

    print("Done!")
