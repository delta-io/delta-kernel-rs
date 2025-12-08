#!/usr/bin/env python3
"""
Script to create a Delta table with a checkpoint that has stats_parsed.
"""

import json
import os
import shutil
import pyarrow as pa
import pyarrow.parquet as pq

# Output directory
TABLE_DIR = "parsed_stats_table"
LOG_DIR = f"{TABLE_DIR}/_delta_log"

def create_table():
    """Create a simple Delta table with parsed stats in checkpoint."""

    # Clean up if exists
    if os.path.exists(TABLE_DIR):
        shutil.rmtree(TABLE_DIR)

    os.makedirs(LOG_DIR)

    # Create two data files
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("value", pa.int64()),
    ])

    # File 1: id 1-100, value 10-1000
    data1 = pa.table({
        "id": list(range(1, 101)),
        "value": list(range(10, 1010, 10)),
    }, schema=schema)
    pq.write_table(data1, f"{TABLE_DIR}/part-00000.parquet")

    # File 2: id 101-200, value 2000-3000
    data2 = pa.table({
        "id": list(range(101, 201)),
        "value": list(range(2000, 3000, 10)),
    }, schema=schema)
    pq.write_table(data2, f"{TABLE_DIR}/part-00001.parquet")

    # Create commit 0 with protocol and metadata
    commit_0 = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {
            "id": "test-table-id",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps({
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "long", "nullable": True, "metadata": {}},
                    {"name": "value", "type": "long", "nullable": True, "metadata": {}}
                ]
            }),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1700000000000
        }},
        {"add": {
            "path": "part-00000.parquet",
            "partitionValues": {},
            "size": 1000,
            "modificationTime": 1700000000000,
            "dataChange": True,
            "stats": json.dumps({
                "numRecords": 100,
                "minValues": {"id": 1, "value": 10},
                "maxValues": {"id": 100, "value": 1000},
                "nullCount": {"id": 0, "value": 0}
            })
        }},
        {"add": {
            "path": "part-00001.parquet",
            "partitionValues": {},
            "size": 1000,
            "modificationTime": 1700000000000,
            "dataChange": True,
            "stats": json.dumps({
                "numRecords": 100,
                "minValues": {"id": 101, "value": 2000},
                "maxValues": {"id": 200, "value": 3000},
                "nullCount": {"id": 0, "value": 0}
            })
        }}
    ]

    with open(f"{LOG_DIR}/00000000000000000000.json", "w") as f:
        for action in commit_0:
            f.write(json.dumps(action) + "\n")

    # Create checkpoint with stats_parsed
    create_checkpoint_with_parsed_stats()

    # Create _last_checkpoint file
    last_checkpoint = {
        "version": 0,
        "size": 4
    }
    with open(f"{LOG_DIR}/_last_checkpoint", "w") as f:
        f.write(json.dumps(last_checkpoint))

    print(f"Created table at {TABLE_DIR}")


def create_checkpoint_with_parsed_stats():
    """Create checkpoint parquet with stats_parsed column."""

    # Build the stats_parsed schema
    stats_parsed_type = pa.struct([
        pa.field("numRecords", pa.int64()),
        pa.field("minValues", pa.struct([
            pa.field("id", pa.int64()),
            pa.field("value", pa.int64()),
        ])),
        pa.field("maxValues", pa.struct([
            pa.field("id", pa.int64()),
            pa.field("value", pa.int64()),
        ])),
        pa.field("nullCount", pa.struct([
            pa.field("id", pa.int64()),
            pa.field("value", pa.int64()),
        ])),
    ])

    # Add action schema with stats_parsed
    add_type = pa.struct([
        pa.field("path", pa.string()),
        pa.field("partitionValues", pa.map_(pa.string(), pa.string())),
        pa.field("size", pa.int64()),
        pa.field("modificationTime", pa.int64()),
        pa.field("dataChange", pa.bool_()),
        pa.field("stats", pa.string()),  # Keep JSON stats for compatibility
        pa.field("stats_parsed", stats_parsed_type),  # Add parsed stats
    ])

    # Protocol action
    protocol_type = pa.struct([
        pa.field("minReaderVersion", pa.int32()),
        pa.field("minWriterVersion", pa.int32()),
    ])

    # MetaData action
    format_type = pa.struct([
        pa.field("provider", pa.string()),
        pa.field("options", pa.map_(pa.string(), pa.string())),
    ])
    metadata_type = pa.struct([
        pa.field("id", pa.string()),
        pa.field("format", format_type),
        pa.field("schemaString", pa.string()),
        pa.field("partitionColumns", pa.list_(pa.string())),
        pa.field("configuration", pa.map_(pa.string(), pa.string())),
        pa.field("createdTime", pa.int64()),
    ])

    # Full checkpoint schema
    checkpoint_schema = pa.schema([
        pa.field("add", add_type),
        pa.field("protocol", protocol_type),
        pa.field("metaData", metadata_type),
    ])

    # Build the data
    # Row 0: protocol
    # Row 1: metadata
    # Row 2: add file 1
    # Row 3: add file 2

    schema_string = json.dumps({
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "value", "type": "long", "nullable": True, "metadata": {}}
        ]
    })

    # Create arrays for each action type
    # Protocol (only row 0 has data)
    protocol_array = pa.StructArray.from_arrays(
        [
            pa.array([1, None, None, None], type=pa.int32()),  # minReaderVersion
            pa.array([2, None, None, None], type=pa.int32()),  # minWriterVersion
        ],
        names=["minReaderVersion", "minWriterVersion"]
    )

    # Metadata (only row 1 has data)
    metadata_array = pa.StructArray.from_arrays(
        [
            pa.array([None, "test-table-id", None, None]),  # id
            pa.StructArray.from_arrays(
                [
                    pa.array([None, "parquet", None, None]),  # provider
                    pa.array([None, [], None, None], type=pa.map_(pa.string(), pa.string())),  # options
                ],
                names=["provider", "options"]
            ),
            pa.array([None, schema_string, None, None]),  # schemaString
            pa.array([None, [], None, None], type=pa.list_(pa.string())),  # partitionColumns
            pa.array([None, [], None, None], type=pa.map_(pa.string(), pa.string())),  # configuration
            pa.array([None, 1700000000000, None, None], type=pa.int64()),  # createdTime
        ],
        names=["id", "format", "schemaString", "partitionColumns", "configuration", "createdTime"]
    )

    # Add actions (rows 2 and 3 have data)
    # stats_parsed for file 1: numRecords=100, min=(1,10), max=(100,1000)
    # stats_parsed for file 2: numRecords=100, min=(101,2000), max=(200,3000)

    stats_json_1 = json.dumps({
        "numRecords": 100,
        "minValues": {"id": 1, "value": 10},
        "maxValues": {"id": 100, "value": 1000},
        "nullCount": {"id": 0, "value": 0}
    })
    stats_json_2 = json.dumps({
        "numRecords": 100,
        "minValues": {"id": 101, "value": 2000},
        "maxValues": {"id": 200, "value": 3000},
        "nullCount": {"id": 0, "value": 0}
    })

    # Build minValues struct arrays
    min_values_array = pa.StructArray.from_arrays(
        [
            pa.array([None, None, 1, 101], type=pa.int64()),  # id
            pa.array([None, None, 10, 2000], type=pa.int64()),  # value
        ],
        names=["id", "value"]
    )

    max_values_array = pa.StructArray.from_arrays(
        [
            pa.array([None, None, 100, 200], type=pa.int64()),  # id
            pa.array([None, None, 1000, 3000], type=pa.int64()),  # value
        ],
        names=["id", "value"]
    )

    null_count_array = pa.StructArray.from_arrays(
        [
            pa.array([None, None, 0, 0], type=pa.int64()),  # id
            pa.array([None, None, 0, 0], type=pa.int64()),  # value
        ],
        names=["id", "value"]
    )

    stats_parsed_array = pa.StructArray.from_arrays(
        [
            pa.array([None, None, 100, 100], type=pa.int64()),  # numRecords
            min_values_array,
            max_values_array,
            null_count_array,
        ],
        names=["numRecords", "minValues", "maxValues", "nullCount"]
    )

    add_array = pa.StructArray.from_arrays(
        [
            pa.array([None, None, "part-00000.parquet", "part-00001.parquet"]),  # path
            pa.array([None, None, [], []], type=pa.map_(pa.string(), pa.string())),  # partitionValues
            pa.array([None, None, 1000, 1000], type=pa.int64()),  # size
            pa.array([None, None, 1700000000000, 1700000000000], type=pa.int64()),  # modificationTime
            pa.array([None, None, True, True]),  # dataChange
            pa.array([None, None, stats_json_1, stats_json_2]),  # stats (JSON)
            stats_parsed_array,  # stats_parsed
        ],
        names=["path", "partitionValues", "size", "modificationTime", "dataChange", "stats", "stats_parsed"]
    )

    # Create the table
    table = pa.table({
        "add": add_array,
        "protocol": protocol_array,
        "metaData": metadata_array,
    })

    # Write checkpoint
    pq.write_table(table, f"{LOG_DIR}/00000000000000000000.checkpoint.parquet")
    print(f"Created checkpoint with stats_parsed")


if __name__ == "__main__":
    create_table()
