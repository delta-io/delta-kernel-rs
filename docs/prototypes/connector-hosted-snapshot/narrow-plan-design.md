# Validated metadata planning without full table schemas

The default declarative metadata plan uses JSON statistics, string partition values, and no
predicate. Its plan construction does not need the logical or physical table schema.

## API and ownership

`MetadataScanPlan` now receives the log segment, optional pruning predicate, optional stats
and partition schemas, output options, and a skip-all flag. It no longer holds `StateInfo`.
Existing native scans adapt their state to these inputs, preserving their output.

`ValidatedMetadataScan::try_new` validates an already loaded immutable snapshot for the default
scan. It checks nonempty schema and scan feature support. Schema/protocol and physical mapping
validation already occurred during snapshot loading. Explicit metadata columns use the existing
scan validator for their additional rules. The token stores only table identity, version, and
freshness; it does not retain a schema or table configuration.

FFI handoff still compares connector state with the original native snapshot. `SnapshotCore`
retains the validation token and caller-minted generation. Each plan call checks generation,
version, and freshness before using the token. A different generation is rejected. The connector
must keep a generation immutable: these checks do not detect mutation under an unchanged ID.

For a valid default plan, Rust reads log paths and checkpoint hints and constructs the metadata
plan. It does not decode metadata/protocol, parse table schema JSON, build TableConfiguration,
or construct StateInfo. Checkpoint shape discovery can still read a checkpoint schema.

Schema getters remain demand-driven: an explicit schema request still returns an owned Rust
schema. A snapshot that cannot be validated for a scan can still be externalized for other getters;
its scan calls follow the existing fallible path, preserving errors such as empty-schema rejection.

## Scope and remaining copies

This prototype handles the existing default full-table metadata plan. Projected data reads,
predicates, and typed stats or partition outputs continue to use their existing schema-dependent
machinery. It does not introduce a general field-level lazy schema representation.

The Java/JNR API is unchanged and still packs the full hint, including schema JSON, for each
plan call. This change removes Rust decoding and schema construction; it does not remove Java
UTF-8 encoding, JNR native buffers, or the FFI calls. Those allocations are outside the Rust
allocator tracker. Initial native snapshot loading also remains unchanged and can set the overall
peak after scan allocations fall.

## Verification

The focused FFI tests compare native and narrow plan protobuf bytes with nested schemas,
partitioned/unpartitioned tables, and both freshness modes. A wrapper panics if metadata planning
requests protocol, metadata, table schema, or CRC. Tests also cover mismatched generations,
handoff mismatches, and successful externalization followed by an empty-schema scan error.

Each benchmark pair uses the same native library, context count, field count, paths, and timing
boundaries. Tracked libraries measure live Rust bytes; untracked libraries measure latency and
CPU. Synchronized runs are stress observations if native Parquet reader allocation errors occur.

## Measured p100 result (25 September 2026)

Fresh pairs used one native library per pair and 1,107 contexts / 475,621 flattened fields.
The 16-worker tracked candidate had a 30.4-30.9 MB scan peak and a 122.4-131.4 MB overall
peak (load dominated), versus 6,138.1-6,138.5 MB baseline overall. These are live Rust bytes,
not whole-process native memory or isolated per-snapshot peaks. Baseline scan handles remain
live until phase cleanup; the candidate has no persistent scan child.

Untracked scan medians were 194 ms candidate versus 675 ms baseline, with all three repeats
reported in the results tab. Timing includes the harness protobuf result-size check. The
synchronized stress pair logged native Parquet allocation errors in both modes and cannot
establish a clean production peak.

23 snapshot-hint FFI tests and 104 kernel metadata-plan tests passed, together with formatting,
workspace Clippy (all features, warnings denied), and workspace documentation checks.

[Self-contained design, measurements, profiles, and caveats](https://docs.google.com/document/d/189Wr6fRnbyj_qbAE-7Dv9pqIdBgwp0C-XkuC7Lh_aGw/edit?tab=t.s65jv6qvrqoi).
