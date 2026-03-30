# Delta Protocol Compliance Fixtures

Portable, runtime-neutral test cases for Delta protocol compliance. Each case encodes what
the Delta spec requires. Runtime-specific harnesses load these fixtures and add concrete
assertions; divergences are documented in hand-written test code under version control.

**Fixture files encode spec requirements only.** `expected_outcome` must be anchored to
the actual spec, not to any runtime's behavior. If a runtime diverges — auto-upgrades
protocols, auto-satisfies feature dependencies, or accepts something the spec forbids —
that is documented in the runtime's `results/` file, not in the fixture files.

**Protocol + metadata legality is judged on what the spec allows to be committed, not on
what the caller supplied.** If a fixture presents a protocol/metadata combination that the
spec forbids as a committed table state, `expected_outcome` is `"failure"`. A runtime that
transforms the input before committing (e.g. auto-generating column mapping annotations,
auto-satisfying feature dependencies) is diverging from the literal request; record the
transformation in `results/` as `committed_protocol` or `committed_schema`, not in the
fixture.

**No runtime is authoritative** -- all have their own bugs and quirks. `expected_outcome`
reflects our best reading of the spec; the optional `note` field gives the spec rationale
for ambiguous or controversial cases. Update based on new spec clarity.

See `errata.json` for open controversies and cases where the spec is ambiguous or silent.
Fixture cases reference errata entries via `"$errata.<key>"` in their `note` field.

---

## Fixture files

Cases are organized into per-feature files under `fixtures/`:

```
fixtures/
  protocol-validity.json           # cross-cutting protocol version and feature list rules
  catalog-managed.json             # catalogManaged and catalogOwned-preview (pre-ratification alias)
  column-mapping.json              # columnMapping
  deletion-vectors.json            # deletionVectors
  timestamp-ntz.json               # timestampNtz
  type-widening.json               # typeWidening and typeWidening-preview
  variant.json                     # variantType and variantType-preview
  variant-shredding-preview.json   # variantShredding-preview
  append-only.json                 # appendOnly
  allow-column-defaults.json       # allowColumnDefaults
  change-data-feed.json            # changeDataFeed
  check-constraints.json           # checkConstraints
  clustering.json                  # clustering
  domain-metadata.json             # domainMetadata
  generated-columns.json           # generatedColumns
  identity-columns.json            # identityColumns
  in-commit-timestamp.json         # inCommitTimestamp
  invariants.json                  # invariants
  materialize-partition-columns.json # materializePartitionColumns
  row-tracking.json                # rowTracking
  v2-checkpoint.json               # v2Checkpoint
  vacuum-protocol-check.json       # vacuumProtocolCheck
  iceberg-compat-v1.json           # icebergCompatV1
  iceberg-compat-v2.json           # icebergCompatV2
  iceberg-compat-v3-preview.json   # icebergCompatV3-preview
  iceberg-writer-compat.json       # icebergWriterCompatV1-preview
```

Each file is self-contained: it includes only the `$schemas`, `$protocols`, and `$notes`
entries used by its own cases.

---

## Fixture file schema

Each fixture file is a top-level JSON object with four keys: `$schemas`, `$protocols`,
`$notes`, and `cases`.

### Shared reference sections

`$schemas`, `$protocols`, and `$notes` are shared-reference dictionaries local to the
fixture file. `$errata` is a global registry loaded from `errata.json`.
Any string value in a case that matches `"$section.key"` is resolved by the
fixture runner before the case is run. Use these to avoid repeating large blobs (schemas,
protocol objects, note text) across cases.

```json
{
  "$schemas": {
    "simple_int": { "type": "struct", "fields": [...] }
  },
  "$protocols": {
    "modern_2_7_deletion_vectors": { "minReaderVersion": 2, "minWriterVersion": 7, "writerFeatures": ["deletionVectors"] }
  },
  "$notes": {
    "feature_active_without_support": "Spec defines supported vs active separately. ..."
  },
  "cases": [...]
}
```

Reference syntax: `"$schemas.simple_int"`, `"$protocols.modern_2_7_deletion_vectors"`,
`"$notes.feature_active_without_support"`. Only add a shared entry when three or more cases
within the file would otherwise repeat the same value verbatim.

### Required fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | snake_case, globally unique across all fixture files; used by test code to load by name |
| `description` | string | One sentence beginning with the operation ("Reading a...", "Creating a...", "Appending to..."): scenario, expected outcome, and the spec reason in brief |
| `operation` | object | The operation to perform (see Operation types below) |
| `expected_outcome` | string | `"success"` or `"failure"` |

### Optional fields

| Field | Type | Description |
|-------|------|-------------|
| `flavor` | string | Sub-category tag for cross-feature filtering. Closed vocabulary: `"orphan"` (activation property present but feature not listed in protocol), `"supported_not_active"` (feature listed in protocol but table conditions for activation not met — no activation property, no qualifying schema elements, etc.), `"missing_dependency"` (required co-feature absent from protocol), `"insufficient_protocol"` (protocol version too low to support the feature, regardless of feature lists), `"legacy"` (tests the pre-(3,7) protocol path where a modern path also exists for the same feature), `"unknown_feature"` (unrecognized feature name in readerFeatures/writerFeatures — primarily protocol-validity and write-behavior). Omit for straightforward happy-path cases where the case name is already self-describing. |
| `feature` | string | Delta feature name for filtering (e.g. `jq '.cases[] | select(.feature=="columnMapping")'`). For `-preview` features, use the preview name (e.g. `"typeWidening-preview"`) so cases are separately filterable from the ratified feature. |
| `setup` | object | Pre-existing table state; required for `read_snapshot` and `empty_commit` operations |
| `note` | string | For spec ambiguity or controversy only — use `"$errata.<key>"` to reference an entry in `errata.json`, or a `"$notes.<key>"` local annotation for a non-obvious spec requirement that applies to multiple cases. Do not use `note` to describe fixture coverage gaps or operation limitations. No runtime error strings. |

#### Pre-ratification `-preview` feature names

Several features shipped under a `-preview` suffix before their RFC was accepted (e.g.
`typeWidening-preview`, `variantType-preview`). The `-preview` name is not in the ratified
spec; whether the preview and ratified specs are semantically equivalent must be verified
per feature by checking the full RFC commit history (not just the acceptance PR), since
RFCs are designed to iterate and often do. Find all commits touching the RFC file via
`gh api "repos/delta-io/delta/commits?path=protocol_rfcs/<feature>.md"` and review each
diff for substantive changes to writer/reader requirements or data format.

Use the same `flavor` on `-preview` cases as you would on the equivalent ratified cases
(or omit it for happy-path creates/reads). Do NOT use `flavor: "unknown_feature"` — that
flavor is for genuinely unrecognized feature names, not pre-ratification names whose
semantics are known. Record the result of the verification (equivalent or not, and the
acceptance PR reference) in a per-fixture `$notes` entry so it does not need to be
re-derived.

### Operation types

#### `create_table`

Commits a Protocol action + Metadata action as the initial table commit (version 0). No
setup needed.

```json
{
  "operation": {
    "type": "create_table",
    "protocol": {
      "minReaderVersion": 2,
      "minWriterVersion": 5,
      "readerFeatures": [],
      "writerFeatures": []
    },
    "metadata": {
      "configuration": { "delta.columnMapping.mode": "name" },
      "schemaString": "{\"type\":\"struct\",\"fields\":[...]}"
    }
  }
}
```

`readerFeatures` and `writerFeatures` are optional; omit them for legacy (pre-3,7) protocols.

#### `read_snapshot`

Opens a pre-existing table and reads its snapshot. Requires `setup`.

```json
{
  "operation": { "type": "read_snapshot" }
}
```

#### `empty_commit`

Opens a write transaction on a pre-existing table and commits it with no actions. Requires
`setup`. This is sufficient for protocol-level write validation (does the writer accept or
reject based on the protocol + feature state?). It is NOT sufficient to test row-level
constraint features (invariants, checkConstraints, generatedColumns) or to exercise data
write paths (column mapping physical name assignment, DV numRecords, etc.), which require
a future `insert_rows` operation type (see TODOs).

```json
{
  "operation": { "type": "empty_commit" }
}
```

### Setup

The `setup` object contains `log_actions`: an array of Delta log action objects written
to `_delta_log/00000000000000000000.json`. The objects use standard Delta log action
wrapper keys (`protocol`, `metaData`, `add`, etc.) as specified in the Delta protocol.

```json
{
  "setup": {
    "log_actions": [
      { "protocol": { "minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"] } },
      { "metaData": { "schemaString": "$schemas.simple_int", "configuration": {"delta.enableDeletionVectors": "true"} } }
    ]
  }
}
```

#### metaData defaults

`metaData` log action entries use the following defaults for fields that are not
semantically meaningful to the test case. Specify only `schemaString` (required) and
any fields that differ from these defaults:

| Field | Default | Notes |
|-------|---------|-------|
| `schemaString` | **none — required** | Must always be provided explicitly |
| `configuration` | `{}` | Omit when empty; include only the properties relevant to the case |
| `partitionColumns` | `[]` | Omit unless the test specifically exercises partitioning |
| `id` | `"00000000-0000-0000-0000-000000000001"` | Never varies; omit in all cases |
| `format` | `{"provider": "parquet", "options": {}}` | Never varies; omit in all cases |

The merge is **shallow**: a fixture-supplied key replaces the default entirely rather than
merging into it. Runners should raise an error at run time if `schemaString` is absent.

#### domainMetadata defaults

`domainMetadata` log action entries use the following defaults:

| Field | Default | Notes |
|-------|---------|-------|
| `domain` | **none — required** | Must always be provided explicitly |
| `configuration` | **none — required** | Must always be provided explicitly |
| `removed` | `false` | Omit unless the test specifically exercises domain metadata removal |

---

## Adding New Cases

1. Choose the appropriate fixture file under `fixtures/` for the feature being tested.
   Create a new file if none exists for that feature.
2. Choose a descriptive `name` in snake_case, unique across all fixture files. Prefix with
   the operation type (`read_snapshot_`, `create_table_`, `empty_commit_`).
3. Write a `description` that opens with the operation ("Reading a...", "Creating a...",
   "Appending to...") and in one sentence covers: scenario, expected outcome, spec reason.
4. Set `feature` to the Delta feature name if applicable.
5. Set `flavor` if the case belongs to a sub-category used for filtering (e.g.
   `"unknown_feature"`, `"writer_only_read"`, `"supported_not_active"`).
6. For `create_table` cases, supply `operation.protocol` and `operation.metadata`.
7. For `read_snapshot`/`empty_commit` cases, supply `setup.log_actions` with at least a
   `protocol` action and a `metaData` action. The `metaData` entry requires only
   `schemaString`; omit `id`, `format`, empty `partitionColumns`, and empty `configuration`
   (all have standard defaults applied by the runner at run time).
8. Set `expected_outcome` to your best reading of the spec.
9. Add `note` only for spec ambiguity/errata OR non-obvious fixture characteristics. For
   errata-level controversies, use `"$errata.<key>"` — add a new entry to `errata.json`
   if none exists yet. No runtime error strings in notes.
10. Use `$schemas.*`, `$protocols.*`, or `$notes.*` references instead of inlining repeated
    values. Add new shared entries to the relevant section when three or more cases share
    the same value.

### Filtering fixtures

```bash
# All columnMapping cases
jq '.cases[] | select(.feature=="columnMapping") | .name' fixtures/column-mapping.json

# All failure cases
jq '.cases[] | select(.expected_outcome=="failure") | .name' fixtures/protocol-validity.json

# All create_table cases
jq '.cases[] | select(.operation.type=="create_table") | .name' fixtures/column-mapping.json

# All cases with a given flavor
jq '.cases[] | select(.flavor=="writer_only_read") | .name' fixtures/column-mapping.json
```

### Formatting fixtures

```bash
# Format all fixture files
python fixtures/format-json.py

# Format one fixture
python fixtures/format-json.py protocol-validity.json
```
---

## TODOs

### `create_table` success cases need postconditions

Currently a `create_table` success case only asserts that no exception was thrown. That is
insufficient: a runtime could succeed by committing something entirely different from what
was requested, and the fixture would pass.

The correct postcondition for a success case is that the requested feature is supported and
active in the committed snapshot — not that the committed protocol matches the requested one
exactly. Runtimes legitimately transform the input (auto-satisfying co-required features,
choosing legacy vs. modern protocol paths), so requiring an exact match would be wrong. But
the feature under test must be present and active. For example, a `create_table` that
requests `deletionVectors` must result in a committed table where `deletionVectors` appears
in both feature lists and `delta.enableDeletionVectors=true` is in the committed
configuration.

There is no immediate easy fix: supporting postconditions requires the harness to read back
the committed snapshot and assert against it, and the fixture format needs a way to express
what "active" means per feature. Until then, `committed_protocol` in the result JSON is
purely diagnostic.

### nullable=false and the invariants feature

The spec says nothing about nullable=false columns requiring any particular feature.
Protocol history: invariants was introduced at writer v2; checkConstraints at writer v3.
The base legacy version with no constraint features is `(1,1)`.

Two cases are interesting:
- `(1,1)`: no constraint feature exists at this version; in theory the writer cannot
  enforce nullable=false and must accept the table. What protocol is actually committed?
- `(3,7)` with no constraint feature listed: does the writer reject or auto-upgrade,
  and to which feature (`invariants` or `checkConstraints`)?

These are covered by `invariants_nullable_false_legacy_1_1_create_table_succeeds` and
`invariants_nullable_false_without_feature_modern_create_table_fails`.

### appendOnly enforcement and dataChange=false rearrangement

The spec's core appendOnly constraint ("new log entries MUST NOT change or remove data") and its
exception ("may rearrange data via add/remove actions where `dataChange=false`") are both
untestable with `empty_commit`, which never issues Remove actions. Two cases are needed once a
write operation type with explicit log actions exists:

- A write that includes a Remove action on an appendOnly-active table → must fail.
- A write that includes remove+add actions with `dataChange=false` on an appendOnly-active
  table → must succeed (the permitted rearrangement exception).

### Row-level constraint and feature testing

`empty_commit` commits no data rows, so constraint features (invariants, checkConstraints,
generatedColumns) and data-reading features (columnMapping, typeWidening) cannot be
validated end-to-end with the current operation types. Two future operation types:

- **`insert_rows`**: commits a specified set of rows (as inline JSON or a parquet snippet).
  Needed for: invariant violations, constraint violations, generated column enforcement.
- **`read_rows`**: reads back rows from a pre-built table (setup includes hand-crafted
  parquet files + injected `add` actions). Needed for: column mapping physical/logical name
  resolution, type widening coercion, variant decoding. Setup would need to include the
  actual parquet file bytes or a reference to a fixture file.

### Invalid schema/metadata combos on pre-existing tables

The current fixture set only injects valid log actions via `setup.log_actions`. Many
validations (e.g. CM annotations present with `mode=none`, duplicate CM IDs, missing
annotations on nested fields) have no coverage because a writer's own code path would
never produce these states. TODO: add setup-based cases that inject these states directly
and test `read_snapshot` / `empty_commit` behavior.

### icebergCompatV1/V2: nested type helper field IDs

ICv1 and ICv2 require Parquet-level column IDs on every field in the Parquet schema,
including intermediate helper nodes for nested types (array `element`, map `key` and `value`).
These nodes have no corresponding `StructField` in the Delta schema and therefore no `metadata`
slot for `delta.columnMapping.id`. The spec stores their IDs in the nearest enclosing
`StructField`'s metadata under dedicated keys — but the exact key names and encoding structure
require re-reading the relevant PROTOCOL.md sections before writing cases. No fixture currently
tests nested CM annotation correctness under icebergCompatV1 or V2.

### columnMapping: nested schema and maxColumnId coverage

`fixtures/column-mapping.json` uses a single top-level integer field for all cases. Two
gaps:

- **Nested schemas**: the spec requires CM annotations on every nested field (struct,
  map key/value, array element). No case exercises annotation propagation through nesting.
  Requires adding a `cm_annotated_nested` schema entry and corresponding read/append cases.
- **`delta.columnMapping.maxColumnId`**: the spec requires this property to increase
  monotonically as columns are introduced. No case verifies its presence or value.
  Requires `schema_evolution` or `insert_rows` operation support.

### inCommitTimestamp: write-path invariant coverage

The following ICT writer requirements are untestable with `empty_commit`, which cannot
control what specific actions the writer includes in the commit:

- **commitInfo absent**: writer must include a `commitInfo` action on every ICT-enabled commit.
- **inCommitTimestamp field absent from commitInfo**: writer must include the field even if
  `commitInfo` is present for other reasons.
- **commitInfo not first action**: spec requires `commitInfo` to be the first action; a commit
  where it appears after protocol or metaData should fail.
- **Non-monotonic timestamp**: writer must ensure each commit's `inCommitTimestamp` is at least
  one millisecond later than the previous commit's value.
- **Provenance tracking on mid-table enablement**: enabling ICT on a table that already has
  non-ICT commits must write `delta.inCommitTimestampEnablementVersion` and
  `delta.inCommitTimestampEnablementTimestamp`, and the enabling commit's timestamp must exceed
  the file modification time of the immediately preceding commit.

All require a future `write_actions` operation type that can specify exact commit payloads.

### domainMetadata: write-path and multi-commit coverage

Several key domainMetadata spec requirements are untestable with the current operation model:

- **System-controlled domain write rejection**: the spec forbids writers from allowing users
  to write `domainMetadata` actions whose domain name starts with `delta.`. Requires a
  future `write_actions` operation type that can specify explicit log action payloads.
- **Tombstone semantics across commits**: `removed=true` marks a domain as logically deleted
  in subsequent snapshots. Requires either multi-commit setup or the `write_actions` type.
- **Action Reconciliation last-write-wins across commits**: when multiple commits each write
  a `domainMetadata` action for the same domain, the latest one wins. Requires multi-commit
  setup support in the fixture model.
- **Writer preservation of unknown user domains**: a writer must carry forward all domains
  it does not understand into its commit. Requires verifying the committed log, not just
  that the operation succeeded.

### Cross-cutting feature combinations: dedicated fixture

Cross-feature test cases are currently scattered across per-feature files and were added
ad-hoc. Several of the existing combinations (e.g. CDF+DV, vacuumProtocolCheck+DV) have no
spec-defined interaction — "this feature is important so test it alongside things" is not a
principle. These cases should be audited; those without a concrete spec rationale should be
removed.

A dedicated `fixtures/cross-cutting.json` should replace the scattered cases and cover only
combinations where the spec defines explicit requirements for the pair. Selection criteria:

- **Explicit spec restrictions**: mutual exclusions (icebergCompatV1 + deletionVectors support
  forbidden; icebergCompatV1 vs icebergCompatV2 co-listing), required co-listings
  (rowTracking + domainMetadata, clustering + domainMetadata).
- **Shared infrastructure with a concrete interaction**: columnMapping + any SQL expression
  feature (invariants, checkConstraints, generatedColumns, allowColumnDefaults) share the
  logical-vs-physical name ambiguity in `$errata.sql_expression_cm_name_resolution`. This is
  a real interaction — the expression references a column name that may resolve differently
  depending on how the runtime handles CM.
- **Spec-cited co-listing examples**: the spec's own protocol examples that show two features
  co-listed (e.g. `columnMapping + identityColumns`).
- **Known implementation divergences**.

Explicitly out of scope: "A is important, let's also test it with B" unless there is a
spec-defined interaction. Two independent features that happen to be co-listed have no
combined case unless the spec says something specific about their combination.

### Additional operation types

CDF reads, schema evolution DDL, OPTIMIZE (file compaction), and VACUUM currently have
no coverage. These exercise feature-specific write paths not reachable via `empty_commit`.

### Feature enablement vs. support state

The kernel tests `is_feature_supported` and `is_feature_enabled` separately —
supported = listed in protocol; enabled = supported AND metadata requirements met. TODO:
add `read_snapshot` cases that assert on the returned state for supported-but-not-enabled
configurations (e.g. appendOnly feature listed, delta.appendOnly absent).

---

## Design Decisions

- **One entry per case**: avoids hidden programmatic expansion; each case is readable on
  its own and grep-able by name.
- **Per-feature files**: cases are grouped by feature in `fixtures/`. Each file is
  self-contained with only the shared references it needs.
- **Protocol-native JSON**: `setup.log_actions` uses the exact Delta log action format,
  making cases verifiable against the spec without translation.
- **No error strings in fixtures**: runtime error messages are not portable. Only spec
  rules go in `note`.
- **`create_table` as starting point**: easier to specify an exact initial protocol state
  than ALTER TABLE. ALTER TABLE tests are also needed and will be added separately.
