# Delta protocol feature implementation in delta-kernel-rs

Generated from the `ProtocolImplement` matrix in `kernel/src/table_features/mod.rs` by the `generate_protocol_support_md` test -- do not edit by hand; regenerate with `scripts/generate-protocol-support.sh`.

Statuses: **yes** = fully implemented; **partial** = implemented under a condition (see Notes); **unimplemented** = operation not implemented; **unimplemented(disallow)** = see note below; **n/a** = not applicable (e.g. read cells on a writer-only feature).

**Note:** statuses are protocol-oriented. For an operation Op and feature A, **yes** means every protocol-legal Op is allowed when A is enabled -- not that Op always succeeds: a protocol-illegal input still fails. E.g. kernel fully implements `icebergCompatV3` for append, yet an append whose addFile omits `stats.numRecords` is rejected, as the protocol requires.

**Note:** some features **disallow** an operation. `unimplemented(disallow)` means kernel does not enforce that restriction, so the operation is permitted when the feature is enabled and the caller must honor the protocol. E.g. `appendOnly` disallows data-changing removes (DELETE/UPDATE/MERGE), and its `dmlRemove` cell is `unimplemented(disallow)`: kernel currently allows such removes when `appendOnly` is enabled (we intend to enforce this in the future).

**Note:** the data-write operations are `append` -- an add-only commit (no removes); `dmlRemove` -- a data-changing commit that removes rows (DELETE/UPDATE/MERGE); and `maintenance` -- a commit that rewrites files without changing data (OPTIMIZE, compaction, stats/row-id backfill).

## Support matrix

| Feature | Type | read-scan | read-cdf | append | dmlRemove | maintenance | alter | create |
|---|---|---|---|---|---|---|---|---|
| `appendOnly` | writer | yes | yes | yes | unimplemented(disallow) | yes | yes | yes |
| `invariants` | writer | n/a | n/a | partial | partial | partial | partial | yes |
| `checkConstraints` | writer | n/a | n/a | unimplemented | unimplemented | unimplemented | unimplemented | unimplemented |
| `changeDataFeed` | writer | yes | yes | yes | partial | yes | yes | yes |
| `generatedColumns` | writer | n/a | n/a | unimplemented | unimplemented | unimplemented | unimplemented | unimplemented |
| `identityColumns` | writer | n/a | n/a | unimplemented | unimplemented | unimplemented | unimplemented | unimplemented |
| `inCommitTimestamp` | writer | yes | yes | yes | yes | yes | yes | yes |
| `rowTracking` | writer | yes | yes | yes | partial | partial | yes | yes |
| `domainMetadata` | writer | yes | yes | yes | yes | yes | yes | yes |
| `icebergCompatV1` | writer | n/a | n/a | unimplemented | unimplemented | unimplemented | unimplemented | unimplemented |
| `icebergCompatV2` | writer | n/a | n/a | unimplemented | unimplemented | unimplemented | unimplemented | unimplemented |
| `icebergCompatV3` | writer | n/a | n/a | yes | partial | partial | partial | yes |
| `clustering` | writer | yes | yes | yes | yes | yes | yes | unimplemented |
| `materializePartitionColumns` | writer | yes | yes | yes | yes | yes | yes | yes |
| `allowColumnDefaults` | writer | n/a | n/a | unimplemented | unimplemented | unimplemented | unimplemented | unimplemented |
| `catalogManaged` | reader+writer | yes | unimplemented | yes | yes | yes | yes | yes |
| `catalogOwned-preview` | reader+writer | yes | unimplemented | yes | yes | yes | yes | unimplemented |
| `columnMapping` | reader+writer | yes | yes | yes | yes | yes | yes | yes |
| `deletionVectors` | reader+writer | yes | yes | yes | yes | yes | yes | yes |
| `timestampNtz` | reader+writer | yes | yes | yes | yes | yes | yes | partial |
| `typeWidening` | reader+writer | yes | yes | unimplemented | unimplemented | unimplemented | unimplemented | yes |
| `typeWidening-preview` | reader+writer | yes | yes | unimplemented | unimplemented | unimplemented | unimplemented | unimplemented |
| `v2Checkpoint` | reader+writer | yes | yes | yes | yes | yes | yes | yes |
| `vacuumProtocolCheck` | reader+writer | yes | yes | yes | yes | yes | yes | yes |
| `variantType` | reader+writer | yes | yes | yes | yes | yes | yes | partial |
| `variantType-preview` | reader+writer | yes | yes | yes | yes | yes | yes | partial |
| `variantShredding` | reader+writer | yes | yes | yes | yes | yes | yes | partial |
| `variantShredding-preview` | reader+writer | yes | yes | yes | yes | yes | yes | partial |

Unknown / unrecognized features are forbidden for every operation.

## Notes (partial support)

- `invariants` / append: a schema with column invariants is not yet supported
- `invariants` / dmlRemove: a schema with column invariants is not yet supported
- `invariants` / maintenance: a schema with column invariants is not yet supported
- `invariants` / alter: a schema with column invariants is not yet supported
- `changeDataFeed` / dmlRemove: CDF writes not yet supported for UPDATE/DELETE/MERGE-shaped commits
- `rowTracking` / dmlRemove: Remove actions are not yet supported when rowTracking is supported and not suspended (#2538)
- `rowTracking` / maintenance: Remove actions are not yet supported when rowTracking is supported and not suspended (#2538)
- `icebergCompatV3` / dmlRemove: Remove actions are not yet supported on icebergCompatV3-enabled tables
- `icebergCompatV3` / maintenance: Remove actions are not yet supported on icebergCompatV3-enabled tables
- `icebergCompatV3` / alter: ALTER TABLE not yet supported on icebergCompatV3-enabled tables
- `timestampNtz` / create: is enabled by schema column type; do not set the corresponding delta.feature.* signal at create time
- `variantType` / create: is enabled by schema column type; do not set the corresponding delta.feature.* signal at create time
- `variantType-preview` / create: is enabled by schema column type; do not set the corresponding delta.feature.* signal at create time
- `variantShredding` / create: is enabled by schema column type; do not set the corresponding delta.feature.* signal at create time
- `variantShredding-preview` / create: is enabled by schema column type; do not set the corresponding delta.feature.* signal at create time
