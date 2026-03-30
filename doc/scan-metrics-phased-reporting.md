# Scan Metrics Phased Reporting Decisions

This document records the first-principles reasoning and tradeoffs behind scan metrics emission
for `scan_metadata`, sequential phase, and parallel phase execution.

## Problem Statement

Parallel phase-2 execution can run across multiple workers and may be sharded. The metrics design
must preserve correlation across phases while remaining useful for both local and distributed
execution.

## Constraints

- Phase-1 and phase-2 may emit independently; connectors may not merge intermediate metrics.
- Phase-2 may run across many workers.
- Runtime reporting objects (reporters) should not be part of serialized replay state.
- Metrics should be emitted on explicit completion boundaries, not inferred from iterator drops.

## Decisions

1. Use a single shared `operation_id` across full scan metadata, phase-1, and all phase-2 reports.
2. Keep one completion event type with `scan_type` discriminator:
   - `Full`
   - `SequentialPhase`
   - `ParallelPhase`
3. Emit phase-2 metrics separately (multi-emission). A single operation can produce multiple
   `ParallelPhase` completion events.
4. Keep reporter ownership in runtime wrappers (`Scan`, `SequentialScanMetadata`, `ParallelState`),
   not in serialized replay state.
5. Report phase-2 completion from `ParallelState::report_metrics()` for explicit control over the
   reporting boundary.

## Runtime vs Serialized State

- `ScanLogReplayProcessor` carries serialized operation correlation context.
- `ParallelState` owns runtime-only reporter behavior and phase-2 timing boundary.
- Serialized state includes operation correlation context required for worker reconstruction.

## Alternatives Considered

### A) Separate metric event variants per phase

Rejected for now. It duplicates event fields and increases maintenance burden. The `scan_type`
discriminator gives equivalent separation with a simpler API surface.

### B) Transition metric event (phase-1 -> phase-2)

Rejected. Transition information is better as logs, not a stable metrics contract.

### C) Reporting automatically on iterator drop

Rejected for phase-2 workers. Drops are ambiguous in retries/cancellation/speculative execution
and produce noisy or misleading completion semantics.

## CUJ: Local Multithreaded Execution

1. Run sequential phase iterator.
2. Call `finish()` -> emits one `SequentialPhase` event.
3. Split files across local threads.
4. Each thread runs its `ParallelScanMetadata` iterator.
5. After all threads complete, call `ParallelState::report_metrics()` for the chosen boundary.

Expected logging shape:

- `ScanMetadataCompleted(id=X, scan_type=sequential, ...)`
- `ScanMetadataCompleted(id=X, scan_type=parallel, ...)`

## CUJ: Distributed Execution

1. Coordinator runs phase-1 and obtains `state + files`.
2. Coordinator serializes state once and dispatches with file partitions.
3. Workers deserialize and execute `ParallelScanMetadata` for assigned shards.
4. Each worker calls `ParallelState::report_metrics()` once for its shard boundary.
5. Downstream correlates all reports by `operation_id`.

Expected logging shape:

- Coordinator: `ScanMetadataCompleted(id=X, scan_type=sequential, ...)`
- Worker A: `ScanMetadataCompleted(id=X, scan_type=parallel, ...)`
- Worker B: `ScanMetadataCompleted(id=X, scan_type=parallel, ...)`
- ...

## Aggregation Guidance

- Group by `operation_id` to correlate all events from one scan operation.
- Split by `scan_type` to separate full-scan, phase-1, and phase-2 behavior.
- For phase-2, assume at-least-once reporting in retry scenarios.
