# Plan: Add Metrics to Log Replay Processing

## Goal
Add metrics tracking to log replay processing with selectivity metrics tracked in the processor, and configurable emission in the phase wrappers.

## Design Principles

1. **Metrics tracked where computed**: `LogReplayProcessor` implementations track metrics internally during `process_actions_batch()`
2. **Configurable emission**: `SequentialPhase` and `ParallelPhase` can optionally emit metrics (via `Option<Arc<dyn MetricsReporter>>`)
3. **Unified path support**: When `scan_metadata` internally uses phases, it extracts metrics from the processor and emits a single combined event
4. **Opaque to public API**: Internal details (operation_id, etc.) are not exposed publicly

## Metrics to Track

In `ScanLogReplayProcessor::process_actions_batch`:
- `total_metadata_rows`: Total action rows processed
- `rows_from_json`: Rows read from JSON log files
- `rows_from_checkpoint`: Rows read from checkpoint files
- `non_file_action_rows`: Rows that are neither Add nor Remove (null rows)
- `rows_filtered_by_partition_pruning`: Rows filtered out by partition filter
- `rows_filtered_by_data_skipping`: Rows filtered out by stats-based skipping
- `adds_emitted`: Number of Add actions in final output
- `removes_processed`: Number of Remove actions processed for deduplication
- `dedup_hashmap_size`: Size of the deduplication hashmap (seen file keys)

## Files to Modify

| File | Changes |
|------|---------|
| `kernel/src/log_replay.rs` | Add `LogReplayMetrics` struct and `metrics()` method to trait |
| `kernel/src/scan/log_replay.rs` | Track metrics in `ScanLogReplayProcessor` |
| `kernel/src/metrics/events.rs` | Add `LogReplayCompleted` event variant |
| `kernel/src/parallel/sequential_phase.rs` | Add optional metrics reporter, emit on finish if configured |
| `kernel/src/parallel/parallel_phase.rs` | Add optional metrics reporter, add `finish()` method |

## Implementation Details

### 1. Add LogReplayMetrics (`kernel/src/log_replay.rs`)

```rust
/// Metrics accumulated during log replay processing.
#[derive(Debug, Default, Clone)]
pub struct LogReplayMetrics {
    /// Total number of action rows processed
    pub total_metadata_rows: u64,
    /// Rows read from JSON log files
    pub rows_from_json: u64,
    /// Rows read from checkpoint files
    pub rows_from_checkpoint: u64,
    /// Rows that are neither Add nor Remove (null rows)
    pub non_file_action_rows: u64,
    /// Rows filtered out by partition pruning
    pub rows_filtered_by_partition_pruning: u64,
    /// Rows filtered out by data skipping (stats-based filtering)
    pub rows_filtered_by_data_skipping: u64,
    /// Number of Add actions emitted in output
    pub adds_emitted: u64,
    /// Number of Remove actions processed for deduplication
    pub removes_processed: u64,
    /// Size of the deduplication hashmap (number of seen file keys)
    pub dedup_hashmap_size: u64,
}
```

### 2. Add `metrics()` to LogReplayProcessor trait (`kernel/src/log_replay.rs`)

```rust
pub trait LogReplayProcessor: Sized {
    type Output: HasSelectionVector;

    fn process_actions_batch(&mut self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output>;

    /// Get accumulated metrics from processing so far.
    fn metrics(&self) -> &LogReplayMetrics;

    // ... existing methods ...
}
```

Also add to `ParallelLogReplayProcessor`:

```rust
pub(crate) trait ParallelLogReplayProcessor {
    type Output;
    fn process_actions_batch(&self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output>;
    fn metrics(&self) -> &LogReplayMetrics;
}
```

The `Arc<T>` impl delegates to `T::metrics()`.

### 3. Track metrics in AddRemoveDedupVisitor and ScanLogReplayProcessor

All metrics are tracked in a single pass inside the visitor to avoid O(n) re-iteration. The visitor takes a mutable reference to `LogReplayMetrics` and updates it directly.

**Update `AddRemoveDedupVisitor` to take metrics reference:**

```rust
struct AddRemoveDedupVisitor<'a, D: Deduplicator> {
    // ... existing fields ...
    metrics: &'a mut LogReplayMetrics,
}

impl<'a, D: Deduplicator> AddRemoveDedupVisitor<'a, D> {
    fn new(
        deduplicator: D,
        selection_vector: Vec<bool>,
        state_info: Arc<StateInfo>,
        partition_filter: Option<PredicateRef>,
        metrics: &'a mut LogReplayMetrics,
    ) -> Self { ... }
}
```

**Track in `AddRemoveDedupVisitor::visit()`:**

```rust
fn visit<'b>(&mut self, row_count: usize, getters: &[&'b dyn GetData<'b>]) -> DeltaResult<()> {
    for i in 0..row_count {
        if !self.selection_vector[i] {
            self.metrics.rows_filtered_by_data_skipping += 1;
            continue;
        }
        self.selection_vector[i] = self.is_valid_add(i, getters)?;
    }
    Ok(())
}
```

**Track in `AddRemoveDedupVisitor::is_valid_add()`:**

```rust
fn is_valid_add<'b>(&mut self, i: usize, getters: &[&'b dyn GetData<'b>]) -> DeltaResult<bool> {
    let Some((file_key, is_add)) = self.deduplicator.extract_file_action(...)?
    else {
        self.metrics.non_file_action_rows += 1;
        return Ok(false);
    };

    if is_add && self.is_file_partition_pruned(&partition_values) {
        self.metrics.rows_filtered_by_partition_pruning += 1;
        return Ok(false);
    }

    if self.deduplicator.check_and_record_seen(file_key) {
        return Ok(false);  // Duplicate
    }

    if !is_add {
        self.metrics.removes_processed += 1;
        return Ok(false);
    }

    self.metrics.adds_emitted += 1;
    // ... rest of transform logic ...
    Ok(true)
}
```

**In `ScanLogReplayProcessor::process_actions_batch()`:**

```rust
fn process_actions_batch(&mut self, actions_batch: ActionsBatch) -> DeltaResult<Self::Output> {
    let ActionsBatch { actions, is_log_batch } = actions_batch;
    let row_count = actions.len() as u64;

    // Track source type
    self.metrics.total_metadata_rows += row_count;
    if is_log_batch {
        self.metrics.rows_from_json += row_count;
    } else {
        self.metrics.rows_from_checkpoint += row_count;
    }

    let selection_vector = self.build_selection_vector(actions.as_ref())?;

    // Visitor updates metrics directly via mutable reference
    let mut visitor = AddRemoveDedupVisitor::new(
        deduplicator,
        selection_vector,
        self.state_info.clone(),
        self.partition_filter.clone(),
        &mut self.metrics,  // Pass mutable reference
    );
    visitor.visit_rows_of(actions.as_ref())?;

    // Update hashmap size after processing
    self.metrics.dedup_hashmap_size = self.seen_file_keys.len() as u64;

    // ... rest of method ...
}
```

### 4. Add metric event (`kernel/src/metrics/events.rs`)

Single event with phase indicator:

```rust
/// The phase/context of log replay that completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogReplayPhase {
    /// Sequential phase of distributed log replay
    Sequential,
    /// Parallel phase of distributed log replay
    Parallel,
    /// Unified scan_metadata (future)
    ScanMetadata,
}

/// Log replay processing completed.
LogReplayCompleted {
    phase: LogReplayPhase,
    duration: Duration,
    total_metadata_rows: u64,
    rows_from_json: u64,
    rows_from_checkpoint: u64,
    non_file_action_rows: u64,
    rows_filtered_by_partition_pruning: u64,
    rows_filtered_by_data_skipping: u64,
    adds_emitted: u64,
    removes_processed: u64,
    dedup_hashmap_size: u64,
},
```

**This PR implements:** `LogReplayPhase::Sequential` and `LogReplayPhase::Parallel`
**Future work:** `LogReplayPhase::ScanMetadata` when scan_metadata unification happens

### 5. Update SequentialPhase (`kernel/src/parallel/sequential_phase.rs`)

```rust
pub struct SequentialPhase<P: LogReplayProcessor> {
    processor: P,
    // ... existing fields ...

    // Metrics emission (optional)
    metrics_reporter: Option<Arc<dyn MetricsReporter>>,
    start_time: Instant,
}

impl<P: LogReplayProcessor> SequentialPhase<P> {
    pub fn try_new(
        processor: P,
        log_segment: &LogSegment,
        engine: Arc<dyn Engine>,
        metrics_reporter: Option<Arc<dyn MetricsReporter>>,  // NEW parameter
    ) -> DeltaResult<Self> { ... }

    pub fn finish(self) -> DeltaResult<AfterSequential<P>> {
        // If metrics_reporter is Some, emit SequentialPhaseCompleted event
        self.metrics_reporter.as_ref().inspect(|r| {
            let m = self.processor.metrics();
            r.report(MetricEvent::SequentialPhaseCompleted {
                duration: self.start_time.elapsed(),
                total_metadata_rows: m.total_metadata_rows,
                rows_from_json: m.rows_from_json,
                rows_from_checkpoint: m.rows_from_checkpoint,
                non_file_action_rows: m.non_file_action_rows,
                // ... other fields from metrics ...
            });
        });

        // Return AfterSequential as before (no changes to enum)
        ...
    }
}
```

### 6. Update ParallelPhase (`kernel/src/parallel/parallel_phase.rs`)

```rust
pub(crate) struct ParallelPhase<P: ParallelLogReplayProcessor> {
    processor: P,
    leaf_checkpoint_reader: Box<dyn Iterator<Item = DeltaResult<ActionsBatch>>>,

    // Metrics emission (optional)
    metrics_reporter: Option<Arc<dyn MetricsReporter>>,
    start_time: Instant,
    is_done: bool,  // Track if iterator is exhausted
}

impl<P: ParallelLogReplayProcessor> ParallelPhase<P> {
    pub(crate) fn try_new(
        engine: Arc<dyn Engine>,
        processor: P,
        leaf_files: Vec<FileMeta>,
        metrics_reporter: Option<Arc<dyn MetricsReporter>>,  // NEW parameter
    ) -> DeltaResult<Self> { ... }

    /// Get accumulated metrics from the processor.
    pub(crate) fn metrics(&self) -> &LogReplayMetrics {
        self.processor.metrics()
    }
}

impl<P: ParallelLogReplayProcessor> Iterator for ParallelPhase<P> {
    type Item = DeltaResult<P::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        // Once done, always return None
        if self.is_done {
            return None;
        }

        let result = self.leaf_checkpoint_reader.next().map(|batch_res| {
            batch_res.and_then(|batch| self.processor.process_actions_batch(batch))
        });

        // Emit metrics when iterator is exhausted (first None)
        if result.is_none() {
            self.is_done = true;
            self.metrics_reporter.as_ref().inspect(|r| {
                let m = self.processor.metrics();
                r.report(MetricEvent::ParallelPhaseCompleted {
                    duration: self.start_time.elapsed(),
                    total_metadata_rows: m.total_metadata_rows,
                    rows_from_json: m.rows_from_json,
                    rows_from_checkpoint: m.rows_from_checkpoint,
                    non_file_action_rows: m.non_file_action_rows,
                    // ... other fields from metrics ...
                });
            });
        }

        result
    }
}
```

### 7. Update scan_metadata to use phases internally (future)

When `scan_metadata` is unified to use `SequentialPhase` + `ParallelPhase`:

```rust
pub fn scan_metadata(&self, engine: &dyn Engine) -> DeltaResult<impl Iterator<...>> {
    let reporter = engine.get_metrics_reporter();
    let start_time = Instant::now();

    // Create phases WITHOUT metrics reporter (they won't emit)
    let mut sequential = SequentialPhase::try_new(processor, log_segment, engine.clone(), None)?;

    // Collect sequential results
    let mut results: Vec<_> = sequential.by_ref().collect();

    let final_metrics = match sequential.finish()? {
        AfterSequential::Done(processor) => processor.metrics().clone(),
        AfterSequential::Parallel { processor, files } => {
            let mut parallel = ParallelPhase::try_new(engine, processor, files, None)?;

            // Collect parallel results
            results.extend(parallel.by_ref());

            // Extract metrics via accessor method
            parallel.metrics().clone()
        }
    };

    // Emit combined metrics (future - when scan_metadata is unified)
    reporter.as_ref().inspect(|r| {
        r.report(MetricEvent::ScanMetadataCompleted {
            duration: start_time.elapsed(),
            total_metadata_rows: final_metrics.total_metadata_rows,
            rows_from_json: final_metrics.rows_from_json,
            rows_from_checkpoint: final_metrics.rows_from_checkpoint,
            non_file_action_rows: final_metrics.non_file_action_rows,
            // ... other fields ...
        });
    });

    Ok(results.into_iter())
}
```

Note: `ParallelPhase` needs a `metrics(&self) -> &LogReplayMetrics` method that delegates to the processor.

## Usage Patterns

### Distributed execution (phases emit their own metrics)
```rust
let reporter = engine.get_metrics_reporter();
let mut sequential = SequentialPhase::try_new(processor, log_segment, engine, reporter.clone())?;
for result in sequential.by_ref() { /* ... */ }
sequential.finish()?;  // Emits SequentialPhaseCompleted

// On worker nodes:
let mut parallel = ParallelPhase::try_new(engine, processor, files, reporter)?;
for result in parallel.by_ref() { /* ... */ }
// Emits ParallelPhaseCompleted automatically when iterator returns None
```

### Single-node unified (future - scan_metadata emits combined)
```rust
// scan_metadata creates phases with None reporter (they don't emit)
// scan_metadata runs both phases, extracts processor.metrics()
// scan_metadata emits single ScanMetadataCompleted event
```

## AfterSequential Enum (unchanged)

```rust
pub enum AfterSequential<P: LogReplayProcessor> {
    Done(P),
    Parallel { processor: P, files: Vec<FileMeta> },
}
```

No changes needed - the processor carries the metrics internally.

## Verification

1. `cargo build -p delta_kernel`
2. `cargo test -p delta_kernel parallel`
3. `cargo clippy -p delta_kernel`
