//! Exercises actual snapshot loading, scan replay, write-state encoding, and transaction commit.

#[cfg(feature = "declarative-benchmark")]
mod declarative;
mod support;

use std::time::Instant;

use delta_kernel::Snapshot;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::memory::KERNEL_ALLOCATOR;
use delta_kernel::transaction::UpdateTableOperation;
use serde::Serialize;

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

#[derive(Serialize)]
struct Stage {
    name: &'static str,
    elapsed_ns: u128,
    rust_heap_live: Option<usize>,
    rust_heap_peak: Option<usize>,
    kernel_live: usize,
    kernel_peak: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // width, tail commits, initial files, cap MiB; zero disables the limit.
    let args: Vec<_> = std::env::args()
        .skip(1)
        .map(|s| s.parse::<usize>())
        .collect::<Result<_, _>>()?;
    let width = args.first().copied().unwrap_or(100);
    let tail = args.get(1).copied().unwrap_or(100);
    let initial = args.get(2).copied().unwrap_or(100);
    let cap = args.get(3).copied().unwrap_or(256);
    let padded = (width == 10_000).then_some(11_301_270);
    let external = std::env::var("KERNEL_MEMORY_TABLE_ROOT").ok();
    let fixture = if external.is_none() {
        Some(support::Fixture::create(
            width,
            initial,
            tail as u64,
            padded,
        )?)
    } else {
        None
    };
    let root = external
        .as_deref()
        .or_else(|| fixture.as_ref().map(|fixture| fixture.table_root.as_str()))
        .ok_or("missing table root")?;
    let limit = cap.checked_mul(1024 * 1024).ok_or("limit overflow")?;
    KERNEL_ALLOCATOR.set_limit((cap != 0).then_some(limit))?;
    let stop_after = std::env::var("KERNEL_MEMORY_STOP_AFTER").unwrap_or_default();
    let mut stages = Vec::with_capacity(8);
    std::fs::create_dir_all("target")?;
    let profile_path = std::env::var_os("KERNEL_MEMORY_PROFILE")
        .unwrap_or_else(|| "target/kernel-lifecycle.dhat.json".into());
    let profiling = std::env::var_os("KERNEL_MEMORY_NO_PROFILE").is_none();
    let profile_from = std::env::var("KERNEL_MEMORY_PROFILE_FROM_STAGE").unwrap_or_default();
    let mut profile = (profiling && profile_from.is_empty())
        .then(|| dhat::Profiler::builder().file_name(&profile_path).build());
    let mut start = Instant::now();
    let mut record = |name| {
        eprintln!("completed {name}");
        if profiling && profile.is_none() && name == profile_from {
            profile = Some(dhat::Profiler::builder().file_name(&profile_path).build());
        }
        let heap = profile.as_ref().map(|_| dhat::HeapStats::get());
        let usage = KERNEL_ALLOCATOR.usage();
        stages.push(Stage {
            name,
            elapsed_ns: start.elapsed().as_nanos(),
            rust_heap_live: heap.as_ref().map(|heap| heap.curr_bytes),
            rust_heap_peak: heap.as_ref().map(|heap| heap.max_bytes),
            kernel_live: usage.reserved,
            kernel_peak: usage.peak,
        });
        start = Instant::now();
    };
    let mut run = || -> Result<_, delta_kernel::Error> {
        if std::env::var("KERNEL_MEMORY_MODE").as_deref() == Ok("declarative") {
            #[cfg(feature = "declarative-benchmark")]
            {
                return declarative::run(root, &mut record)
                    .map(|outcome| serde_json::json!(outcome));
            }
            #[cfg(not(feature = "declarative-benchmark"))]
            {
                return Err(
                    delta_kernel::KernelError::generic("build with declarative-benchmark").into(),
                );
            }
        }
        let engine = SyncEngine::new();
        let builder = Snapshot::builder_for(root);
        let snapshot = builder.build(&engine)?;
        record("snapshot");
        if stop_after == "snapshot" {
            return Ok(serde_json::json!((0, snapshot.version())));
        }
        let scan = snapshot.clone().scan_builder().build()?;
        let mut files = 0;
        for batch in scan.scan_metadata(&engine)? {
            files = batch?.visit_scan_files(files, |count, _| *count += 1)?;
        }
        record("scan_consumed");
        if stop_after == "scan_consumed" {
            return Ok(serde_json::json!((files, snapshot.version())));
        }
        let transaction = snapshot
            .clone()
            .update_table(UpdateTableOperation::AlterTable, "memory-poc", false)
            .with_table_properties_added([("poc.run", "true")])
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        record("transaction_built");
        if stop_after == "transaction_built" {
            return Ok(serde_json::json!((files, snapshot.version())));
        }
        let state = transaction.write_state()?;
        let encoded = state.encode()?;
        record("write_state_encoded");
        if stop_after == "write_state_encoded" {
            return Ok(serde_json::json!((files, snapshot.version())));
        }
        drop(encoded);
        drop(state);
        let committed = transaction.commit(&engine)?.unwrap_committed();
        record("transaction_committed");
        let version = committed.commit_version();
        drop((committed, scan, snapshot, engine));
        record("owners_dropped");
        Ok(serde_json::json!((files, version)))
    };
    let result = run();
    let usage = KERNEL_ALLOCATOR.usage();
    let heap = profile.as_ref().map(|_| dhat::HeapStats::get());
    let report = serde_json::json!({
        "kind": "real_kernel_lifecycle", "width": fixture.as_ref().map_or(width, |fixture| fixture.width),
        "schema_json_bytes": fixture.as_ref().map(|fixture| fixture.schema_bytes).or(padded),
        "initial_files": fixture.as_ref().map_or(initial, |fixture| fixture.initial_files),
        "tail_commits": fixture.as_ref().map_or(tail as u64, |fixture| fixture.tail_commits),
        "in_process_fixture_path": fixture.as_ref().map(|fixture| fixture.directory.path()),
        "fixture": root, "limit_enabled": cap != 0,
        "fixture_details": fixture.as_ref().map(support::Fixture::description),
        "stop_after": stop_after,
        "mode": std::env::var("KERNEL_MEMORY_MODE").unwrap_or_else(|_| "imperative".into()),
        "profile_from_stage": profile_from,
        "limit_bytes": usage.limit, "peak_reserved_bytes": usage.peak,
        "reserved_after_cleanup": usage.reserved, "rejections": usage.rejected,
        "heap_profile_enabled": profiling,
        "rust_heap_peak": heap.as_ref().map(|heap| heap.max_bytes),
        "rust_heap_after_cleanup": heap.as_ref().map(|heap| heap.curr_bytes),
        "result": result.as_ref().ok(), "error": result.as_ref().err().map(ToString::to_string),
        "stages": stages,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    result.map(|_| ()).map_err(Into::into)
}
