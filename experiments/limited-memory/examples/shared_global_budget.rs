//! Demonstrates a singleton budget shared by independent snapshot loads on multiple threads.

mod support;

use std::sync::{Barrier, OnceLock};

use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::memory::MemoryBudget;
use delta_kernel::{Error, KernelError, Snapshot, SnapshotRef};
use serde_json::json;

static KERNEL_BUDGET: OnceLock<MemoryBudget> = OnceLock::new();

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = support::Fixture::create(10, 10, 10, Some(65_536))?;
    let probe = MemoryBudget::new(8 * 1024 * 1024);
    let snapshot = Snapshot::builder_for(&fixture.table_root)
        .with_allocator(probe.allocator())
        .build(&SyncEngine::new())?;
    let retained = probe.usage().reserved;
    drop(snapshot);
    let limit = probe.usage().peak * 2;
    if retained * 4 <= limit {
        return Err("fixture cannot force contention with four retained snapshots".into());
    }
    if KERNEL_BUDGET.set(MemoryBudget::new(limit)).is_err() {
        return Err("budget already initialized".into());
    }
    let budget = KERNEL_BUDGET.get().ok_or("budget not initialized")?;
    let retained_snapshot = load_snapshot(&fixture.table_root)?;
    let start = Barrier::new(3);
    let finish = Barrier::new(3);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let table_root = &fixture.table_root;
                let start = &start;
                let finish = &finish;
                scope.spawn(move || {
                    start.wait();
                    let result = load_snapshot(table_root);
                    finish.wait();
                    result
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join()).collect::<Vec<_>>()
    });
    let mut successes = 1;
    let mut failures = 0;
    for result in results {
        match result.map_err(|_| "snapshot worker panicked")? {
            Ok(snapshot) => {
                successes += 1;
                drop(snapshot);
            }
            Err(Error::Kernel(error))
                if matches!(error.without_backtrace(), KernelError::Memory(_)) =>
            {
                failures += 1;
            }
            Err(error) => return Err(error.into()),
        }
    }
    drop(retained_snapshot);
    let usage = budget.usage();
    if failures == 0 || usage.peak > limit || usage.reserved != 0 {
        return Err("shared budget invariant failed".into());
    }
    // Read all fixture fields so this example also compiles with warnings denied.
    println!(
        "{}",
        json!({
            "successes": successes, "failures": failures, "limit": usage.limit,
            "peak": usage.peak, "live_after_cleanup": usage.reserved,
            "schema_bytes": fixture.schema_bytes, "width": fixture.width,
            "initial_files": fixture.initial_files, "tail_commits": fixture.tail_commits,
            "fixture_exists": fixture.directory.path().exists(),
        })
    );
    Ok(())
}

fn load_snapshot(table_root: &str) -> delta_kernel::Result<SnapshotRef> {
    let budget = KERNEL_BUDGET
        .get()
        .ok_or_else(|| KernelError::generic("Kernel memory budget is not initialized"))?;
    Snapshot::builder_for(table_root)
        .with_allocator(budget.allocator())
        .build(&SyncEngine::new())
}
