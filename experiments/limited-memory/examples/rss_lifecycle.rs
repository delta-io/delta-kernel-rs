//! Fresh-process RSS measurements with one shared budget and unchanged lifecycle calls.

#[cfg(feature = "declarative-benchmark")]
mod declarative;
#[cfg(target_os = "macos")]
mod rss_memory;
mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::memory::MemoryBudget;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{Snapshot, try_parse_uri};
use serde::Serialize;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static WINDOW_PEAK: AtomicUsize = AtomicUsize::new(0);

struct Tracking;

fn added(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
    WINDOW_PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: Storage and layouts are forwarded unchanged to System. Tracking does not allocate.
unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            added(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            added(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, size) };
        if !ptr.is_null() {
            if size >= layout.size() {
                added(size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - size, Ordering::Relaxed);
            }
        }
        ptr
    }
}

#[global_allocator]
static ALLOCATOR: Tracking = Tracking;

#[derive(Clone, Copy, Default, Serialize)]
struct Observation {
    elapsed_us: u128,
    rss_bytes: usize,
    os_peak_rss_bytes: usize,
    rust_requested_live: usize,
    rust_requested_peak: usize,
    limited_live: usize,
    limited_peak: usize,
}

impl Observation {
    fn get(start: Instant) -> Self {
        let usage = MemoryBudget::global_usage();
        Self {
            elapsed_us: start.elapsed().as_micros(),
            rss_bytes: current_rss(),
            os_peak_rss_bytes: peak_rss(),
            rust_requested_live: LIVE.load(Ordering::Relaxed),
            rust_requested_peak: PEAK.load(Ordering::Relaxed),
            limited_live: usage.map_or(0, |usage| usage.reserved),
            limited_peak: usage.map_or(0, |usage| usage.peak),
        }
    }
}

#[derive(Serialize)]
struct Stage {
    worker: usize,
    name: &'static str,
    // Global window resets are meaningful only with one workload thread.
    rust_requested_peak_since_previous_stage: Option<usize>,
    #[serde(flatten)]
    observation: Observation,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Outcome {
    Imperative((usize, u64)),
    #[cfg(feature = "declarative-benchmark")]
    Declarative(declarative::Outcome),
}

#[derive(Default, Serialize)]
struct Samples {
    count: usize,
    at_max_sampled_rss: Observation,
    at_max_sampled_rust_live: Observation,
    at_max_sampled_limited_live: Observation,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("prepare") {
        let width: usize = args[1].parse()?;
        let tail: u64 = args[2].parse()?;
        let initial: usize = args[3].parse()?;
        let stats_bytes = args
            .get(4)
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(0);
        let stats_columns: i64 = args
            .get(5)
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(0);
        let parsed_stats = args.get(6).is_some_and(|value| value == "1");
        let padded = (width == 10_000).then_some(11_301_270);
        let fixture = if stats_columns != 0 || parsed_stats {
            support::Fixture::create_with_options(
                width,
                initial,
                tail,
                padded,
                stats_bytes,
                stats_columns,
                parsed_stats,
            )?
        } else if stats_bytes == 0 {
            support::Fixture::create(width, initial, tail, padded)?
        } else {
            support::Fixture::create_with_action_size(width, initial, tail, padded, stats_bytes)?
        };
        let report = serde_json::json!({
            "path": fixture.directory.path(), "table_root": fixture.table_root,
            "width": fixture.width, "schema_fields": fixture.width * 3,
            "schema_json_bytes": fixture.schema_bytes,
            "initial_files": fixture.initial_files, "tail_commits": fixture.tail_commits,
            "checkpoints": 1,
            "action_stats_bytes": stats_bytes,
            "indexed_stats_columns": stats_columns, "checkpoint_parsed_stats": parsed_stats,
        });
        let _ = fixture.directory.keep();
        println!("{report}");
        return Ok(());
    }
    let root = Path::new(
        args.get(1)
            .ok_or("usage: worker root cap_bytes concurrency")?,
    );
    let cap: usize = args.get(2).ok_or("missing cap")?.parse()?;
    let concurrency: usize = args.get(3).ok_or("missing concurrency")?.parse()?;
    if concurrency == 0 {
        return Err("concurrency must be positive".into());
    }
    if cap != 0 {
        MemoryBudget::install_global(cap)?;
    }
    let start = Instant::now();
    let cold_baseline = Observation::get(start);
    let warmup = root.join("table-warmup");
    let warmed = warmup.is_dir();
    if warmed {
        let url = try_parse_uri(warmup.to_str().ok_or("warmup path")?)?;
        lifecycle(url.as_str(), &|_| {})?;
    }
    let gate = Arc::new(Barrier::new(concurrency + 2));
    let stop = Arc::new(AtomicBool::new(false));
    let stages = Arc::new(Mutex::new(Vec::with_capacity(concurrency * 16)));
    let sample_thread = {
        let gate = gate.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            gate.wait();
            let mut samples = Samples::default();
            while !stop.load(Ordering::Relaxed) {
                let observation = Observation::get(start);
                samples.count += 1;
                if observation.rss_bytes >= samples.at_max_sampled_rss.rss_bytes {
                    samples.at_max_sampled_rss = observation;
                }
                if observation.rust_requested_live
                    >= samples.at_max_sampled_rust_live.rust_requested_live
                {
                    samples.at_max_sampled_rust_live = observation;
                }
                if observation.limited_live >= samples.at_max_sampled_limited_live.limited_live {
                    samples.at_max_sampled_limited_live = observation;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            samples
        })
    };
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let table = try_parse_uri(
            root.join(format!("table-{worker}"))
                .to_str()
                .ok_or("path")?,
        )?;
        let gate = gate.clone();
        let stages = stages.clone();
        workers.push(std::thread::spawn(move || {
            gate.wait();
            let record = |name| {
                let observation = Observation::get(start);
                let window = (concurrency == 1)
                    .then(|| WINDOW_PEAK.swap(LIVE.load(Ordering::Relaxed), Ordering::Relaxed));
                stages.lock().unwrap().push(Stage {
                    worker,
                    name,
                    rust_requested_peak_since_previous_stage: window,
                    observation,
                });
                if matches!(
                    name,
                    "declarative_metadata_plan_proto_live_no_execution"
                        | "declarative_write_state_encoded_live"
                        | "owners_dropped"
                ) {
                    capture_vm_map(name).unwrap();
                }
            };
            let result = lifecycle(table.as_str(), &record);
            record("owners_dropped");
            result.map_err(|error| error.to_string())
        }));
    }
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    WINDOW_PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    let baseline = Observation::get(start);
    gate.wait();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    let cleaned = Observation::get(start);
    stop.store(true, Ordering::Relaxed);
    let samples = sample_thread.join().unwrap();
    capture_vm_map("after_threads_joined")?;
    let before_relief = Observation::get(start);
    let relieved_bytes = allocator_relief();
    let after_relief = Observation::get(start);
    let usage = MemoryBudget::global_usage();
    let report = serde_json::json!({
        "kind": "fresh_process_rss", "cap_bytes": cap, "concurrency": concurrency,
        "mode": std::env::var("KERNEL_MEMORY_MODE").unwrap_or_else(|_| "imperative".into()),
        "warmup_enabled": warmed, "cold_baseline": cold_baseline,
        "vm_maps_enabled": std::env::var_os("KERNEL_MEMORY_VM_MAPS").is_some(),
        "baseline": baseline, "after_cleanup": cleaned, "samples": samples,
        "before_allocator_relief": before_relief, "after_allocator_relief": after_relief,
        "allocator_reported_relieved_bytes": relieved_bytes,
        "workload_elapsed_us": cleaned.elapsed_us - baseline.elapsed_us,
        "limited_rejections": usage.map_or(0, |usage| usage.rejected),
        "results": results, "stages": *stages.lock().unwrap(),
        "measurement": "No DHAT. RSS is OS resident bytes; Rust counts are requested layouts. Samples are adjacent atomic/OS observations, not a transactionally consistent snapshot. Fixture preparation and copies ran in other processes. A zero cap disables participating limits.",
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn capture_vm_map(stage: &str) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    if let Some(directory) = std::env::var_os("KERNEL_MEMORY_VM_MAPS") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory)?;
        rss_memory::capture(&directory, stage)?;
    }
    #[cfg(not(target_os = "macos"))]
    let _ = stage;
    Ok(())
}

fn lifecycle(root: &str, record: &impl Fn(&'static str)) -> delta_kernel::Result<Outcome> {
    if std::env::var("KERNEL_MEMORY_MODE").as_deref() == Ok("declarative") {
        #[cfg(feature = "declarative-benchmark")]
        {
            return declarative::run(root, &mut |name| record(name)).map(Outcome::Declarative);
        }
        #[cfg(not(feature = "declarative-benchmark"))]
        {
            return Err(
                delta_kernel::KernelError::unsupported("build with declarative-benchmark").into(),
            );
        }
    }
    imperative_lifecycle(root, record).map(Outcome::Imperative)
}

fn imperative_lifecycle(
    root: &str,
    record: &impl Fn(&'static str),
) -> delta_kernel::Result<(usize, u64)> {
    let engine = SyncEngine::new();
    #[cfg(feature = "declarative-benchmark")]
    let engine =
        delta_kernel::engine::DelegatingEngine::new(Arc::new(engine)).without_plan_executor();
    let snapshot = Snapshot::builder_for(root).build(&engine)?;
    record("snapshot");
    let scan = snapshot.clone().scan_builder().build()?;
    let mut files = 0;
    for batch in scan.scan_metadata(&engine)? {
        files = batch?.visit_scan_files(files, |count, _| *count += 1)?;
    }
    record("scan_consumed");
    let transaction = snapshot
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "rss-poc", false)
        .with_table_properties_added([("poc.run", "true")])
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    record("transaction_built");
    let state = transaction.write_state()?;
    let encoded = state.encode()?;
    record("write_state_encoded");
    drop((encoded, state));
    let committed = transaction.commit(&engine)?.unwrap_committed();
    record("transaction_committed");
    Ok((files, committed.commit_version()))
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn current_rss() -> usize {
    let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::zeroed();
    let mut count = (size_of::<libc::mach_task_basic_info>() / size_of::<libc::natural_t>()) as u32;
    // SAFETY: task_info receives a correctly sized writable buffer for this process's task.
    let status = unsafe {
        libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast(),
            &mut count,
        )
    };
    assert_eq!(status, libc::KERN_SUCCESS);
    unsafe { info.assume_init() }.resident_size as usize
}

#[cfg(target_os = "linux")]
fn current_rss() -> usize {
    let status = std::fs::read_to_string("/proc/self/statm").unwrap();
    let pages: usize = status.split_whitespace().nth(1).unwrap().parse().unwrap();
    pages * unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize
}

fn peak_rss() -> usize {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage receives a correctly sized writable rusage buffer.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let peak = unsafe { usage.assume_init() }.ru_maxrss as usize;
    if cfg!(target_os = "macos") {
        peak
    } else {
        peak * 1024
    }
}

#[cfg(target_os = "macos")]
fn allocator_relief() -> usize {
    unsafe extern "C" {
        fn malloc_zone_pressure_relief(zone: *mut std::ffi::c_void, goal: usize) -> usize;
    }
    // SAFETY: A null zone requests pressure relief from all malloc zones; zero requests all
    // reclaimable pages. No application pointers or allocation ownership are changed.
    unsafe { malloc_zone_pressure_relief(std::ptr::null_mut(), 0) }
}

#[cfg(not(target_os = "macos"))]
fn allocator_relief() -> usize {
    0
}
