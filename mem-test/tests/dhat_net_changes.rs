//! Measures NetChanges listing time and heap use for repeated deletion-vector updates.
//! Uses synthetic logs without reading Parquet data or deletion-vector payloads.
//!
//! Run with `cargo test -p mem-test --test dhat_net_changes --release -- --ignored --nocapture`.

use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::Instant;

use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::table_changes::{TableChanges, TableChangesListingMode};
use delta_kernel_default_engine::DefaultEngineBuilder;
use serde_json::{json, Value};
use tempfile::tempdir;
use url::Url;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn dv(version: usize) -> Value {
    json!({"storageType": "u", "pathOrInlineDv": "aaaaaaaaaaaaaaaaaaaa",
        "offset": version + 1, "sizeInBytes": 8, "cardinality": 1})
}

#[ignore = "mem-test - run manually"]
#[test]
fn test_dhat_net_changes() -> Result<(), Box<dyn std::error::Error>> {
    // Distinct file paths and updates per path; NET_COMMITS=1 measures a single update per path.
    let paths: usize = std::env::var("NET_PATHS")
        .unwrap_or("1000".into())
        .parse()?;
    let commits: usize = std::env::var("NET_COMMITS")
        .unwrap_or("100".into())
        .parse()?;
    if paths == 0 || commits == 0 {
        return Err("paths and commits must be positive".into());
    }
    let dir = tempdir()?;
    let log = dir.path().join("_delta_log");
    create_dir_all(&log)?;
    // Version 0 initializes the table; versions 1..=commits are the measured change range.
    for version in 0..=commits {
        let mut file = BufWriter::new(File::create(log.join(format!("{version:020}.json")))?);
        if version == 0 {
            writeln!(
                file,
                "{}",
                json!({"protocol": {"minReaderVersion": 3,
                "minWriterVersion": 7, "readerFeatures": ["deletionVectors"],
                "writerFeatures": ["rowTracking", "domainMetadata", "deletionVectors"]}})
            )?;
            writeln!(
                file,
                "{}",
                json!({"metaData": {"id": "net-changes-benchmark",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#,
                "partitionColumns": [], "configuration": {"delta.enableRowTracking": "true",
                    "delta.rowTracking.materializedRowIdColumnName": "_row_id",
                    "delta.rowTracking.materializedRowCommitVersionColumnName": "_row_version"}}})
            )?;
        }
        if version == 0 {
            writeln!(
                file,
                "{}",
                json!({"domainMetadata": {"domain": "delta.rowTracking",
                "configuration": json!({"rowIdHighWaterMark": paths * 10 - 1}).to_string(),
                "removed": false}})
            )?;
        }
        for path in 0..paths {
            let name = format!("part-{path:08}.parquet");
            if version > 0 {
                writeln!(
                    file,
                    "{}",
                    json!({"remove": {"path": name,
                    "dataChange": true, "deletionTimestamp": version,
                    "deletionVector": dv(version - 1), "size": 100,
                    "partitionValues": {}, "extendedFileMetadata": true,
                    "baseRowId": path * 10, "defaultRowCommitVersion": 0}})
                )?;
            }
            writeln!(
                file,
                "{}",
                json!({"add": {"path": name, "partitionValues": {},
                "size": 100, "modificationTime": version, "dataChange": true,
                "deletionVector": dv(version), "stats": r#"{"numRecords":10}"#, "baseRowId": path * 10,
                "defaultRowCommitVersion": 0}})
            )?;
        }
    }
    let engine = Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let changes = Arc::new(TableChanges::try_new_row_tracking_cdf_listing(
        Url::from_directory_path(dir.path()).map_err(|_| "invalid table path")?,
        engine.as_ref(),
        1,
        Some(commits as u64),
    )?);
    // Disable DHAT with NET_PROFILE=0 for timing runs.
    let profile = std::env::var("NET_PROFILE").as_deref() != Ok("0");
    // Discard warmup iterations, then record samples using the same table and engine.
    let warmup: usize = std::env::var("NET_WARMUP").unwrap_or("0".into()).parse()?;
    let samples: usize = std::env::var("NET_SAMPLES").unwrap_or("1".into()).parse()?;
    if samples == 0 || (profile && (warmup != 0 || samples != 1)) {
        return Err(
            "profile one sample without warmup; time repeated samples with NET_PROFILE=0".into(),
        );
    }
    for iteration in 0..warmup + samples {
        // Include log parsing/replay and result collection, but exclude table setup and validation.
        let profiler = profile.then(|| dhat::Profiler::builder().testing().build());
        let started = Instant::now();
        let listing = changes
            .clone()
            .scan_file_listing(engine.clone(), TableChangesListingMode::NetChanges)?
            .collect::<Result<Vec<_>, _>>()?;
        let elapsed = started.elapsed();
        let live = profile.then(dhat::HeapStats::get);
        let output = listing.len();
        if output != paths {
            return Err("incorrect output path count".into());
        }
        for (index, action) in listing.into_iter().enumerate() {
            let remove = action.remove.ok_or("expected remove boundary")?;
            let add = action.add.ok_or("expected add boundary")?;
            let expected_path = format!("part-{index:08}.parquet");
            if remove.commit_version != 1
                || add.commit_version != commits as i64
                || remove.path != expected_path
                || add.path != expected_path
            {
                return Err("incorrect boundary or output ordering".into());
            }
        }
        let final_stats = profile.then(dhat::HeapStats::get);
        drop(profiler);
        if iteration < warmup {
            continue;
        }
        print!("sample={} paths={paths} commits={commits} raw_actions={} output={output} elapsed_ms={:.6}",
            iteration - warmup + 1, paths * commits * 2, elapsed.as_secs_f64() * 1000.0);
        if let (Some(live), Some(final_stats)) = (live, final_stats) {
            print!(
                " peak_bytes={} live_result_bytes={} end_bytes={} allocated_bytes={}",
                live.max_bytes, live.curr_bytes, final_stats.curr_bytes, live.total_bytes
            );
        }
        println!();
    }
    Ok(())
}
