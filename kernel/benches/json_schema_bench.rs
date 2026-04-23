//! Benchmark comparing JSON parse cost for two schema projection strategies:
//!
//! - **Strategy A (P&M only):** Parse only Protocol and Metadata (today's `replay_for_pm` path).
//! - **Strategy B (P&M + extras):** Also parse SetTransaction, DomainMetadata, Add.size, Remove.size.
//!
//! The goal is to measure whether the extra schema fields add meaningful overhead, given that
//! Arrow's JSON reader always tokenizes the entire JSON line (Pass 1) regardless of schema.
//!
//! ## Usage
//!
//! ```bash
//! # Step 1: Generate test files to ~/tmp/json_bench/ (run once)
//! cargo bench --bench json_schema_bench --all-features -- --generate
//!
//! # Step 2: Run the benchmark
//! cargo bench --bench json_schema_bench --all-features
//!
//! # Run only a specific size
//! cargo bench --bench json_schema_bench --all-features -- "1k_adds"
//! ```

use std::collections::HashSet;
use std::fs;
use std::hint::black_box;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use criterion::{criterion_group, BenchmarkId, Criterion};
use url::Url;

use delta_kernel::actions::{
    Add, DomainMetadata, Metadata, Protocol, Remove, SetTransaction, ADD_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, SET_TRANSACTION_NAME,
};
use delta_kernel::engine::default::storage::store_from_url;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::engine_data::{GetData, RowVisitor, TypedGetData};
use delta_kernel::expressions::{column_name, ColumnName};
use delta_kernel::schema::{
    ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType, ToSchema,
};
use delta_kernel::{Engine, FileMeta};

const REMOVE_NAME: &str = "remove";

// ---------------------------------------------------------------------------
// JSON generators (one line per action)
// ---------------------------------------------------------------------------

fn commit_info_json() -> String {
    r#"{"commitInfo":{"timestamp":1700000000000,"operation":"WRITE","operationParameters":{}}}"#
        .to_string()
}

fn protocol_json() -> String {
    r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors","domainMetadata"]}}"#
        .to_string()
}

fn metadata_json() -> String {
    let schema_string = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"active\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]}"#;
    format!(
        r#"{{"metaData":{{"id":"bench-table-id","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":["date"],"configuration":{{"delta.enableDeletionVectors":"true"}},"createdTime":1700000000000}}}}"#,
        schema_string
    )
}

fn set_transaction_json(app_id: &str, version: i64) -> String {
    format!(
        r#"{{"txn":{{"appId":"{}","version":{},"lastUpdated":1700000000000}}}}"#,
        app_id, version,
    )
}

fn domain_metadata_json(domain: &str, config: &str) -> String {
    format!(
        r#"{{"domainMetadata":{{"domain":"{}","configuration":"{}","removed":false}}}}"#,
        domain, config,
    )
}

fn add_json(index: usize) -> String {
    format!(
        r#"{{"add":{{"path":"date=2024-01-15/part-{index:05}-abc123.parquet","partitionValues":{{"date":"2024-01-15"}},"size":{},"modificationTime":1700000000000,"dataChange":true,"stats":"{{\"numRecords\":1000,\"minValues\":{{\"id\":0}},\"maxValues\":{{\"id\":999}},\"nullCount\":{{\"id\":0}}}}"}}}}"#,
        1000 + index,
    )
}

fn remove_json(index: usize) -> String {
    format!(
        r#"{{"remove":{{"path":"date=2024-01-14/part-old-{index:05}-def456.parquet","deletionTimestamp":1700000000000,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{{"date":"2024-01-14"}},"size":{}}}}}"#,
        500 + index,
    )
}

// ---------------------------------------------------------------------------
// File generation
// ---------------------------------------------------------------------------

fn bench_dir() -> PathBuf {
    let home = std::env::var("HOME").expect("HOME env var not set");
    PathBuf::from(home).join("tmp/json_bench")
}

/// Generate a single commit file with adds/removes (and optionally P+M+SetTxn+DomainMetadata).
/// `add_offset` is used to generate globally unique paths across files.
fn generate_commit_bytes(
    num_adds: usize,
    num_removes: usize,
    add_offset: usize,
    include_pm: bool,
    total_across_all_files: usize,
) -> Vec<u8> {
    let report_interval = (total_across_all_files / 10).max(1000);
    let mut lines = Vec::with_capacity(num_adds + num_removes + if include_pm { 203 } else { 1 });

    lines.push(commit_info_json());
    if include_pm {
        lines.push(protocol_json());
        lines.push(metadata_json());
        for i in 0..100 {
            lines.push(set_transaction_json(&format!("app-{i}"), i));
        }
        for i in 0..100 {
            lines.push(domain_metadata_json(
                &format!("domain-{i}"),
                &format!("{{\\\"key\\\":\\\"{i}\\\"}}"),
            ));
        }
    }

    for i in 0..num_adds {
        let global_idx = add_offset + i;
        lines.push(add_json(global_idx));
        if global_idx > 0 && global_idx % report_interval == 0 {
            let pct = (global_idx as f64 / total_across_all_files as f64 * 100.0) as u32;
            eprintln!("  ...{pct}% ({global_idx} adds written)");
        }
    }

    for i in 0..num_removes {
        lines.push(remove_json(add_offset + i));
    }

    lines.join("\n").into_bytes()
}

fn human_bytes(bytes: usize) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{bytes} B")
    }
}

fn human_count(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000 {
        format!("{}K", n / 1_000)
    } else {
        format!("{n}")
    }
}

fn generate_all_files() {
    let base = bench_dir();
    let add_counts: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];
    let file_counts: &[usize] = &[1, 10, 100];
    let total_cases = file_counts.len() * add_counts.len();
    let mut case_num = 0;

    eprintln!("=== Generating {total_cases} benchmark cases to {} ===\n", base.display());

    for &num_files in file_counts {
        for &total_adds in add_counts {
            case_num += 1;
            let total_removes = total_adds / 10;
            let label = format!("{num_files}f_{total_adds}a");
            let dir = base.join(&label).join("_delta_log");
            let first_file = dir.join("00000000000000000000.json");
            if first_file.exists() {
                eprintln!(
                    "[{case_num}/{total_cases}] {label}: already exists, skipping"
                );
                continue;
            }
            fs::create_dir_all(&dir).unwrap();

            let adds_per_file = total_adds / num_files;
            let removes_per_file = total_removes / num_files;
            let total_actions = total_adds + total_removes + 203;
            eprintln!(
                "[{case_num}/{total_cases}] {label}: generating {} adds + {} removes across {num_files} file(s) ({} adds/file)...",
                human_count(total_adds),
                human_count(total_removes),
                human_count(adds_per_file),
            );

            let mut total_bytes = 0usize;
            for file_idx in 0..num_files {
                let add_offset = file_idx * adds_per_file;
                let include_pm = file_idx == 0;
                let data = generate_commit_bytes(
                    adds_per_file,
                    removes_per_file,
                    add_offset,
                    include_pm,
                    total_actions,
                );
                let path = dir.join(format!("{file_idx:020}.json"));
                total_bytes += data.len();
                fs::write(&path, &data).unwrap();

                // Per-file progress for multi-file cases with large files
                if num_files > 1 && adds_per_file >= 10_000 && (file_idx + 1) % (num_files / 5).max(1) == 0 {
                    eprintln!(
                        "  wrote file {}/{num_files} ({})",
                        file_idx + 1,
                        human_bytes(total_bytes),
                    );
                }
            }
            eprintln!(
                "  done: {num_files} file(s), {} total\n",
                human_bytes(total_bytes),
            );
        }
    }
    eprintln!("=== All {total_cases} cases generated ===");
}

// ---------------------------------------------------------------------------
// Schema builders
// ---------------------------------------------------------------------------

fn pm_only_schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        Arc::new(StructType::new_unchecked([
            StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
            StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        ]))
    });
    SCHEMA.clone()
}

fn pm_plus_extras_schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        let narrow_add = StructType::new_unchecked(
            Add::to_schema()
                .fields()
                .filter(|f: &&StructField| f.name() == "size")
                .cloned()
                .collect::<Vec<_>>(),
        );
        let narrow_remove = StructType::new_unchecked(
            Remove::to_schema()
                .fields()
                .filter(|f: &&StructField| f.name() == "size")
                .cloned()
                .collect::<Vec<_>>(),
        );
        Arc::new(StructType::new_unchecked([
            StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
            StructField::nullable(METADATA_NAME, Metadata::to_schema()),
            StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
            StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
            StructField::nullable(ADD_NAME, narrow_add),
            StructField::nullable(REMOVE_NAME, narrow_remove),
        ]))
    });
    SCHEMA.clone()
}

// ---------------------------------------------------------------------------
// PmVisitor: finds Protocol and Metadata (like MetadataVisitor + ProtocolVisitor)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PmVisitor {
    found_protocol: bool,
    found_metadata: bool,
}

impl RowVisitor for PmVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // We only need one sentinel column from each to detect presence.
        // protocol.minReaderVersion (i32) and metaData.id (string)
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![
                column_name!("protocol.minReaderVersion"),
                column_name!("metaData.id"),
            ];
            let types = vec![DataType::INTEGER, DataType::STRING];
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(
        &mut self,
        row_count: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> delta_kernel::DeltaResult<()> {
        for i in 0..row_count {
            if !self.found_protocol {
                let v: Option<i32> = getters[0].get_opt(i, "protocol.minReaderVersion")?;
                if v.is_some() {
                    self.found_protocol = true;
                }
            }
            if !self.found_metadata {
                let v: Option<String> = getters[1].get_opt(i, "metaData.id")?;
                if v.is_some() {
                    self.found_metadata = true;
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ExtrasVisitor: collects SetTxn app_ids, DomainMetadata domains, file sizes
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ExtrasVisitor {
    txn_app_ids: HashSet<String>,
    domain_names: HashSet<String>,
    total_add_size: i64,
    total_remove_size: i64,
}

impl RowVisitor for ExtrasVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![
                column_name!("txn.appId"),
                column_name!("domainMetadata.domain"),
                column_name!("add.size"),
                column_name!("remove.size"),
            ];
            let types = vec![
                DataType::STRING,
                DataType::STRING,
                DataType::LONG,
                DataType::LONG,
            ];
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(
        &mut self,
        row_count: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> delta_kernel::DeltaResult<()> {
        for i in 0..row_count {
            let app_id: Option<String> = getters[0].get_opt(i, "txn.appId")?;
            if let Some(app_id) = app_id {
                self.txn_app_ids.insert(app_id);
            }
            let domain: Option<String> = getters[1].get_opt(i, "domainMetadata.domain")?;
            if let Some(domain) = domain {
                self.domain_names.insert(domain);
            }
            let add_size: Option<i64> = getters[2].get_opt(i, "add.size")?;
            if let Some(size) = add_size {
                self.total_add_size += size;
            }
            let rm_size: Option<i64> = getters[3].get_opt(i, "remove.size")?;
            if let Some(size) = rm_size {
                self.total_remove_size += size;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Benchmark functions (modeled after replay_for_pm)
// ---------------------------------------------------------------------------

fn bench_strategy_a(engine: &dyn Engine, files: &[FileMeta]) {
    let schema = pm_only_schema();
    let batches = engine
        .json_handler()
        .read_json_files(files, schema, None)
        .unwrap();
    let mut pm = PmVisitor::default();
    for batch in batches {
        let data = batch.unwrap();
        pm.visit_rows_of(data.as_ref()).unwrap();
    }
    black_box((pm.found_protocol, pm.found_metadata));
}

fn bench_strategy_b(engine: &dyn Engine, files: &[FileMeta]) {
    let schema = pm_plus_extras_schema();
    let batches = engine
        .json_handler()
        .read_json_files(files, schema, None)
        .unwrap();
    let mut pm = PmVisitor::default();
    let mut extras = ExtrasVisitor::default();
    for batch in batches {
        let data = batch.unwrap();
        pm.visit_rows_of(data.as_ref()).unwrap();
        extras.visit_rows_of(data.as_ref()).unwrap();
    }
    black_box((pm.found_protocol, pm.found_metadata));
    black_box((&extras.txn_app_ids, &extras.domain_names));
    black_box((extras.total_add_size, extras.total_remove_size));
}

// ---------------------------------------------------------------------------
// Criterion setup
// ---------------------------------------------------------------------------

fn json_schema_benchmark(c: &mut Criterion) {
    let base = bench_dir();
    let add_counts: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];
    let file_counts: &[usize] = &[1, 10, 100];
    let mut group = c.benchmark_group("json_schema_projection");

    for &num_files in file_counts {
        for &total_adds in add_counts {
            let label = format!("{num_files}f_{total_adds}a");
            let table_dir = base.join(&label);
            let log_dir = table_dir.join("_delta_log");
            let first_file = log_dir.join("00000000000000000000.json");
            if !first_file.exists() {
                eprintln!("SKIP {label}: run with --generate first");
                continue;
            }

            let url = Url::from_directory_path(&table_dir).unwrap();
            let store = store_from_url(&url).unwrap();
            let engine = Arc::new(DefaultEngineBuilder::new(store).build());

            let files: Vec<FileMeta> = (0..num_files)
                .map(|i| {
                    let path = log_dir.join(format!("{i:020}.json"));
                    FileMeta {
                        location: Url::from_file_path(&path).unwrap(),
                        last_modified: 0,
                        size: fs::metadata(&path).unwrap().len(),
                    }
                })
                .collect();

            if total_adds >= 100_000 {
                group.sample_size(10);
            }

            group.bench_function(BenchmarkId::new("pm_only", &label), |b| {
                b.iter(|| bench_strategy_a(engine.as_ref(), &files));
            });

            group.bench_function(BenchmarkId::new("pm_plus_extras", &label), |b| {
                b.iter(|| bench_strategy_b(engine.as_ref(), &files));
            });
        }
    }
    group.finish();
}

criterion_group!(benches, json_schema_benchmark);

// ---------------------------------------------------------------------------
// Quick benchmark mode: runs all cases with manual timing, prints table + CSV
// ---------------------------------------------------------------------------

struct BenchResult {
    label: String,
    num_files: usize,
    total_adds: usize,
    pm_only_ms: f64,
    pm_plus_extras_ms: f64,
    overhead_pct: f64,
}

fn quick_benchmark() {
    let base = bench_dir();
    let add_counts: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];
    let file_counts: &[usize] = &[1, 10, 100];
    let total_cases = file_counts.len() * add_counts.len();
    let mut results: Vec<BenchResult> = Vec::new();
    let mut case_num = 0;

    // Iterations per case: more for small inputs, fewer for large
    let iterations_for = |total_adds: usize| -> usize {
        match total_adds {
            n if n >= 1_000_000 => 3,
            n if n >= 100_000 => 5,
            n if n >= 10_000 => 10,
            _ => 20,
        }
    };

    eprintln!("\n============================================================");
    eprintln!("  JSON Schema Projection Benchmark (quick mode)");
    eprintln!("  Strategies: A = P&M only, B = P&M + SetTxn + DM + sizes");
    eprintln!("============================================================\n");

    for &num_files in file_counts {
        for &total_adds in add_counts {
            case_num += 1;
            let label = format!("{num_files}f_{total_adds}a");
            let table_dir = base.join(&label);
            let log_dir = table_dir.join("_delta_log");
            let first_file = log_dir.join("00000000000000000000.json");
            if !first_file.exists() {
                eprintln!(
                    "[{case_num}/{total_cases}] SKIP {label}: run with --generate first"
                );
                continue;
            }

            let url = Url::from_directory_path(&table_dir).unwrap();
            let store = store_from_url(&url).unwrap();
            let engine = Arc::new(DefaultEngineBuilder::new(store).build());

            let files: Vec<FileMeta> = (0..num_files)
                .map(|i| {
                    let path = log_dir.join(format!("{i:020}.json"));
                    FileMeta {
                        location: Url::from_file_path(&path).unwrap(),
                        last_modified: 0,
                        size: fs::metadata(&path).unwrap().len(),
                    }
                })
                .collect();

            let total_size: u64 = files.iter().map(|f| f.size).sum();
            let iters = iterations_for(total_adds);

            eprint!(
                "[{case_num}/{total_cases}] {label} ({} files, {} adds, {})  ",
                num_files,
                human_count(total_adds),
                human_bytes(total_size as usize),
            );

            // Warmup
            bench_strategy_a(engine.as_ref(), &files);
            bench_strategy_b(engine.as_ref(), &files);

            // Strategy A
            let start = Instant::now();
            for _ in 0..iters {
                bench_strategy_a(engine.as_ref(), &files);
            }
            let pm_only_ms = start.elapsed().as_secs_f64() * 1000.0 / iters as f64;

            // Strategy B
            let start = Instant::now();
            for _ in 0..iters {
                bench_strategy_b(engine.as_ref(), &files);
            }
            let pm_plus_extras_ms = start.elapsed().as_secs_f64() * 1000.0 / iters as f64;

            let overhead_pct = (pm_plus_extras_ms - pm_only_ms) / pm_only_ms * 100.0;

            eprintln!(
                "A={:.2}ms  B={:.2}ms  overhead={:+.1}%  ({iters} iters)",
                pm_only_ms, pm_plus_extras_ms, overhead_pct,
            );

            results.push(BenchResult {
                label,
                num_files,
                total_adds,
                pm_only_ms,
                pm_plus_extras_ms,
                overhead_pct,
            });
        }
        eprintln!(); // blank line between file-count groups
    }

    // Print summary table
    eprintln!("============================================================");
    eprintln!("  RESULTS SUMMARY");
    eprintln!("============================================================");
    eprintln!(
        "{:<20} {:>8} {:>10} {:>12} {:>12} {:>10}",
        "Case", "Files", "Adds", "A (ms)", "B (ms)", "Overhead"
    );
    eprintln!("{}", "-".repeat(76));
    for r in &results {
        eprintln!(
            "{:<20} {:>8} {:>10} {:>12.2} {:>12.2} {:>9.1}%",
            r.label, r.num_files, human_count(r.total_adds),
            r.pm_only_ms, r.pm_plus_extras_ms, r.overhead_pct,
        );
    }
    eprintln!();

    // Write CSV
    let csv_path = base.join("results.csv");
    let mut f = fs::File::create(&csv_path).unwrap();
    writeln!(f, "case,num_files,total_adds,pm_only_ms,pm_plus_extras_ms,overhead_pct").unwrap();
    for r in &results {
        writeln!(
            f,
            "{},{},{},{:.4},{:.4},{:.2}",
            r.label, r.num_files, r.total_adds,
            r.pm_only_ms, r.pm_plus_extras_ms, r.overhead_pct,
        )
        .unwrap();
    }
    eprintln!("CSV written to {}", csv_path.display());
}

fn main() {
    if std::env::args().any(|a| a == "--generate") {
        generate_all_files();
        return;
    }
    if std::env::args().any(|a| a == "--quick") {
        quick_benchmark();
        return;
    }
    benches();
}
