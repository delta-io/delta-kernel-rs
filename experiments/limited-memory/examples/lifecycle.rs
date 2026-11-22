//! Synthetic storage experiment; does not run Snapshot::build, a scan, or a transaction commit.

use std::error::Error;
use std::time::Instant;

use delta_kernel::schema::{DataType, MetadataValue, StructField, StructType};
use delta_kernel_limited_memory_poc::{
    ActionBuffer, FileKeySet, LimitedSchema, LogTail, MemoryBudget, MemoryResult, MemoryUsage,
    try_parse_json,
};
use serde::Serialize;

const SCHEMA_BYTES: usize = 11_301_270;

#[derive(Serialize)]
struct Stage {
    name: &'static str,
    elapsed_ns: u128,
    reserved_bytes: usize,
    peak_bytes: usize,
    rejected_allocations: usize,
}

#[derive(Serialize)]
struct Report {
    kind: &'static str,
    status: &'static str,
    failure: Option<String>,
    limit_bytes: usize,
    top_level_fields: usize,
    recursive_fields: usize,
    schema_json_bytes: usize,
    tail_commits: usize,
    scan_keys: usize,
    transaction_actions: usize,
    peak_reserved_bytes: usize,
    reserved_after_cleanup: usize,
    stages: [Option<Stage>; 7],
}

#[derive(Serialize)]
struct Action<'a> {
    txn: &'a delta_kernel::actions::SetTransaction,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cap_mib: usize = std::env::args()
        .nth(1)
        .as_deref()
        .unwrap_or("256")
        .parse()?;
    let limit = cap_mib
        .checked_mul(1024 * 1024)
        .ok_or("budget size overflow")?;
    // Input preparation is outside the operation's budget. This source remains pinned throughout.
    let (source, json) = prepare_input()?;
    let budget = MemoryBudget::new(limit);
    let mut stages = std::array::from_fn(|_| None);
    let mut stage = 0;
    let result: MemoryResult<()> = (|| {
        let tree = measure(&mut stages[stage], "schema_json_tree", &budget, || {
            try_parse_json(&budget, &json)
        })?;
        stage += 1;
        let schema = measure(
            &mut stages[stage],
            "schema_copy_with_tree_live",
            &budget,
            || LimitedSchema::try_from_kernel(&budget, &source),
        )?;
        drop(tree);
        stage += 1;
        let projection = measure(
            &mut stages[stage],
            "scan_schema_projection",
            &budget,
            || schema.try_project(&[0, 1, 2, 9999]),
        )?;
        stage += 1;
        let mut tail = LogTail::new(&budget);
        measure(&mut stages[stage], "log_tail_paths", &budget, || {
            for version in 1..=10_000 {
                let path = format!("s3://poc/table/_delta_log/{version:020}.json");
                tail.try_push(version, 128, &path)?;
            }
            Ok(())
        })?;
        stage += 1;
        let mut keys = FileKeySet::new(&budget);
        measure(&mut stages[stage], "scan_keys", &budget, || {
            for index in 0..1000 {
                keys.try_insert(&format!("part-{index}.parquet"), None)?;
            }
            Ok(())
        })?;
        stage += 1;
        let mut actions = ActionBuffer::new(&budget);
        measure(&mut stages[stage], "transaction_actions", &budget, || {
            for version in 1..=1000 {
                let txn =
                    delta_kernel::actions::SetTransaction::new("poc-writer".into(), version, None);
                actions.try_stage(&Action { txn: &txn })?;
            }
            Ok(())
        })?;
        stage += 1;
        let output = measure(&mut stages[stage], "commit_output_overlap", &budget, || {
            actions.try_encode_commit()
        })?;
        drop((schema, projection, tail, keys, actions, output));
        Ok(())
    })();
    let MemoryUsage { peak, reserved, .. } = budget.usage();
    let report = Report {
        kind: "synthetic_limited_storage_poc",
        status: if result.is_ok() {
            "completed"
        } else {
            "allocation_or_input_error"
        },
        failure: result.as_ref().err().map(ToString::to_string),
        limit_bytes: limit,
        top_level_fields: 10_000,
        recursive_fields: 30_000,
        schema_json_bytes: json.len(),
        tail_commits: 10_000,
        scan_keys: 1000,
        transaction_actions: 1000,
        peak_reserved_bytes: peak,
        reserved_after_cleanup: reserved,
        stages,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    if let Err(error) = result {
        return Err(error.into());
    }
    Ok(())
}

fn measure<T>(
    stage: &mut Option<Stage>,
    name: &'static str,
    budget: &MemoryBudget,
    operation: impl FnOnce() -> MemoryResult<T>,
) -> MemoryResult<T> {
    let start = Instant::now();
    let result = operation();
    let usage = budget.usage();
    *stage = Some(Stage {
        name,
        elapsed_ns: start.elapsed().as_nanos(),
        reserved_bytes: usage.reserved,
        peak_bytes: usage.peak,
        rejected_allocations: usage.rejected,
    });
    result
}

fn prepare_input() -> Result<(StructType, String), Box<dyn Error>> {
    let mut fields: Vec<_> = (0..10_000)
        .map(|index| {
            let nested = StructType::try_new([
                StructField::nullable("x", DataType::LONG),
                StructField::nullable("y", DataType::STRING),
            ])?;
            Ok(StructField::nullable(format!("col_{index}"), nested))
        })
        .collect::<Result<_, delta_kernel::KernelError>>()?;
    let unpadded = serde_json::to_string(&StructType::try_new(fields.clone())?)?.len();
    let overhead = br#""comment":"""#.len();
    let padding = SCHEMA_BYTES
        .checked_sub(unpadded + overhead)
        .ok_or("schema padding overflow")?;
    fields[0]
        .metadata
        .insert("comment".into(), MetadataValue::String("x".repeat(padding)));
    let schema = StructType::try_new(fields)?;
    let json = serde_json::to_string(&schema)?;
    if json.len() != SCHEMA_BYTES {
        return Err("schema byte contract mismatch".into());
    }
    Ok((schema, json))
}
