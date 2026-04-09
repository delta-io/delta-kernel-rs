//! Fuzz tests for data skipping predicate rewriting.
//!
//! Generates random column types, row data, stats, and predicate trees, then verifies that
//! data skipping never produces false negatives (incorrectly skipping files that contain
//! matching rows).
//!
//! Inspired by Reyden's `pruning_fuzz_test` framework. The oracle uses
//! `DefaultKernelPredicateEvaluator` to directly evaluate the original predicate against
//! concrete row values -- a completely different code path from the data skipping predicate
//! rewriter (`DataSkippingPredicateCreator`).
//!
//! Configuration via environment variables:
//! - `FUZZ_SEED`: RNG seed (default: 42, deterministic)
//! - `FUZZ_DURATION_SECS`: max duration (default: 30)
//! - `FUZZ_MIN_FILES` / `FUZZ_MAX_FILES`: files per iteration (default: 2..8)
//! - `FUZZ_MIN_ROWS` / `FUZZ_MAX_ROWS`: rows per file (default: 1..10)

use super::*;

use crate::expressions::column_name;
use crate::kernel_predicates::DefaultKernelPredicateEvaluator;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

// ===== Configuration =====

struct FuzzConfig {
    seed: u64,
    duration: Duration,
    min_files: usize,
    max_files: usize,
    min_rows: usize,
    max_rows: usize,
    null_probability: f64,
    all_null_file_probability: f64,
    missing_stats_probability: f64,
    partial_stats_probability: f64,
}

impl FuzzConfig {
    fn from_env() -> Self {
        Self {
            seed: env_u64("FUZZ_SEED", 42),
            duration: Duration::from_secs(env_u64("FUZZ_DURATION_SECS", 30)),
            min_files: env_usize("FUZZ_MIN_FILES", 2),
            max_files: env_usize("FUZZ_MAX_FILES", 8),
            min_rows: env_usize("FUZZ_MIN_ROWS", 1),
            max_rows: env_usize("FUZZ_MAX_ROWS", 10),
            null_probability: 0.15,
            all_null_file_probability: 0.10,
            missing_stats_probability: 0.10,
            partial_stats_probability: 0.10,
        }
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ===== Column types =====

/// Column types eligible for data skipping statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FuzzColumnType {
    Byte,
    Short,
    Integer,
    Long,
    Float,
    Double,
    String,
    Boolean,
    Date,
    Timestamp,
    TimestampNtz,
    Decimal { precision: u8, scale: u8 },
}

impl FuzzColumnType {
    fn data_type(self) -> DataType {
        match self {
            Self::Byte => DataType::BYTE,
            Self::Short => DataType::SHORT,
            Self::Integer => DataType::INTEGER,
            Self::Long => DataType::LONG,
            Self::Float => DataType::FLOAT,
            Self::Double => DataType::DOUBLE,
            Self::String => DataType::STRING,
            Self::Boolean => DataType::BOOLEAN,
            Self::Date => DataType::DATE,
            Self::Timestamp => DataType::TIMESTAMP,
            Self::TimestampNtz => DataType::TIMESTAMP_NTZ,
            Self::Decimal { precision, scale } => DataType::decimal(precision, scale).unwrap(),
        }
    }

    /// Whether this type supports min/max stats (not just nullcount).
    fn has_min_max_stats(self) -> bool {
        !matches!(self, Self::Boolean)
    }
}

impl fmt::Display for FuzzColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Non-decimal type pool (equal probability each).
const SCALAR_TYPES: &[FuzzColumnType] = &[
    FuzzColumnType::Byte,
    FuzzColumnType::Short,
    FuzzColumnType::Integer,
    FuzzColumnType::Long,
    FuzzColumnType::Float,
    FuzzColumnType::Double,
    FuzzColumnType::String,
    FuzzColumnType::Boolean,
    FuzzColumnType::Date,
    FuzzColumnType::Timestamp,
    FuzzColumnType::TimestampNtz,
];

/// Decimal variants covering storage tiers and edge cases.
const DECIMAL_VARIANTS: &[(u8, u8)] = &[
    (1, 0),   // tiny, scale=0
    (9, 0),   // 4-byte, integer-like
    (9, 2),   // 4-byte, fractional
    (18, 6),  // 8-byte
    (38, 18), // 16-byte, max precision
];

fn random_column_type(rng: &mut SmallRng) -> FuzzColumnType {
    // 11 scalar types + 1 decimal category + 0 struct = 12 slots
    let slot = rng.random_range(0..SCALAR_TYPES.len() + 1);
    if slot < SCALAR_TYPES.len() {
        SCALAR_TYPES[slot]
    } else {
        let (precision, scale) = DECIMAL_VARIANTS[rng.random_range(0..DECIMAL_VARIANTS.len())];
        FuzzColumnType::Decimal { precision, scale }
    }
}

// ===== Value generation =====

/// Probability of generating an edge case value instead of a random one.
const EDGE_CASE_PROBABILITY: f64 = 0.20;

/// Probability of picking an existing data value as the filter literal.
const EXISTING_VALUE_PROBABILITY: f64 = 0.70;

/// Probability of generating a NaN filter literal for Float/Double.
const NAN_LITERAL_PROBABILITY: f64 = 0.03;

fn random_value(rng: &mut SmallRng, col_type: FuzzColumnType) -> Scalar {
    if rng.random_bool(EDGE_CASE_PROBABILITY) {
        if let Some(v) = edge_case_value(rng, col_type) {
            return v;
        }
    }
    random_normal_value(rng, col_type)
}

fn random_normal_value(rng: &mut SmallRng, col_type: FuzzColumnType) -> Scalar {
    match col_type {
        FuzzColumnType::Byte => Scalar::Byte(rng.random_range(i8::MIN..=i8::MAX)),
        FuzzColumnType::Short => Scalar::Short(rng.random_range(-1000..=1000)),
        FuzzColumnType::Integer => Scalar::Integer(rng.random_range(-10_000..=10_000)),
        FuzzColumnType::Long => Scalar::Long(rng.random_range(-100_000..=100_000)),
        FuzzColumnType::Float => Scalar::Float(rng.random_range(-1000.0_f32..=1000.0_f32)),
        FuzzColumnType::Double => Scalar::Double(rng.random_range(-1000.0_f64..=1000.0_f64)),
        FuzzColumnType::String => {
            let len = rng.random_range(1..=5_usize);
            let s: std::string::String = (0..len)
                .map(|_| (b'a' + rng.random_range(0..26_u8)) as char)
                .collect();
            Scalar::String(s)
        }
        FuzzColumnType::Boolean => Scalar::Boolean(rng.random_bool(0.5)),
        FuzzColumnType::Date => Scalar::Date(rng.random_range(-1000..=1000)),
        FuzzColumnType::Timestamp => {
            Scalar::Timestamp(rng.random_range(-86_400_000_000_i64..=86_400_000_000_i64))
        }
        FuzzColumnType::TimestampNtz => {
            Scalar::TimestampNtz(rng.random_range(-86_400_000_000_i64..=86_400_000_000_i64))
        }
        FuzzColumnType::Decimal { precision, scale } => {
            let max = 10_i128.saturating_pow(precision as u32) - 1;
            let bound = max.min(10_000_000);
            let val = rng.random_range(-bound..=bound);
            Scalar::decimal(val, precision, scale).expect("generated value should fit precision")
        }
    }
}

fn edge_case_value(rng: &mut SmallRng, col_type: FuzzColumnType) -> Option<Scalar> {
    let edges: Vec<Scalar> = match col_type {
        FuzzColumnType::Byte => return None, // full range already covered
        FuzzColumnType::Boolean => return None,
        FuzzColumnType::Short => {
            vec![
                Scalar::Short(i16::MIN),
                Scalar::Short(i16::MAX),
                Scalar::Short(0),
                Scalar::Short(-1),
                Scalar::Short(1),
            ]
        }
        FuzzColumnType::Integer => {
            vec![
                Scalar::Integer(i32::MIN),
                Scalar::Integer(i32::MAX),
                Scalar::Integer(0),
                Scalar::Integer(-1),
                Scalar::Integer(1),
            ]
        }
        FuzzColumnType::Long => {
            vec![
                Scalar::Long(i64::MIN),
                Scalar::Long(i64::MAX),
                Scalar::Long(0),
                Scalar::Long(-1),
                Scalar::Long(1),
            ]
        }
        FuzzColumnType::Float => {
            vec![
                Scalar::Float(f32::INFINITY),
                Scalar::Float(f32::NEG_INFINITY),
                Scalar::Float(-0.0),
                Scalar::Float(0.0),
                Scalar::Float(f32::MIN),
                Scalar::Float(f32::MAX),
            ]
        }
        FuzzColumnType::Double => {
            vec![
                Scalar::Double(f64::INFINITY),
                Scalar::Double(f64::NEG_INFINITY),
                Scalar::Double(-0.0),
                Scalar::Double(0.0),
                Scalar::Double(f64::MIN),
                Scalar::Double(f64::MAX),
            ]
        }
        FuzzColumnType::String => {
            vec![
                Scalar::String(std::string::String::new()),
                Scalar::String("a".to_owned()),
                Scalar::String("z".repeat(100)),
            ]
        }
        FuzzColumnType::Date => {
            vec![
                Scalar::Date(0),     // epoch
                Scalar::Date(-1),    // 1969-12-31
                Scalar::Date(10957), // 2000-01-01
            ]
        }
        FuzzColumnType::Timestamp => {
            vec![
                Scalar::Timestamp(0),
                Scalar::Timestamp(1),
                Scalar::Timestamp(-1),
                Scalar::Timestamp(1_000_000),
                Scalar::Timestamp(i64::MAX),
                Scalar::Timestamp(i64::MIN),
            ]
        }
        FuzzColumnType::TimestampNtz => {
            vec![
                Scalar::TimestampNtz(0),
                Scalar::TimestampNtz(1),
                Scalar::TimestampNtz(-1),
                Scalar::TimestampNtz(1_000_000),
            ]
        }
        FuzzColumnType::Decimal { precision, scale } => {
            let max = 10_i128.saturating_pow(precision as u32) - 1;
            vec![
                Scalar::decimal(0, precision, scale).unwrap(),
                Scalar::decimal(max, precision, scale).unwrap(),
                Scalar::decimal(-max, precision, scale).unwrap(),
            ]
        }
    };
    Some(edges[rng.random_range(0..edges.len())].clone())
}

fn random_cell(
    rng: &mut SmallRng,
    col_type: FuzzColumnType,
    null_probability: f64,
) -> Option<Scalar> {
    if rng.random_bool(null_probability) {
        None
    } else {
        Some(random_value(rng, col_type))
    }
}

// ===== Predicate generation =====

/// Picks a filter value: 70% from existing data, 30% fresh random.
fn filter_value(
    rng: &mut SmallRng,
    col_type: FuzzColumnType,
    existing_values: &[Scalar],
) -> Scalar {
    if !existing_values.is_empty() && rng.random_bool(EXISTING_VALUE_PROBABILITY) {
        existing_values[rng.random_range(0..existing_values.len())].clone()
    } else {
        random_value(rng, col_type)
    }
}

fn random_predicate(
    rng: &mut SmallRng,
    cols: &[FuzzColumnSpec],
    existing_values: &[Vec<Scalar>],
    depth: usize,
) -> Pred {
    let max_depth = 3;
    if depth >= max_depth {
        return random_leaf_predicate(rng, cols, existing_values);
    }

    // At depth 0: 40% compound, 60% leaf. Deeper: more leaf-heavy.
    let compound_probability = 0.4 / (1.0 + depth as f64);
    if rng.random_bool(compound_probability) {
        random_compound_predicate(rng, cols, existing_values, depth)
    } else {
        random_leaf_predicate(rng, cols, existing_values)
    }
}

fn random_leaf_predicate(
    rng: &mut SmallRng,
    cols: &[FuzzColumnSpec],
    existing_values: &[Vec<Scalar>],
) -> Pred {
    let col_idx = rng.random_range(0..cols.len());
    let col = &cols[col_idx];
    let col_expr = Expr::column([col.name.as_str()]);
    let existing = &existing_values[col_idx];

    // For Float/Double, occasionally use NaN as the filter literal.
    if matches!(col.col_type, FuzzColumnType::Float | FuzzColumnType::Double)
        && rng.random_bool(NAN_LITERAL_PROBABILITY)
    {
        let nan_val = match col.col_type {
            FuzzColumnType::Float => Scalar::Float(f32::NAN),
            FuzzColumnType::Double => Scalar::Double(f64::NAN),
            _ => unreachable!(),
        };
        return Pred::eq(col_expr, nan_val);
    }

    // For types without usable min/max stats, only IS NULL / IS NOT NULL are meaningful.
    // This includes Boolean (no min/max tracked) and Timestamp/TimestampNtz (max stat
    // disabled due to millisecond truncation, TODO #1002).
    let is_timestamp = matches!(
        col.col_type,
        FuzzColumnType::Timestamp | FuzzColumnType::TimestampNtz
    );
    if !col.col_type.has_min_max_stats() || is_timestamp {
        return if rng.random_bool(0.5) {
            Pred::is_null(col_expr)
        } else {
            Pred::is_not_null(col_expr)
        };
    }

    // Pick leaf type. Weights: comparison 50%, IS NULL/NOT NULL 20%, DISTINCT 15%,
    // literal-left 10%, literal 5%.
    let roll: f64 = rng.random();
    if roll < 0.50 {
        // Comparison: col op value
        let val = filter_value(rng, col.col_type, existing);
        match rng.random_range(0..6_u8) {
            0 => Pred::lt(col_expr, val),
            1 => Pred::le(col_expr, val),
            2 => Pred::eq(col_expr, val),
            3 => Pred::ne(col_expr, val),
            4 => Pred::gt(col_expr, val),
            5 => Pred::ge(col_expr, val),
            _ => unreachable!(),
        }
    } else if roll < 0.70 {
        // IS NULL / IS NOT NULL
        if rng.random_bool(0.5) {
            Pred::is_null(col_expr)
        } else {
            Pred::is_not_null(col_expr)
        }
    } else if roll < 0.85 {
        // DISTINCT / NOT DISTINCT
        let val = filter_value(rng, col.col_type, existing);
        if rng.random_bool(0.5) {
            Pred::distinct(col_expr, val)
        } else {
            Pred::not(Pred::distinct(col_expr, val))
        }
    } else if roll < 0.95 {
        // Literal-left comparison: value op col
        let val = filter_value(rng, col.col_type, existing);
        match rng.random_range(0..5_u8) {
            0 => Pred::lt(Expr::literal(val), col_expr),
            1 => Pred::le(Expr::literal(val), col_expr),
            2 => Pred::eq(Expr::literal(val), col_expr),
            3 => Pred::gt(Expr::literal(val), col_expr),
            4 => Pred::ge(Expr::literal(val), col_expr),
            _ => unreachable!(),
        }
    } else {
        // Literal TRUE/FALSE/NULL
        match rng.random_range(0..3_u8) {
            0 => Pred::literal(true),
            1 => Pred::literal(false),
            2 => Pred::null_literal(),
            _ => unreachable!(),
        }
    }
}

fn random_compound_predicate(
    rng: &mut SmallRng,
    cols: &[FuzzColumnSpec],
    existing_values: &[Vec<Scalar>],
    depth: usize,
) -> Pred {
    let roll: f64 = rng.random();
    if roll < 0.40 {
        // AND
        let n = rng.random_range(2..=4_usize);
        let children: Vec<Pred> = (0..n)
            .map(|_| random_predicate(rng, cols, existing_values, depth + 1))
            .collect();
        Pred::and_from(children)
    } else if roll < 0.80 {
        // OR
        let n = rng.random_range(2..=4_usize);
        let children: Vec<Pred> = (0..n)
            .map(|_| random_predicate(rng, cols, existing_values, depth + 1))
            .collect();
        Pred::or_from(children)
    } else {
        // NOT
        Pred::not(random_predicate(rng, cols, existing_values, depth + 1))
    }
}

// ===== Column spec =====

#[derive(Debug, Clone)]
struct FuzzColumnSpec {
    name: std::string::String,
    col_type: FuzzColumnType,
    is_partition: bool,
}

/// Partition column types (simple, commonly used).
const PARTITION_TYPES: &[FuzzColumnType] = &[
    FuzzColumnType::String,
    FuzzColumnType::Integer,
    FuzzColumnType::Long,
    FuzzColumnType::Date,
];

fn random_column_specs(rng: &mut SmallRng) -> Vec<FuzzColumnSpec> {
    let num_cols = rng.random_range(1..=5_usize);
    (0..num_cols)
        .map(|i| {
            // 25% chance first column is a partition column.
            let is_partition = i == 0 && rng.random_bool(0.25);
            let col_type = if is_partition {
                PARTITION_TYPES[rng.random_range(0..PARTITION_TYPES.len())]
            } else {
                random_column_type(rng)
            };
            FuzzColumnSpec {
                name: format!("col_{i}"),
                col_type,
                is_partition,
            }
        })
        .collect()
}

// ===== File data and stats =====

/// Stats for a single file, keyed by stats column name.
struct FileStats {
    resolver: HashMap<ColumnName, Scalar>,
    rows: Vec<Vec<Option<Scalar>>>,
}

/// Generates file data and computes stats for one file.
fn generate_file(
    rng: &mut SmallRng,
    cols: &[FuzzColumnSpec],
    config: &FuzzConfig,
    all_values: &mut Vec<Vec<Scalar>>,
) -> FileStats {
    let num_rows = rng.random_range(config.min_rows..=config.max_rows);
    let is_all_null = rng.random_bool(config.all_null_file_probability);
    let no_stats = rng.random_bool(config.missing_stats_probability);
    let partial_stats = !no_stats && rng.random_bool(config.partial_stats_probability);

    // For partition columns, pick one value per file (all rows share it).
    let partition_values: Vec<Option<Option<Scalar>>> = cols
        .iter()
        .map(|col| {
            if !col.is_partition {
                return None; // not a partition column
            }
            if is_all_null {
                Some(None) // all-null file: partition value is null
            } else if rng.random_bool(0.20) {
                Some(None) // null partition value
            } else {
                Some(Some(random_value(rng, col.col_type)))
            }
        })
        .collect();

    let mut rows: Vec<Vec<Option<Scalar>>> = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        let row: Vec<Option<Scalar>> = cols
            .iter()
            .enumerate()
            .map(|(col_idx, col)| {
                if is_all_null {
                    return None;
                }
                // Partition columns: all rows in a file share the same value.
                if col.is_partition {
                    let cell = partition_values[col_idx]
                        .as_ref()
                        .expect("partition col should have a value entry")
                        .clone();
                    if let Some(ref v) = cell {
                        all_values[col_idx].push(v.clone());
                    }
                    return cell;
                }
                let cell = random_cell(rng, col.col_type, config.null_probability);
                if let Some(ref v) = cell {
                    all_values[col_idx].push(v.clone());
                }
                cell
            })
            .collect();
        rows.push(row);
    }

    let mut resolver: HashMap<ColumnName, Scalar> = HashMap::new();
    resolver.insert(
        column_name!("stats_parsed.numRecords"),
        Scalar::Long(num_rows as i64),
    );

    if no_stats {
        return FileStats { resolver, rows };
    }

    for (col_idx, col) in cols.iter().enumerate() {
        let non_null_values: Vec<&Scalar> = rows
            .iter()
            .filter_map(|row| row[col_idx].as_ref())
            .collect();
        let null_count = (num_rows - non_null_values.len()) as i64;

        // Partition columns use partitionValues_parsed prefix.
        if col.is_partition {
            if let Some(first) = non_null_values.first() {
                resolver.insert(
                    ColumnName::new(["partitionValues_parsed", &col.name]),
                    (*first).clone(),
                );
            } else {
                resolver.insert(
                    ColumnName::new(["partitionValues_parsed", &col.name]),
                    Scalar::Null(col.col_type.data_type()),
                );
            }
            continue;
        }

        // Nullcount
        let drop_nullcount = partial_stats && rng.random_bool(0.33);
        if !drop_nullcount {
            resolver.insert(
                ColumnName::new(["stats_parsed", "nullCount", &col.name]),
                Scalar::Long(null_count),
            );
        }

        if !col.col_type.has_min_max_stats() || non_null_values.is_empty() {
            continue;
        }

        // Compute min/max from actual data.
        let (mut min, mut max) = (non_null_values[0].clone(), non_null_values[0].clone());
        for val in &non_null_values[1..] {
            if let Some(std::cmp::Ordering::Less) = val.logical_partial_cmp(&min) {
                min = (*val).clone();
            }
            if let Some(std::cmp::Ordering::Greater) = val.logical_partial_cmp(&max) {
                max = (*val).clone();
            }
        }

        // Partial stats: sometimes drop min or max.
        let drop_min = partial_stats && rng.random_bool(0.33);
        let drop_max = partial_stats && rng.random_bool(0.33);

        // Timestamp max stats are disabled in kernel (TODO #1002), so always drop max
        // for Timestamp/TimestampNtz to match kernel's behavior.
        let is_timestamp = matches!(
            col.col_type,
            FuzzColumnType::Timestamp | FuzzColumnType::TimestampNtz
        );

        if !drop_min {
            resolver.insert(
                ColumnName::new(["stats_parsed", "minValues", &col.name]),
                min,
            );
        }
        if !drop_max && !is_timestamp {
            resolver.insert(
                ColumnName::new(["stats_parsed", "maxValues", &col.name]),
                max,
            );
        }
    }

    FileStats { resolver, rows }
}

// ===== Oracle =====

/// Evaluates a predicate against a single row. Returns true if the row matches
/// (SQL WHERE semantics: NULL and FALSE both mean "no match").
fn row_matches_predicate(pred: &Pred, row: &[Option<Scalar>], cols: &[FuzzColumnSpec]) -> bool {
    let resolver: HashMap<ColumnName, Scalar> = cols
        .iter()
        .enumerate()
        .filter_map(|(i, col)| {
            row[i]
                .as_ref()
                .map(|v| (ColumnName::new([col.name.as_str()]), v.clone()))
        })
        .collect();
    let evaluator = DefaultKernelPredicateEvaluator::from(resolver);
    evaluator.eval(pred).unwrap_or(false)
}

/// Checks that no file was incorrectly skipped.
fn assert_no_false_skips(
    pred: &Pred,
    files: &[FileStats],
    skip_decisions: &[Option<bool>],
    cols: &[FuzzColumnSpec],
    mode: &str,
    ctx: &IterationContext,
) {
    for (file_idx, (file, decision)) in files.iter().zip(skip_decisions.iter()).enumerate() {
        if *decision != Some(false) {
            continue; // file was kept, no issue
        }
        // File was skipped -- verify no row matches.
        for (row_idx, row) in file.rows.iter().enumerate() {
            if row_matches_predicate(pred, row, cols) {
                panic!(
                    "FALSE SKIP detected!\n\
                     mode={mode}\n\
                     file={file_idx}, row={row_idx}\n\
                     predicate={pred}\n\
                     row={row:?}\n\
                     file_stats={:?}\n\
                     {ctx}",
                    file.resolver,
                );
            }
        }
    }
}

// ===== Iteration context (for repro on failure) =====

struct IterationContext {
    base_seed: u64,
    iteration: u64,
    iteration_seed: u64,
    cols: Vec<FuzzColumnSpec>,
    predicate: std::string::String,
    num_files: usize,
}

impl fmt::Display for IterationContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let col_types: Vec<std::string::String> = self
            .cols
            .iter()
            .map(|c| {
                if c.is_partition {
                    format!("{}:{}(partition)", c.name, c.col_type)
                } else {
                    format!("{}:{}", c.name, c.col_type)
                }
            })
            .collect();
        write!(
            f,
            "iteration={}, base_seed={}, iteration_seed={}, cols=[{}], predicate=[{}], num_files={}\n\
             Reproduce: FUZZ_SEED={} FUZZ_DURATION_SECS=1 cargo nextest run ... fuzz_data_skipping",
            self.iteration,
            self.base_seed,
            self.iteration_seed,
            col_types.join(", "),
            self.predicate,
            self.num_files,
            self.iteration_seed,
        )
    }
}

// ===== Pruning modes =====

#[derive(Debug, Clone, Copy)]
enum PruningMode {
    /// Normal data skipping via `as_data_skipping_predicate`.
    Normal,
    /// SQL WHERE data skipping via `as_sql_data_skipping_predicate`.
    SqlWhere,
    /// Checkpoint pushdown via `as_checkpoint_skipping_predicate`.
    Checkpoint,
}

impl fmt::Display for PruningMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PruningMode::Normal => write!(f, "Normal"),
            PruningMode::SqlWhere => write!(f, "SqlWhere"),
            PruningMode::Checkpoint => write!(f, "Checkpoint"),
        }
    }
}

/// Rewrites a predicate for data skipping and evaluates it against file stats.
fn evaluate_skipping(
    pred: &Pred,
    file_stats: &HashMap<ColumnName, Scalar>,
    mode: PruningMode,
    partition_columns: &HashSet<std::string::String>,
) -> Option<bool> {
    let rewritten = match mode {
        PruningMode::Normal => as_data_skipping_predicate_with_partitions(pred, partition_columns),
        PruningMode::SqlWhere => as_sql_data_skipping_predicate(pred, partition_columns),
        PruningMode::Checkpoint => {
            let partition_cols: Vec<std::string::String> =
                partition_columns.iter().cloned().collect();
            as_checkpoint_skipping_predicate(pred, &partition_cols)
        }
    };
    let rewritten = rewritten?;

    // For checkpoint mode, stats keys use different column name prefixes
    // (no "stats_parsed." prefix). Remap.
    let resolver = if matches!(mode, PruningMode::Checkpoint) {
        remap_stats_for_checkpoint(file_stats)
    } else {
        file_stats.clone()
    };

    let evaluator = DefaultKernelPredicateEvaluator::from(resolver);
    match mode {
        PruningMode::SqlWhere => evaluator.eval_sql_where(&rewritten),
        _ => evaluator.eval(&rewritten),
    }
}

/// Remaps stats column names from `stats_parsed.minValues.col` to `minValues.col`
/// for checkpoint pushdown evaluation (which uses a different column prefix).
fn remap_stats_for_checkpoint(stats: &HashMap<ColumnName, Scalar>) -> HashMap<ColumnName, Scalar> {
    stats
        .iter()
        .filter_map(|(name, val)| {
            let path = name.path();
            if path.len() >= 3 && path[0] == "stats_parsed" {
                // stats_parsed.minValues.col -> minValues.col
                let new_path: Vec<&str> = path[1..].iter().map(|s| s.as_str()).collect();
                Some((ColumnName::new(new_path), val.clone()))
            } else {
                // Keep partition values and numRecords as-is
                Some((name.clone(), val.clone()))
            }
        })
        .collect()
}

// ===== Main fuzz loop =====

#[test]
fn fuzz_data_skipping() {
    let config = FuzzConfig::from_env();

    println!(
        "Pruning fuzz test: seed={}, duration={}s",
        config.seed,
        config.duration.as_secs(),
    );

    let start = Instant::now();
    let mut iteration = 0u64;
    let mut total_skips_checked = 0u64;

    while start.elapsed() < config.duration {
        let iteration_seed = config.seed.wrapping_add(iteration);
        let mut rng = SmallRng::seed_from_u64(iteration_seed);

        // Generate random columns.
        let cols = random_column_specs(&mut rng);
        let partition_columns: HashSet<std::string::String> = cols
            .iter()
            .filter(|c| c.is_partition)
            .map(|c| c.name.clone())
            .collect();

        // Generate file data.
        let num_files = rng.random_range(config.min_files..=config.max_files);
        let mut all_values: Vec<Vec<Scalar>> = cols.iter().map(|_| Vec::new()).collect();
        let files: Vec<FileStats> = (0..num_files)
            .map(|_| generate_file(&mut rng, &cols, &config, &mut all_values))
            .collect();

        // Generate predicate.
        let pred = random_predicate(&mut rng, &cols, &all_values, 0);

        let ctx = IterationContext {
            base_seed: config.seed,
            iteration,
            iteration_seed,
            cols: cols.clone(),
            predicate: format!("{pred}"),
            num_files,
        };

        // Test all applicable pruning modes.
        for mode in [
            PruningMode::Normal,
            PruningMode::SqlWhere,
            PruningMode::Checkpoint,
        ] {
            // Checkpoint mode: skip if any column is a partition column (not supported),
            // or if timestamp columns are present (max stat disabled).
            if matches!(mode, PruningMode::Checkpoint) {
                let has_partition = cols.iter().any(|c| c.is_partition);
                let has_timestamp = cols.iter().any(|c| {
                    matches!(
                        c.col_type,
                        FuzzColumnType::Timestamp | FuzzColumnType::TimestampNtz
                    )
                });
                if has_partition || has_timestamp {
                    continue;
                }
            }

            let skip_decisions: Vec<Option<bool>> = files
                .iter()
                .map(|f| evaluate_skipping(&pred, &f.resolver, mode, &partition_columns))
                .collect();

            assert_no_false_skips(
                &pred,
                &files,
                &skip_decisions,
                &cols,
                &mode.to_string(),
                &ctx,
            );

            total_skips_checked +=
                skip_decisions.iter().filter(|d| **d == Some(false)).count() as u64;
        }

        iteration += 1;
    }

    println!(
        "Pruning fuzz PASSED: {} iterations, {} file skips verified, {:.1}s (seed={})",
        iteration,
        total_skips_checked,
        start.elapsed().as_secs_f64(),
        config.seed,
    );
}
