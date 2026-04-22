//! Harness for Delta protocol compliance tests.
//!
//! Loads fixture files from `compliance-suite/fixtures/`, resolves `$ref`
//! references within each file, and provides [`Fixture`] and [`Case`] for use in
//! per-fixture test modules.
//!
//! ## Macro API
//!
//! Each fixture module declares a `LazyLock<Fixture>` static, then calls one macro per case
//! (in order) followed by [`compliance_case_sentinel!`]:
//!
//! - [`compliance_case_success!`] -- fixture expects `Success`.
//!   No error arg: kernel passes. Error arg: kernel diverges by failing (arg pins the error).
//! - [`compliance_case_failure!`] -- fixture expects `Failure`.
//!   Error arg: kernel fails correctly (arg pins the error). No error arg: kernel diverges by succeeding.
//! - [`compliance_case_inexpressible!`] -- harness cannot express the fixture's protocol shape
//!   via the current API. Passes as long as the harness returns `Inexpressible`.
//! - [`compliance_case_sentinel!`] -- asserts the next case index does not yet exist.

// Each test binary includes this module but uses only a subset of the macros and
// public functions (e.g. a file with only inexpressible cases never calls run_case_success).
// Suppress the resulting lint noise here rather than in every test file.
#![allow(unused_macros, dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::{path::Path as StorePath, DynObjectStore, ObjectStoreExt};
use serde_json::Value;

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

fn errata_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("errata.json")
}

/// Recursively resolve `$section.name` references in `value`, consulting `doc` for the
/// referenced sections.
///
/// - `$schemas.x` → compact JSON string of the schema object (Delta's schemaString format).
/// - All other `$section.name` references → the target value, recursively resolved.
/// - Non-string and non-reference values are returned unchanged.
fn resolve(doc: &Value, value: &Value) -> Value {
    match value {
        Value::String(s) if s.starts_with('$') => {
            let (section, name) = s[1..]
                .split_once('.')
                .unwrap_or_else(|| panic!("invalid $ref {s:?}: expected '$section.name'"));
            let section_key = format!("${section}");
            let resolved = &doc[&section_key][name];
            assert!(
                !resolved.is_null(),
                "unresolved $ref {s:?}: '{name}' not found in '{section_key}'"
            );
            if section == "schemas" {
                // Schemas are serialized to a JSON string (Delta schemaString format).
                Value::String(resolved.to_string())
            } else {
                resolve(doc, resolved)
            }
        }
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), resolve(doc, v)))
                .collect(),
        ),
        Value::Array(arr) => Value::Array(arr.iter().map(|v| resolve(doc, v)).collect()),
        other => other.clone(),
    }
}

/// Outcome of executing a compliance case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The operation succeeded (reader opened the table, writer committed, etc.).
    Success,
    /// The operation was rejected (invalid protocol, unsupported feature, etc.).
    Failure,
    /// The harness cannot attempt this case using the current kernel API.
    ///
    /// Returned when the fixture's protocol shape or operation cannot be produced
    /// via the public API (e.g., `create_table()` always produces `(3,7)`, so
    /// legacy protocol versions are inexpressible). A test that expects
    /// `Inexpressible` will fail automatically when the harness stops returning
    /// it, signaling that the API gap has been addressed and the expectation needs
    /// to be re-evaluated.
    Inexpressible,
}

/// A single resolved fixture case.
#[allow(dead_code)]
pub struct Case {
    /// 1-based ordinal declared in the fixture file and validated against array position on load.
    pub ordinal: usize,
    pub name: String,
    pub description: String,
    pub expected_outcome: Outcome,
    pub feature: Option<String>,
    pub flavor: Option<String>,
    /// Resolved `operation` object. Will be parsed into a typed operation when cases are run.
    pub operation: Value,
    /// Resolved `setup` object, if present.
    pub setup: Option<Value>,
    /// Resolved `note` field, if present. May be a string (`$notes.*`) or an object (`$errata.*`).
    pub note: Option<Value>,
}

/// A loaded and fully resolved fixture file.
pub struct Fixture {
    pub filename: &'static str,
    pub cases: Vec<Case>,
}

impl Fixture {
    /// Load and resolve a fixture file by name.
    ///
    /// `filename` is resolved relative to `compliance-suite/fixtures/`.
    /// Errata from `compliance-suite/errata.json` is injected so that
    /// `$errata.*` references within the file resolve correctly.
    pub fn load(filename: &'static str) -> Self {
        let path = fixtures_dir().join(filename);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {filename:?}: {e}"));
        let mut doc: Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("failed to parse fixture {filename:?}: {e}"));

        // Inject errata so that "$errata.key" references in case `note` fields resolve.
        let errata: Value = std::fs::read_to_string(errata_path())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(Value::Object(Default::default()));
        doc.as_object_mut()
            .expect("fixture root must be a JSON object")
            .insert("$errata".to_string(), errata);

        let raw_cases = doc["cases"]
            .as_array()
            .unwrap_or_else(|| panic!("{filename}: missing 'cases' array"))
            .clone();

        let cases = raw_cases
            .iter()
            .enumerate()
            .map(|(i, raw)| {
                let c = resolve(&doc, raw);
                let position = i + 1;
                let ordinal = c["ordinal"].as_u64().unwrap_or_else(|| {
                    panic!("{filename} case {position}: missing 'ordinal' field")
                }) as usize;
                assert_eq!(
                    ordinal, position,
                    "{filename} case {position}: declared ordinal {ordinal} does not match \
                     array position {position}",
                );
                Case {
                    ordinal,
                    name: c["name"]
                        .as_str()
                        .unwrap_or_else(|| panic!("{filename} case {ordinal}: missing 'name'"))
                        .to_string(),
                    description: c["description"]
                        .as_str()
                        .unwrap_or_else(|| {
                            panic!("{filename} case {ordinal}: missing 'description'")
                        })
                        .to_string(),
                    expected_outcome: match c["expected_outcome"].as_str() {
                        Some("success") => Outcome::Success,
                        Some("failure") => Outcome::Failure,
                        other => {
                            panic!("{filename} case {ordinal}: invalid expected_outcome {other:?}")
                        }
                    },
                    feature: c["feature"].as_str().map(str::to_string),
                    flavor: c["flavor"].as_str().map(str::to_string),
                    operation: c["operation"].clone(),
                    setup: {
                        let v = &c["setup"];
                        (!v.is_null()).then(|| v.clone())
                    },
                    note: {
                        let v = &c["note"];
                        (!v.is_null()).then(|| v.clone())
                    },
                }
            })
            .collect();

        Fixture { filename, cases }
    }

    /// Return the case at 1-based index `n`.
    ///
    /// Panics with a diagnostic if `n` is out of range, e.g. because a case was removed
    /// from the fixture without removing the corresponding `compliance_case_success!` or
    /// `compliance_case_failure!` invocation.
    pub fn case(&self, n: usize) -> &Case {
        self.cases.get(n - 1).unwrap_or_else(|| {
            panic!(
                "compliance_case!({n}) is out of range: '{}' has {} case(s). \
                 If the case was removed from the fixture, delete the corresponding \
                 compliance_case! invocation.",
                self.filename,
                self.cases.len(),
            )
        })
    }

    /// Assert that no case exists at 1-based index `n`.
    ///
    /// Called by `compliance_case_sentinel!` to detect when new cases are added to the
    /// fixture without corresponding `compliance_case_success!` or `compliance_case_failure!`
    /// invocations.
    pub fn assert_sentinel(&self, n: usize) {
        if let Some(case) = self.cases.get(n - 1) {
            panic!(
                "compliance_case_sentinel!({n}) failed: '{}' now has a case at ordinal {n} \
                 (name: '{}').\n\
                 Add a compliance_case! invocation above the sentinel and update to \
                 compliance_case_sentinel!({}).",
                self.filename,
                case.name,
                n + 1,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Case execution
// ---------------------------------------------------------------------------

/// Apply Delta MetaData defaults to the inner metaData object.
///
/// Fills in `id`, `format`, `partitionColumns`, and `configuration` when absent.
/// The Python DBR harness (databricks_compliance.py) applies the same defaults before writing log files.
fn apply_metadata_defaults(metadata: &mut Value) {
    let obj = metadata
        .as_object_mut()
        .expect("metaData value must be a JSON object");
    obj.entry("id")
        .or_insert_with(|| Value::String("00000000-0000-0000-0000-000000000001".to_string()));
    obj.entry("format")
        .or_insert_with(|| serde_json::json!({"provider": "parquet", "options": {}}));
    obj.entry("partitionColumns")
        .or_insert(Value::Array(vec![]));
    obj.entry("configuration")
        .or_insert_with(|| Value::Object(Default::default()));
}

/// Apply Delta domainMetadata defaults to the inner domainMetadata object.
///
/// Fills in `removed=false` when absent.
fn apply_domain_metadata_defaults(domain_metadata: &mut Value) {
    let obj = domain_metadata
        .as_object_mut()
        .expect("domainMetadata value must be a JSON object");
    obj.entry("removed").or_insert(Value::Bool(false));
}

/// Write a list of Delta log actions as version 0 of the table in `store`.
///
/// Actions are written as NDJSON to `_delta_log/00000000000000000000.json`. Any `metaData`
/// or `domainMetadata` action in the list has defaults applied before serialization.
async fn write_log_v0(store: &InMemory, actions: &[Value]) {
    let ndjson = actions
        .iter()
        .cloned()
        .map(|mut a| {
            if let Some(metadata) = a.get_mut("metaData") {
                apply_metadata_defaults(metadata);
            }
            if let Some(domain_metadata) = a.get_mut("domainMetadata") {
                apply_domain_metadata_defaults(domain_metadata);
            }
            serde_json::to_string(&a).expect("serialize log action")
        })
        .collect::<Vec<_>>()
        .join("\n");
    let path = StorePath::from("_delta_log/00000000000000000000.json");
    store.put(&path, ndjson.into()).await.expect("write log v0");
}

/// Create a fresh in-memory object store and a `DefaultEngine` backed by the same store.
///
/// Returns `(store, engine)` where `store` is the raw `Arc<InMemory>` for writing log files
/// and `engine` is configured to read from that same store.
fn new_store_and_engine() -> (Arc<InMemory>, impl delta_kernel::Engine) {
    let store = Arc::new(InMemory::new());
    let engine = DefaultEngineBuilder::new(store.clone() as Arc<DynObjectStore>).build();
    (store, engine)
}

/// Attempt to build a [`Snapshot`] from `memory:///` using the given engine.
fn try_build_snapshot(
    engine: &dyn delta_kernel::Engine,
) -> delta_kernel::DeltaResult<delta_kernel::snapshot::SnapshotRef> {
    delta_kernel::Snapshot::builder_for("memory:///").build(engine)
}

/// Run a compliance case where the fixture expects [`Outcome::Success`].
///
/// - No `error_pattern`: asserts the kernel produces `Success` (passing case).
/// - With `error_pattern`: asserts the kernel produces `Failure` with an error matching the
///   given substring, documenting a known divergence. The test fails automatically when the
///   divergence is resolved, prompting removal of the error arg from `compliance_case_success!`.
pub async fn run_case_success(case: &Case, error_pattern: Option<&str>) {
    assert_eq!(
        case.expected_outcome,
        Outcome::Success,
        "case {} '{}': compliance_case_success! requires expected_outcome=Success, got {:?}",
        case.ordinal,
        case.name,
        case.expected_outcome,
    );
    let (actual, actual_error) = execute_case(case).await;
    match error_pattern {
        None => {
            if actual != Outcome::Success {
                let error_suffix = actual_error
                    .as_deref()
                    .map(|e| format!("\n  error: {e}"))
                    .unwrap_or_default();
                panic!(
                    "case {} '{}': expected Success but got {:?}{}",
                    case.ordinal, case.name, actual, error_suffix,
                );
            }
        }
        Some(pattern) => {
            if actual != Outcome::Failure {
                panic!(
                    "case {} '{}': divergence resolved — spec expects Success and kernel now \
                     produces {:?}; remove the error arg from compliance_case_success!",
                    case.ordinal, case.name, actual,
                );
            }
            let error = actual_error.as_deref().unwrap_or_else(|| {
                panic!(
                    "case {} '{}': kernel produced Failure but captured no error message",
                    case.ordinal, case.name,
                )
            });
            assert!(
                error.contains(pattern),
                "case {} '{}': divergence error does not match expected pattern\n  pattern: {:?}\n  error:   {:?}",
                case.ordinal, case.name, pattern, error,
            );
        }
    }
}

/// Run a compliance case where the fixture expects [`Outcome::Failure`] and the kernel
/// correctly rejects the input.
///
/// Asserts the kernel produces `Failure` with an error containing `error_pattern`.
pub async fn run_case_failure(case: &Case, error_pattern: &str) {
    assert_eq!(
        case.expected_outcome,
        Outcome::Failure,
        "case {} '{}': compliance_case_failure! requires expected_outcome=Failure, got {:?}",
        case.ordinal,
        case.name,
        case.expected_outcome,
    );
    let (actual, actual_error) = execute_case(case).await;
    if actual != Outcome::Failure {
        panic!(
            "case {} '{}': expected Failure but got {:?}; \
             the case may now be diverging — switch to \
             compliance_case_failure!(diverges: \"...\") to document it",
            case.ordinal, case.name, actual,
        );
    }
    let error = actual_error.as_deref().unwrap_or_else(|| {
        panic!(
            "case {} '{}': kernel produced Failure but captured no error message",
            case.ordinal, case.name,
        )
    });
    assert!(
        error.contains(error_pattern),
        "case {} '{}': failure error does not match expected pattern\n  pattern: {:?}\n  error:   {:?}",
        case.ordinal, case.name, error_pattern, error,
    );
}

/// Run a compliance case where the fixture expects [`Outcome::Failure`] but the kernel
/// diverges by succeeding.
///
/// `reason` documents what the kernel is failing to enforce. The test fails automatically
/// when the divergence is resolved, including `reason` in the diagnostic to guide the fix.
pub async fn run_case_failure_diverges(case: &Case, reason: &str) {
    assert_eq!(
        case.expected_outcome,
        Outcome::Failure,
        "case {} '{}': compliance_case_failure! requires expected_outcome=Failure, got {:?}",
        case.ordinal,
        case.name,
        case.expected_outcome,
    );
    let (actual, actual_error) = execute_case(case).await;
    if actual != Outcome::Success {
        let error_suffix = actual_error
            .as_deref()
            .map(|e| format!("\n  error: {e}"))
            .unwrap_or_default();
        panic!(
            "case {} '{}': divergence resolved ({reason}) — spec expects Failure and kernel \
             now produces {:?}{}; switch to compliance_case_failure! with an error arg",
            case.ordinal, case.name, actual, error_suffix,
        );
    }
}

/// Run a compliance case and assert the outcome is [`Outcome::Inexpressible`].
///
/// Use this for cases that cannot be tested via the current kernel API. The assertion
/// fails when the harness stops returning `Inexpressible`, signaling that the API gap
/// has been addressed. At that point, switch to [`compliance_case_success!`] or
/// [`compliance_case_failure!`] and verify the actual outcome matches the fixture's
/// `expected_outcome`.
pub async fn run_case_expect_inexpressible(case: &Case) {
    let (actual, _) = execute_case(case).await;
    assert_eq!(
        actual,
        Outcome::Inexpressible,
        "case {} '{}': expected Inexpressible (API gap) but got {:?}; \
         the gap may have been addressed — switch to the appropriate compliance_case! \
         macro and verify the outcome matches the fixture's expected_outcome ({:?})",
        case.ordinal,
        case.name,
        actual,
        case.expected_outcome,
    );
}

async fn execute_case(case: &Case) -> (Outcome, Option<String>) {
    let op_type = case.operation["type"]
        .as_str()
        .unwrap_or_else(|| panic!("case {}: missing operation.type", case.ordinal));

    match op_type {
        "read_snapshot" => execute_read_snapshot(case).await,
        "empty_commit" => execute_empty_commit(case).await,
        "create_table" => execute_create_table(case).await,
        other => panic!("case {}: unknown operation type {:?}", case.ordinal, other),
    }
}

/// Execute a `read_snapshot` operation.
///
/// Writes the setup log actions to a fresh in-memory store, builds a snapshot, then attempts
/// to start a scan. The scan step triggers `ensure_read_supported`, which validates the reader
/// version and rejects unsupported or unknown reader features. Returns `Success` only if both
/// snapshot construction and scan setup succeed.
async fn execute_read_snapshot(case: &Case) -> (Outcome, Option<String>) {
    let setup = case
        .setup
        .as_ref()
        .unwrap_or_else(|| panic!("case {}: read_snapshot requires setup", case.ordinal));
    let log_actions: Vec<Value> = setup["log_actions"]
        .as_array()
        .unwrap_or_else(|| panic!("case {}: setup.log_actions must be array", case.ordinal))
        .to_vec();

    let (store, engine) = new_store_and_engine();
    write_log_v0(&store, &log_actions).await;

    // Build snapshot: validates protocol structural validity (version/feature consistency).
    let snapshot = match try_build_snapshot(&engine) {
        Ok(s) => s,
        Err(e) => return (Outcome::Failure, Some(e.to_string())),
    };

    // Start a scan: triggers ensure_read_supported, which checks the reader version and rejects
    // unsupported reader features (including Unknown ones).
    match snapshot.scan_builder().build() {
        Ok(_) => (Outcome::Success, None),
        Err(e) => (Outcome::Failure, Some(e.to_string())),
    }
}

/// Execute an `empty_commit` operation.
///
/// Writes the setup log actions to a fresh in-memory store, builds a snapshot, creates a
/// transaction (which triggers `ensure_write_supported`), and commits it. Returns `Success`
/// only if every step succeeds and the commit is confirmed.
async fn execute_empty_commit(case: &Case) -> (Outcome, Option<String>) {
    let setup = case
        .setup
        .as_ref()
        .unwrap_or_else(|| panic!("case {}: empty_commit requires setup", case.ordinal));
    let log_actions: Vec<Value> = setup["log_actions"]
        .as_array()
        .unwrap_or_else(|| panic!("case {}: setup.log_actions must be array", case.ordinal))
        .to_vec();

    let (store, engine) = new_store_and_engine();
    write_log_v0(&store, &log_actions).await;

    // Build snapshot: validates protocol structural validity.
    let snapshot = match try_build_snapshot(&engine) {
        Ok(s) => s,
        Err(e) => return (Outcome::Failure, Some(e.to_string())),
    };

    // Create transaction: triggers ensure_write_supported, which checks the writer version and
    // rejects unsupported or unknown writer features.
    let txn = match snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine) {
        Ok(t) => t,
        Err(e) => return (Outcome::Failure, Some(e.to_string())),
    };

    // Commit the transaction.
    match txn.commit(&engine) {
        Ok(result) if result.is_committed() => (Outcome::Success, None),
        Ok(_) => (
            Outcome::Failure,
            Some("commit did not complete".to_string()),
        ),
        Err(e) => (Outcome::Failure, Some(e.to_string())),
    }
}

/// Execute a `create_table` operation via kernel's public `create_table()` API.
///
/// Returns [`Outcome::Inexpressible`] when the fixture's protocol shape cannot be produced
/// by `create_table()`: the API always produces `(3,7)` with both feature arrays present and
/// symmetric feature placement. Fixtures specifying legacy protocol versions, missing feature
/// arrays, or asymmetric placement all fall back to `Inexpressible`.
///
/// For expressible cases, all features named in `readerFeatures` and `writerFeatures` are
/// submitted as `delta.feature.X = "supported"` table properties. Kernel validates these
/// against its allowed-feature list; unknown or disallowed features cause the build step to
/// return an error, producing `Outcome::Failure`.
async fn execute_create_table(case: &Case) -> (Outcome, Option<String>) {
    let op = &case.operation;
    let protocol_val = op.get("protocol").unwrap_or_else(|| {
        panic!(
            "case {}: create_table operation missing 'protocol'",
            case.ordinal
        )
    });
    let metadata_val = op.get("metadata").unwrap_or_else(|| {
        panic!(
            "case {}: create_table operation missing 'metadata'",
            case.ordinal
        )
    });

    // --- Expressibility check ---
    // The create_table() API always produces (3,7) with both feature arrays present.
    let reader_version = protocol_val["minReaderVersion"].as_i64().unwrap_or(-1);
    let writer_version = protocol_val["minWriterVersion"].as_i64().unwrap_or(-1);
    let reader_features = protocol_val
        .get("readerFeatures")
        .and_then(|v| v.as_array());
    let writer_features = protocol_val
        .get("writerFeatures")
        .and_then(|v| v.as_array());

    // create_table() always produces (3,7) with both feature arrays present.
    // Any other protocol shape is inexpressible via the current API.
    if reader_version != 3 || writer_version != 7 {
        return (Outcome::Inexpressible, None);
    }
    let Some(reader_features) = reader_features else {
        return (Outcome::Inexpressible, None);
    };
    let Some(writer_features) = writer_features else {
        return (Outcome::Inexpressible, None);
    };

    // create_table() enforces symmetric feature placement (all ReaderWriter features
    // appear in both arrays). Asymmetric placement is inexpressible via the current API.
    let writer_feature_names: Vec<&str> =
        writer_features.iter().filter_map(|v| v.as_str()).collect();
    if reader_features
        .iter()
        .filter_map(|v| v.as_str())
        .any(|f| !writer_feature_names.contains(&f))
    {
        return (Outcome::Inexpressible, None);
    }

    // --- Feature translation ---
    // Combine reader + writer feature names (deduplicated) and map to delta.feature.X=supported.
    let mut seen = std::collections::HashSet::new();
    let feature_props: Vec<(String, String)> = reader_features
        .iter()
        .chain(writer_features.iter())
        .filter_map(|v| v.as_str())
        .filter(|&f| seen.insert(f))
        .map(|f| (format!("delta.feature.{f}"), "supported".to_string()))
        .collect();

    // --- Configuration ---
    // Pass metadata.configuration entries (e.g. delta.columnMapping.mode) as table properties.
    let config_props: Vec<(String, String)> = metadata_val
        .get("configuration")
        .and_then(|c| c.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    // --- Schema ---
    let schema_str = metadata_val["schemaString"].as_str().unwrap_or_else(|| {
        panic!(
            "case {}: metadata.schemaString must be a string",
            case.ordinal
        )
    });
    let schema: Arc<delta_kernel::schema::StructType> = Arc::new(
        serde_json::from_str(schema_str)
            .unwrap_or_else(|e| panic!("case {}: failed to parse schemaString: {e}", case.ordinal)),
    );

    // --- Call create_table() ---
    let (_store, engine) = new_store_and_engine();
    let build_result = delta_kernel::transaction::create_table::create_table(
        "memory:///",
        schema,
        "compliance-harness/0.1",
    )
    .with_table_properties(feature_props.into_iter().chain(config_props))
    .build(&engine, Box::new(FileSystemCommitter::new()));

    let txn = match build_result {
        Ok(t) => t,
        Err(e) => return (Outcome::Failure, Some(e.to_string())),
    };

    match txn.commit(&engine) {
        Ok(result) if result.is_committed() => {
            // After a successful commit, read back the snapshot and compare schemas.
            // Schema divergence is not a test failure — kernel may auto-annotate fields
            // (e.g. column mapping metadata). Print differences for debugging only.
            // Both sides are round-tripped through serde to normalize whitespace/field order.
            if let Ok(snapshot) = try_build_snapshot(&engine) {
                let committed_schema = snapshot.schema();
                let committed_json = serde_json::to_string(committed_schema.as_ref())
                    .unwrap_or_else(|e| format!("<serialize error: {e}>"));
                // Normalize the requested schema through the same round-trip so the
                // comparison is structural rather than textual.
                let requested_normalized = serde_json::from_str::<delta_kernel::schema::StructType>(schema_str)
                    .ok()
                    .and_then(|s| serde_json::to_string(&s).ok())
                    .unwrap_or_else(|| schema_str.to_string());
                if committed_json != requested_normalized {
                    println!(
                        "case {} '{}': schema divergence after commit\n  \
                         requested: {}\n  committed: {}",
                        case.ordinal, case.name, requested_normalized, committed_json,
                    );
                }
            }
            (Outcome::Success, None)
        }
        Ok(_) => (
            Outcome::Failure,
            Some("commit did not complete".to_string()),
        ),
        Err(e) => (Outcome::Failure, Some(e.to_string())),
    }
}

// ---------------------------------------------------------------------------
// Macros
// ---------------------------------------------------------------------------

/// Generate a compliance test for case `$n` in fixture `$fixture` where the fixture expects
/// [`Outcome::Success`].
///
/// - No error arg: kernel passes correctly.
/// - With error arg: kernel diverges by failing; arg is a substring that must appear in the
///   error message. The test fails automatically when the divergence is resolved, prompting
///   removal of the error arg.
macro_rules! compliance_case_success {
    ($fixture:ident, $n:literal) => {
        paste::paste! {
            #[tokio::test]
            async fn [< $fixture:lower _case_ $n >]() {
                let case = $fixture.case($n);
                println!("case {}: {}", $n, case.name);
                super::run_case_success(case, None).await;
            }
        }
    };
    ($fixture:ident, $n:literal, $error_pattern:literal) => {
        paste::paste! {
            #[tokio::test]
            async fn [< $fixture:lower _case_ $n >]() {
                let case = $fixture.case($n);
                println!("case {}: {}", $n, case.name);
                super::run_case_success(case, Some($error_pattern)).await;
            }
        }
    };
}

/// Generate a compliance test for case `$n` in fixture `$fixture` where the fixture expects
/// [`Outcome::Failure`].
///
/// - With error arg: kernel fails correctly; arg is a substring that must appear in the error
///   message. Use this to pin the specific failure mode.
/// - With `diverges:` arg: kernel diverges by succeeding; arg is a required explanation of
///   what the kernel is failing to enforce. The test fails automatically when the divergence
///   is resolved, including the explanation in the diagnostic.
macro_rules! compliance_case_failure {
    ($fixture:ident, $n:literal, $error_pattern:literal) => {
        paste::paste! {
            #[tokio::test]
            async fn [< $fixture:lower _case_ $n >]() {
                let case = $fixture.case($n);
                println!("case {}: {}", $n, case.name);
                super::run_case_failure(case, $error_pattern).await;
            }
        }
    };
    ($fixture:ident, $n:literal, diverges: $reason:literal) => {
        paste::paste! {
            #[tokio::test]
            async fn [< $fixture:lower _case_ $n >]() {
                let case = $fixture.case($n);
                println!("case {}: {}", $n, case.name);
                super::run_case_failure_diverges(case, $reason).await;
            }
        }
    };
}

/// Generate a compliance test for case `$n` in fixture `$fixture` that expects the harness
/// to return [`Outcome::Inexpressible`].
///
/// Use this when the fixture's protocol shape or operation cannot be produced via the current
/// kernel API (e.g., legacy protocol versions, missing feature arrays, asymmetric feature
/// placement). The test passes as long as the harness returns `Inexpressible`. When the API
/// gap is addressed and the harness starts returning `Success` or `Failure`, the test fails
/// with a diagnostic prompting a switch to [`compliance_case_success!`] or
/// [`compliance_case_failure!`].
macro_rules! compliance_case_inexpressible {
    ($fixture:ident, $n:literal) => {
        paste::paste! {
            #[tokio::test]
            async fn [< $fixture:lower _case_ $n >]() {
                let case = $fixture.case($n);
                println!("case {}: {}", $n, case.name);
                super::run_case_expect_inexpressible(case).await;
            }
        }
    };
}

/// Sentinel test asserting that case `$n` does not exist in fixture `$fixture`.
///
/// Place this immediately after the last `compliance_case!` invocation for a fixture.
/// It fails with a diagnostic when the fixture gains a new case, prompting you to add
/// a `compliance_case!` entry and advance the sentinel.
macro_rules! compliance_case_sentinel {
    ($fixture:ident, $n:literal) => {
        paste::paste! {
            #[test]
            fn [< $fixture:lower _sentinel >]() {
                $fixture.assert_sentinel($n);
            }
        }
    };
}

// Per-feature compliance tests
mod allow_column_defaults;
mod append_only;
mod catalog_managed;
mod change_data_feed;
mod check_constraints;
mod clustering;
mod column_mapping;
mod deletion_vectors;
mod domain_metadata;
mod generated_columns;
mod iceberg_compat_v1;
mod iceberg_compat_v2;
mod iceberg_compat_v3_preview;
mod iceberg_writer_compat;
mod identity_columns;
mod in_commit_timestamp;
mod invariants;
mod materialize_partition_columns;
mod protocol;
mod row_tracking;
mod timestamp_ntz;
mod type_widening;
mod v2_checkpoint;
mod vacuum_protocol_check;
mod variant;
mod variant_shredding_preview;
