use std::path::Path;
use std::sync::Arc;

use acceptance::{read_dat_case, DatReadMode};
use delta_kernel::arrow::array::{Array, AsArray};
use delta_kernel::arrow::util::display::array_value_to_string;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;

// TODO(zach): skip iceberg_compat_v1 test until DAT is fixed
static SKIPPED_TESTS: &[&str; 1] = &["iceberg_compat_v1"];

fn ci_forced_read_mode(root_dir: &str) -> Option<DatReadMode> {
    let raw = std::env::var("DAT_DF_FSR_CASES").ok()?;
    let wanted = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .any(|suffix| root_dir.ends_with(suffix));
    wanted.then_some(DatReadMode::FsrAddOnly)
}

fn ci_selected_case(root_dir: &str) -> bool {
    let Ok(raw) = std::env::var("DAT_DF_ONLY_CASES") else {
        return true;
    };
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .any(|suffix| root_dir.ends_with(suffix))
}

fn collect_selected_scan_file_paths(
    snapshot: &Arc<Snapshot>,
    engine: Arc<dyn Engine>,
) -> delta_kernel::DeltaResult<Vec<String>> {
    let scan = Arc::clone(snapshot).scan_builder().build()?;
    let mut out = Vec::new();
    for metadata in scan.scan_metadata(engine.as_ref())? {
        let metadata = metadata?;
        let (data, sv) = metadata.scan_files.into_parts();
        let batch = data.try_into_record_batch()?;
        let path_idx = batch.schema().index_of("path").map_err(|e| {
            delta_kernel::Error::generic(format!(
                "scan metadata path column missing in schema {:?}: {e}",
                batch.schema()
            ))
        })?;
        let path_col = batch.column(path_idx);
        for i in 0..batch.num_rows() {
            // Selection-vector semantics: missing entries mean selected.
            let selected = i >= sv.len() || sv[i];
            if selected && path_col.is_valid(i) {
                out.push(array_value_to_string(path_col.as_ref(), i).map_err(|e| {
                    delta_kernel::Error::generic(format!("stringify scan path row {i}: {e}"))
                })?);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn collect_fsr_add_paths(batches: &[delta_kernel::arrow::array::RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let add_idx = batch
            .schema()
            .index_of("add")
            .unwrap_or_else(|e| panic!("full_state add column missing: {e}"));
        let add_col = batch
            .column(add_idx)
            .as_struct_opt()
            .unwrap_or_else(|| panic!("full_state add column must be Struct"));
        let path_col = add_col
            .column_by_name("path")
            .cloned()
            .unwrap_or_else(|| panic!("full_state add.path missing"));
        for i in 0..batch.num_rows() {
            if add_col.is_valid(i) && path_col.is_valid(i) {
                out.push(
                    array_value_to_string(path_col.as_ref(), i)
                        .unwrap_or_else(|e| panic!("stringify fsr path row {i}: {e}")),
                );
            }
        }
    }
    out.sort();
    out
}

async fn assert_fsr_add_only_matches_scan_files(
    engine: Arc<dyn Engine>,
    case: &acceptance::TestCaseInfo,
) -> acceptance::TestResult<()> {
    let table_root = case.table_root()?;
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;

    let scan_paths = collect_selected_scan_file_paths(&snapshot, Arc::clone(&engine))?;

    let executor = DataFusionExecutor::try_new_with_engine(engine).map_err(|e| {
        delta_kernel::Error::generic(format!("create DataFusionExecutor for full_state: {e}"))
    })?;
    let rp = snapshot
        .full_state_builder()
        .build()
        .plans()
        .map_err(|e| delta_kernel::Error::generic(format!("build full_state plans: {e}")))?;
    let fsr_batches = executor.collect_result(rp).await.map_err(|e| {
        delta_kernel::Error::generic(format!(
            "collect full_state result via DataFusionExecutor: {e}"
        ))
    })?;
    let fsr_paths = collect_fsr_add_paths(&fsr_batches);

    if fsr_paths != scan_paths {
        return Err(acceptance::AssertionError::KernelError(
            delta_kernel::Error::generic(format!(
                "FSR add-only path set mismatch\nscan paths: {:?}\nfsr paths: {:?}",
                scan_paths, fsr_paths
            )),
        ));
    }
    Ok(())
}

fn reader_datafusion_test(path: &Path) -> datatest_stable::Result<()> {
    let root_dir = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        path.parent().unwrap().to_str().unwrap()
    );
    for skipped in SKIPPED_TESTS {
        if root_dir.ends_with(skipped) {
            println!("Skipping test: {skipped}");
            return Ok(());
        }
    }
    if !ci_selected_case(&root_dir) {
        return Ok(());
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let case = read_dat_case(root_dir).unwrap();
            let table_root = case.table_root().unwrap();
            let engine = test_utils::create_default_engine(&table_root).unwrap();
            let read_mode = ci_forced_read_mode(case.root_dir().to_string_lossy().as_ref())
                .unwrap_or_else(|| case.read_mode());

            case.assert_metadata(engine.clone()).await.unwrap();
            match read_mode {
                DatReadMode::Scan => {
                    acceptance::data::assert_scan_metadata(engine.clone(), &case)
                        .await
                        .unwrap();
                }
                DatReadMode::FsrAddOnly => {
                    assert_fsr_add_only_matches_scan_files(engine.clone(), &case)
                        .await
                        .unwrap();
                }
            }
        });
    Ok(())
}

datatest_stable::harness! {
    {
        test = reader_datafusion_test,
        root = "tests/dat/out/reader_tests/generated/",
        pattern = r"test_case_info\.json"
    },
}
