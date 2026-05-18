use std::path::Path;
use std::sync::Arc;

use acceptance::data::{collect_fsr_add_paths, collect_selected_scan_file_paths};
use acceptance::{read_dat_case, AssertionError, TestCaseInfo, TestResult};
use delta_kernel::{Engine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;

const FSR_ADD_ONLY: &str = "fsr_add_only";

// TODO(zach): skip iceberg_compat_v1 test until DAT is fixed
static SKIPPED_TESTS: &[&str; 1] = &["iceberg_compat_v1"];

fn ci_forced_read_mode(root_dir: &str) -> Option<&'static str> {
    let raw = std::env::var("DAT_DF_FSR_CASES").ok()?;
    let wanted = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .any(|suffix| root_dir.ends_with(suffix));
    wanted.then_some(FSR_ADD_ONLY)
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

async fn assert_fsr_add_only_matches_scan_files(
    engine: Arc<dyn Engine>,
    case: &TestCaseInfo,
) -> TestResult<()> {
    let snapshot = Snapshot::builder_for(case.table_root()?).build(engine.as_ref())?;
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
        return Err(AssertionError::KernelError(delta_kernel::Error::generic(
            format!(
                "FSR add-only path set mismatch\nscan paths: {scan_paths:?}\nfsr paths: {fsr_paths:?}"
            ),
        )));
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
                .or_else(|| case.read_mode());

            case.assert_metadata(engine.clone()).await.unwrap();
            match read_mode {
                Some(FSR_ADD_ONLY) => {
                    assert_fsr_add_only_matches_scan_files(engine.clone(), &case)
                        .await
                        .unwrap();
                }
                _ => {
                    acceptance::data::assert_scan_metadata(engine.clone(), &case)
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
