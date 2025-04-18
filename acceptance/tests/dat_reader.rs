use std::path::Path;
use std::sync::Arc;

use acceptance::read_dat_case;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;

// TODO(zach): skip iceberg_compat_v1 test until DAT is fixed
static SKIPPED_TESTS: &[&str; 1] = &["iceberg_compat_v1"];

fn reader_test(path: &Path) -> datatest_stable::Result<()> {
    let root_dir = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        path.parent().unwrap().to_str().unwrap()
    );
    for skipped in SKIPPED_TESTS {
        if root_dir.ends_with(skipped) {
            println!("Skipping test: {}", skipped);
            return Ok(());
        }
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let case = read_dat_case(root_dir).unwrap();
            let table_root = case.table_root().unwrap();
            let engine = Arc::new(
                DefaultEngine::try_new(
                    &table_root,
                    std::iter::empty::<(&str, &str)>(),
                    Arc::new(TokioBackgroundExecutor::new()),
                )
                .unwrap(),
            );

            case.assert_metadata(engine.clone()).await.unwrap();
            acceptance::data::assert_scan_metadata(engine.clone(), &case)
                .await
                .unwrap();
        });
    Ok(())
}

datatest_stable::harness! {
    {
        test = reader_test,
        root = "tests/dat/out/reader_tests/generated/",
        pattern = r"test_case_info\.json"
    },
}
