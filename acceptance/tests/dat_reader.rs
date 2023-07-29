use std::path::Path;
use std::sync::Arc;

use acceptance::read_dat_case;
use deltakernel::client::DefaultTableClient;

fn reader_test(path: &Path) -> datatest_stable::Result<()> {
    let root_dir = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        path.parent().unwrap().to_str().unwrap()
    );

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let case = read_dat_case(root_dir).unwrap();
            let table_root = case.table_root().unwrap();
            let table_client = Arc::new(
                DefaultTableClient::try_new(&table_root, std::iter::empty::<(&str, &str)>())
                    .unwrap(),
            );

            case.assert_metadata(table_client.clone()).await.unwrap();
        });
    Ok(())
}

datatest_stable::harness!(
    reader_test,
    "tests/dat/out/reader_tests/generated/",
    r"test_case_info\.json"
);
