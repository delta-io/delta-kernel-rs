use std::path::Path;
use std::process::Command;

use rstest::rstest;

#[rstest]
#[case::default(&[], Some("trace"), true, false)]
#[case::unset_filter(&[], None, false, false)]
#[case::disabled(&["--no-log"], Some("trace"), false, false)]
#[case::metrics(&["--log-metrics"], Some("info"), true, true)]
#[case::filtered_metrics(&["--log-metrics"], Some("off"), false, false)]
fn logging_options_preserve_table_output(
    #[case] args: &[&str],
    #[case] filter: Option<&str>,
    #[case] expect_logs: bool,
    #[case] expect_metrics: bool,
) {
    let table = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/data/with_checkpoint_no_last_checkpoint");
    let mut command = Command::new(env!("CARGO_BIN_EXE_inspect-table"));
    command.env_remove("RUST_LOG");
    if let Some(filter) = filter {
        command.env("RUST_LOG", filter);
    }
    let output = command
        .env("NO_COLOR", "1")
        .args(args)
        .arg(table)
        .arg("table-version")
        .output()
        .unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(output.status.success(), "{stderr}");
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        "Latest table version: 3"
    );
    assert_eq!(!stderr.is_empty(), expect_logs, "{stderr}");
    assert_eq!(
        stderr.contains("SnapshotBuildSuccess("),
        expect_metrics,
        "{stderr}"
    );
}

#[test]
fn disabling_logging_conflicts_with_metrics_logging() {
    let output = Command::new(env!("CARGO_BIN_EXE_inspect-table"))
        .args(["--no-log", "--log-metrics", "unused", "table-version"])
        .output()
        .unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("cannot be used with"), "{stderr}");
    assert!(stderr.contains("--no-log"), "{stderr}");
    assert!(stderr.contains("--log-metrics"), "{stderr}");
}
