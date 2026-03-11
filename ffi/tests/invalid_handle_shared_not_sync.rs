#[test]
#[cfg(feature = "internal-api")]
fn invalid_handle_shared_not_sync() {
    trybuild::TestCases::new().compile_fail("tests/invalid-handle-code/shared-not-sync.rs");
}
