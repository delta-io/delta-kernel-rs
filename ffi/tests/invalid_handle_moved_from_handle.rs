#[test]
#[cfg(feature = "internal-api")]
fn invalid_handle_moved_from_handle() {
    trybuild::TestCases::new().compile_fail("tests/invalid-handle-code/moved-from-handle.rs");
}
