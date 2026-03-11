#[test]
#[cfg(feature = "internal-api")]
fn invalid_handle_shared_as_mut() {
    trybuild::TestCases::new().compile_fail("tests/invalid-handle-code/shared-as-mut.rs");
}
