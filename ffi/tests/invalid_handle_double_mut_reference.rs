#[test]
#[cfg(feature = "internal-api")]
fn invalid_handle_double_mut_reference() {
    trybuild::TestCases::new().compile_fail("tests/invalid-handle-code/double-mut-reference.rs");
}
