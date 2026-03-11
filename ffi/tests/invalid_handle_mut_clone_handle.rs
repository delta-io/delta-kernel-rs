#[test]
#[cfg(feature = "internal-api")]
fn invalid_handle_mut_clone_handle() {
    trybuild::TestCases::new().compile_fail("tests/invalid-handle-code/mut-clone-handle.rs");
}
