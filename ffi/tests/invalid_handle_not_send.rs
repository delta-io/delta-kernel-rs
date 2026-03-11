#[test]
#[cfg(feature = "internal-api")]
fn invalid_handle_not_send() {
    trybuild::TestCases::new()
        .compile_fail("tests/invalid-handle-code/not-send.rs");
}
