#[test]
#[cfg(feature = "internal-api")]
fn invalid_handle_private_constructor() {
    trybuild::TestCases::new()
        .compile_fail("tests/invalid-handle-code/private-constructor.rs");
}
