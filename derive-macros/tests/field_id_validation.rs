#[test]
fn field_id_validation_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/field_id_validation/*.rs");
}
