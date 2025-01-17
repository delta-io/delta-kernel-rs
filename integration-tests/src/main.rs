fn create_arrow_schema() -> arrow::datatypes::Schema {
    use arrow::datatypes::{DataType, Field, Schema};
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Boolean, false);
    Schema::new(vec![field_a, field_b])
}

fn create_kernel_schema() -> delta_kernel::schema::Schema {
    use delta_kernel::schema::{DataType, Schema, StructField};
    let field_a = StructField::not_null("a", DataType::LONG);
    let field_b = StructField::not_null("b", DataType::BOOLEAN);
    Schema::new(vec![field_a, field_b])
}

fn main() {
    let arrow_schema = create_arrow_schema();
    let kernel_schema = create_kernel_schema();
    let converted: delta_kernel::schema::Schema =
        delta_kernel::schema::Schema::try_from(&arrow_schema).expect("couldn't convert");
    assert!(kernel_schema == converted);
    println!("Okay, made it");
}
