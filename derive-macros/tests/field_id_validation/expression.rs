use delta_kernel_derive::ToSchema;

// Dummy types to avoid dependency issues in test
pub trait ToSchema {
    fn to_schema() -> StructType;
}

pub struct StructType;

const MY_ID: i64 = 123;

#[derive(ToSchema)]
struct TestStruct {
    #[field_id = MY_ID]  // Should be literal, not expression
    field: String,
}

fn main() {}
