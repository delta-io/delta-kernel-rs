use delta_kernel_derive::ToSchema;

// Dummy types to avoid dependency issues in test
pub trait ToSchema {
    fn to_schema() -> StructType;
}

pub struct StructType;

#[derive(ToSchema)]
struct TestStruct {
    #[field_id = "123"]  // Should be integer, not string
    field: String,
}

fn main() {}
